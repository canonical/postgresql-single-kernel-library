# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import contextlib
import json
import logging
import os
import random
import string
import subprocess
import tarfile
import tempfile
from datetime import datetime
from tempfile import mkstemp

import kubernetes as kubernetes
import psycopg2
import requests
import yaml
from juju.model import Model
from kubernetes import config
from kubernetes.client.api import core_v1_api
from kubernetes.stream import stream
from lightkube.core.client import Client, GlobalResource
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import (
    PersistentVolume,
    PersistentVolumeClaim,
    Pod,
)
from pytest_operator.plugin import OpsTest
from tenacity import (
    RetryError,
    Retrying,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    KUBECTL,
    PGDATA_PATH,
    app_name,
    get_password,
    get_patroni_cluster,
    get_primary,
    get_substrate,
    get_unit_address,
    run_command_on_unit,
)
from tests.integration.helpers.postgresql import db_connect, execute_query
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

SERVICE_NAME = "snap.charmed-postgresql.patroni.service"
PATRONI_SERVICE_DEFAULT_PATH = f"/etc/systemd/system/{SERVICE_NAME}"
RESTART_CONDITION = "no"
ORIGINAL_RESTART_CONDITION = "always"


class MemberNotListedOnClusterError(Exception):
    """Raised when a member is not listed in the cluster."""


class MemberNotUpdatedOnClusterError(Exception):
    """Raised when a member is not yet updated in the cluster."""


class ProcessError(Exception):
    """Raised when a process fails."""


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


async def are_all_db_processes_down(ops_test: OpsTest, process: str, signal: str) -> bool:
    """Verifies that all units of the charm do not have the DB process running."""
    app = await app_name(ops_test)
    pgrep_cmd = ("pgrep", "-f", process) if "/" in process else ("pgrep", "-x", process)

    try:
        for attempt in Retrying(stop=stop_after_delay(400), wait=wait_fixed(3)):
            with attempt:
                running_process = False
                for unit in ops_test.model.applications[app].units:
                    if get_substrate(ops_test) == Substrate.vm:
                        _, processes, _ = await ops_test.juju("ssh", unit.name, *pgrep_cmd)

                        # Splitting processes by "\n" results in one or more empty lines, hence we
                        # need to process these lines accordingly.
                        processes = [proc for proc in processes.split("\n") if len(proc) > 0]

                        # If something was returned, there is a running process.
                        is_running = len(processes) > 0
                    else:
                        pod_name = unit.name.replace("/", "-")
                        call = subprocess.run(
                            f"{KUBECTL} -n {ops_test.model.info.name} exec {pod_name} -c postgresql -- {' '.join(pgrep_cmd)}",
                            shell=True,
                        )

                        # If something was returned, there is a running process.
                        is_running = call.returncode != 1

                    if is_running:
                        logger.info(f"Unit {unit.name} not yet down")
                        # Try to rekill the unit
                        await send_signal_to_process(ops_test, unit.name, process, signal)
                        running_process = True
                if running_process:
                    raise ProcessRunningError
    except RetryError:
        return False

    return True


async def are_writes_increasing(
    ops_test,
    down_unit: str | None = None,
    use_ip_from_inside: bool = False,
    extra_model: Model = None,
) -> None:
    """Verify new writes are continuing by counting the number of writes."""
    if get_substrate(ops_test) == Substrate.vm:
        down_units = [down_unit] if isinstance(down_unit, str) or not down_unit else down_unit
        writes, _ = await count_writes(
            ops_test,
            down_unit=down_units[0],
            use_ip_from_inside=use_ip_from_inside,
            extra_model=extra_model,
        )
        logger.info(f"Initial writes {writes}")

        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                more_writes, _ = await count_writes(
                    ops_test,
                    down_unit=down_units[0],
                    use_ip_from_inside=use_ip_from_inside,
                    extra_model=extra_model,
                )
                logger.info(f"Retry writes {more_writes}")
                for member, count in writes.items():
                    if "/".join(member.split(".", 1)[-1].rsplit("-", 1)) in down_units:
                        continue
                    assert more_writes[member] > count, (
                        f"{member}: writes not continuing to DB (current writes: {more_writes[member]} - previous writes: {count})"
                    )
        return

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            writes, _ = await count_writes(ops_test, down_unit=down_unit, extra_model=extra_model)
            assert len(writes), "No units report count"
    logger.info(f"Initial writes {writes}")
    for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
        with attempt:
            more_writes, _ = await count_writes(
                ops_test, down_unit=down_unit, extra_model=extra_model
            )
            logger.info(f"Retry writes {more_writes}")
            members_checked = []
            for member, count in writes.items():
                if member in more_writes:
                    members_checked.append(member)
                    assert more_writes[member] > count, (
                        f"{member}: writes not continuing to DB (current writes: {more_writes[member]} - previous writes: {count})"
                    )
            assert len(members_checked), "No member checked from the initial writes"


async def change_patroni_setting(
    ops_test: OpsTest,
    setting: str,
    value: int | bool,
    password: str,
    use_random_unit: bool = False,
    tls: bool = False,
) -> None:
    """Change the value of one of the Patroni settings.

    Args:
        ops_test: ops_test instance.
        setting: the name of the setting.
        value: the value to assign to the setting.
        password: Patroni password.
        use_random_unit: whether to use a random unit (default is False,
            so it uses the primary).
        tls: if Patroni is serving using tls.
    """
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            app = await app_name(ops_test)
            if use_random_unit:
                unit = get_random_unit(ops_test, app)
                unit_ip = await get_unit_address(ops_test, unit)
            else:
                primary_name = await get_primary(ops_test, app)
                unit_ip = await get_unit_address(ops_test, primary_name)
            requests.patch(
                f"https://{unit_ip}:8008/config",
                json={setting: value},
                verify=False,
                auth=requests.auth.HTTPBasicAuth("patroni", password),
            )


async def is_cluster_updated(
    ops_test: OpsTest, primary_name: str, use_ip_from_inside: bool = False
) -> None:
    # Verify that the old primary is now a replica.
    logger.info("checking that the former primary is now a replica")
    assert await is_replica(ops_test, primary_name, use_ip_from_inside), (
        "there are more than one primary in the cluster."
    )

    # Verify that all units are part of the same cluster.
    logger.info("checking that all units are part of the same cluster")
    member_ips = await fetch_cluster_members(ops_test, use_ip_from_inside)
    app = primary_name.split("/")[0]
    ip_addresses = [
        await (
            get_ip_from_inside_the_unit(ops_test, unit.name)
            if use_ip_from_inside
            else get_unit_ip(ops_test, unit.name)
        )
        for unit in ops_test.model.applications[app].units
    ]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

    # Verify that no writes to the database were missed after stopping the writes.
    logger.info("checking that no writes to the database were missed after stopping the writes")
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            total_expected_writes = await check_writes(ops_test, use_ip_from_inside)

    # Verify that old primary is up-to-date.
    logger.info("checking that the former primary is up to date with the cluster after restarting")
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            assert await is_secondary_up_to_date(
                ops_test, primary_name, total_expected_writes, use_ip_from_inside
            ), f"secondary ({primary_name}) not up to date with the cluster after restarting."


async def check_writes(
    ops_test, use_ip_from_inside: bool = False, extra_model: Model = None
) -> int:
    """Gets the total writes from the test charm and compares to the writes from db."""
    total_expected_writes = await stop_continuous_writes(ops_test)
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            actual_writes, max_number_written = await count_writes(
                ops_test, use_ip_from_inside=use_ip_from_inside, extra_model=extra_model
            )
            for member, count in actual_writes.items():
                logger.info(
                    f"member: {member}, count: {count}, max_number_written: {max_number_written[member]}, total_expected_writes: {total_expected_writes}"
                )
                assert count == max_number_written[member], (
                    f"{member}: writes to the db were missed: count of actual writes ({count}) on {member} different from the max number written ({max_number_written[member]})."
                )
                assert total_expected_writes == count, f"{member}: writes to the db were missed."
    return total_expected_writes


def copy_file_into_pod(
    client: kubernetes.client.api.core_v1_api.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str,
    destination_path: str,
    source_path: str,
) -> None:
    """Copy file contents into pod.

    Args:
        client: The kubernetes CoreV1Api client
        namespace: The namespace of the pod to copy files to
        pod_name: The name of the pod to copy files to
        container_name: The name of the pod container to copy files to
        destination_path: The path to which the file should be copied over
        source_path: The path of the file which needs to be copied over
    """
    try:
        exec_command = ["tar", "xvf", "-", "-C", "/"]

        api_response = kubernetes.stream.stream(
            client.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            container=container_name,
            command=exec_command,
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False,
            _preload_content=False,
        )

        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
                tar.add(source_path, destination_path)

            tar_buffer.seek(0)
            commands = []
            commands.append(tar_buffer.read())

            while api_response.is_open():
                api_response.update(timeout=1)

                if commands:
                    command = commands.pop(0)
                    api_response.write_stdin(command.decode())
                else:
                    break

            api_response.close()
    except kubernetes.client.rest.ApiException:
        assert False


async def count_writes(  # noqa: C901
    ops_test: OpsTest,
    down_unit: str | None = None,
    use_ip_from_inside: bool = False,
    extra_model: Model = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Count the number of writes in the database."""
    app = await app_name(ops_test)
    password = await get_password(ops_test, database_app_name=app)
    members = []
    if get_substrate(ops_test) == Substrate.vm:
        for model in [ops_test.model, extra_model]:
            if model is None:
                continue
            for unit in model.applications[app].units:
                if unit.name != down_unit:
                    members_data = get_patroni_cluster(
                        await (
                            get_ip_from_inside_the_unit(ops_test, unit.name)
                            if use_ip_from_inside
                            else get_unit_ip(ops_test, unit.name)
                        )
                    )["members"]
                    for member_data in members_data:
                        member_data["model"] = model.info.name
                    members.extend(members_data)
                    break
        down_ips = []
        if down_unit:
            for unit in ops_test.model.applications[app].units:
                if unit.name == down_unit:
                    down_ips.append(unit.public_address)
                    down_ips.append(await get_unit_ip(ops_test, unit.name))
        return count_writes_on_members(members, password, down_ips)

    for model in [ops_test.model, extra_model]:
        if model is None:
            continue
        status = await model.get_status()
        for unit_name, unit in status["applications"][app]["units"].items():
            if unit_name != down_unit:
                members_data = get_patroni_cluster(unit["address"])["members"]
                for _, member_data in enumerate(members_data):
                    member_data["model"] = model.info.name
                members.extend(members_data)
                break

    count = {}
    maximum = {}
    for member in members:
        if member["role"] != "replica" and member["host"].split(".")[0] != (
            down_unit or ""
        ).replace("/", "-"):
            host = member["host"]

            # Translate the service hostname to an IP address.
            client = Client(namespace=member["model"])
            service = client.get(Pod, name=host.split(".")[0])
            ip = service.status.podIP

            connection_string = (
                f"dbname='{APPLICATION_NAME.replace('-', '_')}_database' user='operator'"
                f" host='{ip}' password='{password}' connect_timeout=10"
            )

            member_name = f"{member['model']}.{member['name']}"
            connection = None
            try:
                with (
                    psycopg2.connect(connection_string) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT COUNT(number), MAX(number) FROM continuous_writes;")
                    results = cursor.fetchone()
                    count[member_name] = results[0]
                    maximum[member_name] = results[1]
            except psycopg2.Error:
                # Error raised when the connection is not possible.
                count[member_name] = -1
                maximum[member_name] = -1
            finally:
                if connection is not None:
                    connection.close()
    return count, maximum


def count_writes_on_members(members, password, down_ips) -> tuple[dict[str, int], dict[str, int]]:
    count = {}
    maximum = {}
    for member in members:
        if member["role"] != "replica" and member["host"] not in down_ips:
            host = member["host"]

            connection_string = (
                f"dbname='{APPLICATION_NAME.replace('-', '_')}_database' user='operator'"
                f" host='{host}' password='{password}' connect_timeout=10"
            )

            member_name = f"{member['model']}.{member['name']}"
            connection = None
            try:
                with (
                    psycopg2.connect(connection_string) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT COUNT(number), MAX(number) FROM continuous_writes;")
                    results = cursor.fetchone()
                    count[member_name] = results[0]
                    maximum[member_name] = results[1]
            except psycopg2.Error:
                # Error raised when the connection is not possible.
                count[member_name] = -1
                maximum[member_name] = -1
            finally:
                if connection is not None:
                    connection.close()
    return count, maximum


async def fetch_cluster_members(ops_test: OpsTest, use_ip_from_inside: bool = False):
    """Fetches the IPs listed by Patroni as cluster members.

    Args:
        ops_test: OpsTest instance.
        use_ip_from_inside: whether to use the IP from inside the unit.
    """
    app = await app_name(ops_test)
    member_ips = {}
    if get_substrate(ops_test) == Substrate.vm:
        for unit in ops_test.model.applications[app].units:
            unit_ip = await (
                get_ip_from_inside_the_unit(ops_test, unit.name)
                if use_ip_from_inside
                else get_unit_ip(ops_test, unit.name)
            )
            cluster_info = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
            if len(member_ips) > 0:
                # If the list of members IPs was already fetched, also compare the
                # list provided by other members.
                assert member_ips == {
                    member["host"] for member in cluster_info.json()["members"]
                }, "members report different lists of cluster members."
            else:
                member_ips = {member["host"] for member in cluster_info.json()["members"]}
        return member_ips

    def get_host_ip(host: str) -> str:
        # Translate the pod hostname to an IP address.
        model = ops_test.model.info
        client = Client(namespace=model.name)
        pod = client.get(Pod, name=host.split(".")[0])
        return pod.status.podIP

    for unit in ops_test.model.applications[app].units:
        unit_address = await get_unit_address(ops_test, unit.name)
        cluster_info = requests.get(f"https://{unit_address}:8008/cluster", verify=False)
        if len(member_ips) > 0:
            # If the list of members IPs was already fetched, also compare the
            # list provided by other members.
            assert member_ips == {
                get_host_ip(member["host"]) for member in cluster_info.json()["members"]
            }, "members report different lists of cluster members."
        else:
            member_ips = {get_host_ip(member["host"]) for member in cluster_info.json()["members"]}
    return member_ips


async def get_ip_from_inside_the_unit(ops_test: OpsTest, unit_name: str) -> str:
    command = f"exec --unit {unit_name} -- hostname -I"
    return_code, stdout, stderr = await ops_test.juju(*command.split())
    if return_code != 0:
        raise ProcessError(
            "Expected command %s to succeed instead it failed: %s %s", command, return_code, stderr
        )
    return stdout.splitlines()[0].strip()


async def get_patroni_setting(ops_test: OpsTest, setting: str, tls: bool = False) -> int | None:
    """Get the value of one of the integer Patroni settings.

    Args:
        ops_test: ops_test instance.
        setting: the name of the setting.
        tls: if Patroni is serving using tls.

    Returns:
        the value of the configuration or None if it's using the default value.
    """
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            app = await app_name(ops_test)
            primary_name = await get_primary(ops_test, app)
            unit_ip = await get_unit_address(ops_test, primary_name)
            configuration_info = requests.get(f"https://{unit_ip}:8008/config", verify=False)
            value = configuration_info.json().get(setting)
            return int(value) if value is not None else None


def get_random_unit(ops_test: OpsTest, app: str) -> str:
    """Returns a random unit name."""
    return random.choice(ops_test.model.applications[app].units).name


async def get_unit_ip(ops_test: OpsTest, unit_name: str, model: Model = None) -> str:
    """Wrapper for getting unit ip.

    Args:
        ops_test: The ops test object passed into every test case
        unit_name: The name of the unit to get the address
        model: Optional model instance to use
    Returns:
        The (str) ip of the unit
    """
    if get_substrate(ops_test) == Substrate.k8s:
        return await get_unit_address(ops_test, unit_name)
    if model is None:
        application = unit_name.split("/")[0]
        for unit in ops_test.model.applications[application].units:
            if unit.name == unit_name:
                break
        return await instance_ip(ops_test, unit.machine.hostname)
    else:
        return await get_unit_address(ops_test, unit_name)


async def is_replica(ops_test: OpsTest, unit_name: str, use_ip_from_inside: bool = False) -> bool:
    """Returns whether the unit a replica in the cluster."""
    unit_ip = await (
        get_ip_from_inside_the_unit(ops_test, unit_name)
        if use_ip_from_inside
        else get_unit_ip(ops_test, unit_name)
    )
    member_name = unit_name.replace("/", "-")

    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
            with attempt:
                cluster_info = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)

                # The unit may take some time to be listed on Patroni REST API cluster endpoint.
                if member_name not in {
                    member["name"] for member in cluster_info.json()["members"]
                }:
                    raise MemberNotListedOnClusterError()

                for member in cluster_info.json()["members"]:
                    if member["name"] == member_name:
                        role = member["role"]

                # A member that restarted has the DB process stopped may
                # take some time to know that a new primary was elected.
                if role != "leader":
                    return True
                else:
                    raise MemberNotUpdatedOnClusterError()
    except RetryError:
        return False


async def get_cluster_roles(
    ops_test: OpsTest, unit_name: str, use_ip_from_inside: bool = False
) -> dict[str, str | list[str] | None]:
    """Returns whether the unit a replica in the cluster."""
    unit_ip = await (
        get_ip_from_inside_the_unit(ops_test, unit_name)
        if use_ip_from_inside
        else get_unit_ip(ops_test, unit_name)
    )

    members = {"replicas": [], "primaries": [], "sync_standbys": []}
    member_list = get_patroni_cluster(unit_ip)["members"]
    logger.info(f"Cluster members are: {member_list}")
    for member in member_list:
        role = member["role"]
        name = "/".join(member["name"].rsplit("-", 1))
        if role == "leader":
            members["primaries"].append(name)
        elif role == "sync_standby":
            members["sync_standbys"].append(name)
        else:
            members["replicas"].append(name)

    return members


async def instance_ip(ops_test: OpsTest, instance: str) -> str:
    """Translate juju instance name to IP.

    Args:
        ops_test: pytest ops test helper
        instance: The name of the instance

    Returns:
        The (str) IP address of the instance
    """
    _, output, _ = await ops_test.juju("machines")

    for line in output.splitlines():
        if instance in line:
            return line.split()[2]


async def modify_pebble_restart_delay(
    ops_test: OpsTest,
    unit_name: str,
    pebble_plan_path: str,
    ensure_replan: bool = False,
) -> None:
    """Modify the pebble restart delay of the underlying process.

    Args:
        ops_test: The ops test framework
        unit_name: The name of unit to extend the pebble restart delay for
        pebble_plan_path: Path to the file with the modified pebble plan
        ensure_replan: Whether to check that the replan command succeeded
    """
    kubernetes.config.load_kube_config()
    client = kubernetes.client.api.core_v1_api.CoreV1Api()

    pod_name = unit_name.replace("/", "-")
    container_name = "postgresql"
    service_name = "postgresql"
    now = datetime.now().isoformat()

    copy_file_into_pod(
        client,
        ops_test.model.info.name,
        pod_name,
        container_name,
        f"/tmp/pebble_plan_{now}.yml",
        pebble_plan_path,
    )

    add_to_pebble_layer_commands = (
        f"/charm/bin/pebble add --combine {service_name} /tmp/pebble_plan_{now}.yml"
    )
    response = kubernetes.stream.stream(
        client.connect_get_namespaced_pod_exec,
        pod_name,
        ops_test.model.info.name,
        container=container_name,
        command=add_to_pebble_layer_commands.split(),
        stdin=False,
        stdout=True,
        stderr=True,
        tty=False,
        _preload_content=False,
    )
    response.run_forever(timeout=5)
    assert response.returncode == 0, (
        f"Failed to add to pebble layer, unit={unit_name}, container={container_name}, service={service_name}"
    )

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            replan_pebble_layer_commands = "/charm/bin/pebble replan"
            response = kubernetes.stream.stream(
                client.connect_get_namespaced_pod_exec,
                pod_name,
                ops_test.model.info.name,
                container=container_name,
                command=replan_pebble_layer_commands.split(),
                stdin=False,
                stdout=True,
                stderr=True,
                tty=False,
                _preload_content=False,
            )
            response.run_forever(timeout=60)
            if ensure_replan and response.returncode != 0:
                assert response.returncode == 0, (
                    f"Failed to replan pebble layer, unit={unit_name}, container={container_name}, service={service_name}"
                )


async def send_signal_to_process(  # noqa: C901
    ops_test: OpsTest, unit_name: str, process: str, signal: str, use_ssh: bool = False
) -> None:
    """Send a signal to an OS process on a specific unit.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit to run the command on
        process: OS process name
        signal: Signal that will be sent to the OS process
            (examples: SIGKILL, SIGTERM, SIGSTOP, SIGCONT)
        use_ssh: whether to use juju ssh instead of kubernetes client (K8s only).

    Returns:
        the command output if it succeeds, otherwise raises an exception.
    """
    # Killing or freezing the only instance can be disastrous.
    app = await app_name(ops_test)
    if len(ops_test.model.applications[app].units) < 2:
        await ops_test.model.applications[app].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=1000)

    opt = "-f" if "/" in process else "-x"

    if get_substrate(ops_test) == Substrate.vm:
        command = f"exec --unit {unit_name} -- pkill --signal {signal} {opt} {process}"

        # Send the signal.
        return_code, _, _ = await ops_test.juju(*command.split())
        if signal != "SIGCONT" and return_code != 0:
            raise ProcessError(
                "Expected command %s to succeed instead it failed: %s",
                command,
                return_code,
            )
        return

    pod_name = unit_name.replace("/", "-")

    if signal not in ["SIGSTOP", "SIGCONT"]:
        _, old_pid, _ = await ops_test.juju(
            "ssh", "--container", "postgresql", unit_name, "pgrep", opt, process
        )

    command = f"pkill --signal {signal} {opt} {process}"

    if use_ssh:
        kill_cmd = f"ssh {unit_name} {command}"
        return_code, _, _ = await asyncio.wait_for(ops_test.juju(*kill_cmd.split()), 10)
        if return_code != 0:
            raise ProcessError(
                "Expected command %s to succeed instead it failed: %s",
                command,
                return_code,
            )
        return

    # Load Kubernetes configuration to connect to the cluster.
    config.load_kube_config()

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            # Send the signal.
            response = stream(
                core_v1_api.CoreV1Api().connect_get_namespaced_pod_exec,
                pod_name,
                ops_test.model.info.name,
                container="postgresql",
                command=command.split(),
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
            )

            response.run_forever(timeout=10)

            if signal not in ["SIGSTOP", "SIGCONT"]:
                _, raw_pid, _ = await ops_test.juju(
                    "ssh", "--container", "postgresql", unit_name, "pgrep", opt, process
                )

                # If the same output was returned, process was not restarted.
                if raw_pid == old_pid:
                    raise ProcessRunningError
            elif response.returncode != 0:
                raise ProcessError(
                    "Expected command %s to succeed instead it failed: %s",
                    command,
                    response.returncode,
                )


async def is_postgresql_ready(ops_test, unit_name: str, use_ip_from_inside: bool = False) -> bool:
    """Verifies a PostgreSQL instance is running and available."""
    unit_ip = (
        (await get_ip_from_inside_the_unit(ops_test, unit_name))
        if use_ip_from_inside
        else await get_unit_address(ops_test, unit_name)
    )
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 10), wait=wait_fixed(3)):
            with attempt:
                instance_health_info = requests.get(f"https://{unit_ip}:8008/health", verify=False)
                assert instance_health_info.status_code == 200
    except RetryError:
        return False

    return True


async def is_secondary_up_to_date(
    ops_test: OpsTest, unit_name: str, expected_writes: int, use_ip_from_inside: bool = False
) -> bool:
    """Checks if secondary is up-to-date with the cluster.

    Retries over the period of one minute to give secondary adequate time to copy over data.
    """
    app = await app_name(ops_test)
    password = await get_password(ops_test, database_app_name=app)
    if get_substrate(ops_test) == Substrate.vm:
        host = await next(
            (
                get_ip_from_inside_the_unit(ops_test, unit.name)
                if use_ip_from_inside
                else get_unit_ip(ops_test, unit.name)
            )
            for unit in ops_test.model.applications[app].units
            if unit.name == unit_name
        )
    else:
        status = await ops_test.model.get_status()
        host = status["applications"][app]["units"][unit_name]["address"]
    connection_string = (
        f"dbname='{APPLICATION_NAME.replace('-', '_')}_database' user='operator'"
        f" host='{host}' password='{password}' connect_timeout=10"
    )

    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
            with (
                attempt,
                psycopg2.connect(connection_string) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("SELECT COUNT(number), MAX(number) FROM continuous_writes;")
                results = cursor.fetchone()
                if get_substrate(ops_test) == Substrate.vm:
                    assert results[0] == expected_writes and results[1] == expected_writes
                elif results[0] != expected_writes or results[1] != expected_writes:
                    async with ops_test.fast_forward(fast_interval="30s"):
                        await ops_test.model.wait_for_idle(
                            apps=[unit_name.split("/")[0]], idle_period=15, timeout=1000
                        )
                        raise Exception
    except RetryError:
        return False
    finally:
        connection.close()

    return True


async def start_continuous_writes(ops_test: OpsTest, app: str, model: Model = None) -> None:
    """Start continuous writes to PostgreSQL."""
    # Start the process by relating the application to the database or
    # by calling the action if the relation already exists.
    if model is None:
        model = ops_test.model
    relations = [
        relation
        for relation in model.applications[app].relations
        if not relation.is_peer
        and f"{relation.requires.application_name}:{relation.requires.name}"
        == f"{APPLICATION_NAME}:database"
    ]
    if not relations:
        await model.relate(app, f"{APPLICATION_NAME}:database")
        await model.wait_for_idle(
            apps=[APPLICATION_NAME, app], status="active", timeout=1000, idle_period=30
        )
    for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3), reraise=True):
        with attempt:
            action = (
                await model
                .applications[APPLICATION_NAME]
                .units[0]
                .run_action("start-continuous-writes")
            )
            await action.wait()
            assert action.results["result"] == "True", "Unable to create continuous_writes table"


async def stop_continuous_writes(ops_test: OpsTest) -> int:
    """Stops continuous writes to PostgreSQL and returns the last written value."""
    action = (
        await ops_test.model
        .applications[APPLICATION_NAME]
        .units[0]
        .run_action("stop-continuous-writes")
    )
    action = await action.wait()
    return int(action.results["writes"])


async def update_restart_condition(ops_test: OpsTest, unit, condition: str):
    """Updates the restart condition in the DB service file.

    When the DB service fails it will now wait for `delay` number of seconds.
    """
    # Load the service file from the unit and update it with the new delay.
    _, temp_path = mkstemp()
    await unit.scp_from(source=PATRONI_SERVICE_DEFAULT_PATH, destination=temp_path)
    with open(temp_path) as patroni_service_file:
        patroni_service = patroni_service_file.readlines()

    for index, line in enumerate(patroni_service):
        if "Restart=" in line:
            patroni_service[index] = f"Restart={condition}\n"

    with open(temp_path, "w") as service_file:
        service_file.writelines(patroni_service)

    # Upload the changed file back to the unit, we cannot scp this file directly to
    # PATRONI_SERVICE_DEFAULT_PATH since this directory has strict permissions, instead we scp it
    # elsewhere and then move it to PATRONI_SERVICE_DEFAULT_PATH.
    await unit.scp_to(source=temp_path, destination="patroni.service")
    mv_cmd = f"mv /home/ubuntu/patroni.service {PATRONI_SERVICE_DEFAULT_PATH}"
    await run_command_on_unit(ops_test, unit.name, mv_cmd)

    # Remove temporary file from machine.
    os.remove(temp_path)

    # Reload the daemon for systemd otherwise changes are not saved.
    reload_cmd = "systemctl daemon-reload"
    await run_command_on_unit(ops_test, unit.name, reload_cmd)
    start_cmd = f"systemctl start {SERVICE_NAME}"
    await run_command_on_unit(ops_test, unit.name, start_cmd)

    await is_postgresql_ready(ops_test, unit.name)


def storage_type(ops_test, app):
    """Retrieves type of storage associated with an application.

    Note: this function exists as a temporary solution until this issue is ported to libjuju 2:
    https://github.com/juju/python-libjuju/issues/694
    """
    model_name = ops_test.model.info.name
    proc = subprocess.check_output(f"juju storage --model={model_name}".split())
    proc = proc.decode("utf-8")
    for line in proc.splitlines():
        if "Storage" in line:
            continue

        if len(line) == 0:
            continue

        if "detached" in line:
            continue

        unit_name = line.split()[0]
        app_name = unit_name.split("/")[0]
        if app_name == app:
            return line.split()[3]


async def get_storage_ids(ops_test: OpsTest, unit_name: str) -> list[str]:
    """Retrieves storage ids associated with provided unit (only the data storage on K8s).

    Note: this function exists as a temporary solution until this issue is ported to libjuju 2:
    https://github.com/juju/python-libjuju/issues/694
    """
    storage_ids = []
    model_name = ops_test.model.info.name
    proc = subprocess.check_output(
        f"juju storage --model={ops_test.controller_name}:{model_name}".split()
    )
    proc = proc.decode("utf-8")
    for line in proc.splitlines():
        if "Storage" in line:
            continue

        if len(line) == 0:
            continue

        if "detached" in line:
            continue

        if line.split()[0] == unit_name and (
            get_substrate(ops_test) == Substrate.vm or line.split()[1].startswith("data")
        ):
            storage_ids.append(line.split()[1])
    return storage_ids


async def add_unit_with_storage(ops_test, app, storages):
    """Adds unit with storages.

    Note: this function exists as a temporary solution until this issue is resolved:
    https://github.com/juju/python-libjuju/issues/695
    """
    original_units = {unit.name for unit in ops_test.model.applications[app].units}
    model_name = ops_test.model.info.name
    add_unit_cmd = f"add-unit {app} --model={model_name}"
    for storage in storages:
        add_unit_cmd = add_unit_cmd + f" --attach-storage={storage}"
    add_unit_cmd = add_unit_cmd.split()
    return_code, _, _ = await ops_test.juju(*add_unit_cmd)
    assert return_code == 0, "Failed to add unit with storage"
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=2000)

    # When removing all units sometimes the last unit remain in the list
    current_units = {unit.name for unit in ops_test.model.applications[app].units}
    original_units.intersection_update(current_units)
    assert original_units.issubset(current_units), "New unit not added to model"

    # verify storage attached
    new_unit = (current_units - original_units).pop()
    assert sorted(await get_storage_ids(ops_test, new_unit)) == sorted(storages), (
        "unit added with incorrect storage"
    )

    # return a reference to newly added unit
    for unit in ops_test.model.applications[app].units:
        if unit.name == new_unit:
            return unit


async def reused_replica_storage(ops_test: OpsTest, unit_name) -> bool:
    """Returns True if storage provided to Postgresql has been reused.

    Checks Patroni logs for when the database was in archive mode.
    """
    await run_command_on_unit(
        ops_test,
        unit_name,
        "grep 'Database cluster state: in archive recovery' "
        "/var/snap/charmed-postgresql/common/var/log/patroni/patroni.log*",
    )
    return True


async def is_storage_exists(ops_test: OpsTest, storage_id: str) -> bool:
    """Returns True if storage exists by provided storage ID."""
    complete_command = [
        "show-storage",
        "-m",
        f"{ops_test.controller_name}:{ops_test.model.info.name}",
        storage_id,
        "--format=json",
    ]
    return_code, stdout, _ = await ops_test.juju(*complete_command)
    if return_code != 0:
        if return_code == 1:
            return storage_id in stdout
        raise Exception(
            "Expected command %s to succeed instead it failed: %s with code: ",
            complete_command,
            stdout,
            return_code,
        )
    return storage_id in str(stdout)


async def create_db(ops_test: OpsTest, app: str, db: str) -> None:
    """Creates database with specified name."""
    unit = ops_test.model.applications[app].units[0]
    unit_address = await get_unit_address(ops_test, unit.name)
    password = await get_password(ops_test, database_app_name=app)

    conn = db_connect(unit_address, password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {db};")
    cursor.close()
    conn.close()


async def check_db(ops_test: OpsTest, app: str, db: str) -> bool:
    """Returns True if database with specified name already exists."""
    unit = ops_test.model.applications[app].units[0]
    unit_address = await get_unit_address(ops_test, unit.name)
    password = await get_password(ops_test, database_app_name=app)

    assert password is not None

    query = execute_query(
        unit_address,
        password,
        f"select datname from pg_catalog.pg_database where datname = '{db}';",
    )

    if "ERROR" in query:
        raise Exception(f"Database check is failed with postgresql err: {query}")

    return db in query


async def get_detached_storages(ops_test: OpsTest) -> list[str]:
    """Returns the current available detached storages (only the data storage on K8s)."""
    return_code, storages_list, stderr = await ops_test.juju(
        "storage", "-m", f"{ops_test.controller_name}:{ops_test.model.info.name}", "--format=json"
    )
    if return_code != 0:
        raise Exception(f"failed to get storages info with error: {stderr}")

    parsed_storages_list = json.loads(storages_list)
    detached_storages = []
    for storage_name, storage in parsed_storages_list["storage"].items():
        if (
            (get_substrate(ops_test) == Substrate.vm or storage_name.startswith("data"))
            and (str(storage["status"]["current"]) == "detached")
            and (str(storage["life"] == "alive"))
        ):
            detached_storages.append(storage_name)

    if len(detached_storages) > 0:
        return detached_storages

    raise Exception("failed to get deatached storage")


async def check_password_auth(ops_test: OpsTest, unit_name: str) -> bool:
    """Checks if "operator" password is valid for current postgresql db."""
    complete_command = [
        "exec",
        "--unit",
        unit_name,
        "--",
        "grep",
        "-E",
        "'password'",
        "/var/snap/charmed-postgresql/common/var/log/postgresql/postgresql*",
    ]
    return_code, stdout, _ = await ops_test.juju(*complete_command)
    if return_code != 0:
        raise Exception(
            "Expected command %s to succeed. Instead it failed: %s with code: ",
            complete_command,
            stdout,
            return_code,
        )
    return 'password authentication failed for user "operator"' not in stdout


async def check_system_id_mismatch(ops_test: OpsTest, unit_name: str) -> bool:
    """Returns True if system id mismatch if found in logs."""
    log_str = f"CRITICAL: system ID mismatch, node {unit_name.replace('/', '-')} belongs to a different cluster"
    stdout = await run_command_on_unit(
        ops_test,
        unit_name,
        "cat /var/lib/pg/logs/16/main/patroni_logs/patroni.log*",
    )

    return log_str in str(stdout)


def delete_pvc(ops_test: OpsTest, pvc: GlobalResource):
    """Deletes PersistentVolumeClaim."""
    client = Client(namespace=ops_test.model.name)
    try:
        client.delete(PersistentVolumeClaim, namespace=ops_test.model.name, name=pvc.metadata.name)
    except ApiError as e:
        logger.warning(f"failed to delete pvc {pvc.metadata.name}: {e}")
        pass


def get_pvcs(ops_test: OpsTest, unit_name: str):
    """Get PersistentVolumeClaims for unit."""
    pvcs = {}
    client = Client(namespace=ops_test.model.name)
    pvc_list = client.list(PersistentVolumeClaim, namespace=ops_test.model.name)
    unit_label = unit_name.replace("/", "-")
    for pvc in pvc_list:
        if unit_label in pvc.metadata.name:
            pvc_storage_name = pvc.metadata.name.replace(unit_name.split("/")[0], "").split("-")[1]
            logger.info(f"got pvc for {pvc_storage_name} storage: {pvc.metadata.name}")
            pvcs[pvc_storage_name] = pvc
    return pvcs


def get_pvs(ops_test: OpsTest, unit_name: str):
    """Get PersistentVolumes for unit."""
    pvs = {}
    client = Client(namespace=ops_test.model.name)
    pv_list = client.list(PersistentVolume, namespace=ops_test.model.name)
    unit_label = unit_name.replace("/", "-")
    for pv in pv_list:
        if pv.spec.claimRef and pv.spec.claimRef.name.endswith(unit_label):
            pvc_storage_name = pv.spec.claimRef.name.replace(unit_name.split("/")[0], "").split(
                "-"
            )[1]
            logger.info(f"got pv for {pvc_storage_name} storage: {pv.metadata.name}")
            pvs[pvc_storage_name] = pv
    return pvs


def change_pvs_reclaim_policy(
    ops_test: OpsTest, pvs_configs: dict[str, PersistentVolume], policy: str
):
    """Change PersistentVolume reclaim policy config value."""
    client = Client(namespace=ops_test.model.name)
    results = {}
    for pvc_storage_name, pv_config in pvs_configs.items():
        results[pvc_storage_name] = client.patch(
            PersistentVolume,
            pv_config.metadata.name,
            {"spec": {"persistentVolumeReclaimPolicy": f"{policy}"}},
            namespace=ops_test.model.name,
        )
    return results


def remove_pv_claimref(ops_test: OpsTest, pv_config: PersistentVolume):
    """Remove claimRef config value for PersistentVolume."""
    client = Client(namespace=ops_test.model.name)
    client.patch(
        PersistentVolume,
        pv_config.metadata.name,
        {"spec": {"claimRef": None}},
        namespace=ops_test.model.name,
    )


def change_pvc_pv_name(
    pvc_config: PersistentVolumeClaim, pv_name_new: str
) -> PersistentVolumeClaim:
    """Change PersistentVolume name config value for PersistentVolumeClaim."""
    pvc_config.spec.volumeName = pv_name_new
    del pvc_config.metadata.annotations["pv.kubernetes.io/bind-completed"]
    del pvc_config.metadata.uid
    return pvc_config


def apply_pvc_config(ops_test: OpsTest, pvc_config: PersistentVolumeClaim):
    """Apply provided PersistentVolumeClaim config."""
    client = Client(namespace=ops_test.model.name)
    pvc_config.metadata.managedFields = None
    client.apply(pvc_config, namespace=ops_test.model.name, field_manager="lightkube")


async def remove_unit_force(ops_test: OpsTest, num_units: int):
    """Removes unit with --force --no-wait."""
    app_name_str = await app_name(ops_test)
    scale = len(ops_test.model.applications[app_name_str].units) - num_units
    complete_command = [
        "remove-unit",
        f"{app_name_str}",
        "--force",
        "--no-wait",
        "--no-prompt",
        "--num-units",
        num_units,
    ]
    return_code, stdout, stderr = await ops_test.juju(*complete_command)
    if return_code != 0:
        raise Exception(
            "Expected command %s to succeed instead it failed: %s with err: %s with code: %s",
            complete_command,
            stdout,
            stderr,
            return_code,
        )

    if scale == 0:
        await ops_test.model.block_until(
            lambda: len(ops_test.model.applications[app_name_str].units) == scale,
            timeout=1000,
        )
    else:
        await ops_test.model.wait_for_idle(
            apps=[app_name_str],
            status="active",
            timeout=1000,
            wait_for_exact_units=scale,
        )


async def change_wal_settings(
    ops_test: OpsTest,
    unit_name: str,
    max_wal_size: int,
    min_wal_size,
    wal_keep_segments,
    password: str,
) -> None:
    """Change WAL settings in the unit.

    Args:
        ops_test: ops_test instance.
        unit_name: name of the unit to change the WAL settings.
        max_wal_size: maximum amount of WAL to keep (MB).
        min_wal_size: minimum amount of WAL to keep (MB).
        wal_keep_segments: number of WAL segments to keep.
        password: Patroni password.
    """
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            unit_ip = await get_unit_address(ops_test, unit_name)
            requests.patch(
                f"https://{unit_ip}:8008/config",
                json={
                    "postgresql": {
                        "parameters": {
                            "max_wal_size": max_wal_size,
                            "min_wal_size": min_wal_size,
                            "wal_keep_segments": wal_keep_segments,
                        }
                    }
                },
                verify=False,
                auth=requests.auth.HTTPBasicAuth("patroni", password),
            )


async def get_postgresql_parameter(ops_test: OpsTest, parameter_name: str) -> int | None:
    """Get the value of a PostgreSQL parameter from Patroni API.

    Args:
        ops_test: ops_test instance.
        parameter_name: the name of the parameter to get the value for.

    Returns:
        the value of the requested PostgreSQL parameter.
    """
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            app = await app_name(ops_test)
            primary_name = await get_primary(ops_test, app)
            unit_ip = await get_unit_address(ops_test, primary_name)
            configuration_info = requests.get(f"https://{unit_ip}:8008/config", verify=False)
            postgresql_dict = configuration_info.json().get("postgresql")
            if postgresql_dict is None:
                return None
            parameters = postgresql_dict.get("parameters")
            if parameters is None:
                return None
            parameter_value = parameters.get(parameter_name)
            return parameter_value


async def list_wal_files(ops_test: OpsTest, app: str) -> set:
    """Returns the list of WAL segment files in each unit."""
    units = [unit.name for unit in ops_test.model.applications[app].units]
    files = {}
    if get_substrate(ops_test) == Substrate.vm:
        command = "ls -1 /var/snap/charmed-postgresql/common/var/lib/postgresql/16/main/pg_wal/"
        for unit in units:
            stdout = await run_command_on_unit(ops_test, unit, command)
            files[unit] = stdout.splitlines()
            files[unit] = {
                i for i in files[unit] if ".history" not in i and i != "" and i != "archive_status"
            }
        return files

    command = f"ls -1 {PGDATA_PATH}/pg_wal/"
    for unit in units:
        complete_command = f"run --unit {unit} -- {command}"
        _, stdout, _ = await ops_test.juju(*complete_command.split())
        files[unit] = stdout.splitlines()
        files[unit] = {
            i for i in files[unit] if ".history" not in i and i != "" and i != "archive_status"
        }
    return files


async def is_connection_possible(
    ops_test: OpsTest, unit_name: str, use_ip_from_inside: bool = False
) -> bool:
    """Test a connection to a PostgreSQL server."""
    try:
        app = unit_name.split("/")[0]
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3)):
            with attempt:
                password = await asyncio.wait_for(
                    get_password(ops_test, database_app_name=app), 15
                )
                address = await asyncio.wait_for(
                    get_ip_from_inside_the_unit(ops_test, unit_name)
                    if use_ip_from_inside
                    else get_unit_ip(ops_test, unit_name),
                    15,
                )

                with (
                    db_connect(host=address, password=password) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT 1;")
                    success = cursor.fetchone()[0] == 1
                connection.close()

                if not success:
                    raise Exception
                return True
    except RetryError:
        # Error raised when the connection is not possible.
        return False


def cut_network_from_unit(machine_name: str) -> None:
    """Cut network from a lxc container.

    Args:
        machine_name: lxc container hostname
    """
    # apply a mask (device type `none`)
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())


def cut_network_from_unit_without_ip_change(machine_name: str) -> None:
    """Cut network from a lxc container (without causing the change of the unit IP address).

    Args:
        machine_name: lxc container hostname
    """
    override_command = f"lxc config device override {machine_name} eth0"
    # Ignore if the interface was already overridden.
    with contextlib.suppress(subprocess.CalledProcessError):
        subprocess.check_call(override_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.egress=0kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.ingress=1kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority=10"
    subprocess.check_call(limit_set_command.split())


async def get_controller_machine(ops_test: OpsTest) -> str:
    """Return controller machine hostname.

    Args:
        ops_test: The ops test framework instance

    Returns:
        Controller hostname (str)
    """
    _, raw_controller, _ = await ops_test.juju("show-controller")

    controller = yaml.safe_load(raw_controller.strip())

    return next(
        machine.get("instance-id")
        for machine in controller[ops_test.controller_name]["controller-machines"].values()
    )


def is_machine_reachable_from(origin_machine: str, target_machine: str) -> bool:
    """Test network reachability between hosts.

    Args:
        origin_machine: hostname of the machine to test connection from
        target_machine: hostname of the machine to test connection to
    """
    try:
        subprocess.check_call(f"lxc exec {origin_machine} -- ping -c 3 {target_machine}".split())
        return True
    except subprocess.CalledProcessError:
        return False


def restore_network_for_unit(machine_name: str) -> None:
    """Restore network from a lxc container.

    Args:
        machine_name: lxc container hostname
    """
    # remove mask from eth0
    restore_network_command = f"lxc config device remove {machine_name} eth0"
    subprocess.check_call(restore_network_command.split())


def restore_network_for_unit_without_ip_change(machine_name: str) -> None:
    """Restore network from a lxc container (without causing the change of the unit IP address).

    Args:
        machine_name: lxc container hostname
    """
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.egress="
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.ingress="
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority="
    subprocess.check_call(limit_set_command.split())


async def wait_network_restore(ops_test: OpsTest, unit_name: str, old_ip: str) -> None:
    """Wait until network is restored.

    Args:
        ops_test: pytest plugin helper
        unit_name: name of the unit
        old_ip: old registered IP address
    """
    # Retrieve the unit IP from inside the unit because it may not be updated in the
    # Juju status too quickly.
    if (await get_ip_from_inside_the_unit(ops_test, unit_name)) == old_ip:
        raise Exception


def get_member_lag(cluster: dict, member_name: str) -> int:
    """Return the lag of a specific member."""
    for member in cluster["members"]:
        if member["name"] == member_name.replace("/", "-"):
            return member.get("lag", 0)
    return 0


async def is_member_isolated(
    ops_test: OpsTest, not_isolated_member: str, isolated_member: str
) -> bool:
    """Check whether the member is isolated from the cluster."""
    # Check if the lag is too high.
    unit_ip = await get_unit_address(ops_test, not_isolated_member)
    try:
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                cluster_info = get_patroni_cluster(unit_ip)
                lag = get_member_lag(cluster_info, isolated_member)
                assert lag > 1000
    except RetryError:
        return False

    return True


def deploy_chaos_mesh(namespace: str) -> None:
    """Deploy chaos mesh to the provided namespace.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        " ".join([
            "tests/integration/charm/ha_tests/scripts/deploy_chaos_mesh.sh",
            namespace,
        ]),
        shell=True,
        env=env,
    )


def destroy_chaos_mesh(namespace: str) -> None:
    """Remove chaos mesh from the provided namespace.

    Args:
        namespace: The namespace to deploy chaos mesh to
    """
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")

    subprocess.check_output(
        f"tests/integration/charm/ha_tests/scripts/destroy_chaos_mesh.sh {namespace}",
        shell=True,
        env=env,
    )


def isolate_instance_from_cluster(ops_test: OpsTest, unit_name: str) -> None:
    """Apply a NetworkChaos file to use chaos-mesh to simulate a network cut."""
    with tempfile.NamedTemporaryFile() as temp_file:
        with open(
            "tests/integration/charm/ha_tests/manifests/chaos_network_loss.yml"
        ) as chaos_network_loss_file:
            template = string.Template(chaos_network_loss_file.read())
            chaos_network_loss = template.substitute(
                namespace=ops_test.model.info.name,
                pod=unit_name.replace("/", "-"),
            )

            temp_file.write(str.encode(chaos_network_loss))
            temp_file.flush()

        env = os.environ
        env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
        subprocess.check_output(
            " ".join([*KUBECTL.split(), "apply", "-f", temp_file.name]), shell=True, env=env
        )


def remove_instance_isolation(ops_test: OpsTest) -> None:
    """Delete the NetworkChaos that is isolating the primary unit of the cluster."""
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
    subprocess.check_output(
        f"{KUBECTL} -n {ops_test.model.info.name} delete --ignore-not-found=true networkchaos network-loss-primary",
        shell=True,
        env=env,
    )
