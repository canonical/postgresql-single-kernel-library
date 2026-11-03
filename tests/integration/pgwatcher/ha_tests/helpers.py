# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import contextlib
import logging
import subprocess

import psycopg2
import requests
from juju.model import Model
from pytest_operator.plugin import OpsTest
from tenacity import (
    RetryError,
    Retrying,
    retry,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from ..helpers import (
    APPLICATION_NAME,
    get_password,
    get_patroni_cluster,
    get_unit_address,
)

logger = logging.getLogger(__name__)

PORT = 5432
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


async def are_writes_increasing(
    ops_test,
    down_unit: str | None = None,
    use_ip_from_inside: bool = False,
    extra_model: Model = None,
) -> None:
    """Verify new writes are continuing by counting the number of writes."""
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


async def app_name(
    ops_test: OpsTest, application_name: str = "postgresql", model: Model = None
) -> str | None:
    """Returns the name of the cluster running PostgreSQL.

    This is important since not all deployments of the PostgreSQL charm have the application name
    "postgresql".

    Note: if multiple clusters are running PostgreSQL this will return the one first found.
    """
    if model is None:
        model = ops_test.model
    status = await model.get_status()
    for app in model.applications:
        if (
            application_name in status["applications"][app]["charm"]
            and APPLICATION_NAME not in status["applications"][app]["charm"]
        ):
            return app

    return None


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
                    f"{member}: writes to the db were missed: count of actual writes different from the max number written."
                )
                assert total_expected_writes == count, f"{member}: writes to the db were missed."
    return total_expected_writes


async def count_writes(
    ops_test: OpsTest,
    down_unit: str | None = None,
    use_ip_from_inside: bool = False,
    extra_model: Model = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Count the number of writes in the database."""
    app = await app_name(ops_test)
    password = await get_password(ops_test, database_app_name=app)
    members = []
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


async def get_ip_from_inside_the_unit(ops_test: OpsTest, unit_name: str) -> str:
    command = f"exec --unit {unit_name} -- hostname -I"
    return_code, stdout, stderr = await ops_test.juju(*command.split())
    if return_code != 0:
        raise ProcessError(
            "Expected command %s to succeed instead it failed: %s %s", command, return_code, stderr
        )
    return stdout.splitlines()[0].strip()


async def get_unit_ip(ops_test: OpsTest, unit_name: str, model: Model = None) -> str:
    """Wrapper for getting unit ip.

    Args:
        ops_test: The ops test object passed into every test case
        unit_name: The name of the unit to get the address
        model: Optional model instance to use
    Returns:
        The (str) ip of the unit
    """
    if model is None:
        application = unit_name.split("/")[0]
        for unit in ops_test.model.applications[application].units:
            if unit.name == unit_name:
                break
        return await instance_ip(ops_test, unit.machine.hostname)
    else:
        return get_unit_address(ops_test, unit_name)


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
    cluster_info = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
    member_list = cluster_info.json()["members"]
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


async def get_primary(ops_test: OpsTest, app, down_unit: str | None = None) -> str:
    """Use the charm action to retrieve the primary from provided application.

    Args:
        ops_test: OpsTest instance.
        app: database application name.
        down_unit: unit that is offline and the action won't run on.

    Returns:
        primary unit name.
    """
    for unit in ops_test.model.applications[app].units:
        if unit.name != down_unit:
            action = await unit.run_action("get-primary")
            action = await action.wait()
            primary = action.results.get("primary", "None")
            if primary == "None":
                continue
            return primary
    return None


async def is_postgresql_ready(ops_test, unit_name: str, use_ip_from_inside: bool = False) -> bool:
    """Verifies a PostgreSQL instance is running and available."""
    unit_ip = (
        (await get_ip_from_inside_the_unit(ops_test, unit_name))
        if use_ip_from_inside
        else get_unit_address(ops_test, unit_name)
    )
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3)):
            with attempt:
                instance_health_info = requests.get(f"https://{unit_ip}:8008/health", verify=False)
                assert instance_health_info.status_code == 200
    except RetryError:
        return False

    return True


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
        await model.wait_for_idle(status="active", timeout=1000)
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


@retry(stop=stop_after_attempt(20), wait=wait_fixed(30))
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
