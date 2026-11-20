# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers specific to the PostgreSQL charm."""

import json
import logging
import random
import subprocess
import tempfile
import time
import tomllib
import zipfile
from datetime import datetime
from enum import Enum

import jubilant
import psycopg2
import requests
import yaml
from lightkube import Client
from lightkube.resources.core_v1 import Service
from single_kernel_postgresql.config.literals import PEER_RELATION, SYSTEM_USERS_PASSWORD_CONFIG
from tenacity import (
    RetryError,
    Retrying,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from tests.integration.helpers.juju import (
    get_app_leader,
    get_app_units,
    get_credentials,
    get_secret_by_label,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect, execute_query
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

DATA_INTEGRATOR_APP_NAME = "data-integrator"

RELATION_ENDPOINT = "postgresql"

MINUTE_SECS = 60

# The resume-refresh action is a one-shot charm_refresh state-machine probe: it fails while
# the charm-side pause has not opened the allow-gate yet ("Unit N is unhealthy. Refresh will
# not resume.") and once the pod-level StatefulSet roll has already completed ("No refresh
# in progress"). Both states are timing windows minutes wide under runner load, so the helper
# keeps retrying within a real budget instead of a 3x3s window.
RESUME_REFRESH_RETRY_BUDGET_SECS = 15 * MINUTE_SECS
RESUME_REFRESH_RETRY_WAIT_SECS = 10


class RoleAttributeValue(Enum):
    NO = 0
    YES = 1
    REQUESTED_DATABASE = 2
    ALL_DATABASES = 3


def consumer_alias_exists(juju: jubilant.Juju, app_name: str, label: str) -> bool:
    """Whether *app*'s leader holds a consumer-side alias for *label*.

    A consumed-secret alias is not listed by ``secret-ids`` (which returns only
    owned secrets), so ``get_async_secret_labels`` can't see it. Probe it directly:
    ``secret-get --label`` returns content when the alias exists and errors with
    ``consumer label "<label>" not found`` when it was never registered.
    """
    leader = get_app_leader(juju, app_name)
    try:
        juju.cli("exec", "--unit", leader, "--", "secret-get", f"--label={label}")
        return True
    except jubilant.CLIError as error:
        haystack = f"{error} {getattr(error, 'stderr', '')} {getattr(error, 'stdout', '')}".lower()
        if "not found" in haystack:
            return False
        raise


def get_async_secret_labels(juju: jubilant.Juju, app_name: str) -> set[str]:
    """Return the async-replication secret labels owned by *app*'s leader unit."""
    leader = get_app_leader(juju, app_name)
    labels: set[str] = set()
    for secret_id in juju.cli("exec", "--unit", leader, "--", "secret-ids").split():
        info = juju.cli("exec", "--unit", leader, "--", "secret-info-get", secret_id)
        for line in info.splitlines():
            stripped = line.strip()
            if stripped.startswith("label:"):
                label = stripped.split(":", 1)[1].strip()
                if "async-replication" in label:
                    labels.add(label)
    return labels


def get_password(juju: jubilant.Juju, app_name: str, user: str = "operator") -> str | None:
    """Return the password of a PostgreSQL system user from the charm's app secret."""
    secret = get_secret_by_label(juju, f"{PEER_RELATION}.{app_name}.app")
    return secret.get(f"{user}-password")


def set_password(
    juju: jubilant.Juju, app_name: str, username: str = "operator", password: str | None = None
) -> None:
    """Set the password of a system user through the system users secret."""
    secret_name = "system_users_secret"
    try:
        secret_id = juju.add_secret(secret_name, {username: password})
        juju.grant_secret(secret_id, app_name)
        # Update the application config to include the secret.
        juju.config(app_name, {SYSTEM_USERS_PASSWORD_CONFIG: str(secret_id)})
    except Exception:
        juju.update_secret(secret_name, {username: password})


def restart_patroni(juju: jubilant.Juju, unit_name: str, password: str) -> None:
    """Restart PostgreSQL on a unit through the Patroni REST API."""
    requests.post(
        f"https://{get_unit_address(juju, unit_name)}:8008/restart",
        auth=requests.auth.HTTPBasicAuth("patroni", password),
        verify=False,
    )


def check_patroni(juju: jubilant.Juju, unit_name: str, restart_time: float) -> bool:
    """Return whether PostgreSQL is running on a unit and was started after restart_time."""
    health_info = requests.get(
        f"https://{get_unit_address(juju, unit_name)}:8008/health", verify=False
    ).json()
    postmaster_start_time = datetime.strptime(
        health_info["postmaster_start_time"], "%Y-%m-%d %H:%M:%S.%f%z"
    ).timestamp()
    return postmaster_start_time > restart_time and health_info["state"] == "running"


def get_primary(juju: jubilant.Juju, unit_name: str) -> str:
    """Return the primary unit, asking unit_name with the get-primary action."""
    primary = juju.run(unit_name, "get-primary").results.get("primary")
    assert primary in juju.status().get_units(unit_name.split("/")[0]), (
        f"Primary unit not found (get-primary returned {primary})"
    )
    return primary


def get_db_primary_unit(juju: jubilant.Juju, app_name: str) -> str:
    """Return the primary unit of a cluster."""
    return get_primary(juju, get_app_leader(juju, app_name))


def wait_for_idle_on_blocked(
    juju: jubilant.Juju,
    database_app_name: str,
    unit_number: int,
    other_app_name: str,
    status_message: str,
    timeout: float = 10 * MINUTE_SECS,
) -> None:
    """Wait for other_app_name to be active and a database unit to be blocked with a message."""
    unit = f"{database_app_name}/{unit_number}"
    juju.wait(
        wait_for_apps_status(jubilant.all_active, other_app_name),
        error=lambda status: jubilant.any_error(status, other_app_name),
        timeout=timeout,
    )
    juju.wait(
        lambda status: (
            status.apps[database_app_name].units[unit].is_blocked
            and status.apps[database_app_name].units[unit].workload_status.message
            == status_message
        ),
        timeout=timeout,
    )


def get_cluster_roles(juju: jubilant.Juju, unit_name: str) -> dict[str, list[str]]:
    """Return the unit names of the cluster members by role, asking the Patroni API."""
    unit_address = get_unit_address(juju, unit_name)
    members = {"replicas": [], "primaries": [], "sync_standbys": []}
    member_list = requests.get(f"https://{unit_address}:8008/cluster", verify=False).json()[
        "members"
    ]
    logging.info(f"Cluster members are: {member_list}")
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


def check_cluster_members(juju: jubilant.Juju, app_name: str, substrate: Substrate) -> None:
    """Check that the Patroni cluster members are the application units.

    On VM the member hosts are also checked against the unit addresses.
    """
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
    ):
        with attempt:
            units = get_app_units(juju, app_name)
            primary = get_primary(juju, next(iter(units)))
            address = get_unit_address(juju, primary)

            members = requests.get(f"https://{address}:8008/cluster", verify=False).json()[
                "members"
            ]
            assert [member["name"] for member in members] == [
                name.replace("/", "-") for name in units
            ]
            if substrate == Substrate.vm:
                assert [member["host"] for member in members] == [
                    unit.public_address for unit in units.values()
                ]


def scale_application(
    juju: jubilant.Juju, app_name: str, count: int, substrate: Substrate
) -> None:
    """Scale an application to a number of units and wait for it to be active.

    On VM the first units are removed; on K8s only the last pods can be removed.
    """
    units = list(get_app_units(juju, app_name))
    change = count - len(units)
    if change > 0:
        juju.add_unit(app_name, num_units=change)
    elif change < 0:
        if substrate == Substrate.vm:
            juju.remove_unit(*units[:-change], destroy_storage=True)
        else:
            juju.remove_unit(app_name, num_units=-change)
    juju.wait(
        lambda status: (
            wait_for_apps_status(jubilant.all_active, app_name)(status)
            and len(status.get_units(app_name)) == count
        ),
        error=jubilant.any_error,
        timeout=2000,
    )


def switchover(
    juju: jubilant.Juju, current_primary: str, password: str, candidate: str | None = None
) -> None:
    """Trigger a switchover with the Patroni API and wait for the sync standbys to be back."""
    primary_address = get_unit_address(juju, current_primary)
    response = requests.post(
        f"https://{primary_address}:8008/switchover",
        json={
            "leader": current_primary.replace("/", "-"),
            "candidate": candidate.replace("/", "-") if candidate else None,
        },
        auth=requests.auth.HTTPBasicAuth("patroni", password),
        verify=False,
    )
    assert response.status_code == 200
    app_name = current_primary.split("/")[0]
    for attempt in Retrying(stop=stop_after_attempt(30), wait=wait_fixed(2), reraise=True):
        with attempt:
            response = requests.get(f"https://{primary_address}:8008/cluster", verify=False)
            assert response.status_code == 200
            standbys = [
                member for member in response.json()["members"] if member["role"] == "sync_standby"
            ]
            assert len(standbys) == len(get_app_units(juju, app_name)) - 1


def run_command_on_unit(
    juju: jubilant.Juju, unit_name: str, command: str, substrate: Substrate
) -> str:
    """Run a shell command on the workload of a unit (the postgresql container on K8s)."""
    if substrate == Substrate.vm:
        return juju.exec(command, unit=unit_name).stdout
    return juju.ssh(unit_name, command, container="postgresql")


def get_alias_from_relation_data(
    juju: jubilant.Juju, unit_name: str, related_unit_name: str
) -> str | None:
    """Return the alias that unit_name assigned to the application of related_unit_name."""
    relation_data = {}
    for relation in juju.show_unit(related_unit_name).relation_info:
        for name, unit in relation.related_units.items():
            if name == unit_name:
                relation_data = unit.data
                break

    if "alias" not in relation_data:
        raise ValueError(f"no alias could be grabbed for {related_unit_name} application/cluster")
    return relation_data["alias"]


def get_application_relation_data(
    juju: jubilant.Juju,
    application_name: str,
    relation_name: str,
    key: str,
    relation_id: int | None = None,
    relation_alias: str | None = None,
) -> str | None:
    """Return a key of the application data that the first unit of an app sees in a relation."""
    unit_name = f"{application_name}/0"
    relation_data = [
        relation
        for relation in juju.show_unit(unit_name).relation_info
        if relation.endpoint == relation_name
    ]
    if relation_id:
        relation_data = [
            relation for relation in relation_data if relation.relation_id == relation_id
        ]
    if relation_alias:
        relation_data = [
            relation
            for relation in relation_data
            if get_alias_from_relation_data(juju, unit_name, next(iter(relation.related_units)))
            == relation_alias
        ]
    if len(relation_data) == 0:
        raise ValueError(
            f"no relation data could be grabbed on relation with endpoint {relation_name} and alias {relation_alias}"
        )
    return relation_data[0].app_data.get(key)


def build_connection_string(
    juju: jubilant.Juju,
    application_name: str,
    relation_name: str,
    substrate: Substrate,
    *,
    relation_id: int | None = None,
    relation_alias: str | None = None,
    read_only_endpoint: bool = False,
    database: str | None = None,
) -> str:
    """Build a PostgreSQL connection string from the data an app gets in a relation."""
    if database is None:
        database = f"{application_name.replace('-', '_')}_{relation_name.replace('-', '_')}"

    if secret_uri := get_application_relation_data(
        juju, application_name, relation_name, "secret-user", relation_id, relation_alias
    ):
        secret_data = juju.show_secret(secret_uri, reveal=True).content
        username = secret_data["username"]
        password = secret_data["password"]
    else:
        username = get_application_relation_data(
            juju, application_name, relation_name, "username", relation_id, relation_alias
        )
        password = get_application_relation_data(
            juju, application_name, relation_name, "password", relation_id, relation_alias
        )

    endpoints = get_application_relation_data(
        juju,
        application_name,
        relation_name,
        "read-only-endpoints" if read_only_endpoint else "endpoints",
        relation_id,
        relation_alias,
    )
    host = endpoints.split(",")[0].split(":")[0]
    if substrate == Substrate.k8s:
        # The endpoint is the service hostname: use its cluster IP.
        client = Client(namespace=(juju.model or "").split(":")[-1])
        host = client.get(Service, name=host.split(".")[0]).spec.clusterIP

    return f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"


def primary_changed(juju: jubilant.Juju, old_primary: str) -> bool:
    """Return whether the primary is no longer old_primary, asking another unit."""
    other_unit = next(
        unit for unit in get_app_units(juju, old_primary.split("/")[0]) if unit != old_primary
    )
    return get_primary(juju, other_unit) != old_primary


def change_patroni_setting(
    juju: jubilant.Juju,
    app_name: str,
    setting: str,
    value: int | bool,
    password: str,
    use_random_unit: bool = False,
) -> None:
    """Change a Patroni setting through the REST API of the primary (or a random unit)."""
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            if use_random_unit:
                unit_name = random.choice(list(get_app_units(juju, app_name)))
            else:
                unit_name = get_db_primary_unit(juju, app_name)
            requests.patch(
                f"https://{get_unit_address(juju, unit_name)}:8008/config",
                json={setting: value},
                verify=False,
                auth=requests.auth.HTTPBasicAuth("patroni", password),
            )


def change_primary_start_timeout(
    juju: jubilant.Juju, unit_name: str, seconds: int | None, password: str
) -> None:
    """Change the Patroni primary_start_timeout setting through the REST API of a unit."""
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            requests.patch(
                f"https://{get_unit_address(juju, unit_name)}:8008/config",
                json={"primary_start_timeout": seconds},
                verify=False,
                auth=requests.auth.HTTPBasicAuth("patroni", password),
            )


def get_tls_ca(juju: jubilant.Juju, unit_name: str, relation: str = "client") -> str:
    """Return the TLS CA of a certificates relation of the unit, or "" if there is none."""
    raw_data = juju.cli("show-unit", unit_name)
    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {unit_name}")
    data = yaml.safe_load(raw_data)
    endpoint = f"{relation}-certificates"
    relation_data = [v for v in data[unit_name]["relation-info"] if v["endpoint"] == endpoint]
    if not relation_data:
        return ""
    return json.loads(relation_data[0]["application-data"]["certificates"])[0].get("ca")


def check_tls(juju: jubilant.Juju, unit_name: str, enabled: bool) -> bool:
    """Return whether TLS is enabled/disabled on the PostgreSQL instance of a unit.

    Besides "SHOW ssl", checks that the connections from the other units are (or aren't)
    encrypted.
    """
    app_name = unit_name.split("/")[0]
    unit_address = get_unit_address(juju, unit_name)
    password = get_password(juju, app_name)
    other_addresses = [
        f"'{get_unit_address(juju, other_unit)}'"
        for other_unit in get_app_units(juju, app_name)
        if other_unit != unit_name
    ]
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                output = execute_query(
                    unit_address,
                    password,
                    "SHOW ssl;",
                    sslmode="require" if enabled else "disable",
                )
                tls_enabled = "on" in output

                # Number of bits of the encryption algorithm of each connection
                # (None if the connection isn't encrypted).
                connections_encryption_info = execute_query(
                    unit_address,
                    password,
                    "SELECT bits FROM pg_stat_ssl INNER JOIN pg_stat_activity"
                    " ON pg_stat_ssl.pid = pg_stat_activity.pid"
                    " WHERE pg_stat_ssl.pid = pg_backend_pid()"
                    f" OR client_addr IN ({','.join(other_addresses)});",
                )
                connections_encrypted = (
                    all(connections_encryption_info)
                    if enabled
                    else any(connections_encryption_info)
                )

                if enabled != tls_enabled or tls_enabled != connections_encrypted:
                    raise ValueError(
                        f"TLS is{' not' if not tls_enabled else ''} enabled on {unit_name}"
                    )
                return True
    except RetryError:
        return False
    return False


def check_tls_replication(juju: jubilant.Juju, unit_name: str, enabled: bool) -> bool:
    """Return whether the replication connections to a unit are (or aren't) encrypted."""
    unit_address = get_unit_address(juju, unit_name)
    password = get_password(juju, unit_name.split("/")[0])
    output = execute_query(
        unit_address,
        password,
        "SELECT pg_ssl.ssl, pg_sa.client_addr FROM pg_stat_ssl pg_ssl"
        " JOIN pg_stat_activity pg_sa ON pg_ssl.pid = pg_sa.pid"
        " AND pg_sa.usename = 'replication';",
    )
    return all(output[i] == enabled for i in range(0, len(output), 2))


def check_tls_patroni_api(
    juju: jubilant.Juju, unit_name: str, enabled: bool, substrate: Substrate
) -> bool:
    """Return whether the Patroni REST API of a unit answers through TLS.

    On VM the server certificate is validated with the peer CA when TLS is enabled. On K8s
    it isn't, since the unit IP doesn't match the certificate hostname (a k8s hostname).
    """
    unit_address = get_unit_address(juju, unit_name)
    tls_ca = get_tls_ca(juju, unit_name, "peer") if substrate == Substrate.vm else ""

    # If there is no TLS CA in the relation, something is wrong in
    # the relation between the TLS Certificates Operator and PostgreSQL.
    if substrate == Substrate.vm and enabled and not tls_ca:
        return False

    try:
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt, tempfile.NamedTemporaryFile() as temp_ca_file:
                temp_ca_file.write(tls_ca.encode("utf-8"))
                temp_ca_file.seek(0)
                verify = temp_ca_file.name if substrate == Substrate.vm and enabled else False
                health_info = requests.get(f"https://{unit_address}:8008/health", verify=verify)
                return health_info.status_code == 200
    except RetryError:
        return False
    return False


def count_switchovers(juju: jubilant.Juju, app_name: str) -> int:
    """Return the number of switchovers performed, from the Patroni history."""
    unit_address = get_unit_address(juju, get_db_primary_unit(juju, app_name))
    return len(requests.get(f"https://{unit_address}:8008/history", verify=False).json())


def get_refresh_versions(charm: str) -> dict:
    """Return the refresh_versions.toml packed inside a built charm."""
    with zipfile.ZipFile(charm) as charm_zip:
        return tomllib.loads(charm_zip.read("refresh_versions.toml").decode())


def get_db_standby_leader_unit(juju: jubilant.Juju, app_name: str) -> str:
    """Return the standby leader unit of a standby cluster, asking the Patroni API."""
    unit_address = get_unit_address(juju, get_app_leader(juju, app_name))
    cluster = requests.get(f"https://{unit_address}:8008/cluster", verify=False).json()
    for member in cluster["members"]:
        if member["role"] == "standby_leader":
            # Patroni member names are the unit names with "-" instead of "/".
            return member["name"][::-1].replace("-", "/", 1)[::-1]
    raise AssertionError(f"No standby leader found for {app_name}")


def get_member_state(juju: jubilant.Juju, app_name: str, member_name: str) -> str | None:
    """Return a cluster member's reported state from the leader's /cluster endpoint."""
    unit_address = get_unit_address(juju, get_app_leader(juju, app_name))
    members = requests.get(f"https://{unit_address}:8008/cluster", verify=False).json()["members"]
    return next((member["state"] for member in members if member["name"] == member_name), None)


def get_db_max_written_value(
    juju: jubilant.Juju, app_name: str, unit_name: str, db_name: str
) -> int:
    """Return the max value written by the test app's continuous writes on a unit."""
    connection = db_connect(
        get_unit_address(juju, unit_name), get_password(juju, app_name), database=db_name
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(number) FROM continuous_writes;")
            return cursor.fetchone()[0]
    finally:
        connection.close()


def start_continuous_writes(model: jubilant.Juju, test_app: str) -> None:
    """Start continuous writes through the test application (retry through transient errors)."""
    for attempt in Retrying(stop=stop_after_attempt(10), reraise=True):
        with attempt:
            model.run(
                unit=get_app_leader(model, test_app), action="start-continuous-writes"
            ).raise_on_failure()


def check_db_units_writes_increment(
    juju: jubilant.Juju,
    app_name: str,
    app_units: list[str] | None = None,
    db_name: str = "postgresql_test_app_database",
) -> None:
    """Ensure that continuous writes is incrementing on all units.

    Also, ensure that all continuous writes up to the max written value is available
    on all units (ensure that no committed data is lost).
    """
    if not app_units:
        app_units = list(get_app_units(juju, app_name))

    app_primary = get_db_primary_unit(juju, app_name)
    app_max_value = get_db_max_written_value(juju, app_name, app_primary, db_name)

    for unit_name in app_units:
        for attempt in Retrying(
            reraise=True,
            stop=stop_after_delay(5 * MINUTE_SECS),
            wait=wait_fixed(10),
        ):
            with attempt:
                unit_max_value = get_db_max_written_value(juju, app_name, unit_name, db_name)
                assert unit_max_value > app_max_value, "Writes not incrementing"
                app_max_value = unit_max_value


def get_published_secret_id(juju: jubilant.Juju, unit_name: str) -> str | None:
    """Return the secret id the offer side publishes in primary-cluster-data."""
    data = json.loads(juju.cli("show-unit", unit_name, "--format", "json"))
    unit = next(iter(data.values()))
    for relation in unit.get("relation-info", []):
        if relation.get("endpoint") == "replication":
            return json.loads(
                relation.get("application-data", {}).get("primary-cluster-data", "{}")
            ).get("secret-id")
    return None


def run_upgrade(
    juju: jubilant.Juju,
    app_name: str,
    charm: str,
    substrate: Substrate,
    resources: dict[str, str] | None,
) -> None:
    """Refresh an application to the charm under test and wait for the refresh to finish.

    The refresh flow differs per substrate: on VM the app blocks as a whole and
    resume-refresh runs on the second unit; on K8s the highest numbered pod is refreshed
    first and resume-refresh runs on the leader.
    """
    if substrate == Substrate.vm:
        _run_upgrade_vm(juju, app_name, charm)
    else:
        _run_upgrade_k8s(juju, app_name, charm, resources)


def _run_upgrade_vm(juju: jubilant.Juju, app_name: str, charm: str) -> None:
    logging.info("Refresh the charm")
    juju.refresh(app=app_name, path=charm)
    logging.info("Wait for refresh to block as paused or incompatible")
    try:
        juju.wait(lambda status: status.apps[app_name].is_blocked, timeout=10 * MINUTE_SECS)

        units = get_app_units(juju, app_name)
        unit_names = sorted(units.keys())

        if "Refresh incompatible" in juju.status().apps[app_name].app_status.message:
            logging.info("Application refresh is blocked due to incompatibility")
            juju.run(
                unit=unit_names[-1],
                action="force-refresh-start",
                params={"check-compatibility": False},
                wait=40 * MINUTE_SECS,
            )

        juju.wait(jubilant.all_agents_idle, timeout=10 * MINUTE_SECS)

        logging.info("Run resume-refresh action")
        juju.run(unit=unit_names[1], action="resume-refresh", wait=40 * MINUTE_SECS)
    except TimeoutError:
        logging.info("Upgrade completed without snap refresh (charm.py upgrade only)")
        assert juju.status().apps[app_name].is_active

    logging.info("Wait for upgrade to complete")
    juju.wait(ready=wait_for_apps_status(jubilant.all_active, app_name), timeout=40 * MINUTE_SECS)


def _run_upgrade_k8s(
    juju: jubilant.Juju, app_name: str, charm: str, resources: dict[str, str] | None
) -> None:
    previous_revision = juju.status().apps[app_name].charm_rev
    logging.info("Refresh the charm")
    juju.refresh(app=app_name, path=charm, resources=resources)
    logging.info("Wait for refresh to block as paused or incompatible")
    units = get_app_units(juju, app_name)
    unit_names = sorted(units.keys())
    try:
        juju.wait(
            lambda status: status.apps[app_name].units[unit_names[-1]].is_blocked,
            timeout=5 * MINUTE_SECS,
        )
        juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

        if (
            "Refresh incompatible"
            in juju.status().apps[app_name].units[unit_names[-1]].workload_status.message
        ):
            logging.info("Application refresh is blocked due to incompatibility")
            juju.run(
                unit=unit_names[-1],
                action="force-refresh-start",
                params={"check-compatibility": False},
                wait=5 * MINUTE_SECS,
            )
    except TimeoutError:
        logging.info("Upgrade started without incompatibility")

    juju.wait(
        lambda status: status.apps[app_name].units[unit_names[-1]].is_active,
        timeout=5 * MINUTE_SECS,
    )
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    logging.info("Run resume-refresh action")
    resume_refresh(juju, app_name, previous_revision)

    logging.info("Wait for upgrade to complete")
    juju.wait(ready=wait_for_apps_status(jubilant.all_active, app_name), timeout=20 * MINUTE_SECS)


def resume_refresh(juju: jubilant.Juju, app_name: str, previous_revision: int) -> None:
    """Run resume-refresh on the app leader, retrying within a generous budget.

    The ``resume-refresh`` action is a one-shot charm_refresh state-machine probe. It fails
    with "Unit N is unhealthy. Refresh will not resume." while the pause allow-gate has not
    opened yet, and with "No refresh in progress" once the pod-level roll has already
    completed. Both are timing windows minutes wide under runner load, so retry within
    ``RESUME_REFRESH_RETRY_BUDGET_SECS`` instead of a 3x3s window.

    "No refresh in progress" is treated as benign termination only after verifying that the
    refresh actually happened (``refresh_completed``); otherwise the failure is real
    (nothing rolled) or the roll is still mid-flight, and retrying keeps looking for a
    resumable refresh window.
    """
    for attempt in Retrying(
        reraise=True,
        stop=stop_after_delay(RESUME_REFRESH_RETRY_BUDGET_SECS),
        wait=wait_fixed(RESUME_REFRESH_RETRY_WAIT_SECS),
    ):
        with attempt:
            try:
                juju.run(
                    unit=get_app_leader(juju, app_name),
                    action="resume-refresh",
                    wait=5 * MINUTE_SECS,
                )
            except jubilant.TaskError as task_error:
                if "No refresh in progress" not in str(task_error):
                    # e.g. "Unit N is unhealthy. Refresh will not resume.": the refresh may
                    # still proceed on its own, so keep retrying within the budget.
                    raise
                if refresh_completed(juju, app_name, previous_revision):
                    logging.info("Refresh completed while resume-refresh was retried")
                    break
                logging.info(
                    "resume-refresh reported 'No refresh in progress' but the refresh is not "
                    f"verified complete; retrying within {RESUME_REFRESH_RETRY_BUDGET_SECS}s"
                )
                raise


def refresh_completed(juju: jubilant.Juju, app_name: str, previous_revision: int) -> bool:
    """Whether the refresh actually rolled the app since ``previous_revision``.

    Evidence is required beyond merely seeing every unit active: the app charm revision must
    have advanced past ``previous_revision`` and the app must report a workload version
    (``application-version-set`` only sets it on the app, not on each unit). A roll where the
    last unit's charm hooks are still pending must not count as completed.
    """
    app_status = juju.status().apps[app_name]
    if not app_status.version:
        return False
    if any(unit.workload_status.current != "active" for unit in app_status.units.values()):
        return False
    return app_status.charm_rev != previous_revision


def roles_attributes(predefined_roles: dict, combination: str) -> dict:
    """Return the expected attributes of a user with the given combination of roles."""
    auto_escalate_to_database_owner = RoleAttributeValue.NO
    connect = RoleAttributeValue.NO
    create_databases = RoleAttributeValue.NO
    create_objects = RoleAttributeValue.NO
    escalate_to_database_owner = RoleAttributeValue.NO
    read_data = RoleAttributeValue.NO
    read_stats = RoleAttributeValue.NO
    run_backup_commands = RoleAttributeValue.NO
    set_up_predefined_catalog_roles = RoleAttributeValue.NO
    set_user = RoleAttributeValue.NO
    write_data = RoleAttributeValue.NO
    for role in combination.split(","):
        # Whether the relation user is auto-escalated to the database owner user at login
        # in the requested database (True value) or in all databases ("*" value).
        will_auto_escalate_to_database_owner = predefined_roles[role][
            "auto-escalate-to-database-owner"
        ]
        if (
            auto_escalate_to_database_owner == RoleAttributeValue.NO
            or will_auto_escalate_to_database_owner == "*"
        ):
            auto_escalate_to_database_owner = will_auto_escalate_to_database_owner

        role_permissions = predefined_roles[role]["permissions"]

        # Permission to connect to the requested database (True value) or to all databases
        # ("*" value).
        role_can_connect = role_permissions["connect"]
        if connect == RoleAttributeValue.NO or role_can_connect == "*":
            connect = role_can_connect

        # Permission to create databases (True or RoleAttributeValue.NO).
        create_databases = (
            role_permissions["create-databases"]
            if create_databases == RoleAttributeValue.NO
            else create_databases
        )

        # Permission to create objects in the requested database (True value) or in all databases
        # ("*" value).
        role_can_create_objects = role_permissions["create-objects"]
        if create_objects == RoleAttributeValue.NO or role_can_create_objects == "*":
            create_objects = role_can_create_objects

        # Permission to escalate to the database owner user in the requested database (True value)
        # or in all databases ("*" value).
        role_can_escalate_to_database_owner = role_permissions["escalate-to-database-owner"]
        if (
            escalate_to_database_owner == RoleAttributeValue.NO
            or role_can_escalate_to_database_owner == "*"
        ):
            escalate_to_database_owner = role_can_escalate_to_database_owner

        # Permission to read data in the requested database (True value) or in all databases
        # ("*" value).
        role_can_read_data = role_permissions["read-data"]
        if read_data == RoleAttributeValue.NO or role_can_read_data == "*":
            read_data = role_can_read_data

        read_stats = (
            role_permissions["read-stats"]
            if role_permissions["read-stats"] != RoleAttributeValue.NO
            else read_stats
        )

        run_backup_commands = (
            role_permissions["run-backup-commands"]
            if role_permissions["run-backup-commands"] != RoleAttributeValue.NO
            else run_backup_commands
        )

        # Permission to set up predefined catalog roles ("*" for all databases or
        # RoleAttributeValue.NO for not being able to do it).
        role_can_set_up_predefined_catalog_roles = role_permissions[
            "set-up-predefined-catalog-roles"
        ]
        if (
            set_up_predefined_catalog_roles == RoleAttributeValue.NO
            or role_can_set_up_predefined_catalog_roles == "*"
        ):
            set_up_predefined_catalog_roles = role_can_set_up_predefined_catalog_roles

        # Permission to call the set_user function (True or RoleAttributeValue.NO).
        set_user = role_permissions["set-user"] if set_user == RoleAttributeValue.NO else set_user

        # Permission to write data in the requested database (True value) or in all databases
        # ("*" value).
        role_can_write_data = role_permissions["write-data"]
        if write_data == RoleAttributeValue.NO or role_can_write_data == "*":
            write_data = role_can_write_data
    return {
        "auto-escalate-to-database-owner": auto_escalate_to_database_owner,
        "permissions": {
            "connect": connect,
            "create-databases": create_databases,
            "create-objects": create_objects,
            "escalate-to-database-owner": escalate_to_database_owner,
            "read-data": read_data,
            "read-stats": read_stats,
            "run-backup-commands": run_backup_commands,
            "set-up-predefined-catalog-roles": set_up_predefined_catalog_roles,
            "set-user": set_user,
            "write-data": write_data,
        },
    }


def get_lxd_machine_name(status, unit_name: str, app_name: str) -> str:
    """Get the LXD machine/container name for a given unit.

    Args:
        status: Juju status object
        unit_name: Full unit name (e.g., "postgresql/0")
        app_name: name of the app

    Returns:
        LXD machine/container name (instance_id)
    """
    unit_info = status.get_units(app_name).get(unit_name)
    if not unit_info:
        raise RuntimeError(f"Unable to find unit {unit_name} in status")

    machine_id = getattr(unit_info, "machine", None)
    if not machine_id:
        raise RuntimeError(f"Unable to find machine ID for unit {unit_name}")

    machine_obj = (
        getattr(status, "machines", {}).get(machine_id) if hasattr(status, "machines") else None
    )
    if not machine_obj:
        raise RuntimeError(f"Unable to find machine object for machine {machine_id}")

    machine_name = getattr(machine_obj, "instance_id", None)
    if not machine_name:
        raise RuntimeError(f"Unable to find instance_id for machine {machine_id}")

    return machine_name


def verify_leader_active(status, unit_name: str, app_name: str) -> None:
    """Verify that a unit is the active leader.

    Args:
        status: Juju status object
        unit_name: Full unit name to verify
        app_name: name of the app

    Raises:
        AssertionError if unit is not active leader
    """
    unit = status.get_units(app_name).get(unit_name)
    assert unit is not None, f"Unit {unit_name} not found in status"
    assert unit.leader, f"Unit {unit_name} is not the leader"
    assert unit.workload_status.current == "active", (
        f"Unit {unit_name} is not active. "
        f"Status: {unit.workload_status.current}, "
        f"Message: {unit.workload_status.message}"
    )


def force_leader_election(juju: jubilant.Juju, original_leader: str, app_name: str) -> str:
    """Force a leader election by stopping the current leader's juju agent.

    Args:
        juju: Juju client
        original_leader: Current leader unit name
        app_name: name of the app

    Returns:
        Name of the newly elected leader unit

    Raises:
        RuntimeError: If no new leader is elected within timeout
    """
    logger.info(f"Stopping juju agent on {original_leader} to force leader election")
    status = juju.status()
    machine_name = get_lxd_machine_name(status, original_leader, app_name)

    # Get the machine ID from the status
    unit_info = status.get_units(app_name).get(original_leader)
    machine_id = getattr(unit_info, "machine", None)
    if not machine_id:
        raise RuntimeError(f"Unable to find machine ID for unit {original_leader}")

    jujud_service = f"jujud-machine-{machine_id}"
    logger.info(f"Stopping {jujud_service} service")

    subprocess.check_call([
        "lxc",
        "exec",
        machine_name,
        "--",
        "systemctl",
        "stop",
        jujud_service,
    ])

    # Allow time for agent shutdown to propagate before polling
    logger.info("Waiting for agent shutdown to propagate")
    time.sleep(5)

    # Wait for a new leader to be elected
    logger.info("Waiting for new leader election")
    new_leader = None
    for _ in range(60):  # Wait up to 60 seconds
        time.sleep(1)
        try:
            status = juju.status()
            for unit_name, unit_status in status.get_units(app_name).items():
                if unit_status.leader and unit_name != original_leader:
                    new_leader = unit_name
                    break
            if new_leader:
                break
        except Exception as e:
            logger.debug(f"Error checking leader status: {e}")
            continue

    if new_leader is None:
        # Restart the original leader's agent in case election failed
        logger.info(f"Restarting {jujud_service} service after failed election")
        subprocess.check_call([
            "lxc",
            "exec",
            machine_name,
            "--",
            "systemctl",
            "start",
            jujud_service,
        ])
        raise RuntimeError("No new leader elected within timeout")

    logger.info(f"New leader elected: {new_leader}")

    # Restart the original leader's agent so the cluster is healthy
    logger.info(f"Restarting {jujud_service} service on {original_leader}")
    subprocess.check_call([
        "lxc",
        "exec",
        machine_name,
        "--",
        "systemctl",
        "start",
        jujud_service,
    ])

    return new_leader


def verify_temp_table_creation(juju: jubilant.Juju) -> None:
    """Test that temporary tables can be created successfully."""
    creds = get_credentials(juju, f"{DATA_INTEGRATOR_APP_NAME}/0")
    uri = creds[RELATION_ENDPOINT]["uris"]

    connection = None
    try:
        connection = psycopg2.connect(uri)
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.execute("CREATE TEMPORARY TABLE test (lines TEXT);")
        logger.info("Successfully created temporary table")
    finally:
        if connection is not None:
            connection.close()


def check_for_fix_log_message(juju: jubilant.Juju, unit_name: str) -> bool:
    """Check if the library fix log message appears in the unit's logs.

    Args:
        juju: Juju client
        unit_name: Unit name to check logs for

    Returns:
        True if the log message was found, False otherwise
    """
    logger.info("Checking debug logs for the library fix log message")
    result = subprocess.run(
        [
            "juju",
            "debug-log",
            "--replay",
            "--no-tail",
            "-m",
            juju.model,
            "--include",
            unit_name,
        ],
        capture_output=True,
        text=True,
    )

    expected_message = (
        "Fixed permissions on temp tablespace directory at /var/snap/charmed-postgresql/common/data/temp/16/main "
        "(persistent storage), existing tablespace remains valid"
    )

    if expected_message in result.stdout:
        logger.info(f"✓ Found expected log message in {unit_name} logs")
        return True

    logger.warning(
        f"Expected log message not found in {unit_name} logs. "
        "This may indicate the code path was not triggered or permissions were already correct."
    )
    return False
