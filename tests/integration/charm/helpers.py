# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers specific to the PostgreSQL charm."""

import contextlib
import json
import logging
import os
import random
import string
import subprocess
import tarfile
import tempfile
import time
import tomllib
import zipfile
from datetime import datetime
from enum import Enum

import jubilant
import kubernetes
import psycopg2
import requests
import yaml
from kubernetes import config
from kubernetes.client.api import core_v1_api
from kubernetes.stream import stream
from lightkube import Client
from lightkube.core.client import GlobalResource
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import PersistentVolume, PersistentVolumeClaim, Pod, Service
from single_kernel_postgresql.config.enums import Substrates
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
    fast_forward,
    get_app_leader,
    get_app_units,
    get_credentials,
    get_secret_by_label,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect, execute_query

logger = logging.getLogger(__name__)

DATA_INTEGRATOR_APP_NAME = "data-integrator"

RELATION_ENDPOINT = "postgresql"

MINUTE_SECS = 60

CHARM_BASE = "ubuntu@24.04"
APPLICATION_NAME = "postgresql-test-app"
SERVICE_NAME = "snap.charmed-postgresql.patroni.service"
PATRONI_SERVICE_DEFAULT_PATH = f"/etc/systemd/system/{SERVICE_NAME}"
RESTART_CONDITION = "no"
ORIGINAL_RESTART_CONDITION = "always"
PGDATA_PATH = "/var/lib/postgresql/16/main"

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


def check_cluster_members(juju: jubilant.Juju, app_name: str, substrate: Substrates) -> None:
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
            if substrate == Substrates.VM:
                assert [member["host"] for member in members] == [
                    unit.public_address for unit in units.values()
                ]


def scale_application(
    juju: jubilant.Juju, app_name: str, count: int, substrate: Substrates
) -> None:
    """Scale an application to a number of units and wait for it to be active.

    On VM the first units are removed; on K8s only the last pods can be removed.
    """
    units = list(get_app_units(juju, app_name))
    change = count - len(units)
    if change > 0:
        juju.add_unit(app_name, num_units=change)
    elif change < 0:
        if substrate == Substrates.VM:
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
    juju: jubilant.Juju, unit_name: str, command: str, substrate: Substrates
) -> str:
    """Run a shell command on the workload of a unit (the postgresql container on K8s)."""
    if substrate == Substrates.VM:
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
    substrate: Substrates,
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
    if substrate == Substrates.K8S:
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
    juju: jubilant.Juju, unit_name: str, enabled: bool, substrate: Substrates
) -> bool:
    """Return whether the Patroni REST API of a unit answers through TLS.

    On VM the server certificate is validated with the peer CA when TLS is enabled. On K8s
    it isn't, since the unit IP doesn't match the certificate hostname (a k8s hostname).
    """
    unit_address = get_unit_address(juju, unit_name)
    tls_ca = get_tls_ca(juju, unit_name, "peer") if substrate == Substrates.VM else ""

    # If there is no TLS CA in the relation, something is wrong in
    # the relation between the TLS Certificates Operator and PostgreSQL.
    if substrate == Substrates.VM and enabled and not tls_ca:
        return False

    try:
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt, tempfile.NamedTemporaryFile() as temp_ca_file:
                temp_ca_file.write(tls_ca.encode("utf-8"))
                temp_ca_file.seek(0)
                verify = temp_ca_file.name if substrate == Substrates.VM and enabled else False
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
    substrate: Substrates,
    resources: dict[str, str] | None,
) -> None:
    """Refresh an application to the charm under test and wait for the refresh to finish.

    The refresh flow differs per substrate: on VM the app blocks as a whole and
    resume-refresh runs on the second unit; on K8s the highest numbered pod is refreshed
    first and resume-refresh runs on the leader.
    """
    if substrate == Substrates.VM:
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


# Faithful synchronous ports of the HA helpers. Keep the existing generic helpers
# above unchanged where their primary selection, retries or storage policy differ.


class MemberNotListedOnClusterError(Exception):
    """Raised when a member is not listed in the cluster."""


class MemberNotUpdatedOnClusterError(Exception):
    """Raised when a member is not yet updated in the cluster."""


def get_postgresql_app_name(
    juju: jubilant.Juju, application_name: str = "postgresql", model: jubilant.Juju | None = None
) -> str | None:
    """Return the first PostgreSQL application, as in the original HA helpers."""
    status = (model or juju).status()
    for app, app_status in status.apps.items():
        if application_name in app_status.charm and APPLICATION_NAME not in app_status.charm:
            return app
    return None


def get_primary_from_app(
    juju: jubilant.Juju, app: str, down_unit: str | None = None
) -> str | None:
    """Ask each available unit for the primary, without changing the original selection."""
    for unit_name in juju.status().apps[app].units:
        if unit_name != down_unit:
            primary = juju.run(unit_name, "get-primary").results.get("primary", "None")
            if primary == "None":
                continue
            return primary
    return None


def get_patroni_cluster(unit_ip: str) -> dict:
    """Read the Patroni cluster with the original 30-second retry window."""
    for attempt in Retrying(stop=stop_after_delay(30), wait=wait_fixed(3)):
        with attempt:
            resp = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
    return resp.json()


def get_ip_from_inside_the_unit(juju: jubilant.Juju, unit_name: str) -> str:
    """Get the addresses reported by hostname -I inside the unit."""
    return juju.cli("exec", "--unit", unit_name, "--", "hostname", "-I").splitlines()[0].strip()


def instance_ip(juju: jubilant.Juju, instance: str) -> str | None:
    """Translate a machine hostname to its IP using the original machines output."""
    output = juju.cli("machines")
    for line in output.splitlines():
        if instance in line:
            return line.split()[2]
    return None


def get_unit_ip(juju: jubilant.Juju, unit_name: str, model: jubilant.Juju | None = None) -> str:
    """Get the VM machine IP or the K8s pod address, as in the HA helpers."""
    status = juju.status()
    if status.model.type == "caas":
        return get_unit_address(juju, unit_name)
    if model is None:
        application = unit_name.split("/")[0]
        for name, _unit in status.apps[application].units.items():
            if name == unit_name:
                break
        return instance_ip(juju, status.machines[_unit.machine].hostname)
    return get_unit_address(juju, unit_name)


def get_machine_from_unit(juju: jubilant.Juju, unit_name: str) -> str:
    """Get the machine hostname by running hostname on the unit."""
    return juju.cli("exec", "--unit", unit_name, "--", "hostname").strip()


def start_machine(machine_name: str) -> None:
    """Start the LXD machine where a unit runs."""
    subprocess.check_call(f"lxc start {machine_name}".split())


def stop_machine(machine_name: str) -> None:
    """Stop the LXD machine where a unit runs."""
    subprocess.check_call(f"lxc stop {machine_name}".split())


def assert_sync_standbys(unit_ip: str, standbys: int) -> None:
    """Check for at least the original number of synchronous standbys."""
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            cluster = get_patroni_cluster(unit_ip)
            cluster_standbys = 0
            for member in cluster["members"]:
                if member["role"] == "sync_standby":
                    cluster_standbys += 1
            assert cluster_standbys >= standbys, "Less than expected standbys"


def check_database_users_existence(
    juju: jubilant.Juju,
    users_that_should_exist: list[str],
    users_that_should_not_exist: list[str],
    admin: bool = False,
    database_app_name: str | None = None,
) -> None:
    """Check database users using the original first unit and SQL assertions."""
    if database_app_name is None:
        database_app_name = get_postgresql_app_name(juju)
    unit_name = next(iter(juju.status().apps[database_app_name].units))
    unit_address = get_unit_address(juju, unit_name)
    password = get_password(juju, database_app_name)

    output = execute_query(
        unit_address,
        password,
        (
            "SELECT CONCAT(usename, ':', usesuper) FROM pg_catalog.pg_user;"
            if admin
            else "SELECT usename FROM pg_catalog.pg_user;"
        ),
    )

    for user in users_that_should_exist:
        if admin:
            assert f"{user}:t" in output
        else:
            assert user in output

    for user in users_that_should_not_exist:
        assert user not in output


def get_patroni_setting(juju: jubilant.Juju, setting: str, tls: bool = False) -> int | None:
    """Get an integer Patroni setting, retaining the original retries and primary selection."""
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            app = get_postgresql_app_name(juju)
            primary_name = get_primary_from_app(juju, app)
            unit_ip = get_unit_address(juju, primary_name)
            configuration_info = requests.get(f"https://{unit_ip}:8008/config", verify=False)
            value = configuration_info.json().get(setting)
            return int(value) if value is not None else None


def change_ha_patroni_setting(
    juju: jubilant.Juju,
    setting: str,
    value: int | bool | None,
    password: str,
    use_random_unit: bool = False,
    tls: bool = False,
) -> None:
    """Change a Patroni setting with the original HA primary selection and retries."""
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            app = get_postgresql_app_name(juju)
            if use_random_unit:
                unit = random.choice(list(juju.status().apps[app].units))
                unit_ip = get_unit_address(juju, unit)
            else:
                primary_name = get_primary_from_app(juju, app)
                unit_ip = get_unit_address(juju, primary_name)
            requests.patch(
                f"https://{unit_ip}:8008/config",
                json={setting: value},
                verify=False,
                auth=requests.auth.HTTPBasicAuth("patroni", password),
            )


def get_ha_cluster_roles(
    juju: jubilant.Juju, unit_name: str, use_ip_from_inside: bool = False
) -> dict[str, list[str]]:
    """Read cluster roles with the original HA address selection and retries."""
    unit_ip = (
        get_ip_from_inside_the_unit(juju, unit_name)
        if use_ip_from_inside
        else get_unit_ip(juju, unit_name)
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


def count_writes(  # noqa: C901
    juju: jubilant.Juju,
    down_unit: str | None = None,
    use_ip_from_inside: bool = False,
    extra_model: jubilant.Juju | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    """Count writes, retaining the HA model selection and member exclusions."""
    app = get_postgresql_app_name(juju)
    password = get_password(juju, app)
    members = []
    if juju.status().model.type != "caas":
        for model in [juju, extra_model]:
            if model is None:
                continue
            status = model.status()
            for unit_name in status.apps[app].units:
                if unit_name != down_unit:
                    members_data = get_patroni_cluster(
                        get_ip_from_inside_the_unit(juju, unit_name)
                        if use_ip_from_inside
                        else get_unit_ip(juju, unit_name)
                    )["members"]
                    for member_data in members_data:
                        member_data["model"] = status.model.name
                    members.extend(members_data)
                    break
        down_ips = []
        if down_unit:
            for unit_name, unit in juju.status().apps[app].units.items():
                if unit_name == down_unit:
                    down_ips.append(unit.public_address)
                    down_ips.append(get_unit_ip(juju, unit_name))
        return count_writes_on_members(members, password, down_ips)

    for model in [juju, extra_model]:
        if model is None:
            continue
        status = model.status()
        for unit_name, unit in status.apps[app].units.items():
            if unit_name != down_unit:
                members_data = get_patroni_cluster(unit.address)["members"]
                for _, member_data in enumerate(members_data):
                    member_data["model"] = status.model.name
                members.extend(members_data)
                break

    count = {}
    maximum = {}
    for member in members:
        if member["role"] != "replica" and member["host"].split(".")[0] != (
            down_unit or ""
        ).replace("/", "-"):
            host = member["host"]
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
                count[member_name] = -1
                maximum[member_name] = -1
            finally:
                if connection is not None:
                    connection.close()
    return count, maximum


def are_writes_increasing(
    juju: jubilant.Juju,
    down_unit: str | list[str] | None = None,
    use_ip_from_inside: bool = False,
    extra_model: jubilant.Juju | None = None,
) -> None:
    """Verify writes increase with the original retries and down-unit handling."""
    if juju.status().model.type != "caas":
        down_units = [down_unit] if isinstance(down_unit, str) or not down_unit else down_unit
        writes, _ = count_writes(
            juju,
            down_unit=down_units[0],
            use_ip_from_inside=use_ip_from_inside,
            extra_model=extra_model,
        )
        logger.info(f"Initial writes {writes}")
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                more_writes, _ = count_writes(
                    juju,
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
            writes, _ = count_writes(juju, down_unit=down_unit, extra_model=extra_model)
            assert len(writes), "No units report count"
    logger.info(f"Initial writes {writes}")
    for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
        with attempt:
            more_writes, _ = count_writes(juju, down_unit=down_unit, extra_model=extra_model)
            logger.info(f"Retry writes {more_writes}")
            members_checked = []
            for member, count in writes.items():
                if member in more_writes:
                    members_checked.append(member)
                    assert more_writes[member] > count, (
                        f"{member}: writes not continuing to DB (current writes: {more_writes[member]} - previous writes: {count})"
                    )
            assert len(members_checked), "No member checked from the initial writes"


def start_ha_continuous_writes(
    juju: jubilant.Juju, app: str, model: jubilant.Juju | None = None
) -> None:
    """Relate the fixed test-app if needed, then start writes on its first unit."""
    if model is None:
        model = juju
    relations = [
        relation
        for relation in model.status().apps[APPLICATION_NAME].relations.get("database", [])
        if relation.related_app == app
    ]
    if not relations:
        model.integrate(app, f"{APPLICATION_NAME}:database")
        model.wait(wait_for_apps_status(jubilant.all_active, APPLICATION_NAME, app), timeout=1000)
    for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3), reraise=True):
        with attempt:
            unit = next(iter(model.status().apps[APPLICATION_NAME].units))
            action = model.run(unit, "start-continuous-writes")
            assert action.results["result"] == "True", "Unable to create continuous_writes table"


def stop_continuous_writes(juju: jubilant.Juju) -> int:
    """Stop continuous writes and return the last written value."""
    unit = next(iter(juju.status().apps[APPLICATION_NAME].units))
    action = juju.run(unit, "stop-continuous-writes")
    return int(action.results["writes"])


def check_writes(
    juju: jubilant.Juju,
    use_ip_from_inside: bool = False,
    extra_model: jubilant.Juju | None = None,
) -> int:
    """Compare the test-app's final count with each member, using the original retries."""
    total_expected_writes = stop_continuous_writes(juju)
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            actual_writes, max_number_written = count_writes(
                juju, use_ip_from_inside=use_ip_from_inside, extra_model=extra_model
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


def fetch_cluster_members(juju: jubilant.Juju, use_ip_from_inside: bool = False):
    """Fetch and compare the member IP lists reported by every unit."""
    app = get_postgresql_app_name(juju)
    member_ips = {}
    if juju.status().model.type != "caas":
        for unit_name in juju.status().apps[app].units:
            unit_ip = (
                get_ip_from_inside_the_unit(juju, unit_name)
                if use_ip_from_inside
                else get_unit_ip(juju, unit_name)
            )
            cluster_info = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
            if len(member_ips) > 0:
                assert member_ips == {
                    member["host"] for member in cluster_info.json()["members"]
                }, "members report different lists of cluster members."
            else:
                member_ips = {member["host"] for member in cluster_info.json()["members"]}
        return member_ips

    def get_host_ip(host: str) -> str:
        client = Client(namespace=juju.status().model.name)
        pod = client.get(Pod, name=host.split(".")[0])
        return pod.status.podIP

    for unit_name in juju.status().apps[app].units:
        unit_address = get_unit_address(juju, unit_name)
        cluster_info = requests.get(f"https://{unit_address}:8008/cluster", verify=False)
        if len(member_ips) > 0:
            assert member_ips == {
                get_host_ip(member["host"]) for member in cluster_info.json()["members"]
            }, "members report different lists of cluster members."
        else:
            member_ips = {get_host_ip(member["host"]) for member in cluster_info.json()["members"]}
    return member_ips


def is_replica(juju: jubilant.Juju, unit_name: str, use_ip_from_inside: bool = False) -> bool:
    """Wait for a former primary to be listed and no longer be leader."""
    unit_ip = (
        get_ip_from_inside_the_unit(juju, unit_name)
        if use_ip_from_inside
        else get_unit_ip(juju, unit_name)
    )
    member_name = unit_name.replace("/", "-")
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
            with attempt:
                cluster_info = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
                if member_name not in {
                    member["name"] for member in cluster_info.json()["members"]
                }:
                    raise MemberNotListedOnClusterError()
                for member in cluster_info.json()["members"]:
                    if member["name"] == member_name:
                        role = member["role"]
                if role != "leader":
                    return True
                else:
                    raise MemberNotUpdatedOnClusterError()
    except RetryError:
        return False


def is_secondary_up_to_date(
    juju: jubilant.Juju,
    unit_name: str,
    expected_writes: int,
    use_ip_from_inside: bool = False,
) -> bool:
    """Check a secondary's writes with the original retries and K8s idle wait."""
    app = get_postgresql_app_name(juju)
    password = get_password(juju, app)
    if juju.status().model.type != "caas":
        host = next(
            (
                get_ip_from_inside_the_unit(juju, name)
                if use_ip_from_inside
                else get_unit_ip(juju, name)
            )
            for name in juju.status().apps[app].units
            if name == unit_name
        )
    else:
        host = juju.status().apps[app].units[unit_name].address
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
                if juju.status().model.type != "caas":
                    assert results[0] == expected_writes and results[1] == expected_writes
                elif results[0] != expected_writes or results[1] != expected_writes:
                    with fast_forward(juju, interval="30s"):
                        juju.wait(
                            lambda status: jubilant.all_agents_idle(
                                status, unit_name.split("/")[0]
                            ),
                            timeout=1000,
                        )
                        raise Exception
    except RetryError:
        return False
    finally:
        connection.close()
    return True


def is_cluster_updated(
    juju: jubilant.Juju, primary_name: str, use_ip_from_inside: bool = False
) -> None:
    logger.info("checking that the former primary is now a replica")
    assert is_replica(juju, primary_name, use_ip_from_inside), (
        "there are more than one primary in the cluster."
    )
    logger.info("checking that all units are part of the same cluster")
    member_ips = fetch_cluster_members(juju, use_ip_from_inside)
    app = primary_name.split("/")[0]
    ip_addresses = [
        (
            get_ip_from_inside_the_unit(juju, unit_name)
            if use_ip_from_inside
            else get_unit_ip(juju, unit_name)
        )
        for unit_name in juju.status().apps[app].units
    ]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."
    logger.info("checking that no writes to the database were missed after stopping the writes")
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            total_expected_writes = check_writes(juju, use_ip_from_inside)
    logger.info("checking that the former primary is up to date with the cluster after restarting")
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            assert is_secondary_up_to_date(
                juju, primary_name, total_expected_writes, use_ip_from_inside
            ), f"secondary ({primary_name}) not up to date with the cluster after restarting."


def scale_ha_application(
    juju: jubilant.Juju,
    application_name: str,
    count: int,
    model: jubilant.Juju | None = None,
) -> None:
    """Scale using the original HA storage policy, then wait for the unit count and active status."""
    if model is None:
        model = juju
    if juju.status().model.type != "caas":
        units = list(model.status().apps[application_name].units)
        change = count - len(units)
        if change > 0:
            model.add_unit(application_name, num_units=change)
        elif change < 0:
            model.remove_unit(*units[0:-change])
        model.wait(
            lambda status: (
                len(status.apps[application_name].units) == count
                and wait_for_apps_status(jubilant.all_active, application_name)(status)
            ),
            timeout=2000,
        )
        return

    model.cli("scale-application", application_name, str(count))
    if count == 0:
        model.wait(
            lambda status: len(status.apps[application_name].units) == count,
            timeout=1200,
            delay=0.5,
            successes=1,
        )
    else:
        model.wait(
            lambda status: (
                len(status.apps[application_name].units) == count
                and wait_for_apps_status(jubilant.all_active, application_name)(status)
            ),
            timeout=1200,
        )


def remove_charm_code(juju: jubilant.Juju, unit_name: str) -> None:
    """Remove src/charm.py from the PostgreSQL unit's charm container."""
    juju.ssh(
        unit_name,
        f"rm /var/lib/juju/agents/unit-{unit_name.replace('/', '-')}/charm/src/charm.py",
        container="charm",
    )


def storage_type(juju: jubilant.Juju, app: str) -> str | None:
    """Retrieve the storage type using the original CLI table parsing."""
    proc = juju.cli("storage")
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


def get_storage_ids(juju: jubilant.Juju, unit_name: str) -> list[str]:
    """Retrieve storage IDs (only data on K8s), using the original CLI table."""
    storage_ids = []
    proc = juju.cli("storage")
    for line in proc.splitlines():
        if "Storage" in line:
            continue
        if len(line) == 0:
            continue
        if "detached" in line:
            continue
        if line.split()[0] == unit_name and (
            juju.status().model.type != "caas" or line.split()[1].startswith("data")
        ):
            storage_ids.append(line.split()[1])
    return storage_ids


def add_unit_with_storage(juju: jubilant.Juju, app: str, storages: list[str]) -> str:
    """Add a unit and verify the attached storage exactly like the async helper."""
    original_units = set(juju.status().apps[app].units)
    add_unit_cmd = ["add-unit", app]
    for storage in storages:
        add_unit_cmd.append(f"--attach-storage={storage}")
    juju.cli(*add_unit_cmd)
    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app), timeout=2000)

    # When removing all units sometimes the last unit remain in the list
    current_units = set(juju.status().apps[app].units)
    original_units.intersection_update(current_units)
    assert original_units.issubset(current_units), "New unit not added to model"

    new_unit = (current_units - original_units).pop()
    assert sorted(get_storage_ids(juju, new_unit)) == sorted(storages), (
        "unit added with incorrect storage"
    )
    return new_unit


def run_ha_command_on_unit(
    juju: jubilant.Juju, unit_name: str, command: str, container: str = "postgresql"
) -> str:
    """Run the original HA CLI command, preserving exec/ssh and argument splitting."""
    if juju.status().model.type != "caas":
        return juju.cli("exec", "--unit", unit_name, "--", *command.split())
    complete_command = f"ssh --container {container} {unit_name} {command}"
    return juju.cli(*complete_command.split())


def reused_replica_storage(juju: jubilant.Juju, unit_name: str) -> bool:
    """Check the original Patroni log marker for reused replica storage."""
    command = (
        "grep 'Database cluster state: in archive recovery' "
        "/var/snap/charmed-postgresql/common/var/log/patroni/patroni.log*"
    )
    run_ha_command_on_unit(juju, unit_name, command)
    return True


def reused_full_cluster_recovery_storage(juju: jubilant.Juju, unit_name: str) -> bool:
    """Check the original Patroni log markers for full cluster recovery."""
    command = (
        "grep -E 'Database cluster state: in archive recovery|Database cluster state: shut down' "
        "/var/snap/charmed-postgresql/common/var/log/patroni/patroni.log*"
    )
    run_ha_command_on_unit(juju, unit_name, command)
    return True


def is_storage_exists(juju: jubilant.Juju, storage_id: str) -> bool:
    """Check storage existence, retaining the original return-code-1 handling."""
    try:
        stdout = juju.cli("show-storage", storage_id, "--format=json")
    except jubilant.CLIError as error:
        if error.returncode == 1:
            return storage_id in error.stdout
        raise
    return storage_id in str(stdout)


def create_db(juju: jubilant.Juju, app: str, db: str) -> None:
    """Create the database using the first unit, like the async HA helper."""
    unit = next(iter(juju.status().apps[app].units))
    unit_address = get_unit_address(juju, unit)
    password = get_password(juju, app)
    conn = db_connect(unit_address, password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {db};")
    cursor.close()
    conn.close()


def check_db(juju: jubilant.Juju, app: str, db: str) -> bool:
    """Check database existence using the original query and first unit."""
    unit = next(iter(juju.status().apps[app].units))
    unit_address = get_unit_address(juju, unit)
    password = get_password(juju, app)
    assert password is not None
    query = execute_query(
        unit_address,
        password,
        f"select datname from pg_catalog.pg_database where datname = '{db}';",
    )
    if "ERROR" in query:
        raise Exception(f"Database check is failed with postgresql err: {query}")
    return db in query


def get_detached_storages(juju: jubilant.Juju) -> list[str]:
    """Get detached storage (only data on K8s), retaining the original life expression."""
    parsed_storages_list = json.loads(juju.cli("storage", "--format=json"))
    detached_storages = []
    for storage_name, storage in parsed_storages_list["storage"].items():
        if (
            (juju.status().model.type != "caas" or storage_name.startswith("data"))
            and (str(storage["status"]["current"]) == "detached")
            and (str(storage["life"] == "alive"))
        ):
            detached_storages.append(storage_name)
    if len(detached_storages) > 0:
        return detached_storages
    raise Exception("failed to get deatached storage")


def check_password_auth(juju: jubilant.Juju, unit_name: str) -> bool:
    """Check the same operator authentication failure marker in PostgreSQL logs."""
    stdout = juju.cli(
        "exec",
        "--unit",
        unit_name,
        "--",
        "grep",
        "-E",
        "'password'",
        "/var/snap/charmed-postgresql/common/var/log/postgresql/postgresql*",
    )
    return 'password authentication failed for user "operator"' not in stdout


def check_system_id_mismatch(juju: jubilant.Juju, unit_name: str) -> bool:
    """Check the original system ID mismatch marker in Patroni logs."""
    log_str = f"CRITICAL: system ID mismatch, node {unit_name.replace('/', '-')} belongs to a different cluster"
    stdout = run_ha_command_on_unit(
        juju, unit_name, "cat /var/lib/pg/logs/16/main/patroni_logs/patroni.log*"
    )
    return log_str in str(stdout)


def delete_pvc(juju: jubilant.Juju, pvc: GlobalResource):
    """Delete a PVC, retaining the original warning-only ApiError handling."""
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    try:
        client.delete(PersistentVolumeClaim, namespace=namespace, name=pvc.metadata.name)
    except ApiError as e:
        logger.warning(f"failed to delete pvc {pvc.metadata.name}: {e}")
        pass


def get_pvcs(juju: jubilant.Juju, unit_name: str):
    """Get the unit's PVCs with the original name matching and storage-name parsing."""
    pvcs = {}
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    pvc_list = client.list(PersistentVolumeClaim, namespace=namespace)
    unit_label = unit_name.replace("/", "-")
    for pvc in pvc_list:
        if unit_label in pvc.metadata.name:
            pvc_storage_name = pvc.metadata.name.replace(unit_name.split("/")[0], "").split("-")[1]
            logger.info(f"got pvc for {pvc_storage_name} storage: {pvc.metadata.name}")
            pvcs[pvc_storage_name] = pvc
    return pvcs


def get_pvs(juju: jubilant.Juju, unit_name: str):
    """Get the unit's PVs with the original claimRef matching and name parsing."""
    pvs = {}
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    pv_list = client.list(PersistentVolume, namespace=namespace)
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
    juju: jubilant.Juju, pvs_configs: dict[str, PersistentVolume], policy: str
):
    """Patch the original PV reclaim policy and return the patched resources."""
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    results = {}
    for pvc_storage_name, pv_config in pvs_configs.items():
        results[pvc_storage_name] = client.patch(
            PersistentVolume,
            pv_config.metadata.name,
            {"spec": {"persistentVolumeReclaimPolicy": f"{policy}"}},
            namespace=namespace,
        )
    return results


def remove_pv_claimref(juju: jubilant.Juju, pv_config: PersistentVolume):
    """Remove the PV's claimRef with the original patch."""
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    client.patch(
        PersistentVolume,
        pv_config.metadata.name,
        {"spec": {"claimRef": None}},
        namespace=namespace,
    )


def change_pvc_pv_name(
    pvc_config: PersistentVolumeClaim, pv_name_new: str
) -> PersistentVolumeClaim:
    """Change the PVC's PV name and clear the same binding metadata as the original."""
    pvc_config.spec.volumeName = pv_name_new
    del pvc_config.metadata.annotations["pv.kubernetes.io/bind-completed"]
    pvc_config.metadata.uid = None
    return pvc_config


def apply_pvc_config(juju: jubilant.Juju, pvc_config: PersistentVolumeClaim):
    """Apply the PVC with the original field manager and metadata mutation."""
    namespace = juju.status().model.name
    client = Client(namespace=namespace)
    pvc_config.metadata.managedFields = None
    client.apply(pvc_config, namespace=namespace, field_manager="lightkube")


def remove_unit_force(juju: jubilant.Juju, num_units: int):
    """Remove K8s units with the original force flags and scale-dependent wait."""
    app_name_str = get_postgresql_app_name(juju)
    scale = len(juju.status().apps[app_name_str].units) - num_units
    juju.cli(
        "remove-unit",
        app_name_str,
        "--force",
        "--no-wait",
        "--no-prompt",
        "--num-units",
        str(num_units),
    )
    if scale == 0:
        juju.wait(
            lambda status: len(status.apps[app_name_str].units) == scale,
            timeout=1000,
            delay=0.5,
            successes=1,
        )
    else:
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name_str), timeout=1000)


def is_postgresql_ready(
    juju: jubilant.Juju, unit_name: str, use_ip_from_inside: bool = False
) -> bool:
    """Check Patroni health with the original ten-minute retry policy."""
    unit_ip = (
        get_ip_from_inside_the_unit(juju, unit_name)
        if use_ip_from_inside
        else get_unit_address(juju, unit_name)
    )
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 10), wait=wait_fixed(3)):
            with attempt:
                instance_health_info = requests.get(f"https://{unit_ip}:8008/health", verify=False)
                assert instance_health_info.status_code == 200
    except RetryError:
        return False
    return True


def update_restart_condition(juju: jubilant.Juju, unit_name: str, condition: str):
    """Update the systemd restart condition with the original SCP/reload/start sequence."""
    _, temp_path = tempfile.mkstemp()
    for attempt in range(10):
        try:
            juju.scp(
                f"ubuntu@{unit_name}:{PATRONI_SERVICE_DEFAULT_PATH}",
                temp_path,
                host_key_checks=False,
                scp_options=["-q", "-B"],
            )
            break
        except jubilant.CLIError:
            time.sleep(2)
            if attempt == 9:
                raise
    with open(temp_path) as patroni_service_file:
        patroni_service = patroni_service_file.readlines()

    for index, line in enumerate(patroni_service):
        if "Restart=" in line:
            patroni_service[index] = f"Restart={condition}\n"

    with open(temp_path, "w") as service_file:
        service_file.writelines(patroni_service)

    # The service directory has strict permissions: upload elsewhere, then move it.
    for attempt in range(10):
        try:
            juju.scp(
                temp_path,
                f"ubuntu@{unit_name}:patroni.service",
                host_key_checks=False,
                scp_options=["-q", "-B"],
            )
            break
        except jubilant.CLIError:
            time.sleep(2)
            if attempt == 9:
                raise
    mv_cmd = f"mv /home/ubuntu/patroni.service {PATRONI_SERVICE_DEFAULT_PATH}"
    run_ha_command_on_unit(juju, unit_name, mv_cmd)
    os.remove(temp_path)

    run_ha_command_on_unit(juju, unit_name, "systemctl daemon-reload")
    run_ha_command_on_unit(juju, unit_name, f"systemctl start {SERVICE_NAME}")
    is_postgresql_ready(juju, unit_name)


def are_all_db_processes_down(juju: jubilant.Juju, process: str, signal: str) -> bool:
    """Check all units and re-send the signal with the original 400-second retry policy."""
    app = get_postgresql_app_name(juju)
    pgrep_cmd = ("pgrep", "-f", process) if "/" in process else ("pgrep", "-x", process)
    is_vm = juju.status().model.type != "caas"
    if not is_vm:
        try:
            subprocess.check_call(["kubectl", "version", "--client=true"])
            kubectl = "kubectl"
        except FileNotFoundError:
            kubectl = "sudo k8s kubectl"
        namespace = juju.status().model.name

    try:
        for attempt in Retrying(stop=stop_after_delay(400), wait=wait_fixed(3)):
            with attempt:
                running_process = False
                for unit in juju.status().apps[app].units:
                    if is_vm:
                        try:
                            processes = juju.cli("ssh", unit, *pgrep_cmd)
                        except jubilant.CLIError as error:
                            # The original ignores the exit code and examines stdout only.
                            processes = error.stdout
                        processes = [proc for proc in processes.split("\n") if len(proc) > 0]
                        is_running = len(processes) > 0
                    else:
                        pod_name = unit.replace("/", "-")
                        call = subprocess.run(
                            f"{kubectl} -n {namespace} exec {pod_name} -c postgresql -- {' '.join(pgrep_cmd)}",
                            shell=True,
                        )
                        is_running = call.returncode != 1

                    if is_running:
                        logger.info(f"Unit {unit} not yet down")
                        send_signal_to_process(juju, unit, process, signal)
                        running_process = True
                if running_process:
                    raise ProcessRunningError
    except RetryError:
        return False
    return True


class ProcessError(Exception):
    """Raised when a process fails."""


class ProcessRunningError(Exception):
    """Raised when a process is running when it is not expected to be."""


def send_signal_to_process(  # noqa: C901
    juju: jubilant.Juju, unit_name: str, process: str, signal: str, use_ssh: bool = False
) -> None:
    """Port the original process signal helper, including PID checks and retries."""
    app = get_postgresql_app_name(juju)
    if len(juju.status().apps[app].units) < 2:
        juju.add_unit(app, num_units=1)
        juju.wait(wait_for_apps_status(jubilant.all_active, app), timeout=1000)

    opt = "-f" if "/" in process else "-x"
    if juju.status().model.type != "caas":
        command = f"exec --unit {unit_name} -- pkill --signal {signal} {opt} {process}"
        try:
            juju.cli(*command.split())
        except jubilant.CLIError as error:
            if signal != "SIGCONT":
                raise ProcessError(
                    "Expected command %s to succeed instead it failed: %s",
                    command,
                    error.returncode,
                ) from error
        return

    pod_name = unit_name.replace("/", "-")
    if signal not in ["SIGSTOP", "SIGCONT"]:
        try:
            old_pid = juju.cli(
                "ssh", "--container", "postgresql", unit_name, "pgrep", opt, process
            )
        except jubilant.CLIError as error:
            old_pid = error.stdout

    command = f"pkill --signal {signal} {opt} {process}"
    if use_ssh:
        # Juju.cli has no timeout argument; retain the original ten-second limit.
        response = subprocess.run(
            [juju.cli_binary, "ssh", "--model", juju.model, unit_name, *command.split()],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if response.returncode != 0:
            raise ProcessError(
                "Expected command %s to succeed instead it failed: %s",
                command,
                response.returncode,
            )
        return

    config.load_kube_config()
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            response = stream(
                core_v1_api.CoreV1Api().connect_get_namespaced_pod_exec,
                pod_name,
                juju.status().model.name,
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
                try:
                    raw_pid = juju.cli(
                        "ssh", "--container", "postgresql", unit_name, "pgrep", opt, process
                    )
                except jubilant.CLIError as error:
                    raw_pid = error.stdout
                if raw_pid == old_pid:
                    raise ProcessRunningError
            elif response.returncode != 0:
                raise ProcessError(
                    "Expected command %s to succeed instead it failed: %s",
                    command,
                    response.returncode,
                )


def copy_file_into_pod(
    client: kubernetes.client.api.core_v1_api.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str,
    destination_path: str,
    source_path: str,
) -> None:
    """Copy file contents into a pod with the original tar/stream implementation."""
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


def modify_pebble_restart_delay(
    juju: jubilant.Juju, unit_name: str, pebble_plan_path: str, ensure_replan: bool = False
) -> None:
    """Add/replan the original Pebble layer, preserving optional validation and retries."""
    kubernetes.config.load_kube_config()
    client = kubernetes.client.api.core_v1_api.CoreV1Api()
    pod_name = unit_name.replace("/", "-")
    container_name = "postgresql"
    service_name = "postgresql"
    now = datetime.now().isoformat()
    namespace = juju.status().model.name
    copy_file_into_pod(
        client,
        namespace,
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
        namespace,
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
                namespace,
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


def change_wal_settings(
    juju: jubilant.Juju,
    unit_name: str,
    max_wal_size: int,
    min_wal_size,
    wal_keep_segments,
    password: str,
) -> None:
    """Change WAL settings with the original parameters and 60-second retry policy."""
    for attempt in Retrying(stop=stop_after_delay(30 * 2), wait=wait_fixed(3)):
        with attempt:
            unit_ip = get_unit_address(juju, unit_name)
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


def get_postgresql_parameter(juju: jubilant.Juju, parameter_name: str) -> int | None:
    """Read a PostgreSQL parameter from Patroni, including the original None cases."""
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            app = get_postgresql_app_name(juju)
            primary_name = get_primary_from_app(juju, app)
            unit_ip = get_unit_address(juju, primary_name)
            configuration_info = requests.get(f"https://{unit_ip}:8008/config", verify=False)
            postgresql_dict = configuration_info.json().get("postgresql")
            if postgresql_dict is None:
                return None
            parameters = postgresql_dict.get("parameters")
            if parameters is None:
                return None
            parameter_value = parameters.get(parameter_name)
            return parameter_value


def list_wal_files(juju: jubilant.Juju, app: str) -> dict[str, set[str]]:
    """List WAL files per unit with the original VM/K8s commands and exclusions."""
    units = list(juju.status().apps[app].units)
    files = {}
    if juju.status().model.type != "caas":
        command = "ls -1 /var/snap/charmed-postgresql/common/var/lib/postgresql/16/main/pg_wal/"
        for unit in units:
            stdout = run_ha_command_on_unit(juju, unit, command)
            files[unit] = stdout.splitlines()
            files[unit] = {
                i for i in files[unit] if ".history" not in i and i != "" and i != "archive_status"
            }
        return files

    command = f"ls -1 {PGDATA_PATH}/pg_wal/"
    for unit in units:
        complete_command = f"run --unit {unit} -- {command}"
        try:
            stdout = juju.cli(*complete_command.split())
        except jubilant.CLIError as error:
            # The original ignores the exit code and examines stdout only.
            stdout = error.stdout
        files[unit] = stdout.splitlines()
        files[unit] = {
            i for i in files[unit] if ".history" not in i and i != "" and i != "archive_status"
        }
    return files


def is_connection_possible(
    juju: jubilant.Juju, unit_name: str, use_ip_from_inside: bool = False
) -> bool:
    """Test a PostgreSQL connection with the original 120-second retry policy.

    Jubilant's public synchronous API has no equivalent to the original two
    asyncio.wait_for(..., 15) calls for password and address lookup. Those lookups
    are not individually bounded; the retry window cannot interrupt a blocked CLI.
    """
    try:
        app = unit_name.split("/")[0]
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3)):
            with attempt:
                password = get_password(juju, app)
                address = (
                    get_ip_from_inside_the_unit(juju, unit_name)
                    if use_ip_from_inside
                    else get_unit_ip(juju, unit_name)
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
        return False


def cut_network_from_unit(machine_name: str) -> None:
    """Cut network from a LXC container by masking eth0."""
    cut_network_command = f"lxc config device add {machine_name} eth0 none"
    subprocess.check_call(cut_network_command.split())


def cut_network_from_unit_without_ip_change(machine_name: str) -> None:
    """Cut network with the original LXC limits, preserving the unit IP address."""
    override_command = f"lxc config device override {machine_name} eth0"
    with contextlib.suppress(subprocess.CalledProcessError):
        subprocess.check_call(override_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.egress=0kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.ingress=1kbit"
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority=10"
    subprocess.check_call(limit_set_command.split())


def get_controller_machine(juju: jubilant.Juju) -> str:
    """Return the first controller machine's instance ID from show-controller."""
    controller_name = juju.status().model.controller
    raw_controller = juju.cli("show-controller", controller_name, include_model=False)
    controller = yaml.safe_load(raw_controller.strip())
    return next(
        machine.get("instance-id")
        for machine in controller[controller_name]["controller-machines"].values()
    )


def is_machine_reachable_from(origin_machine: str, target_machine: str) -> bool:
    """Test reachability with the original three pings from a LXC container."""
    try:
        subprocess.check_call(f"lxc exec {origin_machine} -- ping -c 3 {target_machine}".split())
        return True
    except subprocess.CalledProcessError:
        return False


def restore_network_for_unit(machine_name: str) -> None:
    """Restore network by removing the eth0 mask from a LXC container."""
    restore_network_command = f"lxc config device remove {machine_name} eth0"
    subprocess.check_call(restore_network_command.split())


def restore_network_for_unit_without_ip_change(machine_name: str) -> None:
    """Restore network by clearing the same LXC limits as the original helper."""
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.egress="
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.ingress="
    subprocess.check_call(limit_set_command.split())
    limit_set_command = f"lxc config device set {machine_name} eth0 limits.priority="
    subprocess.check_call(limit_set_command.split())


def wait_network_restore(juju: jubilant.Juju, unit_name: str, old_ip: str) -> None:
    """Check the IP inside the unit once, exactly like the original helper."""
    if get_ip_from_inside_the_unit(juju, unit_name) == old_ip:
        raise Exception


def get_member_lag(cluster: dict, member_name: str) -> int:
    """Return the lag of a specific member."""
    for member in cluster["members"]:
        if member["name"] == member_name.replace("/", "-"):
            return member.get("lag", 0)
    return 0


def is_member_isolated(
    juju: jubilant.Juju, not_isolated_member: str, isolated_member: str
) -> bool:
    """Check for lag above 1000 with the original 60-second retry policy."""
    unit_ip = get_unit_address(juju, not_isolated_member)
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
    """Run the original Chaos Mesh deployment script in the provided namespace."""
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
    subprocess.check_output(
        " ".join([
            "tests/integration/charm/high_availability/scripts/deploy_chaos_mesh.sh",
            namespace,
        ]),
        shell=True,
        env=env,
    )


def destroy_chaos_mesh(namespace: str) -> None:
    """Run the original Chaos Mesh removal script in the provided namespace."""
    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
    subprocess.check_output(
        f"tests/integration/charm/high_availability/scripts/destroy_chaos_mesh.sh {namespace}",
        shell=True,
        env=env,
    )


def isolate_instance_from_cluster(juju: jubilant.Juju, unit_name: str) -> None:
    """Apply the original NetworkChaos manifest to isolate a pod."""
    try:
        subprocess.check_call(["kubectl", "version", "--client=true"])
        kubectl = "kubectl"
    except FileNotFoundError:
        kubectl = "sudo k8s kubectl"

    with tempfile.NamedTemporaryFile() as temp_file:
        with open(
            "tests/integration/charm/high_availability/manifests/chaos_network_loss.yml"
        ) as chaos_network_loss_file:
            template = string.Template(chaos_network_loss_file.read())
            chaos_network_loss = template.substitute(
                namespace=juju.status().model.name,
                pod=unit_name.replace("/", "-"),
            )
            temp_file.write(str.encode(chaos_network_loss))
            temp_file.flush()

        env = os.environ
        env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
        subprocess.check_output(
            " ".join([*kubectl.split(), "apply", "-f", temp_file.name]), shell=True, env=env
        )


def remove_instance_isolation(juju: jubilant.Juju) -> None:
    """Delete the original NetworkChaos policy, ignoring a missing policy."""
    try:
        subprocess.check_call(["kubectl", "version", "--client=true"])
        kubectl = "kubectl"
    except FileNotFoundError:
        kubectl = "sudo k8s kubectl"

    env = os.environ
    env["KUBECONFIG"] = os.path.expanduser("~/.kube/config")
    subprocess.check_output(
        f"{kubectl} -n {juju.status().model.name} delete --ignore-not-found=true networkchaos network-loss-primary",
        shell=True,
        env=env,
    )
