# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers specific to the PostgreSQL charm."""

import json
import logging
import tomllib
import zipfile
from enum import Enum

import jubilant
import requests
from single_kernel_postgresql.config.literals import PEER_RELATION
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.helpers.juju import (
    get_app_leader,
    get_app_units,
    get_secret_by_label,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

DATA_INTEGRATOR_APP_NAME = "data-integrator"

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


def get_password(juju: jubilant.Juju, app_name: str, user: str = "operator") -> str:
    """Return the password of a PostgreSQL system user from the charm's app secret."""
    secret = get_secret_by_label(juju, f"{PEER_RELATION}.{app_name}.app")
    return secret[f"{user}-password"]


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
    have advanced past ``previous_revision``, and every unit must report the same non-empty
    workload version. A roll where the last unit's charm hooks are still pending must not
    count as completed.
    """
    app_status = juju.status().apps[app_name]
    workload_versions = {unit.workload_status.version for unit in app_status.units.values()}
    if not workload_versions or "" in workload_versions or len(workload_versions) != 1:
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
