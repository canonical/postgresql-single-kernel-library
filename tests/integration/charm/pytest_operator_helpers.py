#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""pytest-operator helpers shared by the VM and K8s tests (tests/integration/helpers.py of both repos).

Only for the tests that use pytest-operator in both repos; the rest use charm/helpers.py.
"""

import json
import logging
import subprocess
from subprocess import check_call

import requests
from juju.model import Model
from juju.unit import Unit
from pytest_operator.plugin import OpsTest
from single_kernel_postgresql.config.literals import PEER_RELATION
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.helpers.postgresql import execute_query
from tests.integration.types import Substrate

CHARM_BASE = "ubuntu@24.04"
APPLICATION_NAME = "postgresql-test-app"
PGDATA_PATH = "/var/lib/postgresql/16/main"


class SecretNotFoundError(Exception):
    """Raised when a secret is not found."""


try:
    check_call(["kubectl", "version", "--client=true"])
    KUBECTL = "kubectl"
except FileNotFoundError:
    KUBECTL = "sudo k8s kubectl"

logger = logging.getLogger(__name__)


def get_substrate(ops_test: OpsTest) -> Substrate:
    """Return the substrate of the ops_test model (a caas model on K8s)."""
    return Substrate.k8s if ops_test.model.info.type_ == "caas" else Substrate.vm


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


async def get_leader_unit(ops_test: OpsTest, app: str, model: Model = None) -> Unit | None:
    if model is None:
        model = ops_test.model

    leader_unit = None
    for unit in model.applications[app].units:
        if await unit.is_leader_from_status():
            leader_unit = unit
            break

    return leader_unit


def get_patroni_cluster(unit_ip: str) -> dict[str, str]:
    for attempt in Retrying(stop=stop_after_delay(30), wait=wait_fixed(3)):
        with attempt:
            resp = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
    return resp.json()


async def get_machine_from_unit(ops_test: OpsTest, unit_name: str) -> str:
    """Get the name of the machine from a specific unit.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit to get the machine

    Returns:
        The name of the machine.
    """
    raw_hostname = await run_command_on_unit(ops_test, unit_name, "hostname")
    return raw_hostname.strip()


async def get_password(
    ops_test: OpsTest,
    username: str = "operator",
    database_app_name: str | None = None,
) -> str:
    """Retrieve a user password from the secret.

    Args:
        ops_test: ops_test instance.
        username: the user to get the password.
        database_app_name: the app for getting the secret (defaults to the PostgreSQL cluster)

    Returns:
        the user password.
    """
    if database_app_name is None:
        database_app_name = await app_name(ops_test)
    secret = await get_secret_by_label(ops_test, label=f"{PEER_RELATION}.{database_app_name}.app")
    password = secret.get(f"{username}-password")

    return password


async def get_secret_by_label(ops_test: OpsTest, label: str) -> dict[str, str]:
    secrets_raw = await ops_test.juju("list-secrets")
    secret_ids = [
        secret_line.split()[0] for secret_line in secrets_raw[1].split("\n")[1:] if secret_line
    ]

    for secret_id in secret_ids:
        secret_data_raw = await ops_test.juju(
            "show-secret", "--format", "json", "--reveal", secret_id
        )
        secret_data = json.loads(secret_data_raw[1])

        if label == secret_data[secret_id].get("label"):
            return secret_data[secret_id]["content"]["Data"]

    raise SecretNotFoundError(f"Secret with label {label} not found")


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


async def get_unit_address(ops_test: OpsTest, unit_name: str) -> str:
    """Get unit IP address.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit

    Returns:
        IP address of the unit
    """
    if get_substrate(ops_test) == Substrate.vm:
        return ops_test.model.units.get(unit_name).public_address
    status = await ops_test.model.get_status()
    return status["applications"][unit_name.split("/")[0]].units[unit_name]["address"]


async def run_command_on_unit(ops_test: OpsTest, unit_name: str, command: str) -> str:
    """Run a command on a specific unit (in the postgresql container on K8s).

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit to run the command on
        command: The command to run

    Returns:
        the command output if it succeeds, otherwise raises an exception.
    """
    if get_substrate(ops_test) == Substrate.vm:
        complete_command = ["exec", "--unit", unit_name, "--", *command.split()]
        return_code, stdout, _ = await ops_test.juju(*complete_command)
        if return_code != 0:
            logger.error(stdout)
            raise Exception(
                f"Expected command '{command}' to succeed instead it failed: {return_code}"
            )
        return stdout

    complete_command = f"ssh --container postgresql {unit_name} {command}"
    return_code, stdout, stderr = await ops_test.juju(*complete_command.split())
    if return_code != 0:
        raise Exception(
            f"Expected command {command} to succeed instead it failed: {stderr}. Code: {return_code}"
        )
    return stdout


async def scale_application(
    ops_test: OpsTest, application_name: str, count: int, model: Model = None
) -> None:
    """Scale a given application to a specific unit count.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        count: The desired number of units to scale to
        model: The model to scale the application in
    """
    if model is None:
        model = ops_test.model
    if get_substrate(ops_test) == Substrate.vm:
        change = count - len(model.applications[application_name].units)
        if change > 0:
            await model.applications[application_name].add_units(change)
        elif change < 0:
            units = [unit.name for unit in model.applications[application_name].units[0:-change]]
            await model.applications[application_name].destroy_units(*units)
        await model.wait_for_idle(
            apps=[application_name],
            status="active",
            timeout=2000,
            idle_period=30,
            wait_for_exact_units=count,
        )
        return

    await model.applications[application_name].scale(count)
    if count == 0:
        await model.block_until(
            lambda: len(model.applications[application_name].units) == count, timeout=1200
        )
    else:
        await model.wait_for_idle(
            apps=[application_name],
            status="active",
            timeout=1200,
            wait_for_exact_units=count,
            idle_period=2,
        )


def assert_sync_standbys(unit_ip: str, standbys: int) -> None:
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            cluster = get_patroni_cluster(unit_ip)
            cluster_standbys = 0
            for member in cluster["members"]:
                if member["role"] == "sync_standby":
                    cluster_standbys += 1
            assert cluster_standbys >= standbys, "Less than expected standbys"


async def check_database_users_existence(
    ops_test: OpsTest,
    users_that_should_exist: list[str],
    users_that_should_not_exist: list[str],
    admin: bool = False,
    database_app_name: str | None = None,
) -> None:
    """Checks that applications users exist in the database.

    Args:
        ops_test: The ops test framework
        users_that_should_exist: List of users that should exist in the database
        users_that_should_not_exist: List of users that should not exist in the database
        admin: Whether to check if the existing users are superusers
        database_app_name: Optional database app name
            (defaults to the PostgreSQL cluster)
    """
    if database_app_name is None:
        database_app_name = await app_name(ops_test)
    unit = ops_test.model.applications[database_app_name].units[0]
    unit_address = await get_unit_address(ops_test, unit.name)
    password = await get_password(ops_test, database_app_name=database_app_name)

    # Retrieve all users in the database.
    output = execute_query(
        unit_address,
        password,
        (
            "SELECT CONCAT(usename, ':', usesuper) FROM pg_catalog.pg_user;"
            if admin
            else "SELECT usename FROM pg_catalog.pg_user;"
        ),
    )

    # Assert users that should exist.
    for user in users_that_should_exist:
        if admin:
            # The t flag indicates the user is a superuser.
            assert f"{user}:t" in output
        else:
            assert user in output

    # Assert users that should not exist.
    for user in users_that_should_not_exist:
        assert user not in output


async def start_machine(ops_test: OpsTest, machine_name: str) -> None:
    """Start the machine where a unit run on.

    Args:
        ops_test: The ops test framework instance
        machine_name: The name of the machine to start
    """
    start_machine_command = f"lxc start {machine_name}"
    subprocess.check_call(start_machine_command.split())


async def stop_machine(ops_test: OpsTest, machine_name: str) -> None:
    """Stop the machine where a unit run on.

    Args:
        ops_test: The ops test framework instance
        machine_name: The name of the machine to stop
    """
    stop_machine_command = f"lxc stop {machine_name}"
    subprocess.check_call(stop_machine_command.split())


async def reused_full_cluster_recovery_storage(ops_test: OpsTest, unit_name) -> bool:
    """Returns True if storage provided to Postgresql has been reused.

    Checks Patroni logs for when the database was in archive mode or shut down.
    """
    await run_command_on_unit(
        ops_test,
        unit_name,
        "grep -E 'Database cluster state: in archive recovery|Database cluster state: shut down' "
        "/var/snap/charmed-postgresql/common/var/log/patroni/patroni.log*",
    )
    return True
