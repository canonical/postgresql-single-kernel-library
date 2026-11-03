#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import itertools
import json
import logging
import subprocess

import psycopg2
import requests
from juju.model import Model
from pytest_operator.plugin import OpsTest
from single_kernel_postgresql.config.literals import PEER_RELATION
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)

CHARM_BASE = "ubuntu@22.04"
DATABASE_APP_NAME = "postgresql"
APPLICATION_NAME = "postgresql-test-app"
DATA_INTEGRATOR_APP_NAME = "data-integrator"
DATABASE_DEFAULT_NAME = "postgres"


class SecretNotFoundError(Exception):
    """Raised when a secret is not found."""


logger = logging.getLogger(__name__)


def get_patroni_cluster(unit_ip: str) -> dict[str, str]:
    resp = requests.get(f"https://{unit_ip}:8008/cluster", verify=False)
    return resp.json()


def db_connect(
    host: str, password: str, username: str = "operator", database: str = "postgres"
) -> psycopg2.extensions.connection:
    """Returns psycopg2 connection object linked to postgres db in the given host.

    Args:
        host: the IP of the postgres host
        password: user password
        username: username to connect with
        database: database to connect to

    Returns:
        psycopg2 connection object linked to postgres db, under "operator" user.
    """
    return psycopg2.connect(
        f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"
    )


async def execute_query_on_unit(
    unit_address: str,
    password: str,
    query: str,
    database: str = DATABASE_DEFAULT_NAME,
    sslmode: str | None = None,
):
    """Execute given PostgreSQL query on a unit.

    Args:
        unit_address: The public IP address of the unit to execute the query on.
        password: The PostgreSQL superuser password.
        query: Query to execute.
        database: Optional database to connect to (defaults to postgres database).
        sslmode: Optional ssl mode to use (defaults to None).

    Returns:
        A list of rows that were potentially returned from the query.
    """
    extra_connection_parameters = f"sslmode={sslmode}" if sslmode else ""
    with (
        psycopg2.connect(
            f"dbname='{database}' user='operator' host='{unit_address}'"
            f"password='{password}' connect_timeout=10 {extra_connection_parameters}"
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(query)
        output = list(itertools.chain(*cursor.fetchall()))
    return output


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
    database_app_name: str = DATABASE_APP_NAME,
) -> str:
    """Retrieve a user password from the secret.

    Args:
        ops_test: ops_test instance.
        username: the user to get the password.
        database_app_name: the app for getting the secret

    Returns:
        the user password.
    """
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


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def get_unit_address(ops_test: OpsTest, unit_name: str, model: Model = None) -> str:
    """Get unit IP address.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit
        model: Optional model to use to get the unit address

    Returns:
        IP address of the unit
    """
    if model is None:
        model = ops_test.model
    return model.units.get(unit_name).public_address


def has_relation_exited(
    ops_test: OpsTest, endpoint_one: str, endpoint_two: str, model: Model = None
) -> bool:
    """Returns true if the relation between endpoint_one and endpoint_two has been removed."""
    relations = model.relations if model is not None else ops_test.model.relations
    for rel in relations:
        endpoints = [endpoint.name for endpoint in rel.endpoints]
        if endpoint_one in endpoints and endpoint_two in endpoints:
            return False
    return True


async def run_command_on_unit(ops_test: OpsTest, unit_name: str, command: str) -> str:
    """Run a command on a specific unit.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit to run the command on
        command: The command to run

    Returns:
        the command output if it succeeds, otherwise raises an exception.
    """
    complete_command = ["exec", "--unit", unit_name, "--", *command.split()]
    return_code, stdout, _ = await ops_test.juju(*complete_command)
    if return_code != 0:
        logger.error(stdout)
        raise Exception(
            f"Expected command '{command}' to succeed instead it failed: {return_code}"
        )
    return stdout


async def stop_machine(ops_test: OpsTest, machine_name: str) -> None:
    """Stop the machine where a unit run on.

    Args:
        ops_test: The ops test framework instance
        machine_name: The name of the machine to stop
    """
    stop_machine_command = f"lxc stop {machine_name}"
    subprocess.check_call(stop_machine_command.split())


### Ported Mysql jubilant helpers


def execute_queries_on_unit(
    unit_address: str, username: str, password: str, queries: list[str], database: str
) -> list:
    """Execute given PostgreSQL queries on a unit.

    Args:
        unit_address: The public IP address of the unit to execute the queries on
        username: The PostgreSQL username
        password: The PostgreSQL password
        queries: A list of queries to execute
        database: Database to execute in

    Returns:
        A list of rows that were potentially queried
    """
    with (
        psycopg2.connect(
            f"dbname='{database}' user='{username}' host='{unit_address}' password='{password}' connect_timeout=10"
        ) as connection,
        connection.cursor() as cursor,
    ):
        for query in queries:
            cursor.execute(query)
        output = list(itertools.chain(*cursor.fetchall()))

    return output
