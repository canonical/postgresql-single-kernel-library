#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import re
from time import sleep

import jubilant
import psycopg2
import pytest
from jubilant import Juju

from tests.integration.charm.helpers import get_primary, get_refresh_versions
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_credentials,
    get_unit_address,
    relations,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

DATA_INTEGRATOR_APP_NAME = "data-integrator"
FIRST_DATABASE = "test_1"
SECOND_DATABASE = "test_2"
FIRST_RELATION_USER = "relation_id_0"
SECOND_RELATION_USER = "relation_id_1"
PASSWORD = "test-password"


@pytest.mark.abort_on_fail
def test_pg_hba(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    with fast_forward(juju):
        logger.info("Deploying charms")
        if app_name not in juju.status().apps:
            juju.deploy(
                charm,
                app=app_name,
                base="ubuntu@24.04",
                config={"profile": "testing"},
                num_units=2,
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
        if DATA_INTEGRATOR_APP_NAME not in juju.status().apps:
            juju.deploy(
                DATA_INTEGRATOR_APP_NAME,
                config={"database-name": FIRST_DATABASE, "extra-user-roles": "SUPERUSER"},
            )

        logger.info("Adding relation between charms")
        if not relations(juju, app_name, DATA_INTEGRATOR_APP_NAME):
            juju.integrate(DATA_INTEGRATOR_APP_NAME, app_name)

        juju.wait(
            wait_for_apps_status(jubilant.all_active, DATA_INTEGRATOR_APP_NAME, app_name),
            error=jubilant.any_error,
            timeout=1000,
        )

        primary = get_primary(juju, f"{app_name}/0")
        address = get_unit_address(juju, primary)
        data_integrator_unit = next(iter(get_app_units(juju, DATA_INTEGRATOR_APP_NAME)))
        credentials = get_credentials(juju, data_integrator_unit)
        connection = None
        try:
            connection = db_connect(
                host=address,
                password=credentials["postgresql"]["password"],
                user=credentials["postgresql"]["username"],
                database=FIRST_DATABASE,
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                logger.info("Dropping database objects from the previous test run (if any)")
                cursor.execute("RESET ROLE;")
                cursor.execute(f"DROP USER IF EXISTS {FIRST_RELATION_USER};")
                cursor.execute(
                    f"SELECT datname FROM pg_database WHERE datname='{SECOND_DATABASE}';"
                )
                if cursor.fetchone() is not None:
                    cursor.execute(
                        f"REVOKE ALL ON DATABASE {SECOND_DATABASE} FROM {SECOND_RELATION_USER};"
                    )
                cursor.execute(f"DROP USER IF EXISTS {SECOND_RELATION_USER};")
                cursor.execute(f"DROP DATABASE IF EXISTS {SECOND_DATABASE};")
                cursor.execute("DROP SCHEMA IF EXISTS test;")

                logger.info("Creating database objects needed for the test")
                cursor.execute(
                    f"CREATE USER {FIRST_RELATION_USER} WITH LOGIN SUPERUSER ENCRYPTED PASSWORD '{PASSWORD}';"
                )
                cursor.execute(
                    f"CREATE USER {SECOND_RELATION_USER} WITH LOGIN ENCRYPTED PASSWORD '{PASSWORD}';"
                )
                cursor.execute(f"CREATE DATABASE {SECOND_DATABASE};")
                cursor.execute(
                    f"GRANT CONNECT ON DATABASE {SECOND_DATABASE} TO {SECOND_RELATION_USER};"
                )
                cursor.execute("CREATE SCHEMA test;")
        finally:
            if connection:
                connection.close()

        sleep(90)

        for unit_name in get_app_units(juju, app_name):
            try:
                address = get_unit_address(juju, unit_name)

                logger.info(
                    f"Checking that the user {FIRST_RELATION_USER} can connect to the database {FIRST_DATABASE} on {unit_name}"
                )
                with (
                    db_connect(
                        host=address,
                        password=PASSWORD,
                        user=FIRST_RELATION_USER,
                        database=FIRST_DATABASE,
                    ) as connection,
                    connection.cursor() as cursor,
                ):
                    # Check the version that the application received is the same on the
                    # database server.
                    cursor.execute("SELECT version();")
                    data = cursor.fetchone()[0].split(" ")[1]

                    # Get the version of the database and compare with the information that
                    # was retrieved directly from the database.
                    assert credentials["postgresql"]["version"] == data
                connection.close()

                logger.info(
                    f"Checking that the user {SECOND_RELATION_USER} can connect to the database {SECOND_DATABASE} on {unit_name}"
                )
                with (
                    db_connect(
                        host=address,
                        password=PASSWORD,
                        user=SECOND_RELATION_USER,
                        database=SECOND_DATABASE,
                    ) as connection,
                    connection.cursor() as cursor,
                ):
                    # Check the version that the application received is the same on the
                    # database server.
                    cursor.execute("SELECT VERSION();")

                    # Get the version of the database and compare with the information that
                    # was retrieved directly from the database.
                    assert credentials["postgresql"]["version"] == data

                    assert get_refresh_versions(charm)["workload"] == data

                logger.info(
                    f"Checking that the user {SECOND_RELATION_USER} cannot connect to the database {FIRST_DATABASE} on {unit_name}"
                )
                with db_connect(
                    host=address,
                    password=PASSWORD,
                    user=SECOND_RELATION_USER,
                    database=FIRST_DATABASE,
                ) as connection:
                    assert False, (
                        f"User {SECOND_RELATION_USER} should not be able to connect to the database {FIRST_DATABASE}"
                    )
            except psycopg2.OperationalError as e:
                if (
                    re.search(
                        f'^(connection to server at \\").*(\\", port 5432).*(failed: FATAL:  no pg_hba.conf entry for host \\").*(\\", user \\"{SECOND_RELATION_USER}\\", database \\"{FIRST_DATABASE}\\", no encryption)$',
                        str(e),
                    )
                    is None
                ):
                    raise
            finally:
                if connection:
                    connection.close()
