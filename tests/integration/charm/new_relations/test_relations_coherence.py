#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import secrets
import string

import jubilant
import psycopg2
import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import DATABASE_DEFAULT_NAME

from tests.integration.charm.helpers import (
    CHARM_BASE,
    DATA_INTEGRATOR_APP_NAME,
    build_connection_string,
)
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "postgresql-test-app"
FIRST_DATABASE_RELATION_NAME = "database"


@pytest.mark.abort_on_fail
def test_relations(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrates
):
    """Test that check relation data."""
    app_names = [app_name, DATA_INTEGRATOR_APP_NAME]
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            num_units=1,
            base=CHARM_BASE,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrates.K8S,
        )
        # Creating first time relation with user role. Base is ignored on K8s.
        juju.deploy(DATA_INTEGRATOR_APP_NAME, base=CHARM_BASE)
        juju.config(
            DATA_INTEGRATOR_APP_NAME,
            {"database-name": DATA_INTEGRATOR_APP_NAME.replace("-", "_")},
        )
        juju.integrate(DATA_INTEGRATOR_APP_NAME, app_name)
        juju.wait(wait_for_apps_status(jubilant.all_active, *app_names), timeout=3000)

        connection_string = build_connection_string(
            juju,
            DATA_INTEGRATOR_APP_NAME,
            "postgresql",
            substrate,
            database=DATA_INTEGRATOR_APP_NAME.replace("-", "_"),
        )

        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            random_name = (
                f"test_{''.join(secrets.choice(string.ascii_lowercase) for _ in range(10))}"
            )
            cursor.execute(f"CREATE DATABASE {random_name};")
            assert False, "user role was able to create database"
        except psycopg2.errors.InsufficientPrivilege:
            pass
        finally:
            connection.close()

        with psycopg2.connect(connection_string) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Check that it's possible to write and read data from the database that
                # was created for the application.
                cursor.execute("DROP TABLE IF EXISTS test;")
                cursor.execute("CREATE TABLE test(data TEXT);")
                cursor.execute("INSERT INTO test(data) VALUES('some data');")
                cursor.execute("SELECT data FROM test;")
                data = cursor.fetchone()
                assert data[0] == "some data"
        connection.close()

        juju.remove_relation(f"{app_name}:database", f"{DATA_INTEGRATOR_APP_NAME}:postgresql")

        # Re-relation again with user role and checking write data.
        juju.config(
            DATA_INTEGRATOR_APP_NAME,
            {
                "database-name": DATA_INTEGRATOR_APP_NAME.replace("-", "_"),
                "extra-user-roles": "",
            },
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_blocked, DATA_INTEGRATOR_APP_NAME), timeout=600
        )
        juju.integrate(DATA_INTEGRATOR_APP_NAME, app_name)
        juju.wait(wait_for_apps_status(jubilant.all_active, *app_names), timeout=600)

        for database in [
            DATA_INTEGRATOR_APP_NAME.replace("-", "_"),
            DATABASE_DEFAULT_NAME,
        ]:
            logger.info(f"connecting to the following database: {database}")
            connection_string = build_connection_string(
                juju, DATA_INTEGRATOR_APP_NAME, "postgresql", substrate, database=database
            )
            connection = None
            should_fail = database == DATABASE_DEFAULT_NAME
            try:
                with (
                    psycopg2.connect(connection_string) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT data FROM test;")
                    data = cursor.fetchone()
                    assert data[0] == "some data"

                    # Write some data (it should fail in the default database name).
                    random_name = f"test_{''.join(secrets.choice(string.ascii_lowercase) for _ in range(10))}"
                    cursor.execute(f"CREATE TABLE {random_name}(data TEXT);")
                    if should_fail:
                        assert False, (
                            f"failed to run a statement in the following database: {database}"
                        )
            except psycopg2.errors.InsufficientPrivilege as e:
                if not should_fail:
                    logger.exception(e)
                    assert False, (
                        f"failed to connect to or run a statement in the following database: {database}"
                    )
            except psycopg2.OperationalError as e:
                if not should_fail:
                    logger.exception(e)
            finally:
                if connection is not None:
                    connection.close()
