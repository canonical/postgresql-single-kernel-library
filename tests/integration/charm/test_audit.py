#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import psycopg2
import pytest
from jubilant import Juju
from single_kernel_postgresql.config import literals
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import build_connection_string, run_command_on_unit
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

APPLICATION_NAME = "postgresql-test-app"
RELATION_ENDPOINT = "database"

# Directory of the PostgreSQL logs on each substrate.
VM_POSTGRESQL_LOGS_DIR = f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_LOGS_PATH}"
K8S_POSTGRESQL_LOGS_DIR = f"/{literals.K8S_LOGS_STORAGE_PATH}/{literals.K8S_PG_LOGS_PATH}"


@pytest.mark.abort_on_fail
def test_audit_plugin(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Test the audit plugin."""
    juju.deploy(
        charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(APPLICATION_NAME, base="ubuntu@24.04", channel="latest/edge")
    juju.integrate(f"{APPLICATION_NAME}:{RELATION_ENDPOINT}", app_name)
    with fast_forward(juju):
        juju.wait(
            wait_for_apps_status(jubilant.all_active, APPLICATION_NAME, app_name),
            error=jubilant.any_error,
            timeout=1500,
            successes=30,
        )

    logger.info("Checking that the audit plugin is enabled")
    connection_string = build_connection_string(
        juju, APPLICATION_NAME, RELATION_ENDPOINT, substrate
    )
    connection = None
    try:
        connection = psycopg2.connect(connection_string)
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE test2(value TEXT);")
            cursor.execute("GRANT SELECT ON test2 TO PUBLIC;")
            cursor.execute("SET TIME ZONE 'Europe/Rome';")
    finally:
        if connection is not None:
            connection.close()
    unit_name = f"{app_name}/0"
    logs_dir = VM_POSTGRESQL_LOGS_DIR if substrate == Substrate.vm else K8S_POSTGRESQL_LOGS_DIR
    for attempt in Retrying(stop=stop_after_delay(90), wait=wait_fixed(10), reraise=True):
        with attempt:
            try:
                logs = run_command_on_unit(
                    juju, unit_name, f"grep AUDIT {logs_dir}/postgresql-*.log", substrate
                )
                assert "MISC,BEGIN,,,BEGIN" in logs
                assert (
                    "DDL,CREATE TABLE,TABLE,public.test2,CREATE TABLE test2(value TEXT);" in logs
                )
                assert "ROLE,GRANT,TABLE,,GRANT SELECT ON test2 TO PUBLIC;" in logs
                assert "MISC,SET,,,SET TIME ZONE 'Europe/Rome';" in logs
            except Exception:
                assert False, "Audit logs were not found when the plugin is enabled."

    logger.info("Disabling the audit plugin")
    juju.config(app_name, {"plugin-audit-enable": False})
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=600,
        successes=15,
    )

    logger.info("Removing the previous logs")
    run_command_on_unit(juju, unit_name, f"rm {logs_dir}/postgresql-*.log", substrate)

    logger.info("Checking that the audit plugin is disabled")
    try:
        connection = psycopg2.connect(connection_string)
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE test1(value TEXT);")
            cursor.execute("GRANT SELECT ON test1 TO PUBLIC;")
            cursor.execute("SET TIME ZONE 'Europe/Rome';")
    finally:
        if connection is not None:
            connection.close()
    try:
        logs = run_command_on_unit(
            juju, unit_name, f"grep AUDIT {logs_dir}/postgresql-*.log", substrate
        )
    except Exception:
        pass
    else:
        logger.info(f"Logs: {logs}")
        assert False, "Audit logs were found when the plugin is disabled."
