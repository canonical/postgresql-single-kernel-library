#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for PostgreSQL symlinks (pgdata)."""

import logging

import jubilant
import pytest

from tests.integration.charm.pytest_operator_helpers import CHARM_BASE
from tests.integration.helpers.juju import retry_if_cli_error

logger = logging.getLogger(__name__)

UNIT_IDS = [0, 1, 2]
PGDATA_SYMLINK_PATH = "/var/lib/postgresql/16"
POSTGRESQL_LOGS_TARGET_PATH = "/var/lib/pg/logs/16/main/pg_logs"
PATRONI_LOGS_PATH = "/var/lib/pg/logs/16/main/patroni_logs"
PGBACKREST_LOGS_PATH = "/var/lib/pg/logs/16/main/pgbackrest_logs"
POSTGRESQL_LOGS_PATH = "/var/log/postgresql"
STORAGE_PATH = "/var/lib/pg/data"

pytestmark = pytest.mark.k8s_only


@pytest.mark.abort_on_fail
def test_build_and_deploy(juju: jubilant.Juju, charm, app_name: str, charm_resources):
    """Build the charm-under-test and deploy it.

    Assert on the unit status before any relations/configurations take place.
    """
    if app_name not in juju.status().apps:
        juju.deploy(
            charm,
            app_name,
            base=CHARM_BASE,
            config={"profile": "testing"},
            num_units=len(UNIT_IDS),
            resources=charm_resources,
            trust=True,
        )

    def _all_active_idle(status):
        if not jubilant.all_active(status, app_name):
            return False
        for uid in UNIT_IDS:
            unit = status.apps[app_name].units[f"{app_name}/{uid}"]
            if unit.juju_status.current != "idle":
                return False
        return True

    retry_if_cli_error(lambda: juju.wait(_all_active_idle, timeout=1500))
    status = juju.status()
    for unit_id in UNIT_IDS:
        unit_name = f"{app_name}/{unit_id}"
        assert status.apps[app_name].units[unit_name].is_active


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_pgdata_symlinks(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that symlink for pgdata is correctly created."""
    unit_name = f"{app_name}/{unit_id}"

    # Check pgdata symlink exists and points to correct location
    pgdata_symlink_check = juju.ssh(
        unit_name, "readlink", "-f", PGDATA_SYMLINK_PATH, container="postgresql"
    )
    expected_target = f"{STORAGE_PATH}/16"
    assert pgdata_symlink_check.strip() == expected_target, (
        f"Expected pgdata symlink to point to {expected_target}, got {pgdata_symlink_check.strip()}"
    )

    # Verify symlink is owned by postgres:postgres
    pgdata_owner = juju.ssh(
        unit_name, "stat", "-c", "%U:%G", PGDATA_SYMLINK_PATH, container="postgresql"
    )
    assert pgdata_owner.strip() == "postgres:postgres", (
        f"Expected pgdata symlink to be owned by postgres:postgres, got {pgdata_owner.strip()}"
    )


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_pg_wal_symlink(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that pg_wal symlink inside pgdata points to WAL directory on logs storage."""
    unit_name = f"{app_name}/{unit_id}"

    # Check pg_wal symlink exists and points to correct location
    pg_wal_symlink_path = f"{PGDATA_SYMLINK_PATH}/main/pg_wal"
    expected_target = "/var/lib/pg/logs/16/main/pg_wal"
    result = juju.ssh(unit_name, "readlink", "-f", pg_wal_symlink_path, container="postgresql")
    assert result.strip() == expected_target, (
        f"Expected pg_wal symlink to point to {expected_target}, got {result.strip()}"
    )


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_log_directories_exist(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that dedicated PostgreSQL, Patroni and pgBackRest log directories exist."""
    unit_name = f"{app_name}/{unit_id}"
    for log_path in [POSTGRESQL_LOGS_TARGET_PATH, PATRONI_LOGS_PATH, PGBACKREST_LOGS_PATH]:
        result = juju.ssh(unit_name, "stat", "-c", "%F", log_path, container="postgresql")
        assert result.strip() == "directory", f"Expected {log_path} directory to exist"


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_postgresql_log_path(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that /var/log/postgresql is a symlink pointing to the pg_logs directory."""
    unit_name = f"{app_name}/{unit_id}"
    path_type = juju.ssh(
        unit_name, "stat", "-c", "%F", POSTGRESQL_LOGS_PATH, container="postgresql"
    ).strip()
    assert path_type == "symbolic link", (
        f"Expected {POSTGRESQL_LOGS_PATH} to be a symbolic link, got: {path_type}"
    )
    target = juju.ssh(unit_name, "readlink", "-f", POSTGRESQL_LOGS_PATH, container="postgresql")
    assert target.strip() == POSTGRESQL_LOGS_TARGET_PATH, (
        f"Expected {POSTGRESQL_LOGS_PATH} to point to {POSTGRESQL_LOGS_TARGET_PATH}, "
        f"got {target.strip()}"
    )


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_patroni_log_symlink(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that /var/log/patroni is a symlink pointing to the patroni_logs directory."""
    unit_name = f"{app_name}/{unit_id}"
    symlink_path = "/var/log/patroni"
    path_type = juju.ssh(
        unit_name, "stat", "-c", "%F", symlink_path, container="postgresql"
    ).strip()
    assert path_type == "symbolic link", (
        f"Expected {symlink_path} to be a symbolic link, got: {path_type}"
    )
    target = juju.ssh(unit_name, "readlink", "-f", symlink_path, container="postgresql")
    assert target.strip() == PATRONI_LOGS_PATH, (
        f"Expected {symlink_path} to point to {PATRONI_LOGS_PATH}, got {target.strip()}"
    )


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_pgbackrest_log_symlink(juju: jubilant.Juju, unit_id: int, app_name: str):
    """Test that /var/log/pgbackrest is a symlink pointing to the pgbackrest_logs directory."""
    unit_name = f"{app_name}/{unit_id}"
    symlink_path = "/var/log/pgbackrest"
    path_type = juju.ssh(
        unit_name, "stat", "-c", "%F", symlink_path, container="postgresql"
    ).strip()
    assert path_type == "symbolic link", (
        f"Expected {symlink_path} to be a symbolic link, got: {path_type}"
    )
    target = juju.ssh(unit_name, "readlink", "-f", symlink_path, container="postgresql")
    assert target.strip() == PGBACKREST_LOGS_PATH, (
        f"Expected {symlink_path} to point to {PGBACKREST_LOGS_PATH}, got {target.strip()}"
    )
