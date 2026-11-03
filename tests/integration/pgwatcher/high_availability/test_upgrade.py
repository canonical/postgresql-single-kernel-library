# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import platform
import shutil
import zipfile
from pathlib import Path

import jubilant
import tomli
import tomli_w
from jubilant import Juju

from .high_availability_helpers_new import (
    check_db_units_writes_increment,
    get_app_leader,
    get_app_units,
    verify_raft_cluster_health,
    wait_for_apps_status,
)

DB_APP_NAME = "postgresql"
WATCHER_APP_NAME = "postgresql-watcher"
DB_TEST_APP_NAME = "postgresql-test-app"

MINUTE_SECS = 60

logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def test_deploy_latest(juju: Juju) -> None:
    """Simple test to ensure that the PostgreSQL and application charms get deployed."""
    logging.info("Deploying PostgreSQL cluster")
    juju.deploy(
        charm=DB_APP_NAME,
        app=DB_APP_NAME,
        base="ubuntu@24.04",
        channel="16/edge",
        config={"profile": "testing", "synchronous-mode-strict": False},
        num_units=2,
    )
    juju.deploy(
        charm=WATCHER_APP_NAME,
        app=WATCHER_APP_NAME,
        base="ubuntu@24.04",
        channel="16/edge",
        config={"profile": "testing"},
        num_units=1,
    )
    juju.deploy(
        charm=DB_TEST_APP_NAME,
        app=DB_TEST_APP_NAME,
        base="ubuntu@24.04",
        channel="latest/edge",
        num_units=1,
    )

    juju.integrate(
        f"{DB_APP_NAME}:watcher-offer",
        f"{WATCHER_APP_NAME}:watcher",
    )
    juju.integrate(
        f"{DB_APP_NAME}:database",
        f"{DB_TEST_APP_NAME}:database",
    )

    logging.info("Wait for applications to become active")
    juju.wait(
        ready=wait_for_apps_status(
            jubilant.all_active, DB_APP_NAME, DB_TEST_APP_NAME, WATCHER_APP_NAME
        ),
        timeout=20 * MINUTE_SECS,
    )


def test_pre_refresh_check(juju: Juju) -> None:
    """Test that the pre-refresh-check action runs successfully."""
    watcher_leader = get_app_leader(juju, WATCHER_APP_NAME)

    logging.info("Run pre-refresh-check action")
    juju.run(unit=watcher_leader, action="pre-refresh-check")

    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)


def test_upgrade_from_edge(juju: Juju, charm: str, continuous_writes) -> None:
    """Update the second cluster."""
    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, DB_APP_NAME)

    logging.info("Refresh the charm")
    juju.refresh(app=WATCHER_APP_NAME, path=charm)
    logging.info("Wait for refresh to block as paused or incompatible")
    try:
        juju.wait(lambda status: status.apps[WATCHER_APP_NAME].is_blocked, timeout=5 * MINUTE_SECS)

        units = get_app_units(juju, WATCHER_APP_NAME)
        unit_names = sorted(units.keys())

        if "Refresh incompatible" in juju.status().apps[WATCHER_APP_NAME].app_status.message:
            logging.info("Application refresh is blocked due to incompatibility")
            juju.run(
                unit=unit_names[-1],
                action="force-refresh-start",
                params={"check-compatibility": False},
                wait=5 * MINUTE_SECS,
            )

        juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)
    except TimeoutError:
        logging.info("Upgrade completed without snap refresh (charm.py upgrade only)")
        assert juju.status().apps[WATCHER_APP_NAME].is_active

    logging.info("Wait for upgrade to complete")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, WATCHER_APP_NAME), timeout=20 * MINUTE_SECS
    )

    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, DB_APP_NAME)
    verify_raft_cluster_health(juju, DB_APP_NAME, WATCHER_APP_NAME)


def test_fail_and_rollback(juju: Juju, charm: str, continuous_writes) -> None:
    """Test an upgrade failure and its rollback."""
    watcher_app_leader = get_app_leader(juju, WATCHER_APP_NAME)

    logging.info("Run pre-refresh-check action")
    juju.run(unit=watcher_app_leader, action="pre-refresh-check")

    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    tmp_folder = Path("tmp")
    tmp_folder.mkdir(exist_ok=True)
    tmp_folder_charm = Path(tmp_folder, charm).absolute()

    shutil.copy(charm, tmp_folder_charm)

    logging.info("Inject dependency fault")
    inject_dependency_fault(juju, WATCHER_APP_NAME, tmp_folder_charm)

    logging.info("Refresh the charm")
    juju.refresh(app=WATCHER_APP_NAME, path=tmp_folder_charm)

    logging.info("Wait for upgrade to fail on leader")
    juju.wait(
        ready=wait_for_apps_status(jubilant.any_blocked, WATCHER_APP_NAME),
        timeout=10 * MINUTE_SECS,
    )

    logging.info("Ensure continuous writes on all units")
    check_db_units_writes_increment(juju, DB_APP_NAME)

    logging.info("Re-refresh the charm")
    juju.refresh(app=WATCHER_APP_NAME, path=charm)

    logging.info("Wait for upgrade to complete")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, WATCHER_APP_NAME), timeout=20 * MINUTE_SECS
    )

    logging.info("Ensure continuous writes after rollback procedure")
    check_db_units_writes_increment(juju, DB_APP_NAME)
    verify_raft_cluster_health(juju, DB_APP_NAME, WATCHER_APP_NAME)

    # Remove fault charm file
    tmp_folder_charm.unlink()


def inject_dependency_fault(juju: Juju, app_name: str, charm_file: str | Path) -> None:
    """Inject a dependency fault into the PostgreSQL charm."""
    with Path("refresh_versions.toml").open("rb") as file:
        versions = tomli.load(file)

    versions["charm"] = "16/0.0.0"
    versions["snap"]["revisions"][platform.machine()] = "1"

    # Overwrite refresh_versions.toml with incompatible version.
    with zipfile.ZipFile(charm_file, mode="a") as charm_zip:
        charm_zip.writestr("refresh_versions.toml", tomli_w.dumps(versions))
