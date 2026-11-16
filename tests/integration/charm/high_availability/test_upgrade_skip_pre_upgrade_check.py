# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest
from jubilant import Juju

from tests.integration.charm.helpers import (
    check_db_units_writes_increment,
    count_switchovers,
    run_upgrade,
)
from tests.integration.helpers.juju import get_app_units, wait_for_apps_status
from tests.integration.types import Substrate

DB_TEST_APP_NAME = "postgresql-test-app"

MINUTE_SECS = 60

logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def test_deploy_stable(juju: Juju, app_name: str, substrate: Substrate) -> None:
    """Simple test to ensure that the PostgreSQL and application charms get deployed."""
    logging.info("Deploying PostgreSQL cluster")
    juju.deploy(
        charm=app_name,
        app=app_name,
        base="ubuntu@24.04",
        channel="16/stable",
        config={"profile": "testing"},
        num_units=3,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm=DB_TEST_APP_NAME,
        app=DB_TEST_APP_NAME,
        base="ubuntu@24.04",
        channel="latest/edge",
        num_units=1,
    )

    juju.integrate(
        f"{app_name}:database",
        f"{DB_TEST_APP_NAME}:database",
    )

    logging.info("Wait for applications to become active")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name, DB_TEST_APP_NAME),
        timeout=40 * MINUTE_SECS,
    )


def test_refresh_without_pre_refresh_check(
    juju: Juju,
    app_name: str,
    charm: str,
    charm_resources,
    substrate: Substrate,
    continuous_writes,
) -> None:
    """Test updating from stable channel."""
    initial_number_of_switchovers = count_switchovers(juju, app_name)

    run_upgrade(juju, app_name, charm, substrate, charm_resources)

    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, app_name)

    logging.info("checking the number of switchovers")
    final_number_of_switchovers = count_switchovers(juju, app_name)
    assert (final_number_of_switchovers - initial_number_of_switchovers) <= 2, (
        "Number of switchovers is greater than 2"
    )


def test_rollback_without_pre_refresh_check(
    juju: Juju, app_name: str, substrate: Substrate, continuous_writes
) -> None:
    """Test refresh back to stable channel."""
    if substrate == Substrate.k8s:
        pytest.skip("Unstable on K8s")

    # Early Jubilant 1.X.Y versions do not support the `switch` option
    logging.info("Refresh the charm to stable channel")
    juju.cli("refresh", "--channel=16/stable", f"--switch={app_name}", app_name)

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
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name),
        timeout=40 * MINUTE_SECS,
    )

    check_db_units_writes_increment(juju, app_name)
