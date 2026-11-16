# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import requests
from jubilant import Juju

from tests.integration.charm.helpers import (
    check_db_units_writes_increment,
    count_switchovers,
    run_upgrade,
)
from tests.integration.helpers.juju import (
    get_app_leader,
    get_unit_address,
    wait_for_apps_status,
)
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
        timeout=20 * MINUTE_SECS,
    )


def test_pre_refresh_check(juju: Juju, app_name: str) -> None:
    """Test that the pre-refresh-check action runs successfully."""
    db_leader = get_app_leader(juju, app_name)

    logging.info("Run pre-refresh-check action")
    juju.run(unit=db_leader, action="pre-refresh-check", wait=5 * MINUTE_SECS)

    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)


def test_upgrade_from_stable(
    juju: Juju,
    app_name: str,
    charm: str,
    charm_resources,
    substrate: Substrate,
    continuous_writes,
) -> None:
    """Update the second cluster."""
    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, app_name)

    initial_number_of_switchovers = count_switchovers(juju, app_name)

    run_upgrade(juju, app_name, charm, substrate, charm_resources)

    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, app_name)

    logging.info("checking the number of switchovers")
    final_number_of_switchovers = count_switchovers(juju, app_name)
    assert (final_number_of_switchovers - initial_number_of_switchovers) <= 2, (
        "Number of switchovers is greater than 2"
    )

    leader_ip = get_unit_address(juju, get_app_leader(juju, app_name))
    config = requests.get(f"https://{leader_ip}:8008/config", verify=False).json()
    assert config.get("max_timelines_history") == 50
