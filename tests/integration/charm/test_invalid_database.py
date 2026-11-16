#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import DATA_INTEGRATOR_APP_NAME
from tests.integration.helpers.juju import relations
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

REQUESTED_DATABASE_NAME = "requested-database"
RELATION_ENDPOINT = "postgresql"
TIMEOUT = 15 * 60


def data_integrator_blocked(status: jubilant.Status) -> bool:
    return jubilant.all_blocked(status, DATA_INTEGRATOR_APP_NAME)


def all_units_blocked(status: jubilant.Status) -> bool:
    return all(
        unit.workload_status.current == "blocked"
        for app in status.apps
        for unit in status.get_units(app).values()
    )


@pytest.mark.abort_on_fail
def test_deploy(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy the charms."""
    # Deploy the database charm if not already deployed.
    if app_name not in juju.status().apps:
        logger.info("Deploying database charm")
        juju.deploy(
            charm,
            app=app_name,
            config={"profile": "testing"},
            num_units=1,
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )

    # Deploy the data integrator if not already deployed.
    if DATA_INTEGRATOR_APP_NAME not in juju.status().apps:
        logger.info("Deploying data integrator charm")
        juju.deploy(
            DATA_INTEGRATOR_APP_NAME,
            config={"database-name": REQUESTED_DATABASE_NAME},
        )

    # Relate the data integrator charm to the database charm.
    existing_relations = relations(juju, app_name, DATA_INTEGRATOR_APP_NAME)
    if existing_relations:
        logger.info("Removing existing relation between charms")
        juju.remove_relation(f"{DATA_INTEGRATOR_APP_NAME}:{RELATION_ENDPOINT}", app_name)
        juju.wait(data_integrator_blocked, timeout=TIMEOUT)

    logger.info(
        "Waiting for the database charm to become active and the data integrator charm to block"
    )
    juju.wait(lambda status: jubilant.all_active(status, app_name), timeout=TIMEOUT)
    juju.wait(data_integrator_blocked, timeout=TIMEOUT)


def test_database(juju: jubilant.Juju, app_name: str) -> None:
    """Check that an invalid database name makes the database charm block."""
    invalid_database_names = [
        "databases",
        "postgres",
        "template0",
        "template1",
    ]
    logger.info(f"Invalid database names: {invalid_database_names}")

    for invalid_database_name in invalid_database_names:
        logger.info(f"Requesting invalid database name: {invalid_database_name}")
        juju.config(
            app=DATA_INTEGRATOR_APP_NAME,
            values={
                "database-name": invalid_database_name,
            },
        )
        juju.wait(data_integrator_blocked, timeout=TIMEOUT)

        logger.info("Adding relation between charms")
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5)):
            with attempt:
                juju.integrate(DATA_INTEGRATOR_APP_NAME, app_name)

        logger.info("Waiting for the database charm to block due to invalid database name")
        juju.wait(all_units_blocked)
        assert (
            juju.status().get_units(app_name)[f"{app_name}/0"].workload_status.message
            == "invalid database name"
        ), "The database charm didn't block as expected due to invalid database name."

        logger.info("Removing relation between charms")
        juju.remove_relation(f"{DATA_INTEGRATOR_APP_NAME}:{RELATION_ENDPOINT}", app_name)

        logger.info("Waiting for the database charm to become active again")
        juju.wait(lambda status: jubilant.all_active(status, app_name), timeout=TIMEOUT)
        juju.wait(data_integrator_blocked, timeout=TIMEOUT)
