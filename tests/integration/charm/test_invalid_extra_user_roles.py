#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from itertools import combinations

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import DATA_INTEGRATOR_APP_NAME
from tests.integration.helpers.juju import relations, retry_if_cli_error
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


def test_extra_user_roles(
    juju: jubilant.Juju, app_name: str, predefined_roles, predefined_roles_combinations
) -> None:
    """Check that invalid extra user roles make the database charm block."""
    # Remove the empty role (no extra user roles, i.e., regular relation user).
    del predefined_roles[""]
    invalid_extra_user_roles_combinations = [
        ("backup",),
        ("charmed_backup",),
        ("charmed_dba",),
        ("invalid",),
        ("invalid", "invalid"),
        ("monitoring",),
        ("postgres",),
        ("pg_monitor",),
        ("replication",),
        ("rewind",),
    ]
    invalid_extra_user_roles_combinations.extend([
        combination
        for combination in combinations(predefined_roles.keys(), 2)
        if combination not in predefined_roles_combinations
    ])
    logger.info(f"Invalid combinations: {invalid_extra_user_roles_combinations}")

    for invalid_extra_user_roles_combination in invalid_extra_user_roles_combinations:
        logger.info(
            f"Requesting invalid extra user roles combination: {', '.join(invalid_extra_user_roles_combination)}"
        )
        juju.config(
            app=DATA_INTEGRATOR_APP_NAME,
            values={
                "extra-user-roles": ",".join(invalid_extra_user_roles_combination),
            },
        )
        juju.wait(data_integrator_blocked, timeout=TIMEOUT)

        logger.info("Adding relation between charms")
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(5)):
            with attempt:
                juju.integrate(DATA_INTEGRATOR_APP_NAME, app_name)

        logger.info("Waiting for the database charm to block due to invalid extra user roles")
        juju.wait(all_units_blocked)
        assert (
            juju.status().get_units(app_name)[f"{app_name}/0"].workload_status.message
            == "invalid role(s) for extra user roles"
        ), "The database charm didn't block as expected due to invalid extra user roles."

        logger.info("Removing relation between charms")
        juju.remove_relation(f"{DATA_INTEGRATOR_APP_NAME}:{RELATION_ENDPOINT}", app_name)

        logger.info("Waiting for the database charm to become active again")
        retry_if_cli_error(
            lambda: juju.wait(
                lambda status: jubilant.all_active(status, app_name), timeout=TIMEOUT
            )
        )
        juju.wait(data_integrator_blocked, timeout=TIMEOUT)
