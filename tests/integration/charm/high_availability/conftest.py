#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest
from tenacity import Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from tests.integration.charm import helpers
from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    change_ha_patroni_setting,
    get_postgresql_app_name,
)
from tests.integration.helpers.juju import get_app_leader

logger = logging.getLogger(__name__)

DB_TEST_APP_NAME = "postgresql-test-app"


@pytest.fixture()
def juju_continuous_writes(juju: jubilant.Juju) -> None:
    """Clear continuous writes at the end, like the async HA fixture."""
    yield
    for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3), reraise=True):
        with attempt:
            unit = next(iter(juju.status().apps[APPLICATION_NAME].units))
            action = juju.run(unit, "clear-continuous-writes")
            assert action.results["result"] == "True", "Unable to clear up continuous_writes table"


@pytest.fixture(scope="module")
def juju_primary_start_timeout(juju: jubilant.Juju) -> None:
    """Set and restore primary_start_timeout exactly like the async HA fixture."""
    patroni_password = helpers.get_password(juju, get_postgresql_app_name(juju), "patroni")
    initial_primary_start_timeout = helpers.get_patroni_setting(juju, "primary_start_timeout")
    change_ha_patroni_setting(juju, "primary_start_timeout", 0, patroni_password)
    yield
    change_ha_patroni_setting(
        juju,
        "primary_start_timeout",
        initial_primary_start_timeout,
        patroni_password,
        use_random_unit=True,
    )


@pytest.fixture()
def continuous_writes(juju):
    """Starts continuous writes to the cluster for a test and clear the writes at the end."""
    application_unit = get_app_leader(juju, DB_TEST_APP_NAME)

    logger.info("Clearing continuous writes")
    juju.run(unit=application_unit, action="clear-continuous-writes", wait=120).raise_on_failure()

    logger.info("Starting continuous writes")

    for attempt in Retrying(stop=stop_after_attempt(10), reraise=True):
        with attempt:
            result = juju.run(unit=application_unit, action="start-continuous-writes")
            result.raise_on_failure()

            assert result.results["result"] == "True"

    yield

    logger.info("Clearing continuous writes")
    juju.run(unit=application_unit, action="clear-continuous-writes", wait=120).raise_on_failure()
