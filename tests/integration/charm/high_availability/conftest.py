#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from concurrent.futures import ThreadPoolExecutor

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from tests.integration.charm import helpers
from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    change_ha_patroni_setting,
    get_postgresql_app_name,
)
from tests.integration.helpers.juju import fast_forward, get_app_leader, wait_for_idle

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
def juju_restart_policy(juju: jubilant.Juju, substrate: Substrates) -> None:
    """Set the same Pebble restart policy on all units (K8s only)."""
    if substrate == Substrates.VM:
        yield
        return

    app = helpers.get_postgresql_app_name(juju)
    for unit in juju.status().apps[app].units:
        helpers.modify_pebble_restart_delay(
            juju,
            unit,
            "tests/integration/charm/ha_tests/manifests/extend_pebble_restart_delay.yml",
        )
        with fast_forward(juju):
            wait_for_idle(
                juju,
                apps=[app],
                status="active",
                raise_on_blocked=True,
                timeout=5 * 60,
                idle_period=30,
            )
    yield


@pytest.fixture()
def juju_loop_wait(juju: jubilant.Juju) -> None:
    """Restore the original loop_wait configuration after the test."""
    initial_loop_wait = helpers.get_patroni_setting(juju, "loop_wait")
    yield
    patroni_password = helpers.get_password(juju, helpers.get_postgresql_app_name(juju), "patroni")
    helpers.change_ha_patroni_setting(
        juju, "loop_wait", initial_loop_wait, patroni_password, use_random_unit=True
    )


@pytest.fixture()
def juju_reset_restart_condition(juju: jubilant.Juju, substrate: Substrates):
    """Set and restore the systemd restart condition concurrently on all VM units."""
    if substrate == Substrates.K8S:
        yield
        return

    app = helpers.get_postgresql_app_name(juju)
    units = list(juju.status().apps[app].units)
    with ThreadPoolExecutor(max_workers=len(units)) as executor:
        futures = [
            executor.submit(
                helpers.update_restart_condition, juju, unit, helpers.RESTART_CONDITION
            )
            for unit in units
        ]
        for future in futures:
            future.result()

    yield

    units = list(juju.status().apps[app].units)
    with ThreadPoolExecutor(max_workers=len(units)) as executor:
        futures = [
            executor.submit(
                helpers.update_restart_condition, juju, unit, helpers.ORIGINAL_RESTART_CONDITION
            )
            for unit in units
        ]
        for future in futures:
            future.result()


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


@pytest.fixture()
def juju_chaos_mesh(juju: jubilant.Juju, substrate: Substrates) -> None:
    """Deploy and remove Chaos Mesh exactly like the async fixture (K8s only)."""
    if substrate == Substrates.VM:
        yield
        return

    helpers.deploy_chaos_mesh(juju.status().model.name)
    yield
    helpers.remove_instance_isolation(juju)
    helpers.destroy_chaos_mesh(juju.status().model.name)


@pytest.fixture()
def juju_wal_settings(juju: jubilant.Juju, substrate: Substrates) -> None:
    """Restore the same WAL settings on each unit as the async fixture."""
    initial_max_wal_size = helpers.get_postgresql_parameter(juju, "max_wal_size")
    initial_min_wal_size = helpers.get_postgresql_parameter(juju, "min_wal_size")
    initial_wal_keep_segments = helpers.get_postgresql_parameter(juju, "wal_keep_segments")
    yield
    app = helpers.get_postgresql_app_name(juju)
    for unit in juju.status().apps[app].units:
        if substrate == Substrates.VM:
            helpers.run_ha_command_on_unit(juju, unit, "snap start charmed-postgresql.patroni")
        patroni_password = helpers.get_password(
            juju, helpers.get_postgresql_app_name(juju), "patroni"
        )
        helpers.change_wal_settings(
            juju,
            unit,
            initial_max_wal_size,
            initial_min_wal_size,
            initial_wal_keep_segments,
            patroni_password,
        )
