#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    check_writes,
    get_ha_cluster_roles,
    get_primary_from_app,
    start_ha_continuous_writes,
)
from tests.integration.helpers.juju import fast_forward, wait_for_idle

logger = logging.getLogger(__name__)

charm = None

pytestmark = pytest.mark.vm_only


class ClusterNotReadyError(Exception):
    """The Patroni cluster view is not coherent enough to sample roles."""


@pytest.mark.abort_on_fail
def test_build_and_deploy(juju: jubilant.Juju, charm, app_name: str) -> None:
    """Build and deploy two PostgreSQL clusters."""
    # This is a potentially destructive test, so it shouldn't be run against existing clusters
    with fast_forward(juju):
        # Deploy the first cluster with reusable storage
        juju.deploy(
            charm,
            app=app_name,
            num_units=2,
            base=CHARM_BASE,
            config={"profile": "testing"},
        )
        juju.deploy(
            APPLICATION_NAME,
            app=APPLICATION_NAME,
            base="ubuntu@24.04",
            channel="latest/edge",
        )

        juju.integrate(app_name, f"{APPLICATION_NAME}:database")
        wait_for_idle(juju, status="active", timeout=1800)


@pytest.mark.abort_on_fail
def test_removing_stereo_primary(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    # Start an application that continuously writes data to the database.
    app = app_name
    # wait_for_idle can pass while a standby is still creating its replica.
    original_roles = None
    try:
        for attempt in Retrying(stop=stop_after_delay(60 * 10), wait=wait_fixed(10)):
            with attempt:
                original_roles = get_ha_cluster_roles(
                    juju, next(iter(juju.status().apps[app_name].units))
                )
                if not original_roles["sync_standbys"]:
                    raise ClusterNotReadyError()
    except RetryError:
        pytest.fail("no sync_standby member appeared within 10 minutes of deployment")
    start_ha_continuous_writes(juju, app)
    logger.info("Deleting primary")
    primary = get_primary_from_app(juju, app)
    juju.remove_unit(primary, force=True, destroy_storage=False)
    # libjuju's destroy_unit(max_wait=1500) blocked until the unit was gone; jubilant doesn't.
    juju.wait(lambda status: primary not in status.apps[app_name].units, timeout=1500)

    left_unit = original_roles["sync_standbys"][0]
    for left_unit in juju.status().apps[app_name].units:
        if left_unit not in original_roles["primaries"]:
            break

    juju.wait(
        lambda status: (
            status.apps[app_name].units[left_unit].workload_status.current == "blocked"
            and status.apps[app_name].units[left_unit].workload_status.message
            == "Raft majority loss, run: promote-to-primary"
        ),
        timeout=600,
        delay=0.5,
        successes=1,
    )

    # Run on the surviving unit: the first unit in status may still be the removed primary,
    # leaving the action pending forever on a dead machine.
    # Raft reinitialisation can exceed juju run's default 60s wait; libjuju waited indefinitely.
    juju.run(left_unit, "promote-to-primary", {"scope": "unit", "force": True}, wait=600)

    wait_for_idle(juju, status="active", timeout=600, idle_period=45)

    logger.info("Scaling back up")
    juju.add_unit(app_name, num_units=1)
    wait_for_idle(juju, status="active", timeout=1500)

    are_writes_increasing(juju, primary)

    new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    assert len(new_roles["primaries"]) == 1
    assert len(new_roles["sync_standbys"]) == 1
    assert new_roles["primaries"][0] == original_roles["sync_standbys"][0]

    check_writes(juju)


@pytest.mark.abort_on_fail
def test_removing_stereo_sync_standby(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    # Start an application that continuously writes data to the database.
    app = app_name
    original_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    start_ha_continuous_writes(juju, app)
    logger.info("Deleting sync replica")
    primary = get_primary_from_app(juju, app)
    secondary = next(filter(lambda name: name != primary, juju.status().apps[app_name].units))
    juju.remove_unit(secondary, force=True, destroy_storage=False)
    # Wait for the unit to disappear, as libjuju's destroy_unit(max_wait=1500) did.
    juju.wait(lambda status: secondary not in status.apps[app_name].units, timeout=1500)

    wait_for_idle(juju, status="active", timeout=600, idle_period=45)

    logger.info("Scaling back up")
    juju.add_unit(app_name, num_units=1)
    wait_for_idle(juju, status="active", timeout=1500)

    are_writes_increasing(juju, primary)

    new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    assert len(new_roles["primaries"]) == 1
    assert len(new_roles["sync_standbys"]) == 1
    assert new_roles["primaries"][0] == original_roles["primaries"][0]

    check_writes(juju)


@pytest.mark.abort_on_fail
def test_scale_to_five_units(juju: jubilant.Juju, app_name: str) -> None:
    juju.add_unit(app_name, num_units=3)
    wait_for_idle(juju, status="active", timeout=2000)


@pytest.mark.abort_on_fail
def test_removing_raft_majority(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    # Start an application that continuously writes data to the database.
    app = app_name
    original_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))

    start_ha_continuous_writes(juju, app)
    logger.info("Deleting primary")
    juju.remove_unit(
        original_roles["primaries"][0],
        original_roles["sync_standbys"][0],
        original_roles["sync_standbys"][1],
        force=True,
        destroy_storage=False,
    )

    left_unit = original_roles["sync_standbys"][2]
    juju.wait(
        lambda status: (
            status.apps[app_name].units[left_unit].workload_status.current == "blocked"
            and status.apps[app_name].units[left_unit].workload_status.message
            == "Raft majority loss, run: promote-to-primary"
        ),
        timeout=600,
        delay=0.5,
        successes=1,
    )

    # Raft reinitialisation can exceed juju run's default 60s wait; libjuju waited indefinitely.
    juju.run(left_unit, "promote-to-primary", {"scope": "unit", "force": True}, wait=600)

    wait_for_idle(juju, status="active", timeout=900, idle_period=45)

    are_writes_increasing(
        juju,
        [
            original_roles["primaries"][0],
            original_roles["sync_standbys"][0],
            original_roles["sync_standbys"][1],
        ],
    )

    logger.info("Scaling back up")
    juju.add_unit(app_name, num_units=3)
    wait_for_idle(juju, status="active", timeout=2000)

    check_writes(juju)
    new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    assert len(new_roles["primaries"]) == 1
    assert len(new_roles["sync_standbys"]) == 4
    assert new_roles["primaries"][0] == original_roles["sync_standbys"][2]
