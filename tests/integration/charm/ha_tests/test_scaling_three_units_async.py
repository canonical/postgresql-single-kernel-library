#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from copy import deepcopy
from time import sleep

import jubilant
import pytest

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    check_writes,
    get_ha_cluster_roles,
    get_machine_from_unit,
    start_ha_continuous_writes,
    stop_machine,
)
from tests.integration.helpers.juju import fast_forward, wait_for_idle

logger = logging.getLogger(__name__)

charm = None

pytestmark = pytest.mark.vm_only


@pytest.mark.abort_on_fail
def test_build_and_deploy(juju: jubilant.Juju, charm, app_name: str) -> None:
    """Build and deploy two PostgreSQL clusters."""
    # This is a potentially destructive test, so it shouldn't be run against existing clusters
    with fast_forward(juju):
        # Deploy the first cluster with reusable storage
        juju.deploy(
            charm,
            app=app_name,
            num_units=3,
            base=CHARM_BASE,
            config={"profile": "testing", "synchronous_node_count": "majority"},
        )
        juju.deploy(
            APPLICATION_NAME,
            app=APPLICATION_NAME,
            base="ubuntu@24.04",
            channel="latest/edge",
        )

        juju.integrate(app_name, f"{APPLICATION_NAME}:database")
        wait_for_idle(juju, status="active", timeout=1500)


@pytest.mark.parametrize(
    "roles",
    [
        ["primaries"],
        ["sync_standbys"],
        ["replicas"],
        ["primaries", "replicas"],
        ["sync_standbys", "replicas"],
    ],
)
@pytest.mark.abort_on_fail
def test_removing_unit(
    juju: jubilant.Juju, roles: list[str], juju_continuous_writes, app_name: str
) -> None:
    logger.info(f"removing {', '.join(roles)}")
    # Start an application that continuously writes data to the database.
    app = app_name
    original_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    copied_roles = deepcopy(original_roles)
    start_ha_continuous_writes(juju, app)
    units = [copied_roles[role].pop(0) for role in roles]
    for unit in units:
        logger.info(f"Stopping unit {unit}")
        stop_machine(get_machine_from_unit(juju, unit))
    sleep(15)
    for unit in units:
        logger.info(f"Deleting unit {unit}")
        juju.remove_unit(unit, force=True, destroy_storage=False)

    if len(roles) > 1:
        for left_unit in juju.status().apps[app_name].units:
            if left_unit not in units:
                break
        try:
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
            wait_for_idle(juju, timeout=600, idle_period=45)

            juju.run(
                next(iter(juju.status().apps[app_name].units)),
                "promote-to-primary",
                {"scope": "unit", "force": True},
            )
        except TimeoutError:
            # Check if Patroni self healed
            left_unit_status = juju.status().apps[app_name].units[left_unit]
            assert (
                left_unit_status.workload_status.current == "active"
                and left_unit_status.workload_status.message == "Primary"
            )
            logger.warning(f"Patroni self-healed without raft reinitialisation for roles {roles}")

    wait_for_idle(juju, status="active", timeout=600, idle_period=45)

    logger.info("Scaling back up")
    juju.add_unit(app_name, num_units=len(roles))
    wait_for_idle(juju, status="active", timeout=1500)

    are_writes_increasing(juju, units)

    new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
    assert len(new_roles["primaries"]) == 1
    assert len(new_roles["sync_standbys"]) == 1
    assert len(new_roles["replicas"]) == 1
    if "primaries" in roles:
        assert new_roles["primaries"][0] in original_roles["sync_standbys"]
    else:
        assert new_roles["primaries"][0] == original_roles["primaries"][0]

    check_writes(juju)
