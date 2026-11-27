#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from copy import deepcopy
from time import sleep

import jubilant
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

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
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status

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
            config={"profile": "testing"},
        )
        juju.deploy(
            APPLICATION_NAME,
            app=APPLICATION_NAME,
            base="ubuntu@24.04",
            channel="latest/edge",
        )

        juju.integrate(app_name, f"{APPLICATION_NAME}:database")
        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1500)


@pytest.mark.abort_on_fail
def test_promote_to_primary(juju: jubilant.Juju, app_name: str) -> None:
    """Switch the primary over to a sync standby and check the Primary label follows it."""
    database_app = juju.status().apps[app_name]
    any_unit_name = next(iter(database_app.units))
    # Active units can precede Patroni registering the synchronous standbys.
    for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(3), reraise=True):
        with attempt:
            original_roles = get_ha_cluster_roles(juju, any_unit_name)
            assert (
                len(original_roles["primaries"]) == 1 and len(original_roles["sync_standbys"]) == 2
            ), f"Expected 1 primary and 2 sync standbys, got: {original_roles}"
    old_primary = original_roles["primaries"][0]
    new_primary = original_roles["sync_standbys"][0]

    def primary_labels(status: jubilant.Status) -> dict[str, str]:
        return {
            name: unit.workload_status.message
            for name, unit in status.apps[app_name].units.items()
        }

    expected_labels = {
        name: "Primary" if name == new_primary else "" for name in database_app.units
    }

    # The update-status hook recomputes the label on its own. Keep it from firing,
    # so the label has to be moved by the switchover handling itself.
    juju.model_config({"update-status-hook-interval": "60m"})
    try:
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)
        assert primary_labels(juju.status())[old_primary] == "Primary"

        logger.info(f"Promoting {new_primary} (current primary: {old_primary})")
        run_action = juju.run(new_primary, "promote-to-primary", {"scope": "unit"}, wait=600)
        assert run_action.status == "completed"

        try:
            juju.wait(lambda status: primary_labels(status) == expected_labels, timeout=180)
        except TimeoutError:
            pytest.fail(
                f"Primary label did not move from {old_primary} to {new_primary}: "
                f"{primary_labels(juju.status())}"
            )
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)
        # The label must also stay put once the units settle.
        assert primary_labels(juju.status()) == expected_labels
    finally:
        juju.model_config({"update-status-hook-interval": "5m"})

    new_roles = get_ha_cluster_roles(juju, any_unit_name)
    assert new_roles["primaries"] == [new_primary]
    assert old_primary in new_roles["sync_standbys"]
    assert len(new_roles["sync_standbys"]) == 2
    assert len(new_roles["replicas"]) == 0


@pytest.mark.parametrize(
    "roles",
    [
        ["primaries"],
        ["sync_standbys"],
        ["primaries", "sync_standbys"],
        ["sync_standbys", "sync_standbys"],
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

    # remove-unit returns before the units disappear from Juju's status.
    juju.wait(
        lambda status: all(unit not in status.apps[app_name].units for unit in units),
        timeout=1500,
    )

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
            juju.wait(jubilant.all_agents_idle, timeout=600)

            juju.run(
                left_unit,
                "promote-to-primary",
                {"scope": "unit", "force": True},
                # Raft reinitialisation can exceed juju run's default 60s wait.
                wait=600,
            )
        except TimeoutError:
            # Check if Patroni self healed
            left_unit_status = juju.status().apps[app_name].units[left_unit]
            assert (
                left_unit_status.workload_status.current == "active"
                and left_unit_status.workload_status.message == "Primary"
            )
            logger.warning(f"Patroni self-healed without raft reinitialisation for roles {roles}")

    juju.wait(wait_for_apps_status(jubilant.all_active), timeout=600)

    logger.info("Scaling back up")
    juju.add_unit(app_name, num_units=len(roles))
    juju.wait(
        lambda status: (
            len(status.apps[app_name].units) == 3
            and wait_for_apps_status(jubilant.all_active)(status)
        ),
        timeout=1500,
    )

    are_writes_increasing(juju, units)

    # The new units can be active before Patroni registers them as sync standbys.
    for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(3), reraise=True):
        with attempt:
            new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
            assert len(new_roles["primaries"]) == 1
            assert len(new_roles["sync_standbys"]) == 2
            assert len(new_roles["replicas"]) == 0
            if "primaries" in roles:
                assert new_roles["primaries"][0] in original_roles["sync_standbys"]
            else:
                assert new_roles["primaries"][0] == original_roles["primaries"][0]

    check_writes(juju)
