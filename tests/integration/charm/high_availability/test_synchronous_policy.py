#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.charm.helpers import CHARM_BASE, get_cluster_roles
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju, charm: str, app_name: str, charm_resources, substrate: Substrates
) -> None:
    """Build and deploy three units of PostgreSQL."""
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            num_units=3,
            base=CHARM_BASE,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrates.K8S,
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=1500)


@pytest.mark.abort_on_fail
@pytest.mark.k8s_only
def test_promote_to_primary(juju: jubilant.Juju, app_name: str) -> None:
    """Switch the primary over to a sync standby and check the Primary label follows it."""
    database_app = juju.status().apps[app_name]
    any_unit_name = next(iter(database_app.units))
    original_roles = get_cluster_roles(juju, any_unit_name)
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

    new_roles = get_cluster_roles(juju, any_unit_name)
    assert new_roles["primaries"] == [new_primary]
    assert old_primary in new_roles["sync_standbys"]
    assert len(new_roles["sync_standbys"]) == 2
    assert len(new_roles["replicas"]) == 0


def test_default_all(juju: jubilant.Juju, app_name: str) -> None:
    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0


def test_majority(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, {"synchronous-node-count": "majority"})

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 1
            assert len(roles["replicas"]) == 1


def test_constant(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, {"synchronous-node-count": "2"})

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0
