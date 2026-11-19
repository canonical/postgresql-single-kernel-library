#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import jubilant
import pytest
from jubilant import Juju

from tests.integration.types import Substrate

TEST_CHARMS_PATH = Path(__file__).parents[2] / "charms"


def fetch_charm(substrate: Substrate, architecture: str) -> str:
    """Fetch the packed test charm for a specific architecture, ignoring the host one."""
    charm_dir = TEST_CHARMS_PATH / f"postgresql_{substrate}_test_charm"
    packed_charms = list(charm_dir.glob(f"*-{architecture}.charm"))
    if not packed_charms:
        pytest.fail(
            f"No {architecture} charm found in {charm_dir}, build it with:"
            f" scripts/build_charm.sh -p ubuntu@24.04:{architecture}"
            f" -c tests/charms/postgresql_{substrate}_test_charm"
        )
    return str(packed_charms[0].resolve(strict=True))


def _unit_is_blocked(status: jubilant.Status, app_name: str) -> bool:
    """Return True once the deployed unit reports a blocked workload status."""
    units = status.get_units(app_name)
    return bool(units) and all(
        unit.workload_status.current == "blocked" for unit in units.values()
    )


def _deploy_and_wait_for_blocked(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    juju.deploy(
        charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    # The wrong-architecture charm sets BlockedStatus and exits on every hook, so
    # it never converges to a fully idle state (less so under slow CI machine
    # provisioning). Wait for the blocked status directly rather than for idle.
    juju.wait(lambda status: _unit_is_blocked(status, app_name), timeout=20 * 60)


def test_arm_charm_on_amd_host(
    juju: Juju, app_name: str, charm_resources, architecture: str, substrate: Substrate
) -> None:
    """Try deploying an arm64 charm on an amd64 host."""
    if architecture != "amd64":
        pytest.skip("Requires amd64 architecture")
    charm = fetch_charm(substrate, "arm64")
    _deploy_and_wait_for_blocked(juju, charm, app_name, charm_resources, substrate)


def test_amd_charm_on_arm_host(
    juju: Juju, app_name: str, charm_resources, architecture: str, substrate: Substrate
) -> None:
    """Try deploying an amd64 charm on an arm64 host."""
    if architecture != "arm64":
        pytest.skip("Requires arm64 architecture")
    charm = fetch_charm(substrate, "amd64")
    _deploy_and_wait_for_blocked(juju, charm, app_name, charm_resources, substrate)
