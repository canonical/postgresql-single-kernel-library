#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.charm.ha_tests.helpers import get_cluster_roles
from tests.integration.charm.pytest_operator_helpers import CHARM_BASE
from tests.integration.types import Substrate


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Build and deploy three units of PostgreSQL."""
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            application_name=app_name,
            num_units=3,
            base=CHARM_BASE,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )
        await ops_test.model.wait_for_idle(status="active", timeout=1500)


async def test_default_all(ops_test: OpsTest, app_name: str) -> None:
    app = app_name

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = await get_cluster_roles(
                ops_test, ops_test.model.applications[app].units[0].name
            )

            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0


async def test_majority(ops_test: OpsTest, app_name: str) -> None:
    app = app_name

    await ops_test.model.applications[app].set_config({"synchronous-node-count": "majority"})

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active")

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = await get_cluster_roles(
                ops_test, ops_test.model.applications[app].units[0].name
            )

            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 1
            assert len(roles["replicas"]) == 1


async def test_constant(ops_test: OpsTest, app_name: str) -> None:
    app = app_name

    await ops_test.model.applications[app].set_config({"synchronous-node-count": "2"})

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = await get_cluster_roles(
                ops_test, ops_test.model.applications[app].units[0].name
            )

            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0
