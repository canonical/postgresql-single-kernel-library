#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import pytest as pytest
import requests
from kubernetes import config, dynamic
from kubernetes.client import api_client
from pytest_operator.plugin import OpsTest

from tests.integration.charm.ha_tests.helpers import (
    are_writes_increasing,
    check_writes,
    remove_charm_code,
    start_continuous_writes,
)
from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    get_primary,
    get_unit_address,
)
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)


CLUSTER_SIZE = 3

pytestmark = pytest.mark.k8s_only


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Build and deploy three unit of PostgreSQL."""
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in ops_test.model.applications:
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                charm,
                application_name=app_name,
                num_units=CLUSTER_SIZE,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
    # Deploy the continuous writes application charm if it wasn't already deployed.
    if APPLICATION_NAME not in ops_test.model.applications:
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                APPLICATION_NAME,
                application_name=APPLICATION_NAME,
                base=CHARM_BASE,
                channel="latest/edge",
            )

    await ops_test.model.relate(app_name, f"{APPLICATION_NAME}:database")
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[app_name],
            status="active",
            timeout=1000,
            raise_on_error=False,
        )


@pytest.mark.abort_on_fail
async def test_restart(ops_test: OpsTest, continuous_writes, app_name: str) -> None:
    """Test restart of all the units simultaneously."""
    logger.info("starting continuous writes to the database")
    await start_continuous_writes(ops_test, app_name)

    logger.info("checking whether writes are increasing")
    await are_writes_increasing(ops_test)

    logger.info(
        "removing charm code from one non-primary unit to simulate a crash and prevent firing the update-charm hook"
    )
    primary = await get_primary(ops_test, app_name)
    status = await ops_test.model.get_status()
    for unit in status.applications[app_name].units:
        if unit != primary:
            non_primary = unit
            break
    await remove_charm_code(ops_test, non_primary)
    logger.info(f"removed charm code from {non_primary}")

    logger.info("restarting all the units by deleting their pods")
    client = dynamic.DynamicClient(api_client.ApiClient(configuration=config.load_kube_config()))
    api = client.resources.get(api_version="v1", kind="Pod")
    api.delete(
        namespace=ops_test.model.info.name,
        label_selector=f"app.kubernetes.io/name={app_name}",
    )
    await ops_test.model.block_until(
        lambda: all(
            unit.workload_status == "error"
            for unit in ops_test.model.units.values()
            if unit.name == non_primary
        )
    )

    # Resolve the error on the non-primary unit.
    for unit in ops_test.model.units.values():
        if unit.name == non_primary and unit.workload_status == "error":
            logger.info(f"resolving {non_primary} error")
            await unit.resolved(retry=False)
            break

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[app_name], status="active", raise_on_error=False, timeout=300
        )

    # Check that all replication slots are present in the primary
    # (by checking the list of cluster members).
    logger.info(
        "checking that all the replication slots are present in the primary by checking the list of cluster members"
    )
    primary = await get_primary(ops_test, app_name)
    address = await get_unit_address(ops_test, primary)
    members = requests.get(f"https://{address}:8008/cluster", verify=False).json()["members"]

    assert [member["name"] for member in members] == [
        unit.name.replace("/", "-") for unit in ops_test.model.applications[app_name].units
    ]

    logger.info("ensure continuous_writes after the crashed unit")
    await are_writes_increasing(ops_test)

    # Verify that no writes to the database were missed after stopping the writes
    # (check that all the units have all the writes).
    logger.info("checking whether no writes were lost")
    await check_writes(ops_test)
