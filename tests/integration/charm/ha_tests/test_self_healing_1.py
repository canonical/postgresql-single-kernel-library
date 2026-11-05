#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from time import sleep

import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.ha_tests.helpers import (
    add_unit_with_storage,
    are_writes_increasing,
    check_writes,
    get_storage_ids,
    is_cluster_updated,
    is_postgresql_ready,
    is_replica,
    is_secondary_up_to_date,
    reused_replica_storage,
    send_signal_to_process,
    start_continuous_writes,
    storage_type,
)
from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    get_primary,
)
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

PATRONI_PROCESS = {
    Substrate.vm: "/snap/charmed-postgresql/[0-9]*/usr/bin/patroni",
    Substrate.k8s: "/usr/bin/patroni",
}
POSTGRESQL_PROCESS = "postgres"
# The Patroni path depends on the substrate, so the tests are parametrized by name.
DB_PROCESSES = [POSTGRESQL_PROCESS, "patroni"]
MEDIAN_ELECTION_TIME = 10

VM_STORAGE = {
    "archive": {"pool": "lxd-btrfs", "size": 2048},
    "data": {"pool": "lxd-btrfs", "size": 2048},
    "logs": {"pool": "lxd-btrfs", "size": 2048},
    "temp": {"pool": "lxd-btrfs", "size": 2048},
}


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Build and deploy three unit of PostgreSQL."""
    wait_for_apps = False
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in ops_test.model.applications:
        wait_for_apps = True
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                charm,
                application_name=app_name,
                num_units=3,
                base=CHARM_BASE,
                storage=VM_STORAGE if substrate == Substrate.vm else None,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
    # Deploy the continuous writes application charm if it wasn't already deployed.
    if APPLICATION_NAME not in ops_test.model.applications:
        wait_for_apps = True
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                APPLICATION_NAME,
                application_name=APPLICATION_NAME,
                base=CHARM_BASE,
                channel="latest/edge",
            )

    if wait_for_apps:
        await ops_test.model.relate(app_name, f"{APPLICATION_NAME}:database")
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APPLICATION_NAME, app_name],
                status="active",
                timeout=1500,
                idle_period=30,
            )


@pytest.mark.abort_on_fail
@pytest.mark.vm_only
async def test_storage_re_use(ops_test, app_name: str, continuous_writes):
    """Verifies that database units with attached storage correctly repurpose storage.

    It is not enough to verify that Juju attaches the storage. Hence test checks that the
    postgresql properly uses the storage that was provided. (ie. doesn't just re-sync everything
    from primary, but instead computes a diff between current storage and primary storage.)
    """
    app = app_name
    if storage_type(ops_test, app) == "rootfs":
        pytest.skip(
            "reuse of storage can only be used on deployments with persistent storage not on rootfs deployments"
        )

    # removing the only replica can be disastrous
    if len(ops_test.model.applications[app].units) < 2:
        await ops_test.model.applications[app].add_unit(count=1)
        await ops_test.model.wait_for_idle(apps=[app], status="active", timeout=1500)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # remove a unit and attach it's storage to a new unit
    for unit in ops_test.model.applications[app].units:
        if await is_replica(ops_test, unit.name):
            break
    unit_storage_id = await get_storage_ids(ops_test, unit.name)
    expected_units = len(ops_test.model.applications[app].units) - 1
    await ops_test.model.destroy_unit(unit.name)
    await ops_test.model.wait_for_idle(
        apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )
    new_unit = await add_unit_with_storage(ops_test, app, unit_storage_id)

    assert await reused_replica_storage(ops_test, new_unit.name), (
        "attached storage not properly reused by Postgresql."
    )

    # Verify that no writes to the database were missed after stopping the writes.
    total_expected_writes = await check_writes(ops_test)

    # Verify that new instance is up-to-date.
    assert await is_secondary_up_to_date(ops_test, new_unit.name, total_expected_writes), (
        "new instance not up to date."
    )


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("process", DB_PROCESSES)
@pytest.mark.parametrize("signal", ["SIGTERM", "SIGKILL"])
async def test_interruption_db_process(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    process: str,
    signal: str,
    continuous_writes,
    primary_start_timeout,
    restart_policy,
) -> None:
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Interrupt the database process.
    await send_signal_to_process(ops_test, primary_name, process, signal)

    # Wait some time to elect a new primary.
    sleep(MEDIAN_ELECTION_TIME * 6)

    async with ops_test.fast_forward():
        await are_writes_increasing(ops_test, primary_name)

        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                new_primary_name = await get_primary(ops_test, app, down_unit=primary_name)
                assert new_primary_name != primary_name

        # Verify that the database service got restarted and is ready in the old primary.
        logger.info(f"waiting for the database service to restart on {primary_name}")
        assert await is_postgresql_ready(ops_test, primary_name)

    await is_cluster_updated(ops_test, primary_name)
