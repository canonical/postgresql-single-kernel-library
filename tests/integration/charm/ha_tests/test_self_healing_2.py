#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
from time import sleep

import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.ha_tests.helpers import (
    ORIGINAL_RESTART_CONDITION,
    are_all_db_processes_down,
    are_writes_increasing,
    change_patroni_setting,
    check_writes,
    fetch_cluster_members,
    get_patroni_setting,
    is_cluster_updated,
    is_postgresql_ready,
    modify_pebble_restart_delay,
    send_signal_to_process,
    start_continuous_writes,
    update_restart_condition,
)
from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    get_password,
    get_primary,
    get_unit_address,
    scale_application,
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
@pytest.mark.parametrize("process", DB_PROCESSES)
async def test_freeze_db_process(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    process: str,
    continuous_writes,
    primary_start_timeout,
) -> None:
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Freeze the database process.
    await send_signal_to_process(ops_test, primary_name, process, "SIGSTOP")

    # Wait some time to elect a new primary.
    sleep(MEDIAN_ELECTION_TIME * 6)

    async with ops_test.fast_forward():
        # Verify new writes are continuing by counting the number of writes before and after a
        # 3 minutes wait (this is a little more than the loop wait configuration, that is
        # considered to trigger a fail-over after primary_start_timeout is changed, and also
        # when freezing the DB process it take some more time to trigger the fail-over).
        try:
            await are_writes_increasing(ops_test, primary_name)

            # Verify that a new primary gets elected (ie old primary is secondary).
            for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
                with attempt:
                    new_primary_name = await get_primary(ops_test, app, down_unit=primary_name)
                    assert new_primary_name != primary_name
        finally:
            # Un-freeze the old primary.
            for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
                with attempt:
                    use_ssh = (attempt.retry_state.attempt_number % 2) == 0
                    logger.info(f"unfreezing {process}")
                    await send_signal_to_process(
                        ops_test, primary_name, process, "SIGCONT", use_ssh
                    )

        # Verify that the database service got restarted and is ready in the old primary.
        logger.info(f"waiting for the database service to restart on {primary_name}")
        assert await is_postgresql_ready(ops_test, primary_name)

    await is_cluster_updated(ops_test, primary_name)


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("process", DB_PROCESSES)
@pytest.mark.parametrize("signal", ["SIGTERM", "SIGKILL"])
async def test_full_cluster_restart(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    process: str,
    signal: str,
    continuous_writes,
    reset_restart_condition,
    restart_policy,
    loop_wait,
) -> None:
    """This tests checks that a cluster recovers from a full cluster restart.

    The test can be called a full cluster crash when the signal sent to the OS process
    is SIGKILL.
    """
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    patroni_password = await get_password(ops_test, "patroni")

    # Change the loop wait setting to make Patroni wait more time before restarting PostgreSQL.
    initial_loop_wait = await get_patroni_setting(ops_test, "loop_wait")
    if substrate == Substrate.vm:
        initial_ttl = await get_patroni_setting(ops_test, "ttl")
        # loop_wait parameter is limited by ttl value, thus we should increase it first
        await change_patroni_setting(ops_test, "ttl", 600, patroni_password, use_random_unit=True)
    await change_patroni_setting(
        ops_test, "loop_wait", 300, patroni_password, use_random_unit=True
    )

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Restart all units "simultaneously".
    await asyncio.gather(*[
        send_signal_to_process(ops_test, unit.name, process, signal)
        for unit in ops_test.model.applications[app].units
    ])

    # This test serves to verify behavior when all replicas are down at the same time that when
    # they come back online they operate as expected. This check verifies that we meet the criteria
    # of all replicas being down at the same time.
    try:
        assert await are_all_db_processes_down(ops_test, process, signal), (
            "Not all units down at the same time."
        )
    finally:
        if substrate == Substrate.vm:
            if process == PATRONI_PROCESS[substrate]:
                awaits = []
                for unit in ops_test.model.applications[app].units:
                    awaits.append(
                        update_restart_condition(ops_test, unit, ORIGINAL_RESTART_CONDITION)
                    )
                await asyncio.gather(*awaits)
        else:
            for unit in ops_test.model.applications[app].units:
                await modify_pebble_restart_delay(
                    ops_test,
                    unit.name,
                    "tests/integration/charm/ha_tests/manifests/restore_pebble_restart_delay.yml",
                    ensure_replan=True,
                )
        await change_patroni_setting(
            ops_test, "loop_wait", initial_loop_wait, patroni_password, use_random_unit=True
        )
        if substrate == Substrate.vm:
            await change_patroni_setting(
                ops_test, "ttl", initial_ttl, patroni_password, use_random_unit=True
            )

    # Verify all units are up and running.
    sleep(30)
    for unit in ops_test.model.applications[app].units:
        assert await is_postgresql_ready(ops_test, unit.name), (
            f"unit {unit.name} not restarted after cluster restart."
        )

    # Check if a primary is elected
    for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
        with attempt:
            new_primary_name = await get_primary(ops_test, app)
            assert new_primary_name is not None, "Could not get primary from any unit"

    async with ops_test.fast_forward("60s"):
        await ops_test.model.wait_for_idle(status="active", timeout=1000)
        await are_writes_increasing(ops_test)

    # Verify that all units are part of the same cluster.
    member_ips = await fetch_cluster_members(ops_test)
    ip_addresses = [
        await get_unit_address(ops_test, unit.name)
        for unit in ops_test.model.applications[app].units
    ]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

    # Verify that no writes to the database were missed after stopping the writes.
    async with ops_test.fast_forward():
        await check_writes(ops_test)


@pytest.mark.abort_on_fail
@pytest.mark.k8s_only
async def test_scaling_to_zero(ops_test: OpsTest, app_name: str, continuous_writes) -> None:
    """Scale the database to zero units and scale up again."""
    # Locate primary unit.
    app = app_name

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Scale the database to zero units.
    logger.info("scaling database to zero units")
    await scale_application(ops_test, app, 0)

    # Scale the database to three units.
    logger.info("scaling database to three units")
    await scale_application(ops_test, app, 3)

    # Verify all units are up and running.
    logger.info("waiting for the database service to start in all units")
    for unit in ops_test.model.applications[app].units:
        assert await is_postgresql_ready(ops_test, unit.name), (
            f"unit {unit.name} not restarted after cluster restart."
        )

    logger.info("checking whether writes are increasing")
    await are_writes_increasing(ops_test)

    # Verify that all units are part of the same cluster.
    logger.info("checking whether all units are part of the same cluster")
    member_ips = await fetch_cluster_members(ops_test)
    ip_addresses = [
        await get_unit_address(ops_test, unit.name)
        for unit in ops_test.model.applications[app].units
    ]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

    # Verify that no writes to the database were missed after stopping the writes.
    logger.info("checking whether no writes to the database were missed after stopping the writes")
    await check_writes(ops_test)
