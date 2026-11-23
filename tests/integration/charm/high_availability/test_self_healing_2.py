#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    ORIGINAL_RESTART_CONDITION,
    are_all_db_processes_down,
    are_writes_increasing,
    change_ha_patroni_setting,
    check_writes,
    fetch_cluster_members,
    get_password,
    get_patroni_setting,
    get_primary_from_app,
    is_cluster_updated,
    is_postgresql_ready,
    modify_pebble_restart_delay,
    scale_ha_application,
    send_signal_to_process,
    start_ha_continuous_writes,
    update_restart_condition,
)
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_apps_status

logger = logging.getLogger(__name__)

PATRONI_PROCESS = {
    Substrates.VM: "/snap/charmed-postgresql/[0-9]*/usr/bin/patroni",
    Substrates.K8S: "/usr/bin/patroni",
}
POSTGRESQL_PROCESS = "postgres"
# The Patroni path depends on the substrate, so the tests are parametrized by name.
DB_PROCESSES = [POSTGRESQL_PROCESS, "patroni"]
MEDIAN_ELECTION_TIME = 10

VM_STORAGE = {
    "archive": "lxd-btrfs,2048M",
    "data": "lxd-btrfs,2048M",
    "logs": "lxd-btrfs,2048M",
    "temp": "lxd-btrfs,2048M",
}


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrates
) -> None:
    """Build and deploy three unit of PostgreSQL."""
    wait_for_apps = False
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in juju.status().apps:
        wait_for_apps = True
        with fast_forward(juju):
            juju.deploy(
                charm,
                app=app_name,
                num_units=3,
                base=CHARM_BASE,
                storage=VM_STORAGE if substrate == Substrates.VM else None,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrates.K8S,
            )
    # Deploy the continuous writes application charm if it wasn't already deployed.
    if APPLICATION_NAME not in juju.status().apps:
        wait_for_apps = True
        with fast_forward(juju):
            juju.deploy(
                APPLICATION_NAME,
                app=APPLICATION_NAME,
                base=CHARM_BASE,
                channel="latest/edge",
            )

    if wait_for_apps:
        juju.integrate(app_name, f"{APPLICATION_NAME}:database")
        with fast_forward(juju):
            juju.wait(
                wait_for_apps_status(jubilant.all_active, APPLICATION_NAME, app_name), timeout=1500
            )


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("process", DB_PROCESSES)
def test_freeze_db_process(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    process: str,
    juju_continuous_writes,
    juju_primary_start_timeout,
) -> None:
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Freeze the database process.
    send_signal_to_process(juju, primary_name, process, "SIGSTOP")

    # Wait some time to elect a new primary.
    sleep(MEDIAN_ELECTION_TIME * 6)

    with fast_forward(juju):
        # Verify new writes are continuing by counting the number of writes before and after a
        # 3 minutes wait (this is a little more than the loop wait configuration, that is
        # considered to trigger a fail-over after primary_start_timeout is changed, and also
        # when freezing the DB process it take some more time to trigger the fail-over).
        try:
            are_writes_increasing(juju, primary_name)

            # Verify that a new primary gets elected (ie old primary is secondary).
            for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
                with attempt:
                    new_primary_name = get_primary_from_app(juju, app, down_unit=primary_name)
                    assert new_primary_name != primary_name
        finally:
            # Un-freeze the old primary.
            for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
                with attempt:
                    use_ssh = (attempt.retry_state.attempt_number % 2) == 0
                    logger.info(f"unfreezing {process}")
                    send_signal_to_process(juju, primary_name, process, "SIGCONT", use_ssh)

        # Verify that the database service got restarted and is ready in the old primary.
        logger.info(f"waiting for the database service to restart on {primary_name}")
        assert is_postgresql_ready(juju, primary_name)

    is_cluster_updated(juju, primary_name)


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("process", DB_PROCESSES)
@pytest.mark.parametrize("signal", ["SIGTERM", "SIGKILL"])
def test_full_cluster_restart(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    process: str,
    signal: str,
    juju_continuous_writes,
    juju_reset_restart_condition,
    juju_restart_policy,
    juju_loop_wait,
) -> None:
    """This tests checks that a cluster recovers from a full cluster restart.

    The test can be called a full cluster crash when the signal sent to the OS process
    is SIGKILL.
    """
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    patroni_password = get_password(juju, app, "patroni")

    # Change the loop wait setting to make Patroni wait more time before restarting PostgreSQL.
    initial_loop_wait = get_patroni_setting(juju, "loop_wait")
    if substrate == Substrates.VM:
        initial_ttl = get_patroni_setting(juju, "ttl")
        # loop_wait parameter is limited by ttl value, thus we should increase it first
        change_ha_patroni_setting(juju, "ttl", 600, patroni_password, use_random_unit=True)
    change_ha_patroni_setting(juju, "loop_wait", 300, patroni_password, use_random_unit=True)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Jubilant calls are synchronous: use threads to preserve the original asyncio.gather.
    # Sending signals sequentially could let a unit restart before the others are down.
    units = list(juju.status().apps[app].units)
    with ThreadPoolExecutor(max_workers=len(units)) as executor:
        futures = [
            executor.submit(send_signal_to_process, juju, unit, process, signal) for unit in units
        ]
        for future in futures:
            future.result()

    # This test serves to verify behavior when all replicas are down at the same time that when
    # they come back online they operate as expected. This check verifies that we meet the criteria
    # of all replicas being down at the same time.
    try:
        assert are_all_db_processes_down(juju, process, signal), (
            "Not all units down at the same time."
        )
    finally:
        if substrate == Substrates.VM:
            if process == PATRONI_PROCESS[substrate]:
                # Preserve the concurrent systemd restoration of the original asyncio.gather.
                units = list(juju.status().apps[app].units)
                with ThreadPoolExecutor(max_workers=len(units)) as executor:
                    futures = [
                        executor.submit(
                            update_restart_condition, juju, unit, ORIGINAL_RESTART_CONDITION
                        )
                        for unit in units
                    ]
                    for future in futures:
                        future.result()
        else:
            for unit in juju.status().apps[app].units:
                modify_pebble_restart_delay(
                    juju,
                    unit,
                    "tests/integration/charm/high_availability/manifests/restore_pebble_restart_delay.yml",
                    ensure_replan=True,
                )
        change_ha_patroni_setting(
            juju, "loop_wait", initial_loop_wait, patroni_password, use_random_unit=True
        )
        if substrate == Substrates.VM:
            change_ha_patroni_setting(
                juju, "ttl", initial_ttl, patroni_password, use_random_unit=True
            )

    # Verify all units are up and running.
    sleep(30)
    for unit in juju.status().apps[app].units:
        assert is_postgresql_ready(juju, unit), f"unit {unit} not restarted after cluster restart."

    # Check if a primary is elected
    for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3)):
        with attempt:
            new_primary_name = get_primary_from_app(juju, app)
            assert new_primary_name is not None, "Could not get primary from any unit"

    with fast_forward(juju, "60s"):
        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1000)
        are_writes_increasing(juju)

    # Verify that all units are part of the same cluster.
    member_ips = fetch_cluster_members(juju)
    ip_addresses = [get_unit_address(juju, unit) for unit in juju.status().apps[app].units]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

    # Verify that no writes to the database were missed after stopping the writes.
    with fast_forward(juju):
        check_writes(juju)


@pytest.mark.abort_on_fail
@pytest.mark.k8s_only
def test_scaling_to_zero(juju: jubilant.Juju, app_name: str, juju_continuous_writes) -> None:
    """Scale the database to zero units and scale up again."""
    # Locate primary unit.
    app = app_name

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Scale the database to zero units.
    logger.info("scaling database to zero units")
    scale_ha_application(juju, app, 0)

    # Scale the database to three units.
    logger.info("scaling database to three units")
    scale_ha_application(juju, app, 3)

    # Verify all units are up and running.
    logger.info("waiting for the database service to start in all units")
    for unit in juju.status().apps[app].units:
        assert is_postgresql_ready(juju, unit), f"unit {unit} not restarted after cluster restart."

    logger.info("checking whether writes are increasing")
    are_writes_increasing(juju)

    # Verify that all units are part of the same cluster.
    logger.info("checking whether all units are part of the same cluster")
    member_ips = fetch_cluster_members(juju)
    ip_addresses = [get_unit_address(juju, unit) for unit in juju.status().apps[app].units]
    assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

    # Verify that no writes to the database were missed after stopping the writes.
    logger.info("checking whether no writes to the database were missed after stopping the writes")
    check_writes(juju)
