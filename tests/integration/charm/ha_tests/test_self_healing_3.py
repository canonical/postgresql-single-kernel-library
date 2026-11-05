#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from time import sleep

import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.ha_tests.helpers import (
    are_writes_increasing,
    change_wal_settings,
    cut_network_from_unit,
    cut_network_from_unit_without_ip_change,
    get_controller_machine,
    get_unit_ip,
    is_cluster_updated,
    is_connection_possible,
    is_machine_reachable_from,
    is_member_isolated,
    is_postgresql_ready,
    isolate_instance_from_cluster,
    list_wal_files,
    remove_instance_isolation,
    restore_network_for_unit,
    restore_network_for_unit_without_ip_change,
    start_continuous_writes,
    wait_network_restore,
)
from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    get_machine_from_unit,
    get_password,
    get_primary,
    get_unit_address,
    run_command_on_unit,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

MEDIAN_ELECTION_TIME = 10

CLEAN_DATA_DIR_SCRIPT = {
    Substrate.vm: "tests/integration/charm/ha_tests/clean-data-dir-vm.sh",
    Substrate.k8s: "tests/integration/charm/ha_tests/clean-data-dir-k8s.sh",
}

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
async def test_forceful_restart_without_data_and_transaction_logs(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    continuous_writes,
    primary_start_timeout,
    wal_settings,
) -> None:
    """A forceful restart with deleted data and without transaction logs (forced clone)."""
    if substrate == Substrate.vm:
        pytest.skip("Unstable")

    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)
    patroni_password = await get_password(ops_test, "patroni")

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Copy data dir content removal script.
    await ops_test.juju(
        "scp", CLEAN_DATA_DIR_SCRIPT[substrate], f"{primary_name}:/tmp/clean-data-dir.sh"
    )

    # Halts the execution of `update-status` hook so the pebble service doesn't get restarted
    # (on K8s).
    async with ops_test.fast_forward("24h" if substrate == Substrate.k8s else "10s"):
        # Stop the systemd service on the primary unit.
        logger.info(f"stopping database from {primary_name}")
        if substrate == Substrate.vm:
            await run_command_on_unit(
                ops_test, primary_name, "snap stop charmed-postgresql.patroni"
            )
        else:
            await run_command_on_unit(ops_test, primary_name, "/charm/bin/pebble stop postgresql")

        # Data removal runs within a script, so it allows `*` expansion.
        logger.info(f"removing data from {primary_name}")
        return_code, _, _ = await ops_test.juju(
            "ssh",
            primary_name,
            "sudo" if substrate == Substrate.vm else "bash",
            "/tmp/clean-data-dir.sh",
        )
        assert return_code == 0, "Failed to remove data directory"

        # Wait some time to elect a new primary.
        sleep(MEDIAN_ELECTION_TIME * 2)

        logger.info("checking whether writes are increasing")
        await are_writes_increasing(ops_test, primary_name)

        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                logger.info("checking whether a new primary was elected")
                new_primary_name = await get_primary(ops_test, app)
                assert new_primary_name is not None
                assert new_primary_name != primary_name

        # Change some settings to enable WAL rotation and remove the old WAL files.
        for unit in ops_test.model.applications[app].units:
            if unit.name == primary_name:
                continue
            logger.info(f"enabling WAL rotation on {primary_name}")
            await change_wal_settings(ops_test, unit.name, 32, 32, 1, patroni_password)

        # Rotate the WAL segments.
        logger.info(f"rotating WAL segments on {new_primary_name}")
        files = await list_wal_files(ops_test, app)
        host = await get_unit_address(ops_test, new_primary_name)
        password = await get_password(ops_test)
        with db_connect(host, password) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Run some commands to make PostgreSQL do WAL rotation.
                cursor.execute("SELECT pg_switch_wal();")
                cursor.execute("CHECKPOINT;")
                cursor.execute("SELECT pg_switch_wal();")
        connection.close()
        new_files = await list_wal_files(ops_test, app)

        # Check that the WAL was correctly rotated.
        for unit_name in files:
            assert not files[unit_name].intersection(new_files), (
                f"WAL segments weren't correctly rotated on {unit_name}"
            )

    # Database service in old primary should recover (after update-status run on K8s).
    async with ops_test.fast_forward("10s"):
        if substrate == Substrate.vm:
            # Start the systemd service in the old primary.
            await run_command_on_unit(
                ops_test, primary_name, "snap start charmed-postgresql.patroni"
            )

        logger.info(f"Checking the database service on {primary_name}")
        assert await is_postgresql_ready(ops_test, primary_name)

    await is_cluster_updated(ops_test, primary_name)


@pytest.mark.abort_on_fail
async def test_network_cut(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    architecture: str,
    continuous_writes,
    primary_start_timeout,
    chaos_mesh,
):
    """Completely cut and restore network."""
    if substrate == Substrate.k8s and architecture != "amd64":
        pytest.skip("Chaos Mesh is only deployed on amd64")

    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    if substrate == Substrate.vm:
        # Get unit hostname and IP.
        primary_hostname = await get_machine_from_unit(ops_test, primary_name)
        primary_ip = await get_unit_ip(ops_test, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert await is_connection_possible(ops_test, primary_name), (
        f"Connection {primary_name} is not possible"
    )

    if substrate == Substrate.vm:
        logger.info(f"Cutting network for {primary_name}")
        cut_network_from_unit(primary_hostname)

        # Verify machine is not reachable from peer units.
        all_units_names = [unit.name for unit in ops_test.model.applications[app].units]
        for unit_name in set(all_units_names) - {primary_name}:
            logger.info(f"checking for no connectivity between {primary_name} and {unit_name}")
            hostname = await get_machine_from_unit(ops_test, unit_name)
            assert not is_machine_reachable_from(hostname, primary_hostname), (
                "unit is reachable from peer"
            )

        # Verify machine is not reachable from controller.
        logger.info(f"checking for no connectivity between {primary_name} and the controller")
        controller = await get_controller_machine(ops_test)
        assert not is_machine_reachable_from(controller, primary_hostname), (
            "unit is reachable from controller"
        )
    else:
        # Confirm that the primary is not isolated from the cluster.
        logger.info("confirming that the primary is not isolated from the cluster")
        assert not await is_member_isolated(ops_test, primary_name, primary_name)

        # Create network chaos policy to isolate instance from cluster
        logger.info(f"Cutting network for {primary_name}")
        isolate_instance_from_cluster(ops_test, primary_name)

    # Verify that connection is not possible.
    logger.info("checking whether the connectivity to the database is not working")
    assert not await is_connection_possible(ops_test, primary_name), (
        "Connection is possible after network cut"
    )

    async with ops_test.fast_forward():
        logger.info("checking whether writes are increasing")
        await are_writes_increasing(ops_test, primary_name)

        logger.info("checking whether a new primary was elected")
        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                new_primary_name = await get_primary(ops_test, app, down_unit=primary_name)
                assert new_primary_name != primary_name

    if substrate == Substrate.vm:
        logger.info(f"Restoring network for {primary_name}")
        restore_network_for_unit(primary_hostname)

        # Wait until the cluster becomes idle (some operations like updating the member
        # IP are made).
        logger.info("waiting for cluster to become idle after updating member IP")
        async with ops_test.fast_forward(fast_interval="60s"):
            await ops_test.model.wait_for_idle(
                apps=[app],
                status="active",
                timeout=1000,
                idle_period=30,
                raise_on_error=False,
            )

        # Wait the LXD unit has its IP updated.
        logger.info("waiting for IP address to be updated on Juju unit")
        await wait_network_restore(ops_test, primary_name, primary_ip)
    else:
        # Confirm that the former primary is isolated from the cluster.
        logger.info("confirming that the former primary is isolated from the cluster")
        assert await is_member_isolated(ops_test, new_primary_name, primary_name)

        # Remove network chaos policy isolating instance from cluster.
        logger.info(f"Restoring network for {primary_name}")
        remove_instance_isolation(ops_test)

    # The unit IP changes on VM, so it's retrieved from inside the unit.
    use_ip_from_inside = substrate == Substrate.vm

    # Verify that the database service got restarted and is ready in the old primary.
    logger.info(f"waiting for the database service to be ready on {primary_name}")
    assert await is_postgresql_ready(ops_test, primary_name, use_ip_from_inside=use_ip_from_inside)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert await is_connection_possible(
        ops_test, primary_name, use_ip_from_inside=use_ip_from_inside
    ), "Connection is not possible after network restore"

    await is_cluster_updated(ops_test, primary_name, use_ip_from_inside=use_ip_from_inside)


@pytest.mark.abort_on_fail
@pytest.mark.vm_only
async def test_network_cut_without_ip_change(
    ops_test: OpsTest, app_name: str, continuous_writes, primary_start_timeout
):
    """Completely cut and restore network (situation when the unit IP doesn't change)."""
    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Get unit hostname and IP.
    primary_hostname = await get_machine_from_unit(ops_test, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert await is_connection_possible(ops_test, primary_name), (
        f"Connection {primary_name} is not possible"
    )

    logger.info(f"Cutting network for {primary_name}")
    cut_network_from_unit_without_ip_change(primary_hostname)

    # Verify machine is not reachable from peer units.
    all_units_names = [unit.name for unit in ops_test.model.applications[app].units]
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            for unit_name in set(all_units_names) - {primary_name}:
                logger.info(f"checking for no connectivity between {primary_name} and {unit_name}")
                hostname = await get_machine_from_unit(ops_test, unit_name)
                assert not is_machine_reachable_from(hostname, primary_hostname), (
                    "unit is reachable from peer"
                )

    # Verify machine is not reachable from controller.
    logger.info(f"checking for no connectivity between {primary_name} and the controller")
    controller = await get_controller_machine(ops_test)
    assert not is_machine_reachable_from(controller, primary_hostname), (
        "unit is reachable from controller"
    )

    # Verify that connection is not possible.
    logger.info("checking whether the connectivity to the database is not working")
    assert not await is_connection_possible(ops_test, primary_name), (
        "Connection is possible after network cut"
    )

    async with ops_test.fast_forward():
        logger.info("checking whether writes are increasing")
        await are_writes_increasing(ops_test, primary_name, use_ip_from_inside=True)

        logger.info("checking whether a new primary was elected")
        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                new_primary_name = await get_primary(ops_test, app, down_unit=primary_name)
                assert new_primary_name != primary_name

    logger.info(f"Restoring network for {primary_name}")
    restore_network_for_unit_without_ip_change(primary_hostname)

    # Wait until the cluster becomes idle.
    logger.info("waiting for cluster to become idle")
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active")

    # Verify that the database service got restarted and is ready in the old primary.
    logger.info(f"waiting for the database service to be ready on {primary_name}")
    assert await is_postgresql_ready(ops_test, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert await is_connection_possible(ops_test, primary_name), (
        "Connection is not possible after network restore"
    )

    await is_cluster_updated(ops_test, primary_name, use_ip_from_inside=True)
