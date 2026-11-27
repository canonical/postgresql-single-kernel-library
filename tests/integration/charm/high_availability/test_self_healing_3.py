#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from time import sleep

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    change_wal_settings,
    cut_network_from_unit,
    cut_network_from_unit_without_ip_change,
    get_controller_machine,
    get_machine_from_unit,
    get_password,
    get_postgresql_app_name,
    get_primary_from_app,
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
    run_ha_command_on_unit,
    start_ha_continuous_writes,
    wait_network_restore,
)
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_apps_status
from tests.integration.helpers.postgresql import db_connect

logger = logging.getLogger(__name__)

MEDIAN_ELECTION_TIME = 10

CLEAN_DATA_DIR_SCRIPT = {
    Substrates.VM: "tests/integration/charm/high_availability/clean-data-dir-vm.sh",
    Substrates.K8S: "tests/integration/charm/high_availability/clean-data-dir-k8s.sh",
}

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
def test_forceful_restart_without_data_and_transaction_logs(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    juju_continuous_writes,
    juju_primary_start_timeout,
    juju_wal_settings,
) -> None:
    """A forceful restart with deleted data and without transaction logs (forced clone)."""
    if substrate == Substrates.VM:
        pytest.skip("Unstable")

    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)
    patroni_password = get_password(juju, get_postgresql_app_name(juju), "patroni")

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Copy data dir content removal script.
    juju.scp(CLEAN_DATA_DIR_SCRIPT[substrate], f"{primary_name}:/tmp/clean-data-dir.sh")

    # Halts the execution of `update-status` hook so the pebble service doesn't get restarted
    # (on K8s).
    with fast_forward(juju, "24h" if substrate == Substrates.K8S else "10s"):
        # Stop the systemd service on the primary unit.
        logger.info(f"stopping database from {primary_name}")
        if substrate == Substrates.VM:
            run_ha_command_on_unit(juju, primary_name, "snap stop charmed-postgresql.patroni")
        else:
            run_ha_command_on_unit(juju, primary_name, "/charm/bin/pebble stop postgresql")

        # Data removal runs within a script, so it allows `*` expansion.
        logger.info(f"removing data from {primary_name}")
        juju.ssh(
            primary_name,
            "sudo" if substrate == Substrates.VM else "bash",
            "/tmp/clean-data-dir.sh",
        )

        # Wait some time to elect a new primary.
        sleep(MEDIAN_ELECTION_TIME * 2)

        logger.info("checking whether writes are increasing")
        are_writes_increasing(juju, primary_name)

        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                logger.info("checking whether a new primary was elected")
                new_primary_name = get_primary_from_app(juju, app)
                assert new_primary_name is not None
                assert new_primary_name != primary_name

        # Change some settings to enable WAL rotation and remove the old WAL files.
        for unit in juju.status().apps[app].units:
            if unit == primary_name:
                continue
            logger.info(f"enabling WAL rotation on {primary_name}")
            change_wal_settings(juju, unit, 32, 32, 1, patroni_password)

        # Rotate the WAL segments.
        logger.info(f"rotating WAL segments on {new_primary_name}")
        files = list_wal_files(juju, app)
        host = get_unit_address(juju, new_primary_name)
        password = get_password(juju, get_postgresql_app_name(juju))
        with db_connect(host, password) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Run some commands to make PostgreSQL do WAL rotation.
                cursor.execute("SELECT pg_switch_wal();")
                cursor.execute("CHECKPOINT;")
                cursor.execute("SELECT pg_switch_wal();")
        connection.close()
        new_files = list_wal_files(juju, app)

        # Check that the WAL was correctly rotated.
        for unit_name in files:
            assert not files[unit_name].intersection(new_files), (
                f"WAL segments weren't correctly rotated on {unit_name}"
            )

    # Database service in old primary should recover (after update-status run on K8s).
    with fast_forward(juju, "10s"):
        if substrate == Substrates.VM:
            # Start the systemd service in the old primary.
            run_ha_command_on_unit(juju, primary_name, "snap start charmed-postgresql.patroni")

        logger.info(f"Checking the database service on {primary_name}")
        assert is_postgresql_ready(juju, primary_name)

    is_cluster_updated(juju, primary_name)


@pytest.mark.abort_on_fail
def test_network_cut(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    architecture: str,
    juju_continuous_writes,
    juju_primary_start_timeout,
    juju_chaos_mesh,
):
    """Completely cut and restore network."""
    if substrate == Substrates.K8S and architecture != "amd64":
        pytest.skip("Chaos Mesh is only deployed on amd64")

    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    if substrate == Substrates.VM:
        # Get unit hostname and IP.
        primary_hostname = get_machine_from_unit(juju, primary_name)
        primary_ip = get_unit_ip(juju, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert is_connection_possible(juju, primary_name), f"Connection {primary_name} is not possible"

    if substrate == Substrates.VM:
        logger.info(f"Cutting network for {primary_name}")
        cut_network_from_unit(primary_hostname)

        # Verify machine is not reachable from peer units.
        all_units_names = list(juju.status().apps[app].units)
        for unit_name in set(all_units_names) - {primary_name}:
            logger.info(f"checking for no connectivity between {primary_name} and {unit_name}")
            hostname = get_machine_from_unit(juju, unit_name)
            assert not is_machine_reachable_from(hostname, primary_hostname), (
                "unit is reachable from peer"
            )

        # Verify machine is not reachable from controller.
        logger.info(f"checking for no connectivity between {primary_name} and the controller")
        controller = get_controller_machine(juju)
        assert not is_machine_reachable_from(controller, primary_hostname), (
            "unit is reachable from controller"
        )
    else:
        # Confirm that the primary is not isolated from the cluster.
        logger.info("confirming that the primary is not isolated from the cluster")
        assert not is_member_isolated(juju, primary_name, primary_name)

        # Create network chaos policy to isolate instance from cluster
        logger.info(f"Cutting network for {primary_name}")
        isolate_instance_from_cluster(juju, primary_name)

    # Verify that connection is not possible.
    logger.info("checking whether the connectivity to the database is not working")
    assert not is_connection_possible(juju, primary_name), (
        "Connection is possible after network cut"
    )

    with fast_forward(juju):
        logger.info("checking whether writes are increasing")
        are_writes_increasing(juju, primary_name)

        logger.info("checking whether a new primary was elected")
        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                new_primary_name = get_primary_from_app(juju, app, down_unit=primary_name)
                assert new_primary_name != primary_name

    if substrate == Substrates.VM:
        logger.info(f"Restoring network for {primary_name}")
        restore_network_for_unit(primary_hostname)

        # Wait until the cluster becomes idle (some operations like updating the member
        # IP are made).
        logger.info("waiting for cluster to become idle after updating member IP")
        with fast_forward(juju, "60s"):
            juju.wait(wait_for_apps_status(jubilant.all_active, app), timeout=1000)

        # Wait the LXD unit has its IP updated.
        logger.info("waiting for IP address to be updated on Juju unit")
        wait_network_restore(juju, primary_name, primary_ip)
    else:
        # Confirm that the former primary is isolated from the cluster.
        logger.info("confirming that the former primary is isolated from the cluster")
        assert is_member_isolated(juju, new_primary_name, primary_name)

        # Remove network chaos policy isolating instance from cluster.
        logger.info(f"Restoring network for {primary_name}")
        remove_instance_isolation(juju)

    # The unit IP changes on VM, so it's retrieved from inside the unit.
    use_ip_from_inside = substrate == Substrates.VM

    # Verify that the database service got restarted and is ready in the old primary.
    logger.info(f"waiting for the database service to be ready on {primary_name}")
    assert is_postgresql_ready(juju, primary_name, use_ip_from_inside=use_ip_from_inside)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert is_connection_possible(juju, primary_name, use_ip_from_inside=use_ip_from_inside), (
        "Connection is not possible after network restore"
    )

    is_cluster_updated(juju, primary_name, use_ip_from_inside=use_ip_from_inside)


@pytest.mark.abort_on_fail
@pytest.mark.vm_only
def test_network_cut_without_ip_change(
    juju: jubilant.Juju, app_name: str, juju_continuous_writes, juju_primary_start_timeout
):
    """Completely cut and restore network (situation when the unit IP doesn't change)."""
    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Get unit hostname and IP.
    primary_hostname = get_machine_from_unit(juju, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert is_connection_possible(juju, primary_name), f"Connection {primary_name} is not possible"

    logger.info(f"Cutting network for {primary_name}")
    cut_network_from_unit_without_ip_change(primary_hostname)

    # Verify machine is not reachable from peer units.
    all_units_names = list(juju.status().apps[app].units)
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            for unit_name in set(all_units_names) - {primary_name}:
                logger.info(f"checking for no connectivity between {primary_name} and {unit_name}")
                hostname = get_machine_from_unit(juju, unit_name)
                assert not is_machine_reachable_from(hostname, primary_hostname), (
                    "unit is reachable from peer"
                )

    # Verify machine is not reachable from controller.
    logger.info(f"checking for no connectivity between {primary_name} and the controller")
    controller = get_controller_machine(juju)
    assert not is_machine_reachable_from(controller, primary_hostname), (
        "unit is reachable from controller"
    )

    # Verify that connection is not possible.
    logger.info("checking whether the connectivity to the database is not working")
    assert not is_connection_possible(juju, primary_name), (
        "Connection is possible after network cut"
    )

    with fast_forward(juju):
        logger.info("checking whether writes are increasing")
        are_writes_increasing(juju, primary_name, use_ip_from_inside=True)

        logger.info("checking whether a new primary was elected")
        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                new_primary_name = get_primary_from_app(juju, app, down_unit=primary_name)
                assert new_primary_name != primary_name

    logger.info(f"Restoring network for {primary_name}")
    restore_network_for_unit_without_ip_change(primary_hostname)

    # Wait until the cluster becomes idle.
    logger.info("waiting for cluster to become idle")
    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app), timeout=600)

    # Verify that the database service got restarted and is ready in the old primary.
    logger.info(f"waiting for the database service to be ready on {primary_name}")
    assert is_postgresql_ready(juju, primary_name)

    # Verify that connection is possible.
    logger.info("checking whether the connectivity to the database is working")
    assert is_connection_possible(juju, primary_name), (
        "Connection is not possible after network restore"
    )

    is_cluster_updated(juju, primary_name, use_ip_from_inside=True)
