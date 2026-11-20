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
    add_unit_with_storage,
    are_writes_increasing,
    check_writes,
    get_primary_from_app,
    get_storage_ids,
    is_cluster_updated,
    is_postgresql_ready,
    is_replica,
    is_secondary_up_to_date,
    reused_replica_storage,
    send_signal_to_process,
    start_ha_continuous_writes,
    storage_type,
)
from tests.integration.helpers.juju import fast_forward, wait_for_idle

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
            wait_for_idle(
                juju,
                apps=[APPLICATION_NAME, app_name],
                status="active",
                timeout=1500,
                idle_period=30,
            )


@pytest.mark.abort_on_fail
@pytest.mark.vm_only
def test_storage_re_use(juju: jubilant.Juju, app_name: str, juju_continuous_writes):
    """Verifies that database units with attached storage correctly repurpose storage.

    It is not enough to verify that Juju attaches the storage. Hence test checks that the
    postgresql properly uses the storage that was provided. (ie. doesn't just re-sync everything
    from primary, but instead computes a diff between current storage and primary storage.)
    """
    app = app_name
    if storage_type(juju, app) == "rootfs":
        pytest.skip(
            "reuse of storage can only be used on deployments with persistent storage not on rootfs deployments"
        )

    # removing the only replica can be disastrous
    if len(juju.status().apps[app].units) < 2:
        juju.add_unit(app, num_units=1)
        wait_for_idle(juju, apps=[app], status="active", timeout=1500)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # remove a unit and attach it's storage to a new unit
    for unit in juju.status().apps[app].units:
        if is_replica(juju, unit):
            break
    unit_storage_id = get_storage_ids(juju, unit)
    expected_units = len(juju.status().apps[app].units) - 1
    juju.remove_unit(unit)
    wait_for_idle(
        juju, apps=[app], status="active", timeout=1000, wait_for_exact_units=expected_units
    )
    new_unit = add_unit_with_storage(juju, app, unit_storage_id)

    assert reused_replica_storage(juju, new_unit), (
        "attached storage not properly reused by Postgresql."
    )

    # Verify that no writes to the database were missed after stopping the writes.
    total_expected_writes = check_writes(juju)

    # Verify that new instance is up-to-date.
    assert is_secondary_up_to_date(juju, new_unit, total_expected_writes), (
        "new instance not up to date."
    )


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("process", DB_PROCESSES)
@pytest.mark.parametrize("signal", ["SIGTERM", "SIGKILL"])
def test_interruption_db_process(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    process: str,
    signal: str,
    juju_continuous_writes,
    juju_primary_start_timeout,
    juju_restart_policy,
) -> None:
    process = PATRONI_PROCESS[substrate] if process == "patroni" else process

    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Interrupt the database process.
    send_signal_to_process(juju, primary_name, process, signal)

    # Wait some time to elect a new primary.
    sleep(MEDIAN_ELECTION_TIME * 6)

    with fast_forward(juju):
        are_writes_increasing(juju, primary_name)

        # Verify that a new primary gets elected (ie old primary is secondary).
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                new_primary_name = get_primary_from_app(juju, app, down_unit=primary_name)
                assert new_primary_name != primary_name

        # Verify that the database service got restarted and is ready in the old primary.
        logger.info(f"waiting for the database service to restart on {primary_name}")
        assert is_postgresql_ready(juju, primary_name)

    is_cluster_updated(juju, primary_name)
