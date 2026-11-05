#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import psycopg2
import pytest
from lightkube.core.client import Client
from lightkube.resources.core_v1 import Pod
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from tests.integration.charm.ha_tests.helpers import (
    are_writes_increasing,
    check_writes,
    fetch_cluster_members,
    is_cluster_updated,
    start_continuous_writes,
)
from tests.integration.charm.pytest_operator_helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    get_password,
    get_primary,
    get_unit_address,
    scale_application,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate


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
        async with ops_test.fast_forward():
            await ops_test.model.relate(app_name, f"{APPLICATION_NAME}:database")
            await ops_test.model.wait_for_idle(status="active", timeout=1500)


async def test_reelection(
    ops_test: OpsTest,
    app_name: str,
    substrate: Substrate,
    continuous_writes,
    primary_start_timeout,
) -> None:
    """Kill primary unit, check reelection."""
    app = app_name
    if len(ops_test.model.applications[app].units) < 2:
        await scale_application(ops_test, app, 2)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    primary_name = await get_primary(ops_test, app)
    if substrate == Substrate.vm:
        # Remove the primary unit.
        await ops_test.model.destroy_units(primary_name)
    else:
        # Kill the current primary.
        client = Client(namespace=ops_test.model.info.name)
        client.delete(Pod, name=primary_name.replace("/", "-"))

    # Wait and get the primary again (which can be any unit, including the previous primary).
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[app], status="active")

    # Check whether writes are increasing.
    await are_writes_increasing(ops_test, primary_name)

    # Verify that a new primary gets elected (ie old primary is secondary).
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            new_primary_name = await get_primary(ops_test, app, down_unit=primary_name)
            assert new_primary_name != primary_name, "primary reelection hasn't happened"

    if substrate == Substrate.vm:
        # Verify that all units are part of the same cluster.
        member_ips = await fetch_cluster_members(ops_test)
        app = primary_name.split("/")[0]
        ip_addresses = [unit.public_address for unit in ops_test.model.applications[app].units]
        assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

        # Verify that no writes to the database were missed after stopping the writes.
        await check_writes(ops_test)
    else:
        # Verify that all the units from the first cluster are up-to-date.
        await is_cluster_updated(ops_test, primary_name)


async def test_consistency(ops_test: OpsTest, app_name: str, continuous_writes) -> None:
    """Write to primary, read data from secondaries (check consistency)."""
    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Check whether writes are increasing.
    await are_writes_increasing(ops_test, primary_name)

    # Verify that no writes to the database were missed after stopping the writes
    # (check that all the units have all the writes).
    await check_writes(ops_test)


async def test_no_data_replicated_between_clusters(
    ops_test: OpsTest,
    charm,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    continuous_writes,
) -> None:
    """Check that writes in one cluster are not replicated to another cluster."""
    # Locate primary unit.
    app = app_name
    primary_name = await get_primary(ops_test, app)

    # Deploy another cluster.
    new_cluster_app = f"second-{app}"
    if new_cluster_app not in ops_test.model.applications:
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                charm,
                application_name=new_cluster_app,
                num_units=2,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
            await ops_test.model.wait_for_idle(
                apps=[new_cluster_app], status="active", timeout=1500
            )

    # Start an application that continuously writes data to the database.
    await start_continuous_writes(ops_test, app)

    # Check whether writes are increasing.
    await are_writes_increasing(ops_test, primary_name)

    # Verify that no writes to the first cluster were missed after stopping the writes.
    await check_writes(ops_test)

    # Verify that the data from the first cluster wasn't replicated to the second cluster.
    password = await get_password(ops_test, database_app_name=new_cluster_app)
    for unit in ops_test.model.applications[new_cluster_app].units:
        address = await get_unit_address(ops_test, unit.name)
        for attempt in Retrying(
            stop=stop_after_delay(120),
            wait=wait_fixed(3),
            retry=retry_if_exception_type((psycopg2.InterfaceError, psycopg2.OperationalError)),
            reraise=True,
        ):
            with attempt:
                connection = None
                try:
                    with (
                        db_connect(host=address, password=password) as connection,
                        connection.cursor() as cursor,
                    ):
                        cursor.execute(
                            "SELECT EXISTS (SELECT FROM information_schema.tables"
                            " WHERE table_schema = 'public' AND table_name = 'continuous_writes');"
                        )
                        assert not cursor.fetchone()[0], (
                            "table 'continuous_writes' was replicated to the second cluster"
                        )
                finally:
                    if connection is not None:
                        connection.close()
