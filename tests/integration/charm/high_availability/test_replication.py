#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import jubilant
import psycopg2
import pytest
from lightkube.core.client import Client
from lightkube.resources.core_v1 import Pod
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    check_writes,
    fetch_cluster_members,
    get_password,
    get_patroni_cluster,
    get_primary_from_app,
    is_cluster_updated,
    scale_ha_application,
    start_ha_continuous_writes,
)
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_apps_status
from tests.integration.helpers.postgresql import db_connect


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
        with fast_forward(juju):
            juju.integrate(app_name, f"{APPLICATION_NAME}:database")
            juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1500)


def test_reelection(
    juju: jubilant.Juju,
    app_name: str,
    substrate: Substrates,
    juju_continuous_writes,
    juju_primary_start_timeout,
) -> None:
    """Kill primary unit, check reelection."""
    app = app_name
    if len(juju.status().apps[app].units) < 2:
        scale_ha_application(juju, app, 2)

    # Failover needs an operational synchronous standby before the primary is killed.
    for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(3), reraise=True):
        with attempt:
            units = juju.status().apps[app].units
            members = get_patroni_cluster(get_unit_address(juju, next(iter(units))))["members"]
            assert (
                len(members) == len(units)
                and all(member["state"] in ("running", "streaming") for member in members)
                and sum(member["role"] == "leader" for member in members) == 1
                and any(member["role"] == "sync_standby" for member in members)
            ), f"Cluster not ready for failover: {members}"

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    primary_name = get_primary_from_app(juju, app)
    if substrate == Substrates.VM:
        # Remove the primary unit.
        juju.remove_unit(primary_name)
    else:
        # Kill the current primary.
        client = Client(namespace=juju.status().model.name)
        client.delete(Pod, name=primary_name.replace("/", "-"))

    # Wait and get the primary again (which can be any unit, including the previous primary).
    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app), timeout=600)

    # Check whether writes are increasing.
    are_writes_increasing(juju, primary_name)

    # Verify that a new primary gets elected (ie old primary is secondary).
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            new_primary_name = get_primary_from_app(juju, app, down_unit=primary_name)
            assert new_primary_name != primary_name, "primary reelection hasn't happened"

    if substrate == Substrates.VM:
        # Verify that all units are part of the same cluster.
        member_ips = fetch_cluster_members(juju)
        app = primary_name.split("/")[0]
        ip_addresses = [unit.public_address for unit in juju.status().apps[app].units.values()]
        assert set(member_ips) == set(ip_addresses), "not all units are part of the same cluster."

        # Verify that no writes to the database were missed after stopping the writes.
        check_writes(juju)
    else:
        # Verify that all the units from the first cluster are up-to-date.
        is_cluster_updated(juju, primary_name)


def test_consistency(juju: jubilant.Juju, app_name: str, juju_continuous_writes) -> None:
    """Write to primary, read data from secondaries (check consistency)."""
    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Check whether writes are increasing.
    are_writes_increasing(juju, primary_name)

    # Verify that no writes to the database were missed after stopping the writes
    # (check that all the units have all the writes).
    check_writes(juju)


def test_no_data_replicated_between_clusters(
    juju: jubilant.Juju,
    charm,
    app_name: str,
    charm_resources,
    substrate: Substrates,
    juju_continuous_writes,
) -> None:
    """Check that writes in one cluster are not replicated to another cluster."""
    # Locate primary unit.
    app = app_name
    primary_name = get_primary_from_app(juju, app)

    # Deploy another cluster.
    new_cluster_app = f"second-{app}"
    if new_cluster_app not in juju.status().apps:
        with fast_forward(juju):
            juju.deploy(
                charm,
                app=new_cluster_app,
                num_units=2,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrates.K8S,
            )
            juju.wait(wait_for_apps_status(jubilant.all_active, new_cluster_app), timeout=1500)

    # Start an application that continuously writes data to the database.
    start_ha_continuous_writes(juju, app)

    # Check whether writes are increasing.
    are_writes_increasing(juju, primary_name)

    # Verify that no writes to the first cluster were missed after stopping the writes.
    check_writes(juju)

    # Verify that the data from the first cluster wasn't replicated to the second cluster.
    password = get_password(juju, new_cluster_app)
    for unit_name in juju.status().apps[new_cluster_app].units:
        address = get_unit_address(juju, unit_name)
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
