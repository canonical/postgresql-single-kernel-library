#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest

from tests.integration.charm.helpers import (
    CHARM_BASE,
    add_unit_with_storage,
    db_connect,
    get_password,
    get_patroni_cluster,
    get_primary_from_app,
    get_storage_ids,
    reused_full_cluster_recovery_storage,
    set_password,
)
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_idle

FIRST_APPLICATION = "first-cluster"
SECOND_APPLICATION = "second-cluster"

logger = logging.getLogger(__name__)

charm = None

pytestmark = pytest.mark.vm_only


@pytest.mark.abort_on_fail
def test_build_and_deploy(juju: jubilant.Juju, charm) -> None:
    """Build and deploy two PostgreSQL clusters."""
    # This is a potentially destructive test, so it shouldn't be run against existing clusters
    with fast_forward(juju):
        # Deploy the first cluster with reusable storage
        juju.deploy(
            charm,
            app=FIRST_APPLICATION,
            num_units=3,
            base=CHARM_BASE,
            storage={
                "archive": "lxd-btrfs,8046M",
                "data": "lxd-btrfs,8046M",
                "logs": "lxd-btrfs,8046M",
                "temp": "lxd-btrfs,8046M",
            },
            config={"profile": "testing"},
        )

        # Deploy the second cluster
        juju.deploy(
            charm,
            app=SECOND_APPLICATION,
            num_units=1,
            base=CHARM_BASE,
            config={"profile": "testing"},
        )

        wait_for_idle(juju, status="active", timeout=1500)

        # TODO have a better way to bootstrap clusters with existing storage
        for user in ["monitoring", "operator", "replication", "rewind"]:
            password = get_password(juju, FIRST_APPLICATION, user)
            set_password(juju, SECOND_APPLICATION, username=user, password=password)
            # wait for the password changes to be processed
            wait_for_idle(juju, apps=[SECOND_APPLICATION], status="active", timeout=1500)

        second_primary = next(iter(juju.status().apps[SECOND_APPLICATION].units))
        juju.remove_unit(second_primary)


def test_cluster_restore(juju: jubilant.Juju):
    """Recreates the cluster from storage volumes."""
    # Write some data.
    primary = get_primary_from_app(juju, FIRST_APPLICATION)
    password = get_password(juju, FIRST_APPLICATION)
    address = get_unit_address(juju, primary)
    logger.info("creating a table in the database")
    with db_connect(host=address, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute(
            "CREATE TABLE IF NOT EXISTS restore_table_1 (test_collumn INT );"
        )
    connection.close()

    logger.info("Downscaling the existing cluster")
    storages = []
    for unit in juju.status().apps[FIRST_APPLICATION].units:
        storages.append(get_storage_ids(juju, unit))
        juju.remove_unit(unit)

    juju.remove_application(FIRST_APPLICATION)
    juju.wait(
        lambda status: FIRST_APPLICATION not in status.apps,
        timeout=float("inf"),
        delay=0.5,
        successes=1,
    )

    # Recreate cluster
    logger.info("Upscaling the second cluster with the old data")
    for storage in storages:
        unit = add_unit_with_storage(juju, SECOND_APPLICATION, storage)
        assert reused_full_cluster_recovery_storage(juju, unit), (
            "attached storage not properly reused by Postgresql."
        )

    primary = get_primary_from_app(juju, SECOND_APPLICATION)
    address = get_unit_address(juju, primary)
    logger.info("checking that data was persisted")
    with db_connect(host=address, password=password) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables"
            " WHERE table_schema = 'public' AND table_name = 'restore_table_1');"
        )
        assert cursor.fetchone()[0], (
            "data wasn't correctly restored: table 'restore_table_1' doesn't exist"
        )
    connection.close()

    # check that there is only one primary
    cluster = get_patroni_cluster(
        next(iter(juju.status().apps[SECOND_APPLICATION].units.values())).public_address
    )
    primaries = [member for member in cluster["members"] if member["role"] == "leader"]
    assert len(primaries) == 1, "There isn't just a single primary"

    # check that all units are member of the new cluster
    members = [member["name"] for member in cluster["members"]]
    for unit in juju.status().apps[SECOND_APPLICATION].units:
        assert unit.replace("/", "-") in members, "Unit missing from cluster"
    assert len(members) == len(storages), "Number of restored units and reused storages diverge"
