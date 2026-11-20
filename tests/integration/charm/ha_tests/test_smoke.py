#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import pytest
from juju import tag
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import PersistentVolumeClaim
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.ha_tests.helpers import (
    add_unit_with_storage,
    apply_pvc_config,
    change_pvc_pv_name,
    change_pvs_reclaim_policy,
    check_db,
    check_password_auth,
    check_system_id_mismatch,
    create_db,
    delete_pvc,
    get_detached_storages,
    get_pvcs,
    get_pvs,
    get_storage_ids,
    is_postgresql_ready,
    is_storage_exists,
    remove_pv_claimref,
    remove_unit_force,
)
from tests.integration.charm.pytest_operator_helpers import CHARM_BASE, scale_application
from tests.integration.types import Substrate

TEST_DATABASE_NAME = "test_database"
DUP_DATABASE_APP_NAME = "postgres-test-dup"

VM_STORAGE = {
    "archive": {"pool": "lxd-btrfs", "size": 2048},
    "data": {"pool": "lxd-btrfs", "size": 2048},
    "logs": {"pool": "lxd-btrfs", "size": 2048},
    "temp": {"pool": "lxd-btrfs", "size": 2048},
}

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_app_force_removal(
    ops_test: OpsTest, charm: str, app_name: str, charm_resources, substrate: Substrate
):
    """Remove unit with force while storage is alive."""
    global primary_pvs, primary_pvcs
    async with ops_test.fast_forward():
        # Deploy the charm.
        logger.info("deploying charm")
        await ops_test.model.deploy(
            charm,
            application_name=app_name,
            num_units=1,
            base=CHARM_BASE,
            storage=VM_STORAGE if substrate == Substrate.vm else None,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )

        logger.info("waiting for idle")
        await ops_test.model.wait_for_idle(apps=[app_name], status="active", timeout=1500)
        assert ops_test.model.applications[app_name].units[0].workload_status == "active"

        primary_name = ops_test.model.applications[app_name].units[0].name

        logger.info("waiting for postgresql")
        for attempt in Retrying(stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert await is_postgresql_ready(ops_test, primary_name)

        if substrate == Substrate.k8s:
            logger.info(f"get pvc for {primary_name}")
            primary_pvcs = get_pvcs(ops_test, primary_name)

            assert primary_pvcs

            logger.info(f"get pv for {primary_name}")
            primary_pvs = get_pvs(ops_test, primary_name)

            assert len(primary_pvs)

        logger.info("getting storage id")
        storage_ids = await get_storage_ids(ops_test, primary_name)

        assert len(storage_ids)

        # Check if storage exists after application deployed
        logger.info("verifying that storage exists")
        for storage_id in storage_ids:
            for attempt in Retrying(
                stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True
            ):
                with attempt:
                    assert await is_storage_exists(ops_test, storage_id)

        # Create test database to check there is no resources conflicts
        logger.info("creating db")
        await create_db(ops_test, app_name, TEST_DATABASE_NAME)

        # Check that test database is not exists for new unit
        logger.info("checking db")
        assert await check_db(ops_test, app_name, TEST_DATABASE_NAME)

        # Force remove unit without storage removal
        if substrate == Substrate.vm:
            logger.info("force removing charm")
            await ops_test.model.destroy_unit(
                primary_name, force=True, destroy_storage=False, max_wait=1500
            )
        else:
            logger.info("scale to 0 with force")
            await remove_unit_force(ops_test, 1)

        # Storage should remain
        logger.info("verifying that storage exists")
        for storage_id in storage_ids:
            for attempt in Retrying(
                stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True
            ):
                with attempt:
                    assert await is_storage_exists(ops_test, storage_id)


@pytest.mark.abort_on_fail
async def test_app_garbage_ignorance(ops_test: OpsTest, app_name: str, substrate: Substrate):
    """Test charm deploy in dirty environment with garbage storage."""
    global primary_pvs, primary_pvcs
    async with ops_test.fast_forward():
        logger.info("checking garbage storage")
        garbage_storages = None
        for attempt in Retrying(stop=stop_after_delay(30 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                garbage_storages = await get_detached_storages(ops_test)
                if substrate == Substrate.vm:
                    assert len(garbage_storages) == 4
                logger.info(f"Collected storages: {garbage_storages}")

        if substrate == Substrate.vm:
            logger.info("add unit with attached storage")
            await add_unit_with_storage(ops_test, app_name, garbage_storages)
        else:
            logger.info("scale to 1")
            await scale_application(ops_test, app_name, 1)

            # Timeout is increeced due to k8s Init:CrashLoopBackOff status of postgresql pod
            logger.info("waiting for idle")
            await ops_test.model.wait_for_idle(status="active", timeout=2000)
            assert ops_test.model.applications[app_name].units[0].workload_status == "active"

        primary_name = ops_test.model.applications[app_name].units[0].name

        logger.info("waiting for postgresql")
        for attempt in Retrying(stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert await is_postgresql_ready(ops_test, primary_name)

        logger.info("getting storage id")
        storage_ids = await get_storage_ids(ops_test, primary_name)

        assert len(storage_ids)

        for storage_id in storage_ids:
            assert storage_id in garbage_storages

        # Check if storage exists after application deployed
        logger.info("verifying is storage exists")
        for storage_id in storage_ids:
            for attempt in Retrying(
                stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True
            ):
                with attempt:
                    assert await is_storage_exists(ops_test, storage_id)

        # Check that test database exists for new unit
        logger.info("checking db")
        assert await check_db(ops_test, app_name, TEST_DATABASE_NAME)

        if substrate == Substrate.vm:
            logger.info("removing charm")
            await ops_test.model.destroy_unit(primary_name)
        else:
            logger.info("scale to 0")
            await scale_application(ops_test, app_name, 0)

            logger.info("changing pv reclaim policy")
            primary_pvs = change_pvs_reclaim_policy(ops_test, primary_pvs, "Retain")

            logger.info("remove application")
            await ops_test.model.remove_application(app_name, block_until_done=True)

            for _, primary_pvc in primary_pvcs.items():
                logger.info(f"delete pvc {primary_pvc.metadata.name}")
                delete_pvc(ops_test, primary_pvc)


@pytest.mark.vm_only
@pytest.mark.abort_on_fail
@pytest.mark.skip(reason="Unstable")
async def test_app_resources_conflicts_v3(ops_test: OpsTest, charm: str):
    """Test application deploy in dirty environment with garbage storage from another application."""
    async with ops_test.fast_forward():
        logger.info("checking garbage storage")
        garbage_storages = None
        for attempt in Retrying(stop=stop_after_delay(30 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                garbage_storages = await get_detached_storages(ops_test)
                assert len(garbage_storages) == 4
                logger.info(f"Collected storages: {garbage_storages}")

        logger.info("deploying duplicate application with attached storage")
        await ops_test.model.deploy(
            charm,
            application_name=DUP_DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            attach_storage=[tag.storage(storage) for storage in garbage_storages],
            config={"profile": "testing"},
        )

        # Reducing the update status frequency to speed up the triggering of deferred events.
        await ops_test.model.set_config({"update-status-hook-interval": "10s"})

        logger.info("waiting for duplicate application to be waiting")
        try:
            await ops_test.model.wait_for_idle(
                apps=[DUP_DATABASE_APP_NAME], timeout=60, idle_period=30, status="waiting"
            )
        except TimeoutError:
            logger.info("Application is not in waiting state. Checking logs...")

        for attempt in Retrying(stop=stop_after_delay(60 * 10), wait=wait_fixed(3), reraise=True):
            with attempt:
                # Since application have postgresql db in storage from external application it should not be able to connect due to new password
                assert not await check_password_auth(
                    ops_test, ops_test.model.applications[DUP_DATABASE_APP_NAME].units[0].name
                )


@pytest.mark.k8s_only
@pytest.mark.abort_on_fail
async def test_app_resources_conflicts(ops_test: OpsTest, charm: str, charm_resources):  # noqa: C901
    """Test application deploy in dirty environment with garbage storage from another application."""
    global primary_pvs, primary_pvcs
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources=charm_resources,
            application_name=DUP_DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            trust=True,
            config={"profile": "testing"},
        )

        logger.info("waiting for idle")
        await ops_test.model.wait_for_idle(status="active", timeout=1000)
        assert (
            ops_test.model.applications[DUP_DATABASE_APP_NAME].units[0].workload_status == "active"
        )

        dup_primary_name = ops_test.model.applications[DUP_DATABASE_APP_NAME].units[0].name

        assert dup_primary_name

        logger.info(f"get pvc for {dup_primary_name}")
        dup_primary_pvcs = get_pvcs(ops_test, dup_primary_name)

        assert len(dup_primary_pvcs)

        logger.info("scale to 0")
        await scale_application(ops_test, DUP_DATABASE_APP_NAME, 0)

        for pvc_storage_name, pvc in dup_primary_pvcs.items():
            logger.info(f"load and change pv-name config for pvc {pvc.metadata.name}")
            dup_primary_pvcs[pvc_storage_name] = change_pvc_pv_name(
                pvc, primary_pvs[pvc_storage_name].metadata.name
            )

        for _, dup_primary_pvc in dup_primary_pvcs.items():
            logger.info(f"delete pvc {dup_primary_pvc.metadata.name}")
            delete_pvc(ops_test, dup_primary_pvc)

        client = Client(namespace=ops_test.model.name)
        for _, dup_primary_pvc in dup_primary_pvcs.items():
            logger.info(f"waiting for pvc {dup_primary_pvc.metadata.name} to be deleted")
            for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True):
                with attempt:
                    try:
                        client.get(PersistentVolumeClaim, name=dup_primary_pvc.metadata.name)
                    except ApiError as e:
                        if e.status.code == 404:
                            break
                        raise
                    raise Exception(f"pvc {dup_primary_pvc.metadata.name} still exists")

        for _, primary_pv in primary_pvs.items():
            logger.info(f"remove claimref from pv {primary_pv.metadata.name}")
            remove_pv_claimref(ops_test, primary_pv)

        for _, dup_primary_pvc in dup_primary_pvcs.items():
            logger.info(f"apply pvc {dup_primary_pvc.metadata.name}")
            apply_pvc_config(ops_test, dup_primary_pvc)

        logger.info("scale to 1")
        await ops_test.model.applications[DUP_DATABASE_APP_NAME].scale(1)

        logger.info("waiting for duplicate application to be blocked")
        try:
            await ops_test.model.wait_for_idle(
                apps=[DUP_DATABASE_APP_NAME], timeout=500, status="blocked"
            )
        except TimeoutError:
            logger.info("Application is not in blocked state. Checking logs...")

        # Since application have postgresql db in storage from external application it should not be able to connect due to new password
        logger.info("checking operator password auth")
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert await check_system_id_mismatch(ops_test, dup_primary_name)
