#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import PersistentVolumeClaim
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    CHARM_BASE,
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
    scale_ha_application,
)
from tests.integration.helpers.juju import fast_forward, wait_for_idle

TEST_DATABASE_NAME = "test_database"
DUP_DATABASE_APP_NAME = "postgres-test-dup"

VM_STORAGE = {
    "archive": "lxd-btrfs,2048M",
    "data": "lxd-btrfs,2048M",
    "logs": "lxd-btrfs,2048M",
    "temp": "lxd-btrfs,2048M",
}

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
def test_app_force_removal(
    juju: jubilant.Juju, charm: str, app_name: str, charm_resources, substrate: Substrates
):
    """Remove unit with force while storage is alive."""
    global primary_pvs, primary_pvcs
    with fast_forward(juju):
        # Deploy the charm.
        logger.info("deploying charm")
        juju.deploy(
            charm,
            app=app_name,
            num_units=1,
            base=CHARM_BASE,
            storage=VM_STORAGE if substrate == Substrates.VM else None,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrates.K8S,
        )

        logger.info("waiting for idle")
        wait_for_idle(juju, apps=[app_name], status="active", timeout=1500)
        assert (
            next(iter(juju.status().apps[app_name].units.values())).workload_status.current
            == "active"
        )

        primary_name = next(iter(juju.status().apps[app_name].units))

        logger.info("waiting for postgresql")
        for attempt in Retrying(stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert is_postgresql_ready(juju, primary_name)

        if substrate == Substrates.K8S:
            logger.info(f"get pvc for {primary_name}")
            primary_pvcs = get_pvcs(juju, primary_name)

            assert primary_pvcs

            logger.info(f"get pv for {primary_name}")
            primary_pvs = get_pvs(juju, primary_name)

            assert len(primary_pvs)

        logger.info("getting storage id")
        storage_ids = get_storage_ids(juju, primary_name)

        assert len(storage_ids)

        # Check if storage exists after application deployed
        logger.info("verifying that storage exists")
        for storage_id in storage_ids:
            for attempt in Retrying(
                stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True
            ):
                with attempt:
                    assert is_storage_exists(juju, storage_id)

        # Create test database to check there is no resources conflicts
        logger.info("creating db")
        create_db(juju, app_name, TEST_DATABASE_NAME)

        # Check that test database is not exists for new unit
        logger.info("checking db")
        assert check_db(juju, app_name, TEST_DATABASE_NAME)

        # Force remove unit without storage removal
        if substrate == Substrates.VM:
            logger.info("force removing charm")
            juju.remove_unit(primary_name, force=True, destroy_storage=False)
        else:
            logger.info("scale to 0 with force")
            remove_unit_force(juju, 1)

        # Storage should remain
        logger.info("verifying that storage exists")
        for storage_id in storage_ids:
            for attempt in Retrying(
                stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True
            ):
                with attempt:
                    assert is_storage_exists(juju, storage_id)


@pytest.mark.abort_on_fail
def test_app_garbage_ignorance(juju: jubilant.Juju, app_name: str, substrate: Substrates):
    """Test charm deploy in dirty environment with garbage storage."""
    global primary_pvs, primary_pvcs
    with fast_forward(juju):
        logger.info("checking garbage storage")
        garbage_storages = None
        for attempt in Retrying(stop=stop_after_delay(30 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                garbage_storages = get_detached_storages(juju)
                if substrate == Substrates.VM:
                    assert len(garbage_storages) == 4
                logger.info(f"Collected storages: {garbage_storages}")

        if substrate == Substrates.VM:
            logger.info("add unit with attached storage")
            add_unit_with_storage(juju, app_name, garbage_storages)
        else:
            logger.info("scale to 1")
            scale_ha_application(juju, app_name, 1)

            # Timeout is increeced due to k8s Init:CrashLoopBackOff status of postgresql pod
            logger.info("waiting for idle")
            wait_for_idle(juju, status="active", timeout=2000)
            assert (
                next(iter(juju.status().apps[app_name].units.values())).workload_status.current
                == "active"
            )

        primary_name = next(iter(juju.status().apps[app_name].units))

        logger.info("waiting for postgresql")
        for attempt in Retrying(stop=stop_after_delay(15 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert is_postgresql_ready(juju, primary_name)

        logger.info("getting storage id")
        storage_ids = get_storage_ids(juju, primary_name)

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
                    assert is_storage_exists(juju, storage_id)

        # Check that test database exists for new unit
        logger.info("checking db")
        assert check_db(juju, app_name, TEST_DATABASE_NAME)

        if substrate == Substrates.VM:
            logger.info("removing charm")
            juju.remove_unit(primary_name)
        else:
            logger.info("scale to 0")
            scale_ha_application(juju, app_name, 0)

            logger.info("changing pv reclaim policy")
            primary_pvs = change_pvs_reclaim_policy(juju, primary_pvs, "Retain")

            logger.info("remove application")
            juju.remove_application(app_name)
            juju.wait(
                lambda status: app_name not in status.apps,
                timeout=1000,
                delay=0.5,
                successes=1,
            )

            for _, primary_pvc in primary_pvcs.items():
                logger.info(f"delete pvc {primary_pvc.metadata.name}")
                delete_pvc(juju, primary_pvc)


@pytest.mark.vm_only
@pytest.mark.abort_on_fail
@pytest.mark.skip(reason="Unstable")
def test_app_resources_conflicts_v3(juju: jubilant.Juju, charm: str):
    """Test application deploy in dirty environment with garbage storage from another application."""
    with fast_forward(juju):
        logger.info("checking garbage storage")
        garbage_storages = None
        for attempt in Retrying(stop=stop_after_delay(30 * 3), wait=wait_fixed(3), reraise=True):
            with attempt:
                garbage_storages = get_detached_storages(juju)
                assert len(garbage_storages) == 4
                logger.info(f"Collected storages: {garbage_storages}")

        logger.info("deploying duplicate application with attached storage")
        juju.deploy(
            charm,
            app=DUP_DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            attach_storage=garbage_storages,
            config={"profile": "testing"},
        )

        # Reducing the update status frequency to speed up the triggering of deferred events.
        juju.model_config({"update-status-hook-interval": "10s"})

        logger.info("waiting for duplicate application to be waiting")
        try:
            wait_for_idle(
                juju, apps=[DUP_DATABASE_APP_NAME], timeout=60, idle_period=30, status="waiting"
            )
        except TimeoutError:
            logger.info("Application is not in waiting state. Checking logs...")

        for attempt in Retrying(stop=stop_after_delay(60 * 10), wait=wait_fixed(3), reraise=True):
            with attempt:
                # Since application have postgresql db in storage from external application it should not be able to connect due to new password
                assert not check_password_auth(
                    juju, next(iter(juju.status().apps[DUP_DATABASE_APP_NAME].units))
                )


@pytest.mark.k8s_only
@pytest.mark.abort_on_fail
def test_app_resources_conflicts(juju: jubilant.Juju, charm: str, charm_resources):  # noqa: C901
    """Test application deploy in dirty environment with garbage storage from another application."""
    global primary_pvs, primary_pvcs
    with fast_forward(juju):
        juju.deploy(
            charm,
            resources=charm_resources,
            app=DUP_DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            trust=True,
            config={"profile": "testing"},
        )

        logger.info("waiting for idle")
        wait_for_idle(juju, status="active", timeout=1000)
        assert (
            next(
                iter(juju.status().apps[DUP_DATABASE_APP_NAME].units.values())
            ).workload_status.current
            == "active"
        )

        dup_primary_name = next(iter(juju.status().apps[DUP_DATABASE_APP_NAME].units))

        assert dup_primary_name

        logger.info(f"get pvc for {dup_primary_name}")
        dup_primary_pvcs = get_pvcs(juju, dup_primary_name)

        assert len(dup_primary_pvcs)

        logger.info("scale to 0")
        scale_ha_application(juju, DUP_DATABASE_APP_NAME, 0)

        for pvc_storage_name, pvc in dup_primary_pvcs.items():
            logger.info(f"load and change pv-name config for pvc {pvc.metadata.name}")
            dup_primary_pvcs[pvc_storage_name] = change_pvc_pv_name(
                pvc, primary_pvs[pvc_storage_name].metadata.name
            )

        for _, dup_primary_pvc in dup_primary_pvcs.items():
            logger.info(f"delete pvc {dup_primary_pvc.metadata.name}")
            delete_pvc(juju, dup_primary_pvc)

        client = Client(namespace=juju.status().model.name)
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
            remove_pv_claimref(juju, primary_pv)

        for _, dup_primary_pvc in dup_primary_pvcs.items():
            logger.info(f"apply pvc {dup_primary_pvc.metadata.name}")
            apply_pvc_config(juju, dup_primary_pvc)

        logger.info("scale to 1")
        juju.cli("scale-application", DUP_DATABASE_APP_NAME, "1")

        logger.info("waiting for duplicate application to be blocked")
        try:
            wait_for_idle(juju, apps=[DUP_DATABASE_APP_NAME], timeout=500, status="blocked")
        except TimeoutError:
            logger.info("Application is not in blocked state. Checking logs...")

        # Since application have postgresql db in storage from external application it should not be able to connect due to new password
        logger.info("checking operator password auth")
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
            with attempt:
                assert check_system_id_mismatch(juju, dup_primary_name)
