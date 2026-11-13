#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging

import jubilant
import pytest
from jubilant import Juju

from tests.integration.helpers.juju import wait_for_apps_status

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.vm_only

# rootfs (machine-scoped) is the only pool that reproduces the stuck unmount on
# Juju 4.0: the detachable lxd pool destroys the container before detaching.
ROOTFS_STORAGE = dict.fromkeys(("archive", "data", "logs", "temp"), "rootfs")


@pytest.mark.abort_on_fail
def test_storage_released_on_removal(juju: Juju, charm, app_name: str):
    """Graceful removal must release the storage so teardown completes.

    Regression test for canonical/postgresql-operator#1550: without stopping the
    workload in the ``storage-detaching`` hook the snap keeps the storage mounts
    busy, so Juju's unmount fails ("target is busy") and teardown hangs.
    """
    juju.deploy(
        charm,
        app_name,
        num_units=1,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        storage=ROOTFS_STORAGE,
        force=True,
    )
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), error=jubilant.any_error)

    logger.info("Removing %s and waiting for a clean teardown", app_name)
    juju.remove_application(app_name, destroy_storage=True)
    juju.wait(lambda status: app_name not in status.apps, timeout=15 * 60)

    # Juju 4.0 prints nothing (not `{}`) for a model with no storage.
    raw = juju.cli("list-storage", "--format", "json")
    storages = json.loads(raw).get("storage", {}) if raw.strip() else {}
    detaching = [key for key, storage in storages.items() if storage["life"] == "detaching"]
    assert not detaching, f"storage stuck detaching after removal: {detaching}"
