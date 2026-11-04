#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging

import jubilant
import pytest
from jubilant import Juju
from single_kernel_postgresql.config import literals

from tests.integration.charm.helpers import get_db_primary_unit, run_command_on_unit
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

# Debian compatibility symlink to the pgdata directory on K8s.
K8S_PGDATA_PATH = f"/{literals.K8S_DEBIAN_DATA_SYMLINK}/main"


@pytest.mark.abort_on_fail
def test_storage(juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate):
    """Build and deploy the charm and check its storage list."""
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            base="ubuntu@24.04",
            config={"profile": "testing"},
            num_units=1,
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_active, app_name),
            error=jubilant.any_error,
            timeout=1000,
        )

        logger.info("Checking charm storages")
        expected_storages = ["archive/0", "data/1", "logs/2", "temp/3"]
        storages = json.loads(juju.cli("storage", "--format", "json"))["storage"]
        assert sorted(storages) == expected_storages, f"Unexpected storages: {sorted(storages)}"
        for key, storage in storages.items():
            assert f"{app_name}/0" in storage["attachments"]["units"], (
                f"Storage {key} not attached to {app_name}/0"
            )


@pytest.mark.k8s_only
def test_filling_and_emptying_pgdata_storage(
    juju: Juju, app_name: str, architecture: str, substrate: Substrate
):
    """Saturate charm's pgdata volume."""
    if architecture != "amd64":
        pytest.skip("Requires amd64 architecture")

    # Saturate pgdata storage with random data
    primary = get_db_primary_unit(juju, app_name)
    run_command_on_unit(
        juju,
        primary,
        f"FREE_SPACE=$(df --output=avail {K8S_PGDATA_PATH} | tail -1) && dd if=/dev/urandom of={K8S_PGDATA_PATH}/tmp bs=1M count=$(( (FREE_SPACE * 91 / 100) / 1024 ))",
        substrate,
    )

    # wait for charm to get blocked
    with fast_forward(juju):
        juju.wait(
            lambda status: any(
                unit.workload_status.current == "blocked"
                and unit.workload_status.message == literals.INSUFFICIENT_SIZE_WARNING
                for unit in status.get_units(app_name).values()
            ),
            timeout=500,
        )

    # Delete big file to release storage space
    run_command_on_unit(juju, primary, f"rm {K8S_PGDATA_PATH}/tmp", substrate)

    # wait for charm to resolve
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=1000,
    )
