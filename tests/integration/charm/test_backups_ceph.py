#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
from jubilant import Juju

from tests.integration.charm.backup_helpers import CEPH, backup_operations
from tests.integration.types import Substrate

S3_INTEGRATOR_APP_NAME = "s3-integrator"


def test_backup_ceph(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    ceph_cloud_configs: tuple[dict, dict],
) -> None:
    """Build and deploy two units of PostgreSQL in microceph, test backup and restore actions."""
    config, credentials = ceph_cloud_configs

    backup_operations(
        juju,
        charm,
        app_name,
        charm_resources,
        substrate,
        S3_INTEGRATOR_APP_NAME,
        None,
        None,
        credentials,
        CEPH,
        config,
    )
