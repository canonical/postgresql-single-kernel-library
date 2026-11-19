#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest
from jubilant import Juju

from tests.integration.charm.backup_helpers import GCP, pitr_backup_operations
from tests.integration.types import Substrate

S3_INTEGRATOR_APP_NAME = "s3-integrator"
TLS_CERTIFICATES_APP_NAME = "self-signed-certificates"
TLS_CHANNEL = "1/stable"


@pytest.mark.abort_on_fail
def test_pitr_backup_gcp(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    gcp_cloud_configs: tuple[dict, dict],
) -> None:
    """Build, deploy two units of PostgreSQL and do backup in GCP. Then, write new data into DB, switch WAL file and test point-in-time-recovery restore action."""
    config, credentials = gcp_cloud_configs

    pitr_backup_operations(
        juju,
        charm,
        app_name,
        charm_resources,
        substrate,
        S3_INTEGRATOR_APP_NAME,
        TLS_CERTIFICATES_APP_NAME,
        TLS_CHANNEL,
        credentials,
        GCP,
        config,
    )
