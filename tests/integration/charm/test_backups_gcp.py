#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import uuid

import jubilant
import pytest
from jubilant import Juju
from lightkube import Client
from lightkube.resources.core_v1 import Pod
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tests.integration.charm.backup_helpers import GCP, backup_operations
from tests.integration.charm.helpers import get_password, wait_for_idle_on_blocked
from tests.integration.helpers.juju import (
    fast_forward,
    get_unit_address,
    remove_application,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE = "the S3 repository has backups from another cluster"
FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE = (
    "failed to access/create the bucket, check your S3 settings"
)
FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE = "failed to initialize stanza, check your S3 settings"
S3_INTEGRATOR_APP_NAME = "s3-integrator"
tls_certificates_app_name = "self-signed-certificates"
tls_channel = "1/stable"

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
def test_backup_gcp(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    gcp_cloud_configs: tuple[dict, dict],
) -> None:
    """Build and deploy two units of PostgreSQL in GCP, test backup and restore actions."""
    config = gcp_cloud_configs[0]
    credentials = gcp_cloud_configs[1]

    backup_operations(
        juju,
        charm,
        app_name,
        charm_resources,
        substrate,
        S3_INTEGRATOR_APP_NAME,
        tls_certificates_app_name,
        tls_channel,
        credentials,
        GCP,
        config,
    )
    database_app_name = f"{app_name}-gcp"

    # Remove the database app.
    remove_application(juju, database_app_name)

    # Remove the TLS operator.
    remove_application(juju, tls_certificates_app_name)


@pytest.mark.abort_on_fail
def test_restore_on_new_cluster(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    gcp_cloud_configs: tuple[dict, dict],
) -> None:
    """Test that is possible to restore a backup to another PostgreSQL cluster."""
    # VM reuses the name of the removed app (so its unit numbering continues at 2);
    # K8s uses a new name.
    if substrate == Substrate.vm:
        previous_database_app_name = f"{app_name}-gcp"
        previous_unit_number = 2
    else:
        previous_database_app_name = f"old-{app_name}-gcp"
        previous_unit_number = 0
    database_app_name = f"new-{app_name}"
    for name in (previous_database_app_name, database_app_name):
        juju.deploy(
            charm,
            app=name,
            base="ubuntu@24.04",
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )
    juju.integrate(previous_database_app_name, S3_INTEGRATOR_APP_NAME)
    juju.integrate(database_app_name, S3_INTEGRATOR_APP_NAME)
    with fast_forward(juju):
        logger.info(
            "waiting for the database charm to become blocked due to existing backups from another cluster in the repository"
        )
        wait_for_idle_on_blocked(
            juju,
            previous_database_app_name,
            previous_unit_number,
            S3_INTEGRATOR_APP_NAME,
            ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
        )
        logger.info(
            "waiting for the database charm to become blocked due to existing backups from another cluster in the repository"
        )
        wait_for_idle_on_blocked(
            juju,
            database_app_name,
            0,
            S3_INTEGRATOR_APP_NAME,
            ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
        )

    # Remove the database app with the same name as the previous one (that was used only to test
    # that the cluster becomes blocked).
    remove_application(juju, previous_database_app_name)

    # Run the "list backups" action.
    unit_name = f"{database_app_name}/0"
    logger.info("listing the available backups")
    backups = juju.run(unit_name, "list-backups").results.get("backups")
    assert backups, "backups not outputted"
    wait_for_idle_on_blocked(
        juju,
        database_app_name,
        0,
        S3_INTEGRATOR_APP_NAME,
        ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    )

    # Run the "restore backup" action.
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
    ):
        with attempt:
            logger.info("restoring the backup")
            # The list-backups output contains real backup entries (action = "full backup",
            # "differential backup", "incremental backup") as well as restore/timeline entries
            # (action = "restore") that cannot be used without the restore-to-time parameter.
            # Select only lines where the action column contains "backup" to avoid picking
            # a timeline entry that would cause: "Cannot restore to the timeline without
            # restore-to-time parameter".
            real_backup_lines = [
                line
                for line in backups.splitlines()
                for parts in [line.split("|")]
                if len(parts) > 1 and "backup" in parts[1].lower()
            ]
            assert real_backup_lines, (
                f"No valid backup entry found in list-backups output:\n{backups}"
            )
            backup_id = real_backup_lines[-1].split()[0]
            task = juju.run(unit_name, "restore", {"backup-id": backup_id})
            restore_status = task.results.get("restore-status")
            assert restore_status, f"restore hasn't succeeded: {task.results}"

    # Wait for the restore to complete.
    with fast_forward(juju):
        wait_for_idle_on_blocked(
            juju,
            database_app_name,
            0,
            S3_INTEGRATOR_APP_NAME,
            ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
        )

    # Check that the backup was correctly restored by having only the first created table.
    logger.info("checking that the backup was correctly restored")
    password = get_password(juju, database_app_name)
    address = get_unit_address(juju, unit_name)
    with db_connect(host=address, password=password) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables"
            " WHERE table_schema = 'public' AND table_name = 'backup_table_1');"
        )
        assert cursor.fetchone()[0], (
            "backup wasn't correctly restored: table 'backup_table_1' doesn't exist"
        )
    connection.close()


def test_invalid_config_and_recovery_after_fixing_it(
    juju: Juju, app_name: str, gcp_cloud_configs: tuple[dict, dict]
) -> None:
    """Test that the charm can handle invalid and valid backup configurations."""
    database_app_name = f"new-{app_name}"

    # Provide invalid backup configurations.
    logger.info("configuring S3 integrator for an invalid cloud")
    juju.config(
        S3_INTEGRATOR_APP_NAME,
        {
            "endpoint": "endpoint",
            "bucket": "bucket",
            "path": "path",
            "region": "region",
        },
    )
    juju.run(
        f"{S3_INTEGRATOR_APP_NAME}/0",
        "sync-s3-credentials",
        {"access-key": "access-key", "secret-key": "secret-key"},
    )
    logger.info("waiting for the database charm to become blocked")
    wait_for_idle_on_blocked(
        juju,
        database_app_name,
        0,
        S3_INTEGRATOR_APP_NAME,
        FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE,
    )

    # Provide valid backup configurations, but from another cluster repository.
    logger.info(
        "configuring S3 integrator for a valid cloud, but with the path of another cluster repository"
    )
    juju.config(S3_INTEGRATOR_APP_NAME, gcp_cloud_configs[0])
    juju.run(f"{S3_INTEGRATOR_APP_NAME}/0", "sync-s3-credentials", gcp_cloud_configs[1])
    logger.info("waiting for the database charm to become blocked")
    wait_for_idle_on_blocked(
        juju,
        database_app_name,
        0,
        S3_INTEGRATOR_APP_NAME,
        ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
        timeout=30 * 60,
    )

    # Provide valid backup configurations, with another path in the S3 bucket.
    logger.info("configuring S3 integrator for a valid cloud")
    config = gcp_cloud_configs[0].copy()
    config["path"] = f"/{app_name}/{uuid.uuid1()}"
    juju.config(S3_INTEGRATOR_APP_NAME, config)
    logger.info("waiting for the database charm to become active")
    juju.wait(
        wait_for_apps_status(jubilant.all_active, database_app_name, S3_INTEGRATOR_APP_NAME),
        error=jubilant.any_error,
        timeout=10 * 60,
    )


@pytest.mark.k8s_only
def test_delete_pod(juju: Juju, app_name: str, gcp_cloud_configs: tuple[dict, dict]) -> None:
    logger.info("Getting original backup config")
    database_app_name = f"new-{app_name}"
    unit_name = f"{database_app_name}/0"
    original_pgbackrest_config = juju.ssh(
        unit_name, "cat /etc/pgbackrest.conf", container="postgresql"
    )

    # delete the pod
    logger.info("Deleting the pod")
    client = Client(namespace=(juju.model or "").split(":")[-1])
    client.delete(Pod, name=f"{database_app_name}-0")

    # Wait and get the primary again (which can be any unit, including the previous primary).
    with fast_forward(juju):
        juju.wait(
            wait_for_apps_status(jubilant.all_active, database_app_name),
            error=jubilant.any_error,
            timeout=10 * 60,
        )

    new_pgbackrest_config = juju.ssh(unit_name, "cat /etc/pgbackrest.conf", container="postgresql")
    assert original_pgbackrest_config == new_pgbackrest_config, "Pgbackrest config not rerendered"


def test_block_on_missing_region(
    juju: Juju, app_name: str, gcp_cloud_configs: tuple[dict, dict]
) -> None:
    juju.config(S3_INTEGRATOR_APP_NAME, {**gcp_cloud_configs[0], "region": ""})
    database_app_name = f"new-{app_name}"
    logger.info("waiting for the database charm to become blocked")
    unit = f"{database_app_name}/0"
    juju.wait(
        lambda status: (
            status.apps[database_app_name].units[unit].workload_status.message
            == FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE
        ),
        timeout=30 * 60,
    )
