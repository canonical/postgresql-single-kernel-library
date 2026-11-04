# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for the backup tests."""

import logging
import os
import uuid

import boto3
import botocore.loaders
import botocore.regions
import jubilant
from jubilant import Juju
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tests.integration.charm.helpers import get_password, get_primary, scale_application
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

AWS = "AWS"
GCP = "GCP"


def construct_endpoint(endpoint: str, region: str) -> str:
    """Construct the S3 service endpoint using the region.

    This is needed when the provided endpoint is from AWS, and it doesn't contain the region.
    """
    # Load endpoints data.
    loader = botocore.loaders.create_loader()
    data = loader.load_data("endpoints")

    # Construct the endpoint using the region.
    resolver = botocore.regions.EndpointResolver(data)
    endpoint_data = resolver.construct_endpoint("s3", region)

    # Use the built endpoint if it is an AWS endpoint.
    if endpoint_data and endpoint.endswith(endpoint_data["dnsSuffix"]):
        endpoint = f"{endpoint.split('://')[0]}://{endpoint_data['hostname']}"

    return endpoint


def get_cloud_config(cloud: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return the S3 integrator config and credentials of a cloud."""
    if cloud == AWS:
        return {
            "endpoint": "https://s3.amazonaws.com",
            "bucket": "data-charms-testing",
            "path": f"/postgresql-k8s/{uuid.uuid1()}",
            "region": "us-east-1",
        }, {
            "access-key": os.environ["AWS_ACCESS_KEY"],
            "secret-key": os.environ["AWS_SECRET_KEY"],
        }
    if cloud == GCP:
        return {
            "endpoint": "https://storage.googleapis.com",
            "bucket": "data-charms-testing",
            "path": f"/postgresql-k8s/{uuid.uuid1()}",
            "region": "us-east-1",
        }, {
            "access-key": os.environ["GCP_ACCESS_KEY"],
            "secret-key": os.environ["GCP_SECRET_KEY"],
        }
    raise ValueError(f"Unknown cloud {cloud}")


def cleanup_cloud(config: dict[str, str], credentials: dict[str, str]) -> None:
    """Delete the objects created by the tests in the bucket path."""
    logger.info("deleting the previously created backups")
    session = boto3.session.Session(
        aws_access_key_id=credentials["access-key"],
        aws_secret_access_key=credentials["secret-key"],
        region_name=config["region"],
    )
    s3 = session.resource(
        "s3", endpoint_url=construct_endpoint(config["endpoint"], config["region"])
    )
    bucket = s3.Bucket(config["bucket"])
    # GCS doesn't support batch delete operation, so delete the objects one by one.
    for bucket_object in bucket.objects.filter(Prefix=config["path"].lstrip("/")):
        bucket_object.delete()


def _wait_for_active(juju: Juju, *apps: str, timeout: float = 1000, successes: int = 3) -> None:
    """Wait for the apps (all of them if none given) to be active and idle."""
    juju.wait(
        wait_for_apps_status(jubilant.all_active, *apps),
        error=jubilant.any_error,
        timeout=timeout,
        successes=successes,
    )


def _assert_tables(host: str, password: str, expected: dict[str, bool]) -> None:
    """Assert which of the backup test tables exist."""
    with db_connect(host=host, password=password) as connection, connection.cursor() as cursor:
        for table, exists in expected.items():
            cursor.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables"
                " WHERE table_schema = 'public' AND table_name = %s);",
                (table,),
            )
            assert cursor.fetchone()[0] is exists, (
                f"backup wasn't correctly restored: table '{table}'"
                f" {"doesn't exist" if exists else 'exists'}"
            )
    connection.close()


def _create_table(host: str, password: str, table: str) -> None:
    with db_connect(host=host, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table} (test_collumn INT );")
    connection.close()


def _restore(juju: Juju, unit_name: str, backup_line: str) -> None:
    """Run the restore action with the backup id of a list-backups line, with retries."""
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
    ):
        with attempt:
            logger.info("restoring the backup")
            backup_id = backup_line.split()[0]
            task = juju.run(unit_name, "restore", {"backup-id": backup_id})
            assert task.results.get("restore-status"), "restore hasn't succeeded"


def backup_deploy(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources: dict[str, str] | None,
    substrate: Substrate,
    s3_integrator_app_name: str,
    tls_certificates_app_name: str | None,
    tls_channel: str | None,
    credentials: dict[str, str],
    cloud: str,
    config: dict[str, str],
) -> str:
    """Deploy PostgreSQL related to the S3 integrator (and TLS) and return its app name."""
    use_tls = all([tls_certificates_app_name, tls_channel])
    # Deploy S3 Integrator and TLS Certificates Operator.
    juju.deploy(s3_integrator_app_name)
    if use_tls:
        juju.deploy(tls_certificates_app_name, channel=tls_channel)

    # Deploy and relate PostgreSQL to S3 integrator (one database app for each cloud for now
    # as archive_mode is disabled after restoring the backup) and to TLS Certificates Operator
    # (to be able to create backups from replicas).
    database_app_name = f"{app_name}-{cloud.lower()}"
    juju.deploy(
        charm,
        app=database_app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        num_units=2,
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )

    if use_tls:
        juju.integrate(
            f"{database_app_name}:client-certificates", f"{tls_certificates_app_name}:certificates"
        )
        juju.integrate(
            f"{database_app_name}:peer-certificates", f"{tls_certificates_app_name}:certificates"
        )
    with fast_forward(juju, "60s"):
        juju.wait(wait_for_apps_status(jubilant.all_active, database_app_name), timeout=1000)

    # Configure and set access and secret keys.
    logger.info(f"configuring S3 integrator for {cloud}")
    juju.config(s3_integrator_app_name, config)
    juju.run(f"{s3_integrator_app_name}/0", "sync-s3-credentials", credentials)

    juju.integrate(database_app_name, s3_integrator_app_name)
    with fast_forward(juju, "60s"):
        _wait_for_active(juju, database_app_name, s3_integrator_app_name, timeout=1500)
    return database_app_name


def backup_operations(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources: dict[str, str] | None,
    substrate: Substrate,
    s3_integrator_app_name: str,
    tls_certificates_app_name: str | None,
    tls_channel: str | None,
    credentials: dict[str, str],
    cloud: str,
    config: dict[str, str],
) -> None:
    """Basic set of operations for backup testing in different cloud providers."""
    database_app_name = backup_deploy(
        juju,
        charm,
        app_name,
        charm_resources,
        substrate,
        s3_integrator_app_name,
        tls_certificates_app_name,
        tls_channel,
        credentials,
        cloud,
        config,
    )

    primary = get_primary(juju, f"{database_app_name}/0")
    replica = next(unit for unit in get_app_units(juju, database_app_name) if unit != primary)

    # Write some data.
    password = get_password(juju, database_app_name)
    address = get_unit_address(juju, primary)
    logger.info("creating a table in the database")
    _create_table(address, password, "backup_table_1")

    # With a stable cluster, run the "create backup" action.
    with fast_forward(juju):
        _wait_for_active(juju, successes=30)
    logger.info("creating a backup")
    task = juju.run(replica, "create-backup")
    assert task.results.get("backup-status"), "backup hasn't succeeded"
    with fast_forward(juju):
        _wait_for_active(juju, successes=30)

    # Run the "list backups" action.
    logger.info("listing the available backups")
    backups = juju.run(replica, "list-backups").results.get("backups")
    # 5 lines for header output, 1 backup line ==> 6 total lines
    assert len(backups.split("\n")) == 6, "full backup is not outputted"
    _wait_for_active(juju)

    # Write some data.
    logger.info("creating a second table in the database")
    _create_table(address, password, "backup_table_2")

    # Run the "create backup" action.
    logger.info("creating a backup")
    task = juju.run(replica, "create-backup", {"type": "differential"})
    assert task.results.get("backup-status"), "backup hasn't succeeded"
    with fast_forward(juju):
        _wait_for_active(juju)

    # Run the "list backups" action.
    logger.info("listing the available backups")
    backups = juju.run(replica, "list-backups").results.get("backups")
    # 5 lines for header output, 2 backup lines ==> 7 total lines
    assert len(backups.split("\n")) == 7, "differential backup is not outputted"
    _wait_for_active(juju)

    # Write some data.
    logger.info("creating a third table in the database")
    _create_table(address, password, "backup_table_3")

    # Scale down to be able to restore (on K8s only the last pod can be removed).
    with fast_forward(juju):
        if substrate == Substrate.vm:
            juju.remove_unit(replica)
            juju.wait(
                lambda status: len(status.get_units(database_app_name)) == 1, timeout=10 * 60
            )
        else:
            scale_application(juju, database_app_name, 1, substrate)

    remaining_unit = next(iter(get_app_units(juju, database_app_name)))

    # Run the "restore backup" action for differential backup.
    _restore(juju, remaining_unit, backups.split("\n")[-1])

    # Wait for the restore to complete.
    with fast_forward(juju):
        _wait_for_active(juju)

    # Check that the backup was correctly restored by having only the first two tables.
    logger.info("checking that the backup was correctly restored")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    _assert_tables(
        address,
        password,
        {"backup_table_1": True, "backup_table_2": True, "backup_table_3": False},
    )

    # Run the "restore backup" action for full backup.
    _restore(juju, remaining_unit, backups.split("\n")[-2])

    # Wait for the restore to complete.
    with fast_forward(juju):
        _wait_for_active(juju)

    # Check that the backup was correctly restored by having only the first created table.
    logger.info("checking that the backup was correctly restored")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    _assert_tables(
        address,
        password,
        {"backup_table_1": True, "backup_table_2": False, "backup_table_3": False},
    )
