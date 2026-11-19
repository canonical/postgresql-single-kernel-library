# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for the backup tests."""

import base64
import dataclasses
import json
import logging
import os
import subprocess
import uuid

import boto3
import botocore.loaders
import botocore.regions
import jubilant
import pytest
from jubilant import Juju
from single_kernel_postgresql.utils.backup import CANNOT_RESTORE_PITR
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tests.integration.charm.helpers import get_password, get_primary, scale_application
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_unit_address,
    remove_application,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

AWS = "AWS"
GCP = "GCP"
CEPH = "CEPH"


@dataclasses.dataclass(frozen=True)
class ConnectionInformation:
    access_key_id: str
    secret_access_key: str
    bucket: str
    host: str
    cert: str


def _get_non_loopback_host_ip() -> str:
    """Return the host IP reachable by the units (LXD containers or K8s pods)."""
    output = subprocess.run(
        ["ip", "-4", "route", "get", "1.1.1.1"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return output.split("src", maxsplit=1)[1].split()[0]


def setup_microceph() -> ConnectionInformation:
    """Install microceph on the host with a TLS-enabled RGW and return its S3 connection info.

    The TLS certificates are written to the current directory.
    """
    logger.info("Setting up TLS certificates")
    subject = "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com"
    subprocess.run(["openssl", "genrsa", "-out", "./ca.key", "2048"], check=True)
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            "./ca.key",
            "-days",
            "1024",
            "-out",
            "./ca.crt",
            "-outform",
            "PEM",
            "-subj",
            subject,
        ],
        check=True,
    )
    subprocess.run(["openssl", "genrsa", "-out", "./server.key", "2048"], check=True)
    subprocess.run(
        [
            "openssl",
            "req",
            "-new",
            "-key",
            "./server.key",
            "-out",
            "./server.csr",
            "-subj",
            subject,
        ],
        check=True,
    )
    host_ip = _get_non_loopback_host_ip()
    with open("./extfile.cnf", "w") as extfile:
        extfile.write(f"subjectAltName = IP:{host_ip}\n")
    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            "./server.csr",
            "-CA",
            "./ca.crt",
            "-CAkey",
            "./ca.key",
            "-CAcreateserial",
            "-out",
            "./server.crt",
            "-days",
            "365",
            "-extfile",
            "./extfile.cnf",
        ],
        check=True,
    )

    logger.info("Setting up microceph")
    subprocess.run(
        ["sudo", "snap", "install", "microceph", "--channel", "squid/stable"], check=True
    )
    subprocess.run(["sudo", "microceph", "cluster", "bootstrap"], check=True)
    subprocess.run(["sudo", "microceph", "disk", "add", "loop,1G,3"], check=True)
    subprocess.run(
        'sudo microceph enable rgw --ssl-certificate="$(sudo base64 -w0 ./server.crt)" --ssl-private-key="$(sudo base64 -w0 ./server.key)"',
        shell=True,
        check=True,
    )
    output = subprocess.run(
        [
            "sudo",
            "microceph.radosgw-admin",
            "user",
            "create",
            "--uid",
            "test",
            "--display-name",
            "test",
        ],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout
    key = json.loads(output)["keys"][0]
    logger.info("Set up microceph")
    with open("./ca.crt", "rb") as ca_file:
        ca_cert = base64.b64encode(ca_file.read()).decode()
    return ConnectionInformation(
        key["access_key"], key["secret_key"], "testbucket", host_ip, ca_cert
    )


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


def pitr_backup_operations(
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
    """PITR backup and timelines management operations for the cloud tests.

    Below is presented algorithm in the next format: "(timeline): action_1 -> action_2".
    1: table -> backup_b1 -> test_data_td1 -> timestamp_ts1 -> test_data_td2 -> restore_ts1 => 2
    2: check_td1 -> check_not_td2 -> test_data_td3 -> restore_b1_latest => 3
    3: check_td1 -> check_td2 -> check_not_td3 -> test_data_td4 -> restore_t2_latest => 4
    4: check_td1 -> check_not_td2 -> check_td3 -> check_not_td4
    """
    use_tls = all([tls_certificates_app_name, tls_channel])
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
    password = get_password(juju, database_app_name)
    address = get_unit_address(juju, primary)

    logger.info("1: creating table")
    _create_pitr_table(address, password)

    logger.info("1: creating backup b1")
    task = juju.run(replica, "create-backup")
    assert task.results.get("backup-status"), "backup hasn't succeeded"
    with fast_forward(juju):
        _wait_for_active(juju, successes=15)
    backup_b1 = _get_most_recent_backup(juju, replica)

    logger.info("1: creating test data td1")
    _insert_test_data("test_data_td1", address, password)

    logger.info("1: get timestamp ts1")
    timestamp_ts1 = _get_current_timestamp(address, password)
    # Wrong timestamp pointing to one year ahead
    unreachable_timestamp_ts1 = timestamp_ts1.replace(
        timestamp_ts1[:4], str(int(timestamp_ts1[:4]) + 1), 1
    )

    logger.info("1: creating test data td2")
    _insert_test_data("test_data_td2", address, password)

    logger.info("1: switching wal")
    _switch_wal(address, password)

    logger.info("1: scaling down to do restore")
    # On K8s only the last pod can be removed.
    with fast_forward(juju, "60s"):
        if substrate == Substrate.vm:
            juju.remove_unit(replica)
            juju.wait(
                lambda status: (
                    len(status.get_units(database_app_name)) == 1
                    and jubilant.all_active(status, database_app_name)
                    and jubilant.all_agents_idle(status, database_app_name)
                ),
                error=jubilant.any_error,
                timeout=1000,
            )
        else:
            scale_application(juju, database_app_name, 1, substrate)
    remaining_unit = next(iter(get_app_units(juju, database_app_name)))

    logger.info("1: restoring the backup b1 with bad restore-to-time parameter")
    with pytest.raises(jubilant.TaskError):
        juju.run(
            remaining_unit, "restore", {"backup-id": backup_b1, "restore-to-time": "bad data"}
        )

    logger.info("1: restoring the backup b1 with unreachable restore-to-time parameter")
    juju.run(
        remaining_unit,
        "restore",
        {"backup-id": backup_b1, "restore-to-time": unreachable_timestamp_ts1},
    )
    logger.info("1: waiting for the database charm to become blocked after restore")
    with fast_forward(juju):
        juju.wait(
            lambda status: (
                status.get_units(database_app_name)[remaining_unit].workload_status.message
                == CANNOT_RESTORE_PITR
            ),
            timeout=1000,
        )
    logger.info(
        "1: database charm become in blocked state after restore, as supposed to be with unreachable PITR parameter"
    )

    _pitr_restore(
        juju,
        remaining_unit,
        {"restore-to-time": timestamp_ts1},
        "1: restore to the timestamp ts1",
    )

    logger.info("2: successful restore")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    timeline_t2 = _get_most_recent_backup(juju, remaining_unit)
    assert backup_b1 != timeline_t2, "2: timeline 2 do not exist in list-backups action or bad"

    logger.info("2: checking test data td1")
    assert _check_test_data("test_data_td1", address, password), "2: test data td1 should exist"

    logger.info("2: checking not test data td2")
    assert not _check_test_data("test_data_td2", address, password), (
        "2: test data td2 shouldn't exist"
    )

    logger.info("2: creating test data td3")
    _insert_test_data("test_data_td3", address, password)

    logger.info("2: get timestamp ts2")
    timestamp_ts2 = _get_current_timestamp(address, password)

    logger.info("2: creating test data td4")
    _insert_test_data("test_data_td4", address, password)

    logger.info("2: switching wal")
    _switch_wal(address, password)

    _pitr_restore(
        juju,
        remaining_unit,
        {"backup-id": backup_b1, "restore-to-time": "latest"},
        "2: restore the backup b1 to the latest",
    )

    logger.info("3: successful restore")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    timeline_t3 = _get_most_recent_backup(juju, remaining_unit)
    assert backup_b1 != timeline_t3 and timeline_t2 != timeline_t3, (
        "3: timeline 3 do not exist in list-backups action or bad"
    )

    logger.info("3: checking test data td1")
    assert _check_test_data("test_data_td1", address, password), "3: test data td1 should exist"

    logger.info("3: checking test data td2")
    assert _check_test_data("test_data_td2", address, password), "3: test data td2 should exist"

    logger.info("3: checking not test data td3")
    assert not _check_test_data("test_data_td3", address, password), (
        "3: test data td3 shouldn't exist"
    )

    logger.info("3: checking not test data td4")
    assert not _check_test_data("test_data_td4", address, password), (
        "3: test data td4 shouldn't exist"
    )

    logger.info("3: switching wal")
    _switch_wal(address, password)

    _pitr_restore(
        juju,
        remaining_unit,
        {"backup-id": timeline_t2, "restore-to-time": "latest"},
        "3: restore the timeline 2 to the latest",
    )

    logger.info("4: successful restore")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    timeline_t4 = _get_most_recent_backup(juju, remaining_unit)
    assert (
        backup_b1 != timeline_t4 and timeline_t2 != timeline_t4 and timeline_t3 != timeline_t4
    ), "4: timeline 4 do not exist in list-backups action or bad"

    logger.info("4: checking test data td1")
    assert _check_test_data("test_data_td1", address, password), "4: test data td1 should exist"

    logger.info("4: checking not test data td2")
    assert not _check_test_data("test_data_td2", address, password), (
        "4: test data td2 shouldn't exist"
    )

    logger.info("4: checking test data td3")
    assert _check_test_data("test_data_td3", address, password), "4: test data td3 should exist"

    logger.info("4: checking test data td4")
    assert _check_test_data("test_data_td4", address, password), "4: test data td4 should exist"

    logger.info("4: switching wal")
    _switch_wal(address, password)

    _pitr_restore(
        juju,
        remaining_unit,
        {"restore-to-time": timestamp_ts2},
        "4: restore to the timestamp ts2",
    )

    logger.info("5: successful restore")
    address = get_unit_address(juju, get_primary(juju, remaining_unit))
    timeline_t5 = _get_most_recent_backup(juju, remaining_unit)
    assert (
        backup_b1 != timeline_t5
        and timeline_t2 != timeline_t5
        and timeline_t3 != timeline_t5
        and timeline_t4 != timeline_t5
    ), "5: timeline 5 do not exist in list-backups action or bad"

    logger.info("5: checking test data td1")
    assert _check_test_data("test_data_td1", address, password), "5: test data td1 should exist"

    logger.info("5: checking not test data td2")
    assert not _check_test_data("test_data_td2", address, password), (
        "5: test data td2 shouldn't exist"
    )

    logger.info("5: checking test data td3")
    assert _check_test_data("test_data_td3", address, password), "5: test data td3 should exist"

    logger.info("5: checking not test data td4")
    assert not _check_test_data("test_data_td4", address, password), (
        "5: test data td4 shouldn't exist"
    )

    _wait_for_active(juju, successes=15)

    # Remove the database app and the TLS operator.
    remove_application(juju, database_app_name)
    if use_tls:
        remove_application(juju, tls_certificates_app_name)


def _pitr_restore(juju: Juju, unit_name: str, params: dict[str, str], description: str) -> None:
    """Run the restore action with retries and wait for the cluster to be active again."""
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
    ):
        with attempt:
            logger.info(description)
            task = juju.run(unit_name, "restore", params)
            assert task.results.get("restore-status"), f"{description} hasn't succeeded"
    _wait_for_active(juju, successes=30)


def _create_pitr_table(host: str, password: str) -> None:
    with db_connect(host=host, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute("CREATE TABLE IF NOT EXISTS backup_table (test_column TEXT);")
    connection.close()


def _insert_test_data(td: str, host: str, password: str) -> None:
    with db_connect(host=host, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute(
            "INSERT INTO backup_table (test_column) VALUES (%s);",
            (td,),
        )
    connection.close()


def _check_test_data(td: str, host: str, password: str) -> bool:
    with db_connect(host=host, password=password) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM backup_table WHERE test_column = %s);",
            (td,),
        )
        res = cursor.fetchone()[0]
    connection.close()
    return res


def _get_current_timestamp(host: str, password: str) -> str:
    with db_connect(host=host, password=password) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT current_timestamp;")
        timestamp = str(cursor.fetchone()[0])
    connection.close()
    return timestamp


def _switch_wal(host: str, password: str) -> None:
    with db_connect(host=host, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute("SELECT pg_switch_wal();")
    connection.close()


def _get_most_recent_backup(juju: Juju, unit_name: str) -> str:
    logger.info("listing the available backups")
    backups = juju.run(unit_name, "list-backups").results.get("backups")
    assert backups, "backups not outputted"
    _wait_for_active(juju, successes=15)
    most_recent_backup = backups.split("\n")[-1]
    return most_recent_backup.split()[0]
