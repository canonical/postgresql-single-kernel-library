#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from time import sleep
from typing import get_args

import jubilant
import psycopg2
import pytest
import requests
from jubilant import Juju
from lightkube import Client
from lightkube.resources.core_v1 import Endpoints, Pod, Service
from psycopg2 import sql
from single_kernel_postgresql.config import literals
from single_kernel_postgresql.config.locales import K8S_LOCALES, SNAP_LOCALES
from tenacity import Retrying, stop_after_attempt, stop_after_delay, wait_exponential, wait_fixed

from tests.integration.charm.helpers import (
    check_cluster_members,
    get_cluster_roles,
    get_db_primary_unit,
    get_password,
    get_primary,
    scale_application,
    switchover,
)
from tests.integration.helpers.juju import (
    get_app_leader,
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

UNIT_IDS = [0, 1, 2]

# VM: same path layout the library's VMPaths composes.
VM_POSTGRESQL_DATA_DIR = (
    f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_DATA_PATH}/16/main"
)

# K8s: paths inside the workload container.
K8S_STORAGE_PATH = f"/{literals.K8S_DATA_PATH}"
K8S_POSTGRESQL_DATA_DIR = f"{K8S_STORAGE_PATH}/16/main"


def _first_unit(juju: Juju, app_name: str) -> str:
    return next(iter(get_app_units(juju, app_name)))


def _wait_for_active_units(
    juju: Juju, app_name: str, num_units: int, raise_on_error: bool = True, **kwargs
) -> None:
    """Wait for the app to be active and idle with exactly num_units units."""
    juju.wait(
        lambda status: (
            wait_for_apps_status(jubilant.all_active, app_name)(status)
            and len(status.get_units(app_name)) == num_units
        ),
        error=jubilant.any_error if raise_on_error else None,
        **kwargs,
    )


def _namespace(juju: Juju) -> str:
    """Kubernetes namespace of the model (the model name without the controller)."""
    return (juju.model or "").split(":")[-1]


def _get_existing_k8s_resources(namespace: str, app_name: str) -> set[str]:
    """Return the k8s resources created by the charm and Patroni."""
    client = Client(namespace=namespace)
    resources = set()
    for kind in [Endpoints, Service]:
        # Resources with Juju's created-by label.
        juju_resources = client.list(
            kind, namespace=namespace, labels={"app.juju.is/created-by": app_name}
        )
        resources.update({f"{kind.__name__}/{x.metadata.name}" for x in juju_resources})

        # Since Juju 3.6.13 the mutating webhook no longer processes Endpoints, so the
        # ones Patroni creates never get Juju labels and are queried separately.
        patroni_resources = client.list(
            kind,
            namespace=namespace,
            labels={"application": "patroni", "cluster-name": f"patroni-{app_name}"},
        )
        resources.update({f"{kind.__name__}/{x.metadata.name}" for x in patroni_resources})
    return resources


def _get_expected_k8s_resources(app_name: str) -> set[str]:
    """Return the k8s resources the charm and Patroni should create."""
    return {
        f"Endpoints/patroni-{app_name}",
        f"Endpoints/patroni-{app_name}-config",
        f"Endpoints/patroni-{app_name}-sync",
        f"Endpoints/{app_name}-primary",
        f"Endpoints/{app_name}-replicas",
        f"Service/patroni-{app_name}-config",
        f"Service/{app_name}-primary",
        f"Service/{app_name}-replicas",
    }


def _wait_for_app_removal(juju: Juju, app_name: str) -> None:
    """Block until the app is removed, or any of its units gets in an error state."""
    juju.wait(
        lambda status: app_name not in status.apps or jubilant.any_error(status, app_name),
        timeout=20 * 60,
    )


@pytest.mark.abort_on_fail
def test_deploy(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy the charm-under-test.

    Assert on the unit status before any relations/configurations take place.
    """
    juju.deploy(
        charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        num_units=len(UNIT_IDS),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )

    # Reducing the update status frequency to speed up the triggering of deferred events.
    juju.model_config({"update-status-hook-interval": "10s"})

    _wait_for_active_units(juju, app_name, len(UNIT_IDS), timeout=1500)
    for unit in get_app_units(juju, app_name).values():
        assert unit.workload_status.current == "active"


@pytest.mark.k8s_only
def test_application_created_required_resources(juju: Juju, app_name: str) -> None:
    # Compare the k8s resources that the charm and Patroni should create with
    # the currently created k8s resources.
    existing_resources = _get_existing_k8s_resources(_namespace(juju), app_name)
    assert set(existing_resources) == _get_expected_k8s_resources(app_name)


@pytest.mark.k8s_only
@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_labels_consistency_across_pods(juju: Juju, app_name: str, unit_id: int) -> None:
    client = Client(namespace=_namespace(juju))
    pod = client.get(Pod, name=f"{app_name}-{unit_id}")
    # Ensures that the correct kubernetes labels are set
    # (these ones guarantee the correct working of replication).
    assert pod.metadata.labels["application"] == "patroni"
    assert pod.metadata.labels["cluster-name"] == f"patroni-{app_name}"


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_database_is_up(juju: Juju, app_name: str, unit_id: int):
    # Query Patroni REST API and check the status that indicates
    # both Patroni and PostgreSQL are up and running.
    host = get_unit_address(juju, f"{app_name}/{unit_id}")
    result = requests.get(f"https://{host}:8008/health", verify=False)
    assert result.status_code == 200


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_exporter_is_up(juju: Juju, app_name: str, unit_id: int):
    # Query exporter metrics endpoint and check the status that indicates
    # metrics are available for scraping.
    host = get_unit_address(juju, f"{app_name}/{unit_id}")
    result = requests.get(f"http://{host}:9187/metrics")
    assert result.status_code == 200
    assert "pg_exporter_last_scrape_error 0" in result.content.decode("utf8"), (
        "Scrape error in postgresql_prometheus_exporter"
    )


@pytest.mark.parametrize("unit_id", UNIT_IDS)
def test_settings_are_correct(juju: Juju, app_name: str, substrate: Substrate, unit_id: int):
    password = get_password(juju, app_name)

    # Connect to PostgreSQL.
    unit_name = f"{app_name}/{unit_id}"
    host = get_unit_address(juju, unit_name)
    logger.info("connecting to the database host: %s", host)
    with db_connect(host, password) as connection:
        assert connection.status == psycopg2.extensions.STATUS_READY

        # Retrieve settings from PostgreSQL pg_settings table.
        # Here the SQL query gets a key-value pair composed by the name of the setting
        # and its value, filtering the retrieved data to return only the settings
        # that were set by Patroni.
        settings_names = [
            "archive_command",
            "archive_mode",
            "autovacuum",
            "data_directory",
            "cluster_name",
            "data_checksums",
            "fsync",
            "full_page_writes",
            "lc_messages",
            "listen_addresses",
            "log_autovacuum_min_duration",
            "log_checkpoints",
            "log_destination",
            "log_temp_files",
            "log_timezone",
            "max_connections",
            "wal_level",
        ]
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT name,setting FROM pg_settings WHERE name IN ({});").format(
                    sql.SQL(", ").join(sql.Placeholder() * len(settings_names))
                ),
                settings_names,
            )
            settings = dict(cursor.fetchall())
    connection.close()

    # Validate each configuration set by Patroni on PostgreSQL.
    if substrate == Substrate.vm:
        assert settings["cluster_name"] == app_name
        assert settings["data_directory"] == VM_POSTGRESQL_DATA_DIR
        assert settings["listen_addresses"] == host
    else:
        assert settings["cluster_name"] == f"patroni-{app_name}"
        assert settings["data_directory"] == K8S_POSTGRESQL_DATA_DIR
        assert settings["listen_addresses"] == "*"
    assert settings["archive_command"] == "/bin/true"
    assert settings["archive_mode"] == "on"
    assert settings["autovacuum"] == "on"
    assert settings["data_checksums"] == "on"
    assert settings["fsync"] == "on"
    assert settings["full_page_writes"] == "on"
    assert settings["lc_messages"] == "en_US.UTF8"
    assert settings["log_autovacuum_min_duration"] == "60000"
    assert settings["log_checkpoints"] == "on"
    assert settings["log_destination"] == "stderr"
    assert settings["log_temp_files"] == "1"
    assert settings["log_timezone"] == "UTC"
    assert settings["max_connections"] == "100"
    assert settings["wal_level"] == "logical"

    # Retrieve settings from Patroni REST API.
    result = requests.get(f"https://{host}:8008/config", verify=False)
    settings = result.json()

    # Validate each configuration related to Patroni
    assert settings["postgresql"]["use_pg_rewind"] is True
    assert settings["postgresql"]["remove_data_directory_on_rewind_failure"] is False
    assert settings["postgresql"]["remove_data_directory_on_diverged_timelines"] is False
    if substrate == Substrate.vm:
        assert settings["loop_wait"] == 10
        assert settings["retry_timeout"] == 10
    assert settings["maximum_lag_on_failover"] == 1048576

    logger.warning("Asserting port ranges")
    unit = get_app_units(juju, app_name)[unit_name]
    assert unit.open_ports
    assert unit.open_ports[0] == "5432/tcp"


def test_postgresql_locales(juju: Juju, app_name: str, substrate: Substrate) -> None:
    unit_name = _first_unit(juju, app_name)
    if substrate == Substrate.vm:
        raw_locales = juju.exec(
            "ls", "/snap/charmed-postgresql/current/usr/lib/locale", unit=unit_name
        ).stdout
        locales = raw_locales.splitlines()
        locales.append("C")
        expected_locales = get_args(SNAP_LOCALES)
    else:
        raw_locales = juju.ssh(unit_name, "locale -a", container="postgresql")
        locales = raw_locales.splitlines()
        expected_locales = get_args(K8S_LOCALES)
    locales.sort()

    # Juju 2 has an extra empty element
    if "" in locales:
        locales.remove("")
    assert locales == sorted(expected_locales)


def test_postgresql_parameters_change(juju: Juju, app_name: str) -> None:
    """Test that's possible to change PostgreSQL parameters."""
    juju.config(
        app_name,
        {
            "memory-max-prepared-transactions": "100",
            "memory-shared-buffers": "32768",  # 2 * 128MB. Patroni may refuse the config if < 128MB
            "response-lc-monetary": "en_GB.utf8",
            "experimental-max-connections": "200",
        },
    )
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=10 * 60,
        successes=30,
    )
    password = get_password(juju, app_name)

    # Connect to PostgreSQL.
    for unit_id in UNIT_IDS:
        host = get_unit_address(juju, f"{app_name}/{unit_id}")
        logger.info("connecting to the database host: %s", host)
        with db_connect(host, password) as connection, connection.cursor() as cursor:
            settings_names = [
                "max_prepared_transactions",
                "shared_buffers",
                "lc_monetary",
                "max_connections",
            ]
            cursor.execute(
                sql.SQL("SELECT name,setting FROM pg_settings WHERE name IN ({});").format(
                    sql.SQL(", ").join(sql.Placeholder() * len(settings_names))
                ),
                settings_names,
            )
            settings = dict(cursor.fetchall())
        connection.close()

        # Validate each configuration set by Patroni on PostgreSQL.
        assert settings["max_prepared_transactions"] == "100"
        assert settings["shared_buffers"] == "32768"
        assert settings["lc_monetary"] == "en_GB.utf8"
        assert settings["max_connections"] == "200"


@pytest.mark.k8s_only
def test_cluster_is_stable_after_leader_deletion(juju: Juju, app_name: str) -> None:
    """Tests that the cluster maintains a primary after the primary is deleted."""
    # Find the current primary unit.
    primary = get_db_primary_unit(juju, app_name)

    # Delete the primary pod.
    Client(namespace=_namespace(juju)).delete(Pod, name=primary.replace("/", "-"))
    logger.info(f"deleted pod {primary}")

    # Wait and get the primary again (which can be any unit, including the previous primary).
    _wait_for_active_units(juju, app_name, len(UNIT_IDS), timeout=1000)
    primary = get_db_primary_unit(juju, app_name)

    # We also need to check that a replica can see the leader
    # to make sure that the cluster is stable again.
    replica = next(unit for unit in get_app_units(juju, app_name) if unit != primary)
    get_primary(juju, replica)


def test_scale_down_and_up(juju: Juju, app_name: str, substrate: Substrate):
    """Test data is replicated to new units after a scale up."""
    # Ensure the initial number of units in the application.
    initial_scale = len(UNIT_IDS)
    scale_application(juju, app_name, initial_scale, substrate)

    # Scale down the application.
    scale_application(juju, app_name, initial_scale - 1, substrate)

    # Ensure the member was correctly removed from the cluster
    # (by comparing the cluster members and the current units).
    check_cluster_members(juju, app_name, substrate)

    # Scale up the application (2 more units than the current scale).
    scale_application(juju, app_name, initial_scale + 1, substrate)

    # Assert the correct members are part of the cluster.
    check_cluster_members(juju, app_name, substrate)

    # On K8s only the last pods can be removed, so the leader and primary
    # deletions are only tested on VM.
    if substrate == Substrate.vm:
        # Test the deletion of the unit that is both the leader and the primary.
        any_unit_name = _first_unit(juju, app_name)
        primary = get_primary(juju, any_unit_name)
        leader_unit = get_app_leader(juju, app_name)

        # Trigger a switchover if the primary and the leader are not the same unit.
        patroni_password = get_password(juju, app_name, "patroni")

        if primary != leader_unit:
            switchover(juju, primary, patroni_password, leader_unit)

            # Get the new primary unit.
            primary = get_primary(juju, any_unit_name)
            # Check that the primary changed.
            for attempt in Retrying(
                stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30)
            ):
                with attempt:
                    assert primary == leader_unit

        juju.remove_unit(leader_unit, destroy_storage=True)
        _wait_for_active_units(juju, app_name, initial_scale, timeout=1000)

        # Assert the correct members are part of the cluster.
        check_cluster_members(juju, app_name, substrate)

        # Scale up the application (2 more units than the current scale).
        scale_application(juju, app_name, initial_scale + 2, substrate)

        # Test the deletion of both the unit that is the leader and the unit that is the primary.
        any_unit_name = _first_unit(juju, app_name)
        primary = get_primary(juju, any_unit_name)
        leader_unit = get_app_leader(juju, app_name)

        # Trigger a switchover if the primary and the leader are the same unit.
        if primary == leader_unit:
            switchover(juju, primary, patroni_password)

            # Get the new primary unit.
            primary = get_primary(juju, any_unit_name)
            # Check that the primary changed.
            for attempt in Retrying(
                stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30)
            ):
                with attempt:
                    assert primary != leader_unit

        juju.remove_unit(primary, leader_unit, destroy_storage=True)
        _wait_for_active_units(juju, app_name, initial_scale, timeout=2000, successes=30)

        # Wait some time to elect a new primary.
        sleep(30)

        # Assert the correct members are part of the cluster.
        check_cluster_members(juju, app_name, substrate)

    # End with the cluster having the initial number of units.
    scale_application(juju, app_name, initial_scale, substrate)


def test_switchover_sync_standby(juju: Juju, app_name: str):
    original_roles = get_cluster_roles(juju, _first_unit(juju, app_name))
    juju.run(original_roles["sync_standbys"][0], "promote-to-primary", {"scope": "unit"})

    juju.wait(wait_for_apps_status(jubilant.all_active), error=jubilant.any_error, timeout=200)

    new_roles = get_cluster_roles(juju, _first_unit(juju, app_name))
    assert new_roles["primaries"][0] == original_roles["sync_standbys"][0]


@pytest.mark.vm_only
def test_persist_data_through_primary_deletion(juju: Juju, app_name: str):
    """Test data persists through a primary deletion."""
    any_unit_name = _first_unit(juju, app_name)
    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            primary = get_primary(juju, any_unit_name)
            password = get_password(juju, app_name)

    # Write data to primary IP.
    host = get_unit_address(juju, primary)
    logger.info(f"connecting to primary {primary} on {host}")
    with db_connect(host, password) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE primarydeletiontest (testcol INT);")
    connection.close()

    # Remove one unit.
    juju.remove_unit(primary)
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=1500,
    )

    # Add the unit again.
    juju.add_unit(app_name)
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=2000,
    )

    # Testing write occurred to every postgres instance by reading from them
    for unit_name in get_app_units(juju, app_name):
        host = get_unit_address(juju, unit_name)
        logger.info("connecting to the database host: %s", host)
        with db_connect(host, password) as connection, connection.cursor() as cursor:
            # Ensure we can read from "primarydeletiontest" table
            cursor.execute("SELECT * FROM primarydeletiontest;")
        connection.close()


@pytest.mark.k8s_only
def test_persist_data_through_graceful_restart(juju: Juju, app_name: str):
    """Test data persists through a graceful restart."""
    primary = get_db_primary_unit(juju, app_name)
    password = get_password(juju, app_name)
    address = get_unit_address(juju, primary)

    # Write data to primary IP.
    logger.info(f"connecting to primary {primary} on {address}")
    with db_connect(host=address, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute("CREATE TABLE gracetest (testcol INT );")

    # Restart all nodes by scaling to 0, then back up
    # These have to run sequentially for the test to be valid/stable.
    juju.cli("scale-application", app_name, "0")
    juju.cli("scale-application", app_name, str(len(UNIT_IDS)))
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=1000,
    )

    # Testing write occurred to every postgres instance by reading from them
    for unit_name in get_app_units(juju, app_name):
        host = get_unit_address(juju, unit_name)
        logger.info("connecting to the database host: %s", host)
        with db_connect(host=host, password=password) as connection:
            # Ensure we can read from "gracetest" table
            connection.cursor().execute("SELECT * FROM gracetest;")


@pytest.mark.k8s_only
def test_persist_data_through_failure(juju: Juju, app_name: str):
    """Test data persists through a failure."""
    primary = get_db_primary_unit(juju, app_name)
    password = get_password(juju, app_name)
    address = get_unit_address(juju, primary)

    # Write data to primary IP.
    logger.info(f"connecting to primary {primary} on {address}")
    with db_connect(host=address, password=password) as connection:
        connection.autocommit = True
        connection.cursor().execute("CREATE TABLE failtest (testcol INT );")

    # Cause a machine failure by killing a unit in k8s
    Client(namespace=_namespace(juju)).delete(Pod, name=primary.replace("/", "-"))
    logger.info("primary pod deleted")

    # Wait for juju to notice one of the pods is gone and fix it
    logger.info("wait for juju to reset postgres container")
    _wait_for_active_units(juju, app_name, len(UNIT_IDS), timeout=1000, delay=2, successes=23)
    logger.info("juju has reset postgres container")

    # Testing write occurred to every postgres instance by reading from them
    for unit_name in get_app_units(juju, app_name):
        host = get_unit_address(juju, unit_name)
        logger.info("connecting to the database host: %s", host)
        with db_connect(host=host, password=password) as connection:
            # Ensure we can read from "failtest" table
            connection.cursor().execute("SELECT * FROM failtest;")


@pytest.mark.k8s_only
def test_automatic_failover_after_leader_issue(juju: Juju, app_name: str) -> None:
    """Tests that an automatic failover is triggered after an issue happens in the leader."""
    # Find the current primary unit.
    primary = get_db_primary_unit(juju, app_name)

    # Crash PostgreSQL by removing the data directory.
    juju.exec("rm", "-rf", K8S_POSTGRESQL_DATA_DIR, unit=primary)

    # Wait for charm to stabilise
    _wait_for_active_units(juju, app_name, len(UNIT_IDS), timeout=1000)

    # Primary doesn't have to be different, but it does have to exist.
    get_db_primary_unit(juju, app_name)


@pytest.mark.k8s_only
def test_application_removal(juju: Juju, app_name: str) -> None:
    # Remove the application to trigger some hooks (like peer relation departed).
    juju.remove_application(app_name)

    # Block until the application is completely removed, or any unit gets in an error state.
    _wait_for_app_removal(juju, app_name)

    # Check that all k8s resources created by the charm and Patroni were removed.
    namespace = _namespace(juju)
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
        with attempt:
            existing_resources = _get_existing_k8s_resources(namespace, app_name)
            logger.info(f"existing_resources: {existing_resources}")
            assert set(existing_resources) == set()

    # Check whether the application is gone
    # (in that situation, the units aren't in an error state).
    assert app_name not in juju.status().apps


@pytest.mark.k8s_only
@pytest.mark.skip(reason="Unstable")
def test_redeploy_charm_same_model(juju: Juju, charm: str, app_name: str, charm_resources):
    """Redeploy the charm in the same model to test that it works."""
    juju.deploy(
        charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        num_units=len(UNIT_IDS),
        resources=charm_resources,
        trust=True,
    )

    # This check is enough to ensure that the charm/workload is working for this specific test.
    _wait_for_active_units(juju, app_name, len(UNIT_IDS), timeout=1000)


@pytest.mark.k8s_only
@pytest.mark.skip(reason="Unstable")
def test_redeploy_charm_same_model_after_forcing_removal(
    juju: Juju, charm: str, app_name: str, charm_resources
) -> None:
    """Redeploy the charm in the same model to test that it works after a forceful removal."""
    juju.cli(
        "remove-application", app_name, "--destroy-storage", "--force", "--no-prompt", "--no-wait"
    )

    # Block until the application is completely removed, or any unit gets in an error state.
    _wait_for_app_removal(juju, app_name)

    # Check that all k8s resources created by the charm and Patroni were still present.
    existing_resources = _get_existing_k8s_resources(_namespace(juju), app_name)
    assert set(existing_resources) == _get_expected_k8s_resources(app_name)

    # Check that the charm can be deployed again.
    juju.deploy(
        charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        num_units=len(UNIT_IDS),
        resources=charm_resources,
        trust=True,
    )

    # This check is enough to ensure that the charm/workload is working for this specific test.
    _wait_for_active_units(juju, app_name, len(UNIT_IDS), raise_on_error=False, timeout=1000)


@pytest.mark.k8s_only
def test_storage_with_more_restrictive_permissions(
    juju: Juju, charm: str, app_name: str, charm_resources
):
    """Test that the charm can be deployed with a storage with more restrictive permissions."""
    test_app_name = f"test-storage-{app_name}"
    # Deploy and wait for the charm to get into the install hook (maintenance status).
    juju.deploy(
        charm,
        app=test_app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        num_units=1,
        resources=charm_resources,
        trust=True,
    )

    # Restrict the permissions of the storage.
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
        with attempt:
            juju.ssh(f"{test_app_name}/0", f"chmod 755 {K8S_STORAGE_PATH}", container="postgresql")

    # This check is enough to ensure that the charm/workload is working for this specific test.
    juju.wait(
        wait_for_apps_status(jubilant.all_active, test_app_name),
        error=jubilant.any_error,
        timeout=1000,
    )
