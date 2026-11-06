# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import platform
import shutil
import zipfile
from pathlib import Path

import jubilant
import tomli_w
from jubilant import Juju
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    check_db_units_writes_increment,
    count_switchovers,
    get_db_primary_unit,
    get_password,
    get_refresh_versions,
    run_upgrade,
)
from tests.integration.helpers.juju import (
    get_app_leader,
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

DB_TEST_APP_NAME = "postgresql-test-app"

MINUTE_SECS = 60

logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def test_deploy_latest(juju: Juju, app_name: str, substrate: Substrate) -> None:
    """Simple test to ensure that the PostgreSQL and application charms get deployed."""
    logging.info("Deploying PostgreSQL cluster")
    juju.deploy(
        charm=app_name,
        app=app_name,
        base="ubuntu@24.04",
        channel="16/edge",
        config={"profile": "testing"},
        num_units=3,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm=DB_TEST_APP_NAME,
        app=DB_TEST_APP_NAME,
        base="ubuntu@24.04",
        channel="latest/edge",
        num_units=1,
    )

    juju.integrate(
        f"{app_name}:database",
        f"{DB_TEST_APP_NAME}:database",
    )

    logging.info("Wait for applications to become active")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name, DB_TEST_APP_NAME),
        timeout=20 * MINUTE_SECS,
    )


def test_pre_refresh_check(juju: Juju, app_name: str) -> None:
    """Test that the pre-refresh-check action runs successfully."""
    db_leader = get_app_leader(juju, app_name)

    logging.info("Run pre-refresh-check action")
    # Units can report active before all replicas have joined the Patroni cluster,
    # so retry until the check passes.
    for attempt in Retrying(
        retry=retry_if_exception_type(jubilant.TaskError),
        stop=stop_after_delay(10 * MINUTE_SECS),
        wait=wait_fixed(30),
        reraise=True,
    ):
        with attempt:
            juju.run(unit=db_leader, action="pre-refresh-check", wait=5 * MINUTE_SECS)

    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)


def test_upgrade_from_edge(
    juju: Juju,
    app_name: str,
    charm: str,
    charm_resources,
    substrate: Substrate,
    continuous_writes,
) -> None:
    """Update the second cluster."""
    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, app_name)

    initial_number_of_switchovers = count_switchovers(juju, app_name)

    run_upgrade(juju, app_name, charm, substrate, charm_resources)

    expected_version = get_refresh_versions(charm)["workload"]
    current_primary = get_db_primary_unit(juju, app_name)
    connection = db_connect(
        get_unit_address(juju, current_primary),
        get_password(juju, app_name),
        database="postgresql_test_app_database",
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            database_version = cursor.fetchone()[0].split(" ")[1]
    finally:
        connection.close()
    assert expected_version == database_version, (
        f"PostgreSQL version mismatch: expected {expected_version}, got {database_version}"
    )

    logging.info("Ensure continuous writes are incrementing")
    check_db_units_writes_increment(juju, app_name)

    logging.info("checking the number of switchovers")
    final_number_of_switchovers = count_switchovers(juju, app_name)
    assert (final_number_of_switchovers - initial_number_of_switchovers) <= 2, (
        "Number of switchovers is greater than 2"
    )


def test_fail_and_rollback(
    juju: Juju,
    app_name: str,
    charm: str,
    charm_resources,
    substrate: Substrate,
    tmp_path: Path,
    continuous_writes,
) -> None:
    """Test an upgrade failure and its rollback."""
    db_app_leader = get_app_leader(juju, app_name)
    unit_names = sorted(get_app_units(juju, app_name).keys())

    logging.info("Run pre-refresh-check action")
    juju.run(unit=db_app_leader, action="pre-refresh-check", wait=5 * MINUTE_SECS)

    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    fault_charm = tmp_path / Path(charm).name
    shutil.copy(charm, fault_charm)

    logging.info("Inject dependency fault")
    inject_dependency_fault(substrate, fault_charm)

    logging.info("Refresh the charm")
    juju.refresh(app=app_name, path=fault_charm, resources=charm_resources)

    if substrate == Substrate.vm:
        logging.info("Wait for upgrade to fail on leader")
        juju.wait(
            ready=wait_for_apps_status(jubilant.any_blocked, app_name),
            timeout=10 * MINUTE_SECS,
        )

        logging.info("Ensure continuous writes on all units")
        check_db_units_writes_increment(juju, app_name, unit_names)

        logging.info("Re-refresh the charm")
        juju.refresh(app=app_name, path=charm)
    else:
        # K8s refreshes the highest numbered pod first.
        logging.info("Wait for upgrade to fail on the last unit")
        juju.wait(
            lambda status: status.apps[app_name].units[unit_names[-1]].is_blocked,
            timeout=5 * MINUTE_SECS,
        )

        logging.info("Ensure continuous writes on all active units")
        check_db_units_writes_increment(juju, app_name, unit_names[:-1])

        logging.info("Re-refresh the charm")
        juju.refresh(app=app_name, path=charm, resources=charm_resources)

        juju.wait(
            lambda status: status.apps[app_name].units[unit_names[-1]].is_active,
            timeout=5 * MINUTE_SECS,
        )
        juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

        logging.info("Run resume-refresh action")
        juju.run(
            unit=get_app_leader(juju, app_name), action="resume-refresh", wait=5 * MINUTE_SECS
        )

    logging.info("Wait for upgrade to complete")
    juju.wait(ready=wait_for_apps_status(jubilant.all_active, app_name), timeout=20 * MINUTE_SECS)

    logging.info("Ensure continuous writes after rollback procedure")
    check_db_units_writes_increment(juju, app_name, unit_names)


def inject_dependency_fault(substrate: Substrate, charm_file: Path) -> None:
    """Inject a dependency fault into the PostgreSQL charm."""
    versions = get_refresh_versions(str(charm_file))

    versions["charm"] = "16/0.0.0"
    if substrate == Substrate.vm:
        versions["snap"]["revisions"][platform.machine()] = "1"
    else:
        versions["workload"] = "16.14"

    # Overwrite refresh_versions.toml with incompatible version.
    with zipfile.ZipFile(charm_file, mode="a") as charm_zip:
        charm_zip.writestr("refresh_versions.toml", tomli_w.dumps(versions))
