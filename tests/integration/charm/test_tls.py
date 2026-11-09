#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import time

import jubilant
import pytest
from jubilant import Juju
from single_kernel_postgresql.config import literals
from tenacity import Retrying, stop_after_attempt, stop_after_delay, wait_exponential, wait_fixed

from tests.integration.charm.helpers import (
    change_patroni_setting,
    change_primary_start_timeout,
    check_tls,
    check_tls_patroni_api,
    check_tls_replication,
    get_password,
    get_primary,
    primary_changed,
    run_command_on_unit,
)
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

tls_certificates_app_name = "self-signed-certificates"
tls_channel = "1/stable"
tls_config = {"ca-common-name": "Test CA"}
DATABASE_UNITS = 3

# VM: same path layout the library's VMPaths composes.
VM_POSTGRESQL_DATA_DIR = (
    f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_DATA_PATH}/16/main"
)
VM_POSTGRESQL_LOGS_DIR = f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_LOGS_PATH}"

# K8s: paths inside the workload container.
K8S_POSTGRESQL_DATA_DIR = f"/{literals.K8S_DATA_PATH}/16/main"
K8S_POSTGRESQL_LOGS_DIR = f"/{literals.K8S_LOGS_STORAGE_PATH}/{literals.K8S_PG_LOGS_PATH}"

REWIND_TLS_LOG = "connection authorized: user=rewind database=postgres SSL enabled"


def check_tls_rewind(juju: Juju, app_name: str, substrate: Substrate) -> None:
    """Check that pg_rewind connected through TLS, looking at the logs of every unit."""
    logs_dir = VM_POSTGRESQL_LOGS_DIR if substrate == Substrate.vm else K8S_POSTGRESQL_LOGS_DIR
    logs = ""
    for unit_name in get_app_units(juju, app_name):
        logger.info(f"checking if pg_rewind used TLS on {unit_name}")
        try:
            logs = run_command_on_unit(
                juju, unit_name, f"grep rewind {logs_dir}/postgresql-*.log", substrate
            )
        except Exception:
            continue
        if REWIND_TLS_LOG in logs:
            break
    assert REWIND_TLS_LOG in logs, "TLS is not being used on pg_rewind connections"


@pytest.mark.abort_on_fail
def test_deploy(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy three units of PostgreSQL."""
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            base="ubuntu@24.04",
            config={"profile": "testing"},
            num_units=DATABASE_UNITS,
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )
        # No wait between deploying charms, since we can't guarantee users will wait. Furthermore,
        # bundles don't wait between deploying charms.


@pytest.mark.abort_on_fail
def test_tls_enabled(juju: Juju, app_name: str, substrate: Substrate) -> None:
    """Test that TLS is enabled when relating to the TLS Certificates Operator."""
    with fast_forward(juju):
        # Deploy TLS Certificates operator.
        juju.deploy(
            tls_certificates_app_name,
            base="ubuntu@24.04",
            channel=tls_channel,
            config=tls_config,
        )

        # Relate it to the PostgreSQL to enable TLS.
        juju.integrate(
            f"{app_name}:client-certificates", f"{tls_certificates_app_name}:certificates"
        )
        juju.integrate(
            f"{app_name}:peer-certificates", f"{tls_certificates_app_name}:certificates"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1500)

        # Wait for all units enabling TLS.
        for unit_name in get_app_units(juju, app_name):
            assert check_tls(juju, unit_name, enabled=True)
            assert check_tls_patroni_api(juju, unit_name, enabled=True, substrate=substrate)

        # Test TLS being used by pg_rewind. To accomplish that, get the primary unit
        # and a replica that will be promoted to primary (this should trigger a rewind
        # operation when the old primary is started again).
        any_unit = next(iter(get_app_units(juju, app_name)))
        primary = get_primary(juju, any_unit)
        replica = next(unit for unit in get_app_units(juju, app_name) if unit != primary)

        # Check if TLS enabled for replication
        assert check_tls_replication(juju, primary, enabled=True)

        patroni_password = get_password(juju, app_name, "patroni")

        # Enable additional logs on the PostgreSQL instance to check TLS
        # being used in a later step.
        juju.config(app_name, {"logging-log-connections": "True"})
        juju.wait(
            wait_for_apps_status(jubilant.all_active, app_name),
            error=jubilant.any_error,
            timeout=10 * 60,
            successes=30,
        )
        if substrate == Substrate.vm:
            # Make the fail-over happen faster.
            change_primary_start_timeout(juju, primary, 0, patroni_password)

        # Pause Patroni so it doesn't wipe the custom changes
        change_patroni_setting(
            juju, app_name, "pause", True, patroni_password, use_random_unit=True
        )

    with fast_forward(juju, "24h"):
        if substrate == Substrate.vm:
            promote_command = f"sudo charmed-postgresql.pg-ctl -D {VM_POSTGRESQL_DATA_DIR} promote"
        else:
            promote_command = (
                "su postgres -c"
                f' "/usr/lib/postgresql/16/bin/pg_ctl -D {K8S_POSTGRESQL_DATA_DIR} promote"'
            )
        for attempt in Retrying(
            stop=stop_after_delay(60 * 5), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                # Promote the replica to primary.
                run_command_on_unit(juju, replica, promote_command, substrate)

                # Check that the replica was promoted.
                host = get_unit_address(juju, replica)
                password = get_password(juju, app_name)
                with db_connect(host, password) as connection:
                    connection.autocommit = True
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_is_in_recovery();")
                        in_recovery = cursor.fetchone()[0]
                        logger.info(f"in_recovery: {in_recovery}")
                        # If the instance is not in recovery mode anymore it was promoted.
                        assert not in_recovery
                connection.close()

        # Write some data to the initial primary (this causes a divergence
        # in the instances' timelines).
        host = get_unit_address(juju, primary)
        password = get_password(juju, app_name)
        with db_connect(host, password) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("CREATE TABLE IF NOT EXISTS pgrewindtest (testcol INT);")
                cursor.execute("INSERT INTO pgrewindtest SELECT generate_series(1,1000);")
        connection.close()

        # Stop the initial primary.
        logger.info(f"stopping database on {primary}")
        if substrate == Substrate.vm:
            # Kill both Patroni and PostgreSQL OS processes (the snap restarts Patroni).
            run_command_on_unit(
                juju,
                primary,
                "pkill --signal SIGKILL -f /snap/charmed-postgresql/current/usr/lib/postgresql/16/bin/postgres",
                substrate,
            )
            run_command_on_unit(
                juju,
                primary,
                "pkill --signal SIGKILL -f /snap/charmed-postgresql/[0-9]*/usr/bin/patroni",
                substrate,
            )
        else:
            run_command_on_unit(juju, primary, "/charm/bin/pebble stop postgresql", substrate)

        # Check that the primary changed.
        for attempt in Retrying(
            stop=stop_after_attempt(10),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            reraise=True,
        ):
            with attempt:
                assert primary_changed(juju, primary), "primary not changed"
        if substrate == Substrate.vm:
            change_primary_start_timeout(juju, primary, 300, patroni_password)

        # Check the logs to ensure TLS is being used by pg_rewind.
        for attempt in Retrying(stop=stop_after_delay(60 * 3), wait=wait_fixed(2), reraise=True):
            with attempt:
                check_tls_rewind(juju, app_name, substrate)

        change_patroni_setting(
            juju, app_name, "pause", False, patroni_password, use_random_unit=True
        )

    with fast_forward(juju):
        # Wait for PostgreSQL to be stable if not already.
        juju.wait(
            wait_for_apps_status(jubilant.all_active, app_name),
            error=jubilant.any_error,
            timeout=10 * 60,
        )


def test_remove_tls(juju: Juju, app_name: str, substrate: Substrate) -> None:
    with fast_forward(juju):
        # Remove the relation.
        if substrate == Substrate.vm:
            juju.remove_relation(
                f"{app_name}:client-certificates", f"{tls_certificates_app_name}:certificates"
            )
            juju.remove_relation(
                f"{app_name}:peer-certificates", f"{tls_certificates_app_name}:certificates"
            )
            # Add a sleep to avoid immediate passing of the idle check.
            time.sleep(100)
            juju.wait(
                wait_for_apps_status(jubilant.all_active, app_name),
                error=jubilant.any_error,
                timeout=1800,
                successes=30,
            )
        else:
            juju.remove_relation(
                f"{app_name}:peer-certificates", f"{tls_certificates_app_name}:certificates"
            )
            juju.remove_relation(
                f"{app_name}:client-certificates", f"{tls_certificates_app_name}:certificates"
            )
            juju.wait(
                wait_for_apps_status(jubilant.all_active, app_name),
                error=jubilant.any_error,
                timeout=1000,
                successes=15,
            )

        # Wait for all units disabling TLS.
        for unit_name in get_app_units(juju, app_name):
            assert check_tls(juju, unit_name, enabled=False)
            assert check_tls_patroni_api(juju, unit_name, enabled=False, substrate=substrate)
