# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json

import jubilant
import psycopg2
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import CHARM_BASE, build_connection_string, get_cluster_roles
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_leader,
    remove_application,
    wait_for_apps_status,
)
from tests.integration.types import Substrate

DATABASE_APP_NAME = "postgresql"
SECOND_DATABASE_APP_NAME = "postgresql2"
THIRD_DATABASE_APP_NAME = "postgresql3"

DATA_INTEGRATOR_APP_NAME = "data-integrator"
SECOND_DATA_INTEGRATOR_APP_NAME = "data-integrator2"
THIRD_DATA_INTEGRATOR_APP_NAME = "data-integrator3"
DATA_INTEGRATOR_RELATION = "postgresql"

DATABASE_APP_CONFIG = {"profile": "testing"}

TESTING_DATABASE = "testdb"

FAST_INTERVAL = "60s"
TIMEOUT = 10 * 60


@pytest.mark.abort_on_fail
def test_deploy(juju: jubilant.Juju, charm, charm_resources, substrate: Substrate):
    juju.deploy(
        charm,
        app=DATABASE_APP_NAME,
        num_units=3,
        base=CHARM_BASE,
        config=DATABASE_APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm,
        app=SECOND_DATABASE_APP_NAME,
        num_units=3,
        base=CHARM_BASE,
        config=DATABASE_APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm,
        app=THIRD_DATABASE_APP_NAME,
        num_units=1,
        base=CHARM_BASE,
        config=DATABASE_APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app=DATA_INTEGRATOR_APP_NAME,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app=SECOND_DATA_INTEGRATOR_APP_NAME,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app=THIRD_DATA_INTEGRATOR_APP_NAME,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    # Tolerate transient error spikes during PostgreSQL deployment.
    juju.wait(
        wait_for_apps_status(
            jubilant.all_active,
            DATABASE_APP_NAME,
            SECOND_DATABASE_APP_NAME,
            THIRD_DATABASE_APP_NAME,
        ),
        timeout=2500,
    )
    if substrate == Substrate.vm:
        with fast_forward(juju, interval=FAST_INTERVAL):
            juju.integrate(DATABASE_APP_NAME, DATA_INTEGRATOR_APP_NAME)
            juju.integrate(SECOND_DATABASE_APP_NAME, SECOND_DATA_INTEGRATOR_APP_NAME)
            juju.integrate(THIRD_DATABASE_APP_NAME, THIRD_DATA_INTEGRATOR_APP_NAME)
            juju.integrate(
                f"{DATABASE_APP_NAME}:logical-replication-offer",
                f"{SECOND_DATABASE_APP_NAME}:logical-replication",
            )
            juju.wait(wait_for_apps_status(jubilant.all_active), timeout=TIMEOUT)
    else:
        # No fast_forward: frequent update-status hooks leave no idle window under load.
        # Integration is relation-event driven and needs no accelerated cadence.
        juju.integrate(DATABASE_APP_NAME, DATA_INTEGRATOR_APP_NAME)
        juju.integrate(SECOND_DATABASE_APP_NAME, SECOND_DATA_INTEGRATOR_APP_NAME)
        juju.integrate(THIRD_DATABASE_APP_NAME, THIRD_DATA_INTEGRATOR_APP_NAME)
        juju.integrate(
            f"{DATABASE_APP_NAME}:logical-replication-offer",
            f"{SECOND_DATABASE_APP_NAME}:logical-replication",
        )
        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=900)


@pytest.mark.abort_on_fail
def test_pg2_publisher_error(juju: jubilant.Juju, substrate: Substrate):
    _create_test_table(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate)

    pg2_config = DATABASE_APP_CONFIG.copy()
    pg2_config["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.test_table"]
    })
    juju.config(SECOND_DATABASE_APP_NAME, pg2_config)
    _wait_for_leader_on_blocked(juju, SECOND_DATABASE_APP_NAME)


@pytest.mark.abort_on_fail
def test_pg3_local_error(juju: jubilant.Juju):
    pg3_config = DATABASE_APP_CONFIG.copy()
    pg3_config["logical-replication-subscription-request"] = json.dumps({
        "bad_database": ["bad_format"]
    })
    juju.config(THIRD_DATABASE_APP_NAME, pg3_config)
    _wait_for_leader_on_blocked(juju, THIRD_DATABASE_APP_NAME)

    juju.integrate(
        f"{DATABASE_APP_NAME}:logical-replication-offer",
        f"{THIRD_DATABASE_APP_NAME}:logical-replication",
    )
    _wait_for_leader_on_blocked(juju, THIRD_DATABASE_APP_NAME)

    pg3_config["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.test_table2"]
    })
    juju.config(THIRD_DATABASE_APP_NAME, pg3_config)
    _wait_for_leader_on_blocked(juju, THIRD_DATABASE_APP_NAME)


@pytest.mark.abort_on_fail
def test_resolve_errors(juju: jubilant.Juju, substrate: Substrate):
    _create_test_table(juju, DATA_INTEGRATOR_APP_NAME, substrate)
    _create_test_table(juju, DATA_INTEGRATOR_APP_NAME, substrate, "test_table2")
    _create_test_table(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "test_table2")
    _wait_for_active(juju, substrate)


@pytest.mark.abort_on_fail
def test_replication(juju: jubilant.Juju, substrate: Substrate):
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "first")
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "first", "test_table2")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "first")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "first", "test_table2")


@pytest.mark.abort_on_fail
def test_switchover(juju: jubilant.Juju, substrate: Substrate):
    publisher_leader = get_app_leader(juju, DATABASE_APP_NAME)
    publisher_roles = get_cluster_roles(juju, publisher_leader)
    publisher_candidate = publisher_roles["sync_standbys"][0]
    juju.run(
        publisher_candidate, "promote-to-primary", {"scope": "unit", "force": True}
    ).raise_on_failure()

    subscriber_leader = get_app_leader(juju, SECOND_DATABASE_APP_NAME)
    subscriber_roles = get_cluster_roles(juju, subscriber_leader)
    subscriber_candidate = subscriber_roles["sync_standbys"][0]
    juju.run(
        subscriber_candidate, "promote-to-primary", {"scope": "unit", "force": True}
    ).raise_on_failure()
    _wait_for_active(juju, substrate)


@pytest.mark.abort_on_fail
def test_replication_after_switchover(juju: jubilant.Juju, substrate: Substrate):
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "second", "test_table2")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "second", "test_table2")


@pytest.mark.abort_on_fail
def test_pg3_extend_subscription(juju: jubilant.Juju, substrate: Substrate):
    _create_test_table(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate)
    pg3_config = DATABASE_APP_CONFIG.copy()
    pg3_config["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.test_table", "public.test_table2"]
    })
    juju.config(THIRD_DATABASE_APP_NAME, pg3_config)
    _wait_for_active(juju, substrate)
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "second", "test_table2")


@pytest.mark.abort_on_fail
def test_pg2_change_subscription(juju: jubilant.Juju, substrate: Substrate):
    _create_test_table(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "test_table2")
    pg2_config = DATABASE_APP_CONFIG.copy()
    pg2_config["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.test_table2"]
    })
    juju.config(SECOND_DATABASE_APP_NAME, pg2_config)
    _wait_for_active(juju, substrate)
    # Old data will be left behind.
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "second", "test_table2")


@pytest.mark.abort_on_fail
def test_replication_after_subscriptions_changes(juju: jubilant.Juju, substrate: Substrate):
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "third")
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "third", "test_table2")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "third", "test_table2")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "third")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "third", "test_table2")


@pytest.mark.abort_on_fail
def test_pg2_dynamic_error(juju: jubilant.Juju):
    pg2_config = DATABASE_APP_CONFIG.copy()
    pg2_config["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.test_table", "public.test_table2"]
    })
    juju.config(SECOND_DATABASE_APP_NAME, pg2_config)
    _wait_for_leader_on_blocked(juju, SECOND_DATABASE_APP_NAME)


@pytest.mark.abort_on_fail
def test_replication_during_dynamic_error(juju: jubilant.Juju, substrate: Substrate):
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "fourth")
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "fourth", "test_table2")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "second")
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "fourth", "test_table2")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "fourth")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "fourth", "test_table2")


@pytest.mark.abort_on_fail
def test_pg2_resolve_dynamic_error(juju: jubilant.Juju, substrate: Substrate):
    connection_string = build_connection_string(
        juju,
        SECOND_DATA_INTEGRATOR_APP_NAME,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        connection.autocommit = True
        cursor.execute("DELETE FROM test_table;")
    _wait_for_active(juju, substrate)
    _check_test_data(juju, SECOND_DATA_INTEGRATOR_APP_NAME, substrate, "fourth")


@pytest.mark.abort_on_fail
def test_pg2_remove(juju: jubilant.Juju, substrate: Substrate):
    remove_application(juju, SECOND_DATA_INTEGRATOR_APP_NAME)
    remove_application(juju, SECOND_DATABASE_APP_NAME)
    _wait_for_active(juju, substrate, vm_timeout=1000)
    connection_string = build_connection_string(
        juju,
        DATA_INTEGRATOR_APP_NAME,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM pg_replication_slots where slot_type='logical';")
        assert cursor.fetchone()[0] == 1, (
            "unused replication slot should be removed in the publisher cluster"
        )


@pytest.mark.abort_on_fail
def test_replication_after_pg2_removal(juju: jubilant.Juju, substrate: Substrate):
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "fifth")
    _insert_test_data(juju, DATA_INTEGRATOR_APP_NAME, substrate, "fifth", "test_table2")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "fifth")
    _check_test_data(juju, THIRD_DATA_INTEGRATOR_APP_NAME, substrate, "fifth", "test_table2")


@pytest.mark.abort_on_fail
def test_pg3_remove_relation(juju: jubilant.Juju, substrate: Substrate):
    juju.remove_relation(
        f"{DATABASE_APP_NAME}:logical-replication-offer",
        f"{THIRD_DATABASE_APP_NAME}:logical-replication",
    )
    _wait_for_active(juju, substrate)
    connection_string = build_connection_string(
        juju,
        THIRD_DATA_INTEGRATOR_APP_NAME,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM pg_subscription;")
        assert cursor.fetchone()[0] == 0, (
            "unused PostgreSQL subscription should be removed in the subscriber cluster"
        )


def _wait_for_active(juju: jubilant.Juju, substrate: Substrate, vm_timeout: int = TIMEOUT) -> None:
    if substrate == Substrate.vm:
        with fast_forward(juju, interval=FAST_INTERVAL):
            juju.wait(wait_for_apps_status(jubilant.all_active), timeout=vm_timeout)
    else:
        with fast_forward(juju):
            # Idle windows under fast_forward are 3-9s. Jubilant's three successful
            # polls one second apart fit here, unlike libjuju's default 15s idle period.
            juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1800)


def _wait_for_leader_on_blocked(juju: jubilant.Juju, app_name: str) -> None:
    leader_unit = get_app_leader(juju, app_name)
    juju.wait(lambda status: status.get_units(app_name)[leader_unit].is_blocked)


def _create_test_table(
    juju: jubilant.Juju,
    data_integrator_app_name: str,
    substrate: Substrate,
    table_name: str = "test_table",
) -> None:
    connection_string = build_connection_string(
        juju,
        data_integrator_app_name,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    connection = None
    try:
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True):
            with attempt:
                connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {table_name} (test_column text);")
    finally:
        if connection is not None:
            connection.close()


def _insert_test_data(
    juju: jubilant.Juju,
    data_integrator_app_name: str,
    substrate: Substrate,
    data: str,
    table_name: str = "test_table",
) -> None:
    connection_string = build_connection_string(
        juju,
        data_integrator_app_name,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    connection = None
    try:
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True):
            with attempt:
                connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table_name} (test_column) VALUES (%s);", (data,))
    finally:
        if connection is not None:
            connection.close()


def _check_test_data(
    juju: jubilant.Juju,
    data_integrator_app_name: str,
    substrate: Substrate,
    data: str,
    table_name: str = "test_table",
) -> bool:
    connection_string = build_connection_string(
        juju,
        data_integrator_app_name,
        DATA_INTEGRATOR_RELATION,
        substrate,
        database=TESTING_DATABASE,
    )
    connection = None
    try:
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True):
            with attempt:
                connection = psycopg2.connect(connection_string)
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE test_column = %s);", (data,)
            )
            return cursor.fetchone()[0]
    finally:
        if connection is not None:
            connection.close()
