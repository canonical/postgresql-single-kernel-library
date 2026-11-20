# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for logical replication circular detection.

This test module focuses on detecting and preventing circular replication
at the table level for logical replication setups.
"""

import json

import jubilant
import psycopg2
import pytest
from tenacity import Retrying, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import CHARM_BASE, build_connection_string
from tests.integration.helpers.juju import fast_forward, get_app_leader, wait_for_apps_status
from tests.integration.types import Substrate

APP_NAME_A = "postgresql-a"
APP_NAME_B = "postgresql-b"
APP_NAME_C = "postgresql-c"

DATA_INTEGRATOR_A = "data-integrator-a"
DATA_INTEGRATOR_B = "data-integrator-b"
DATA_INTEGRATOR_C = "data-integrator-c"
DATA_INTEGRATOR_RELATION = "postgresql"

APP_CONFIG = {"profile": "testing"}
TESTING_DATABASE = "testdb"


@pytest.mark.abort_on_fail
def test_deploy_clusters(juju: jubilant.Juju, charm, charm_resources, substrate: Substrate):
    """Deploy three PostgreSQL clusters for testing circular replication."""
    juju.deploy(
        charm,
        app=APP_NAME_A,
        num_units=1,
        base=CHARM_BASE,
        config=APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm,
        app=APP_NAME_B,
        num_units=1,
        base=CHARM_BASE,
        config=APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        charm,
        app=APP_NAME_C,
        num_units=1,
        base=CHARM_BASE,
        config=APP_CONFIG.copy(),
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )
    juju.deploy(
        "data-integrator",
        app=DATA_INTEGRATOR_A,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    juju.deploy(
        "data-integrator",
        app=DATA_INTEGRATOR_B,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    juju.deploy(
        "data-integrator",
        app=DATA_INTEGRATOR_C,
        num_units=1,
        channel="latest/stable",
        config={"database-name": TESTING_DATABASE},
    )
    juju.wait(
        wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B, APP_NAME_C),
        timeout=2500,
    )

    # Integrate data integrators with their clusters.
    with fast_forward(juju):
        juju.integrate(APP_NAME_A, DATA_INTEGRATOR_A)
        juju.integrate(APP_NAME_B, DATA_INTEGRATOR_B)
        juju.integrate(APP_NAME_C, DATA_INTEGRATOR_C)
        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=500)


@pytest.mark.abort_on_fail
def test_setup_tables(juju: jubilant.Juju, substrate: Substrate):
    """Create test tables in all clusters."""
    _create_test_table(juju, DATA_INTEGRATOR_A, substrate, "users")
    _create_test_table(juju, DATA_INTEGRATOR_A, substrate, "orders")
    _create_test_table(juju, DATA_INTEGRATOR_B, substrate, "users")
    _create_test_table(juju, DATA_INTEGRATOR_B, substrate, "orders")
    _create_test_table(juju, DATA_INTEGRATOR_C, substrate, "users")
    _create_test_table(juju, DATA_INTEGRATOR_C, substrate, "orders")


@pytest.mark.abort_on_fail
def test_bidirectional_same_table_blocked(juju: jubilant.Juju, substrate: Substrate):
    """Test that bidirectional replication of the same table is blocked.

    Scenario: Cluster A <-> Cluster B (both trying to replicate public.users)
    Expected: Blocked with circular replication error
    """
    # First, setup the logical replication relation A -> B.
    with fast_forward(juju):
        juju.integrate(
            f"{APP_NAME_A}:logical-replication-offer", f"{APP_NAME_B}:logical-replication"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    # Configure B to subscribe to users from A.
    config_b = APP_CONFIG.copy()
    config_b["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.users"]
    })
    juju.config(APP_NAME_B, config_b)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    # Verify B is subscribed and replication works.
    _insert_test_data(juju, DATA_INTEGRATOR_A, substrate, "from_a", "users")
    _check_test_data(juju, DATA_INTEGRATOR_B, substrate, "from_a", "users")

    # Now setup B -> A replication (reverse direction).
    with fast_forward(juju):
        juju.integrate(
            f"{APP_NAME_B}:logical-replication-offer", f"{APP_NAME_A}:logical-replication"
        )
        juju.wait(
            lambda status: jubilant.all_agents_idle(status, APP_NAME_A, APP_NAME_B), timeout=500
        )

    # Try to configure A to subscribe to the SAME table from B.
    # This should be blocked due to circular replication.
    config_a = APP_CONFIG.copy()
    config_a["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.users"]
    })
    juju.config(APP_NAME_A, config_a)

    # Wait for the leader unit to go to blocked status.
    leader_unit_a = get_app_leader(juju, APP_NAME_A)
    status = juju.wait(
        lambda status: status.get_units(APP_NAME_A)[leader_unit_a].is_blocked, timeout=120
    )
    assert "circular replication" in (
        status.get_units(APP_NAME_A)[leader_unit_a].workload_status.message.lower()
    )


@pytest.mark.abort_on_fail
def test_bidirectional_different_tables_allowed(juju: jubilant.Juju, substrate: Substrate):
    """Test that bidirectional replication of different tables is allowed.

    Scenario: Cluster A -> B (public.users), Cluster B -> A (public.orders)
    Expected: Both work fine, no circular replication
    """
    # A is already subscribed to users from B (but blocked). Change to orders instead.
    config_a = APP_CONFIG.copy()
    config_a["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.orders"]
    })
    juju.config(APP_NAME_A, config_a)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    # Verify bidirectional replication works for different tables.
    _insert_test_data(juju, DATA_INTEGRATOR_A, substrate, "a_users", "users")
    _insert_test_data(juju, DATA_INTEGRATOR_B, substrate, "b_orders", "orders")
    # Check A -> B (users).
    _check_test_data(juju, DATA_INTEGRATOR_B, substrate, "a_users", "users")
    # Check B -> A (orders).
    _check_test_data(juju, DATA_INTEGRATOR_A, substrate, "b_orders", "orders")


@pytest.mark.abort_on_fail
def test_cleanup_ab_relations(juju: jubilant.Juju, substrate: Substrate):
    """Clean up A-B relations before testing multi-hop scenario."""
    juju.remove_relation(
        f"{APP_NAME_A}:logical-replication-offer", f"{APP_NAME_B}:logical-replication"
    )
    juju.remove_relation(
        f"{APP_NAME_B}:logical-replication-offer", f"{APP_NAME_A}:logical-replication"
    )

    # Clear subscription configs.
    config_a = APP_CONFIG.copy()
    config_a["logical-replication-subscription-request"] = ""
    juju.config(APP_NAME_A, config_a)

    config_b = APP_CONFIG.copy()
    config_b["logical-replication-subscription-request"] = ""
    juju.config(APP_NAME_B, config_b)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    # Truncate tables to start fresh.
    _truncate_table(juju, DATA_INTEGRATOR_A, substrate, "users")
    _truncate_table(juju, DATA_INTEGRATOR_B, substrate, "users")


@pytest.mark.abort_on_fail
def test_multihop_circular_blocked(juju: jubilant.Juju, substrate: Substrate):
    """Test that multi-hop circular replication is blocked.

    Scenario: Cluster A -> B -> C -> A (all for public.users)
    Expected: The final A subscription is blocked with circular replication error
    """
    # Setup A -> B replication.
    with fast_forward(juju):
        juju.integrate(
            f"{APP_NAME_A}:logical-replication-offer", f"{APP_NAME_B}:logical-replication"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    config_b = APP_CONFIG.copy()
    config_b["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.users"]
    })
    juju.config(APP_NAME_B, config_b)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_B), timeout=500)

    # Verify A -> B works.
    _insert_test_data(juju, DATA_INTEGRATOR_A, substrate, "from_a", "users")
    _check_test_data(juju, DATA_INTEGRATOR_B, substrate, "from_a", "users")

    # Setup B -> C replication.
    with fast_forward(juju):
        juju.integrate(
            f"{APP_NAME_B}:logical-replication-offer", f"{APP_NAME_C}:logical-replication"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_B, APP_NAME_C), timeout=500)

    config_c = APP_CONFIG.copy()
    config_c["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.users"]
    })
    juju.config(APP_NAME_C, config_c)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_B, APP_NAME_C), timeout=500)

    # Verify A -> B -> C works.
    _insert_test_data(juju, DATA_INTEGRATOR_A, substrate, "from_a_via_b", "users")
    _check_test_data(juju, DATA_INTEGRATOR_C, substrate, "from_a_via_b", "users")

    # Now try to setup C -> A replication (completing the circle).
    with fast_forward(juju):
        juju.integrate(
            f"{APP_NAME_C}:logical-replication-offer", f"{APP_NAME_A}:logical-replication"
        )
        juju.wait(
            lambda status: jubilant.all_agents_idle(status, APP_NAME_A, APP_NAME_C), timeout=500
        )

    # Try to configure A to subscribe to users from C.
    # This should be blocked because the chain is: A -> B -> C -> (back to A).
    config_a = APP_CONFIG.copy()
    config_a["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.users"]
    })
    juju.config(APP_NAME_A, config_a)

    leader_unit_a = get_app_leader(juju, APP_NAME_A)
    status = juju.wait(
        lambda status: status.get_units(APP_NAME_A)[leader_unit_a].is_blocked, timeout=120
    )
    assert "circular replication" in (
        status.get_units(APP_NAME_A)[leader_unit_a].workload_status.message.lower()
    )


@pytest.mark.abort_on_fail
def test_multihop_different_table_allowed(juju: jubilant.Juju, substrate: Substrate):
    """Test that multi-hop replication of different table works.

    Scenario: A -> B -> C (public.users), C -> A (public.orders)
    Expected: Works fine since orders doesn't create a cycle
    """
    # Change A's subscription to orders instead of users.
    config_a = APP_CONFIG.copy()
    config_a["logical-replication-subscription-request"] = json.dumps({
        TESTING_DATABASE: ["public.orders"]
    })
    juju.config(APP_NAME_A, config_a)

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME_A, APP_NAME_C), timeout=500)

    # Verify the replication works.
    _insert_test_data(juju, DATA_INTEGRATOR_C, substrate, "c_orders", "orders")
    _check_test_data(juju, DATA_INTEGRATOR_A, substrate, "c_orders", "orders")
    # Verify the A -> B -> C chain still works for users.
    _insert_test_data(juju, DATA_INTEGRATOR_A, substrate, "final_test", "users")
    _check_test_data(juju, DATA_INTEGRATOR_C, substrate, "final_test", "users")


# Helper functions


def _create_test_table(
    juju: jubilant.Juju, data_integrator_app_name: str, substrate: Substrate, table_name: str
) -> None:
    """Create a test table in the specified database."""
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


def _truncate_table(
    juju: jubilant.Juju, data_integrator_app_name: str, substrate: Substrate, table_name: str
) -> None:
    """Truncate a table in the specified database."""
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
            cursor.execute(f"TRUNCATE TABLE {table_name};")
    finally:
        if connection is not None:
            connection.close()


def _insert_test_data(
    juju: jubilant.Juju,
    data_integrator_app_name: str,
    substrate: Substrate,
    data: str,
    table_name: str,
) -> None:
    """Insert test data into a table."""
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
    table_name: str,
) -> bool:
    """Check if test data exists in a table."""
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
