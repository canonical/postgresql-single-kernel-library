# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the client-relation events handler."""

from contextlib import contextmanager
from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, Unit
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_RELATION,
    PostgreSQLCreateDatabaseError,
    PostgreSQLCreateUserError,
    PostgreSQLGetPostgreSQLVersionError,
)

RELATION_NAME = "database"
DATABASE = "test_database"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
POSTGRESQL_VERSION = "16"
USER_HASH = "relhash"


@pytest.fixture
def events(harness):
    """A begun leader charm with the cluster initialised and a client relation."""
    with harness.hooks_disabled():
        harness.set_leader(True)
        peer_rel_id = harness.model.get_relation(PEER_RELATION).id
        harness.update_relation_data(
            peer_rel_id, harness.charm.app.name, {"cluster_initialised": "True"}
        )
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/0", {"user_hash": USER_HASH}
        )
    harness.add_relation(RELATION_NAME, "application")
    return harness.charm.database


@pytest.fixture
def postgresql():
    client = Mock()
    client.get_postgresql_version.return_value = POSTGRESQL_VERSION
    client.list_users.return_value = set()
    client.list_valid_privileges_and_roles.return_value = (set(), set())
    return client


@contextmanager
def cluster_ready(harness, postgresql, ready=True):
    """Patch every readiness input the two substrates' guards consult."""
    with (
        patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)),
        patch.object(type(harness.charm), "update_config", Mock(return_value=True)),
        patch.object(
            type(harness.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="1.1.1.1" if ready else None,
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.member_started",
            new_callable=PropertyMock,
            return_value=ready,
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=ready,
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.get_plugins",
            return_value=["pgaudit"],
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.update_endpoints"
        ) as update_endpoints,
        patch(
            "single_kernel_postgresql.managers.database.new_password",
            return_value="test-password",
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.user_hash",
            new_callable=PropertyMock,
            return_value=USER_HASH,
        ),
    ):
        yield update_endpoints


def request_database(harness, database=DATABASE):
    """Drive a real database_requested through the provider library."""
    rel_id = harness.model.get_relation(RELATION_NAME).id
    harness.update_relation_data(
        rel_id, "application", {"database": database, "extra-user-roles": EXTRA_USER_ROLES}
    )
    return rel_id


def test_database_requested_creates_the_user_and_database(harness, events, postgresql):
    with cluster_ready(harness, postgresql) as update_endpoints:
        rel_id = request_database(harness)

    user = events.manager.relation_username(rel_id)
    postgresql.create_database.assert_called_once_with(DATABASE, plugins=["pgaudit"])
    postgresql.create_user.assert_called_once_with(
        user,
        "test-password",
        extra_user_roles=["createdb", "createrole", ACCESS_GROUP_RELATION],
        database=DATABASE,
    )
    update_endpoints.assert_called_once_with(rel_id)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert data["username"] == user
    assert data["password"] == "test-password"
    assert data["version"] == POSTGRESQL_VERSION
    assert data["database"] == DATABASE
    assert not isinstance(harness.model.unit.status, BlockedStatus)


def test_database_requested_defers_until_the_cluster_is_ready(harness, events, postgresql):
    with (
        cluster_ready(harness, postgresql, ready=False),
        patch("ops.framework.EventBase.defer") as _defer,
    ):
        request_database(harness)

    _defer.assert_called_once()
    postgresql.create_database.assert_not_called()


def test_database_requested_defers_until_peers_have_synced(harness, events, postgresql):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/1")
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/1", {"user_hash": "stale"}
        )

    with (
        cluster_ready(harness, postgresql),
        patch("ops.framework.EventBase.defer") as _defer,
    ):
        request_database(harness)

    _defer.assert_called_once()
    postgresql.create_database.assert_not_called()


def test_database_requested_blocks_with_the_error_message(harness, events, postgresql):
    postgresql.create_database.side_effect = PostgreSQLCreateDatabaseError("bad name")
    with cluster_ready(harness, postgresql):
        request_database(harness)

    assert harness.model.unit.status == BlockedStatus("bad name")


def test_database_requested_blocks_generically_without_a_message(harness, events, postgresql):
    postgresql.create_user.side_effect = PostgreSQLCreateUserError()
    with cluster_ready(harness, postgresql):
        request_database(harness)

    assert harness.model.unit.status == BlockedStatus(events.manager.failed_init_message)


def test_database_requested_blocks_on_a_version_error(harness, events, postgresql):
    postgresql.get_postgresql_version.side_effect = PostgreSQLGetPostgreSQLVersionError()
    with cluster_ready(harness, postgresql):
        request_database(harness)

    assert isinstance(harness.model.unit.status, BlockedStatus)


def test_database_requested_publishes_k8s_service_endpoints_inline(
    substrate, harness, events, postgresql
):
    """K8s wrote the primary Service endpoint inside the request handler; VM did not."""
    with cluster_ready(harness, postgresql):
        rel_id = request_database(harness)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    if substrate == "k8s":
        app = harness.charm.app.name
        assert data["endpoints"] == f"{app}-primary.test-model.svc.cluster.local:5432"
        assert data["uris"].endswith(
            f"@{app}-primary.test-model.svc.cluster.local:5432/{DATABASE}"
        )
    else:
        assert "endpoints" not in data


def test_relation_departed_flags_only_this_unit(harness, events):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    event = Mock()
    event.departing_unit = harness.charm.unit
    events._on_relation_departed(event)
    assert "departing" in harness.get_relation_data(peer_rel_id, harness.charm.unit)

    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel_id, harness.charm.unit.name, {"departing": ""})
    event.departing_unit = Unit(
        f"{harness.charm.app.name}/1", None, harness.charm.app._backend, {}
    )
    events._on_relation_departed(event)
    assert "departing" not in harness.get_relation_data(peer_rel_id, harness.charm.unit)


def test_relation_broken_deletes_the_user(harness, events, postgresql):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_relation_broken(event)

    postgresql.delete_user.assert_called_once_with(events.manager.relation_username(rel_id))


def test_relation_broken_skips_a_departing_unit(harness, events, postgresql):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel_id, harness.charm.unit.name, {"departing": "True"})
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_relation_broken(event)

    postgresql.delete_user.assert_not_called()


def test_relation_broken_defers_until_the_cluster_is_ready(harness, events, postgresql):
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql, ready=False):
        events._on_relation_broken(event)

    event.defer.assert_called_once()
    postgresql.delete_user.assert_not_called()


def test_relation_broken_on_a_follower_defers_while_the_user_exists(harness, events, postgresql):
    with harness.hooks_disabled():
        harness.set_leader(False)
    rel_id = harness.model.get_relation(RELATION_NAME).id
    postgresql.list_users.return_value = {events.manager.relation_username(rel_id)}
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_relation_broken(event)

    event.defer.assert_called_once()
    postgresql.delete_user.assert_not_called()


def test_relation_broken_on_a_follower_updates_config_once_the_user_is_gone(
    harness, events, postgresql
):
    with harness.hooks_disabled():
        harness.set_leader(False)
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with (
        cluster_ready(harness, postgresql),
        patch.object(type(harness.charm), "update_config") as _update_config,
    ):
        events._on_relation_broken(event)

    event.defer.assert_not_called()
    _update_config.assert_called_once()
    postgresql.delete_user.assert_not_called()


def test_k8s_removal_guard_does_not_wait_on_the_primary_endpoint(
    substrate, harness, events, postgresql
):
    """K8s' relation_broken settles for a started member; VM also needs a primary."""
    with (
        patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)),
        patch.object(type(harness.charm), "update_config", Mock(return_value=True)),
        patch.object(
            type(harness.charm), "primary_endpoint", new_callable=PropertyMock, return_value=None
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch("single_kernel_postgresql.managers.database.DatabaseManager.update_endpoints"),
    ):
        assert events._ready_for_removal() is (substrate == "k8s")


def test_update_unit_status_runs_before_the_departing_check(harness, events, postgresql):
    """A stale blocking status clears even when the unit itself is going away."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel_id, harness.charm.unit.name, {"departing": "True"})
    harness.model.unit.status = BlockedStatus("Failed to initialize relation database")
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_relation_broken(event)

    assert harness.model.unit.status == ActiveStatus()
