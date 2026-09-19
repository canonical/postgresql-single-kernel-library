# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the client-relation events handler."""

import logging
from contextlib import contextmanager
from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, ModelError
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequestedEvent,
)
from single_kernel_postgresql.managers.database import (
    FORBIDDEN_USER_MSG,
    NO_ACCESS_TO_SECRET_MSG,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_RELATION,
    INVALID_DATABASE_NAME_BLOCKING_MESSAGE,
    INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE,
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
            "single_kernel_postgresql.managers.patroni.PatroniManager.online_cluster_members",
            return_value=[],
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


def test_database_requested_creates_the_user_and_database(substrate, harness, events, postgresql):
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
    if substrate == "k8s":
        # K8s refreshed every relation from the request handler; VM scoped to the requester.
        update_endpoints.assert_called_once_with(client_tls_files=(None, None, None))
    else:
        update_endpoints.assert_called_once_with(
            rel_id, online_members=[], client_tls_files=(None, None, None)
        )

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


def test_the_request_defer_message_is_substrate_specific(
    substrate, harness, events, postgresql, caplog
):
    caplog.set_level(logging.DEBUG, logger="single_kernel_postgresql.events.database")
    with (
        cluster_ready(harness, postgresql, ready=False),
        patch("ops.framework.EventBase.defer"),
    ):
        request_database(harness)

    if substrate == "k8s":
        assert "Cluster must be initialized before database can be requested" in caplog.text
    else:
        assert (
            "cluster not initialized, Patroni not started or primary endpoint not available"
            in caplog.text
        )


def test_the_request_guard_reads_substrate_specific_inputs(substrate, harness, events):
    """K8s waits only on the primary endpoint; VM also requires a started member."""
    with (
        patch.object(
            type(harness.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="1.1.1.1",
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=True,
        ),
    ):
        assert events._ready_for_request() is (substrate == "k8s")


def test_the_guards_defer_while_the_cluster_is_not_initialised(substrate, harness, events):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(
            peer_rel_id, harness.charm.app.name, {"cluster_initialised": ""}
        )
    with (
        patch.object(
            type(harness.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="1.1.1.1",
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=True,
        ),
    ):
        assert events._ready_for_request() is False
        assert events._ready_for_removal() is False


def test_database_requested_blocks_before_creating_when_the_username_is_taken(
    harness, events, postgresql
):
    postgresql.list_users.return_value = {"taken"}
    with (
        cluster_ready(harness, postgresql),
        patch.object(
            DatabaseRequestedEvent,
            "requested_entity_secret_content",
            new_callable=PropertyMock,
            return_value={"taken": "pw"},
        ),
    ):
        rel_id = request_database(harness)

    assert harness.model.unit.status == BlockedStatus("Requesting an existing username")
    postgresql.create_database.assert_not_called()
    assert "username" not in harness.get_relation_data(rel_id, harness.charm.app.name)


def test_database_requested_blocks_before_creating_when_the_prefix_is_too_short(
    harness, events, postgresql
):
    with cluster_ready(harness, postgresql):
        rel_id = request_database(harness, database="a*")

    assert harness.model.unit.status == BlockedStatus("Prefix too short")
    postgresql.create_database.assert_not_called()
    assert "username" not in harness.get_relation_data(rel_id, harness.charm.app.name)


def test_database_requested_publishes_nothing_when_creation_fails(harness, events, postgresql):
    postgresql.create_database.side_effect = PostgreSQLCreateDatabaseError("bad name")
    with cluster_ready(harness, postgresql):
        rel_id = request_database(harness)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert "username" not in data
    assert "uris" not in data
    assert "version" not in data
    assert "database" not in data


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

    assert harness.model.unit.status == BlockedStatus("Failed to initialize database relation")


def test_database_requested_blocks_on_a_version_error(harness, events, postgresql):
    postgresql.get_postgresql_version.side_effect = PostgreSQLGetPostgreSQLVersionError()
    with cluster_ready(harness, postgresql):
        request_database(harness)

    assert isinstance(harness.model.unit.status, BlockedStatus)


def test_database_requested_blocks_when_the_requested_entity_secret_is_not_readable(
    harness, events, postgresql
):
    """An ungranted requested-entity secret blocks the unit instead of crashing the hook."""
    with (
        cluster_ready(harness, postgresql),
        patch.object(
            DatabaseRequestedEvent,
            "requested_entity_secret_content",
            new_callable=PropertyMock,
        ) as _content,
    ):
        _content.side_effect = ModelError()
        request_database(harness)

    assert harness.model.unit.status == BlockedStatus("Missing grant to requested entity secret")
    postgresql.create_database.assert_not_called()


def test_the_unreadable_secret_block_routes_through_the_status_bridge(harness, events, postgresql):
    """The missing-grant block goes through the charm's set_unit_status on both substrates."""
    with (
        cluster_ready(harness, postgresql),
        patch.object(type(harness.charm), "set_unit_status", Mock()) as _gated,
        patch.object(
            DatabaseRequestedEvent,
            "requested_entity_secret_content",
            new_callable=PropertyMock,
        ) as _content,
    ):
        _content.side_effect = ModelError()
        request_database(harness)

    _gated.assert_called_once()
    assert _gated.call_args.args[0] == BlockedStatus("Missing grant to requested entity secret")
    assert not isinstance(harness.model.unit.status, BlockedStatus)
    postgresql.create_database.assert_not_called()


def test_database_requested_is_a_noop_for_a_follower(harness, events, postgresql):
    """The provider library already filters non-leaders; the handler guard is belt-and-braces."""
    with harness.hooks_disabled():
        harness.set_leader(False)
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_database_requested(event)

    postgresql.create_database.assert_not_called()
    rel_id = harness.model.get_relation(RELATION_NAME).id
    assert "username" not in harness.get_relation_data(rel_id, harness.charm.app.name)


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


def test_the_k8s_inline_publish_sets_the_tls_fields_when_tls_is_enabled(
    substrate, harness, events, postgresql
):
    if substrate != "k8s":
        pytest.skip("Only K8s wrote the TLS fields inside the request handler")
    with (
        cluster_ready(harness, postgresql),
        patch.object(
            type(events.tls_manager),
            "get_client_tls_files",
            return_value=("key", "CA", "cert"),
        ),
    ):
        rel_id = request_database(harness)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert data["tls"] == "True"
    assert data["tls-ca"] == "CA"


def test_the_relation_departed_observer_flags_the_departing_unit(harness, events):
    """Emitted through the framework, so the observer registration is itself exercised."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    relation = harness.model.get_relation(RELATION_NAME)

    harness.charm.on[RELATION_NAME].relation_departed.emit(
        relation, departing_unit_name=harness.charm.unit.name
    )

    assert "departing" in harness.get_relation_data(peer_rel_id, harness.charm.unit)


@pytest.mark.parametrize(
    "blocked_message",
    [
        INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE,
        INVALID_DATABASE_NAME_BLOCKING_MESSAGE,
        NO_ACCESS_TO_SECRET_MSG,
        FORBIDDEN_USER_MSG,
    ],
)
def test_relation_broken_survives_on_a_blocked_follower(
    harness, events, postgresql, blocked_message
):
    """A non-leader unit carrying a qualifying Blocked status must survive relation-broken.

    ops forbids a follower to read its own application databag; the post-broken
    status cleanup must therefore only scan remote databags, which is also where
    requested fields live. Emitted through the framework so the strict relation
    data access rules apply. The three messages exercise both cleanup paths
    (direct scan and unblock_custom_user_errors).
    """
    with harness.hooks_disabled():
        harness.add_relation(RELATION_NAME, "application2")
        harness.set_leader(False)
        harness.charm.unit.status = BlockedStatus(blocked_message)

    rel = next(
        r for r in harness.charm.model.relations[RELATION_NAME] if r.app.name == "application"
    )
    with cluster_ready(harness, postgresql):
        harness.charm.on[RELATION_NAME].relation_broken.emit(rel)

    assert isinstance(harness.charm.unit.status, ActiveStatus)


def test_the_relation_broken_observer_deletes_the_user(harness, events, postgresql):
    """Emitted through the framework, so the observer registration is itself exercised."""
    rel_id = harness.model.get_relation(RELATION_NAME).id

    with cluster_ready(harness, postgresql):
        harness.charm.on[RELATION_NAME].relation_broken.emit(
            harness.model.get_relation(RELATION_NAME)
        )

    postgresql.delete_user.assert_called_once_with(events.manager.relation_username(rel_id))


def test_relation_departed_flags_only_this_unit(harness, events):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    event = Mock()
    event.departing_unit = harness.charm.unit
    events._on_relation_departed(event)
    assert "departing" in harness.get_relation_data(peer_rel_id, harness.charm.unit)

    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel_id, harness.charm.unit.name, {"departing": ""})
    event.departing_unit = harness.model.get_unit(f"{harness.charm.app.name}/1")
    events._on_relation_departed(event)
    assert "departing" not in harness.get_relation_data(peer_rel_id, harness.charm.unit)


def test_the_broken_defer_message_is_substrate_specific(
    substrate, harness, events, postgresql, caplog
):
    caplog.set_level(logging.DEBUG, logger="single_kernel_postgresql.events.database")
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql, ready=False):
        events._on_relation_broken(event)

    if substrate == "k8s":
        assert "Cluster must be initialized before user can be deleted" in caplog.text
    else:
        assert (
            "Cluster must be initialized and primary available before user can be deleted"
            in caplog.text
        )


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
    harness.model.unit.status = BlockedStatus("Failed to initialize database relation")
    event = Mock()
    event.relation = harness.model.get_relation(RELATION_NAME)

    with cluster_ready(harness, postgresql):
        events._on_relation_broken(event)

    assert harness.model.unit.status == ActiveStatus()


def test_a_replayed_request_whose_request_data_is_gone_is_skipped(
    substrate, harness, events, postgresql, caplog
):
    """A deferred notice replayed after its relation died reads the name back as absent."""
    caplog.set_level(logging.WARNING, logger="single_kernel_postgresql.managers.database")
    with (
        cluster_ready(harness, postgresql) as update_endpoints,
        patch("ops.framework.EventBase.defer") as _defer,
    ):
        rel = harness.model.get_relation(RELATION_NAME)
        # The relation databag carries no request: that is what ops supplies when it
        # re-emits a deferred notice for a relation removed while the request waited.
        events.database_provides.on.database_requested.emit(rel, app=rel.app)

    assert "Database name is not set in the relation data, skipping." in caplog.text
    postgresql.create_database.assert_not_called()
    postgresql.create_user.assert_not_called()
    update_endpoints.assert_not_called()
    _defer.assert_not_called()
