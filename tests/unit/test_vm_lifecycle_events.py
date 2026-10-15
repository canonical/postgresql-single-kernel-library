# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the migrated VM lifecycle flows in the events handler.

Ports the load-bearing cases of the VM charm's tests (password ensure,
leader members bootstrap, update-status gates, teardown guards).
"""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ModelError
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import PEER_RELATION


@pytest.fixture(autouse=True)
def _vm_only(substrate):
    if substrate != Substrates.VM:
        pytest.skip("the VM lifecycle flows are VM-only")


@pytest.fixture
def handler(harness):
    handler = harness.charm.postgresql_events_handler
    # Test doubles for the collaborators whose interactions these tests assert.
    handler.observer_manager = Mock()
    handler.backup_manager = Mock()
    handler.restore_manager = Mock()
    return handler


def _rel_id(harness):
    return harness.model.get_relation(PEER_RELATION).id


def test_on_leader_elected_ensures_missing_passwords(handler, harness):
    """Missing secrets are generated when the leader-elected flow runs."""
    handler.state.config = Mock()
    handler.state.config.system_users = None
    with patch.object(handler.charm, "update_config"):
        harness.set_leader(True)
    for key in ("operator-password", "replication-password", "rewind-password"):
        assert handler.state.get_secret("app", key)


def test_on_leader_elected_adopts_configured_passwords(handler, harness):
    handler.state.config = Mock()
    handler.state.config.system_users = "secret:1"
    with (
        patch.object(
            type(handler.state),
            "get_secret_from_id",
            return_value={"operator-password": "configured"},
        ),
        patch.object(handler.charm, "update_config"),
    ):
        harness.set_leader(True)
    assert handler.state.get_secret("app", "operator-password") == "configured"


def test_on_leader_elected_defers_on_unreadable_system_secret(handler, harness):
    handler.state.config = Mock()
    handler.state.config.system_users = "secret:broken"
    event = Mock()
    with (
        patch.object(
            type(handler.state),
            "get_secret_from_id",
            side_effect=ModelError("unreadable"),
        ),
        patch.object(type(handler.state), "has_raft_keys", Mock(return_value=True)),
        patch.object(handler.membership_manager, "raft_reinitialisation") as _reinit,
    ):
        handler._on_leader_elected(event)
        event.defer.assert_called_once()
        # The flow still proceeds to the raft recovery gate (passwords generated).
        _reinit.assert_called_once()


def test_update_status_gates_on_uninitialised_cluster(handler, harness):
    with (
        patch.object(
            type(handler.state),
            "has_raft_keys",
            Mock(return_value=False),
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=False,
        ),
    ):
        assert handler._can_run_on_update_status() is False


def test_update_status_gates_on_raft_recovery(handler, harness):
    with patch.object(type(handler.state), "has_raft_keys", Mock(return_value=True)):
        assert handler._can_run_on_update_status() is False


def test_storage_detaching_noop_on_scale_down(handler, harness):
    with patch.object(
        type(handler.state.application),
        "planned_units",
        new_callable=PropertyMock,
        return_value=2,
    ):
        handler._on_storage_detaching(Mock())
    handler.observer_manager.stop_observer.assert_not_called()
    handler.backup_manager.stop_log_rotation.assert_not_called()


def test_storage_detaching_stops_on_teardown(handler, harness):
    with patch.object(
        type(handler.state.application),
        "planned_units",
        new_callable=PropertyMock,
        return_value=0,
    ):
        handler._on_storage_detaching(Mock())
    handler.observer_manager.stop_observer.assert_called_once()
    handler.backup_manager.stop_log_rotation.assert_called_once()
