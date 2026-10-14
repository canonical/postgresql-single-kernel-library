# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the VM start flow (typed user-setup failures)."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops.model import BlockedStatus
from single_kernel_postgresql.config.exceptions import RaftRecoveryTimeoutError
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQLCreatePredefinedRolesError,
    PostgreSQLCreateUserError,
    PostgreSQLGrantDatabasePrivilegesToUserError,
    PostgreSQLListUsersError,
)


@pytest.fixture
def handler(harness):
    return harness.charm.postgresql_events_handler


@pytest.fixture
def vm_primary(harness, handler, substrate):
    """A VM handler whose bootstrap checks all pass, ready for _start_primary."""
    if substrate != "vm":
        pytest.skip("VM flow")
    with (
        patch.object(
            type(harness.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="10.0.0.1",
        ),
        patch.object(handler.patroni_manager, "bootstrap_cluster", return_value=True),
        patch.object(type(handler.patroni_manager), "member_started", property(lambda _: True)),
        patch.object(handler.cluster_manager, "can_connect_to_postgresql", return_value=True),
        patch.object(handler, "postgresql", lambda: Mock()),
    ):
        yield handler


def test_start_primary_defers_on_list_users_error(vm_primary):
    event = Mock()
    with patch.object(
        vm_primary.cluster_manager, "setup_instance_users", side_effect=PostgreSQLListUsersError
    ):
        vm_primary._start_primary(event)
    event.defer.assert_called_once()
    assert not isinstance(vm_primary.charm.unit.status, BlockedStatus)


def test_start_primary_blocks_on_predefined_roles_error(vm_primary):
    event = Mock()
    with patch.object(
        vm_primary.cluster_manager,
        "setup_instance_users",
        side_effect=PostgreSQLCreatePredefinedRolesError,
    ):
        vm_primary._start_primary(event)
    assert vm_primary.charm.unit.status == BlockedStatus("Failed to create pre-defined roles")
    event.defer.assert_not_called()


def test_start_primary_blocks_on_grant_privileges_error(vm_primary):
    event = Mock()
    with patch.object(
        vm_primary.cluster_manager,
        "setup_instance_users",
        side_effect=PostgreSQLGrantDatabasePrivilegesToUserError,
    ):
        vm_primary._start_primary(event)
    assert vm_primary.charm.unit.status == BlockedStatus(
        "Failed to grant database privileges to user"
    )


def test_start_primary_blocks_on_create_user_error(vm_primary):
    event = Mock()
    with patch.object(
        vm_primary.cluster_manager, "setup_instance_users", side_effect=PostgreSQLCreateUserError
    ):
        vm_primary._start_primary(event)
    assert vm_primary.charm.unit.status == BlockedStatus("Failed to create postgres user")


def test_can_start_stops_during_raft_recovery(harness, handler, substrate):
    if substrate != "vm":
        pytest.skip("VM flow")
    with (
        patch.object(handler, "_check_detached_storage"),
        patch.object(type(handler.state), "has_raft_keys", lambda self: True),
    ):
        assert handler._can_start(Mock()) is False


def test_can_start_proceeds_without_raft_keys(harness, handler, substrate):
    if substrate != "vm":
        pytest.skip("VM flow")
    with (
        patch.object(handler, "_check_detached_storage"),
        patch.object(type(handler.state), "has_raft_keys", lambda self: False),
    ):
        assert handler._can_start(Mock()) is True


def test_check_detached_storage_restores_status_through_the_config_manager(
    harness, handler, substrate
):
    if substrate != "vm":
        pytest.skip("VM flow")
    with (
        patch.object(handler.workload, "is_storage_attached", return_value=True),
        patch.object(handler.config_manager, "_restore_unit_status") as restore,
    ):
        handler._check_detached_storage(handler.workload)
    restore.assert_called_once_with(harness.charm.unit.status)


def test_charm_inherits_the_restart_callback(harness, substrate):
    if substrate != "vm":
        pytest.skip("VM flow")
    event = Mock()
    with patch.object(harness.charm.postgresql_events_handler, "_restart") as restart:
        harness.charm._restart(event)
    restart.assert_called_once_with(event)


def test_leader_elected_defers_on_raft_recovery_timeout(harness, handler, substrate):
    if substrate != "vm":
        pytest.skip("VM flow")
    with patch.object(
        handler.membership_manager,
        "raft_reinitialisation",
        side_effect=RaftRecoveryTimeoutError("timed out"),
    ), patch.object(type(handler.state), "has_raft_keys", lambda self: True):
        handler._on_leader_elected(Mock())


def test_promote_primary_unit_defers_on_raft_recovery_timeout(harness, handler, substrate):
    if substrate != "vm":
        pytest.skip("VM flow")
    event = Mock()
    with (
        patch.object(type(handler.state), "has_raft_keys", lambda self: True),
        patch.object(
            handler.membership_manager,
            "raft_reinitialisation",
            side_effect=RaftRecoveryTimeoutError("timed out"),
        ),
    ):
        handler._promote_primary_unit(event)
    event.defer.assert_called_once()
