# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the migrated VM peer-relation-changed and config-changed flows."""

from unittest.mock import Mock, patch

import pytest
from ops import BlockedStatus, MaintenanceStatus, WaitingStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.utils.postgresql import PostgreSQLUpdateUserPasswordError
from tenacity import RetryError


@pytest.fixture(autouse=True)
def _vm_only(substrate):
    if substrate != Substrates.VM:
        pytest.skip("the VM peer/config flows are VM-only")


@pytest.fixture
def handler(harness):
    handler = harness.charm.postgresql_events_handler
    handler.observer_manager = Mock()
    handler.backup_manager = Mock()
    handler.restore_manager = Mock()
    handler.charm.async_replication = Mock()
    handler.charm.watcher_handler = Mock()
    # Default the cheap running-probe to "not running" so the peers hook still
    # enqueues the start (pre-gate behavior); tests exercising the running-state
    # gate override this with return_value=True. Patching also keeps the suite
    # deterministic on hosts where snapd has no charmed-postgresql snap.
    with patch.object(handler.workload, "is_patroni_running", return_value=False):
        yield handler


def _mock_membership(handler):
    """Membership double whose early-exit checks pass so the peer flow proceeds."""
    membership = Mock()
    membership.state.application.is_cluster_initialised = True
    membership.state.application.members_ips = {"10.1.1.1"}
    membership.state.unit_ip = "10.1.1.1"
    membership.state.model.unit.is_leader.return_value = False
    membership.has_raft_keys.return_value = False
    handler.membership_manager = membership
    return membership


def _seed_initialised(handler):
    handler.state.application.data.update({"cluster_initialised": "True"})


# -- peer-relation-changed -----------------------------------------------------


def test_on_peer_relation_changed_sets_maintenance_on_patroni_retry(handler):
    membership = _mock_membership(handler)
    with patch.object(handler.charm, "update_config", side_effect=RetryError(Mock())):
        handler._on_peer_relation_changed(Mock())
    status = membership.set_unit_status.call_args[0][0]
    assert isinstance(status, MaintenanceStatus)
    assert status.message == "cluster member update failed, retrying"


def test_on_peer_relation_changed_blocks_on_invalid_config(handler):
    """The invalid-config blocked persists through the status machinery.

    A directly-written blocked is wiped by the collect handler's all-active
    recompute on the same hook; the databag record is what collect replays.
    """
    _mock_membership(handler)
    with patch.object(handler.charm, "update_config", side_effect=ValueError("bad")):
        handler._on_peer_relation_changed(Mock())
    records = handler.state.statuses.get("unit", handler.config_manager.name).root
    assert any(
        r.status == "blocked" and r.message == "Configuration Error. Please check the logs"
        for r in records
    )


def test_on_peer_relation_changed_exits_when_unit_blocked(handler, harness):
    _mock_membership(handler)
    handler.patroni_manager = Mock()
    harness.model.unit.status = BlockedStatus("blocked")
    with patch.object(handler.charm, "update_config"):
        handler._on_peer_relation_changed(Mock())
    handler.patroni_manager.start_patroni.assert_not_called()


def test_on_peer_relation_changed_defers_until_member_started(handler):
    membership = _mock_membership(handler)
    handler.patroni_manager = Mock()
    handler.patroni_manager.member_started = False
    event = Mock()
    with patch.object(handler.charm, "update_config"):
        handler._on_peer_relation_changed(event)
    status = membership.set_unit_status.call_args[0][0]
    assert isinstance(status, WaitingStatus)
    assert status.message == "awaiting for member to start"
    event.defer.assert_called_once()


def test_on_peer_relation_changed_starts_patroni_before_the_member_gate(handler):
    """The inline Patroni start runs BEFORE the member_started defer gate (union order)."""
    membership = _mock_membership(handler)
    handler.patroni_manager = Mock()
    handler.patroni_manager.member_started = False
    event = Mock()
    with patch.object(handler.charm, "update_config"):
        handler._on_peer_relation_changed(event)
    handler.patroni_manager.start_patroni.assert_called_once()
    membership.set_unit_status.assert_called_once_with(
        WaitingStatus("awaiting for member to start")
    )
    event.defer.assert_called_once()


def test_on_peer_relation_changed_leader_updates_endpoints(handler):
    _mock_membership(handler)
    handler.patroni_manager = Mock()
    handler.patroni_manager.member_started = True
    handler.patroni_manager.is_member_registered_in_cluster.return_value = True
    handler.backup_manager.start_stop_pgbackrest_service.return_value = True
    handler.state.peer.data.update({
        "exporter-started": "True",
        "pgbackrest-exporter-started": "True",
    })
    handler.charm.update_relation_endpoints = Mock()
    handler.charm.primary_endpoint = "10.1.1.1"
    event = Mock()
    with (
        patch.object(handler.charm, "update_config"),
        patch.object(handler.state.model.unit, "is_leader", return_value=True),
    ):
        handler._on_peer_relation_changed(event)
    handler.charm.update_relation_endpoints.assert_called_once()
    handler.charm.async_replication.handle_read_only_mode.assert_called_once()
    handler.charm.watcher_handler.update_endpoints.assert_called()


def test_on_peer_relation_changed_skips_start_when_patroni_running(handler):
    """Patroni already running: the hook must NOT enqueue another snap start.

    The unconditional per-hook start raced deliberate service stops (systemd
    canceled the stop job or re-activated the stopped service); the gate uses
    the cheap workload running-probe, not the 60s-retrying member_started.
    """
    membership = _mock_membership(handler)
    handler.patroni_manager = Mock()
    handler.patroni_manager.member_started = True
    handler.patroni_manager.is_member_registered_in_cluster.return_value = True
    handler.backup_manager.start_stop_pgbackrest_service.return_value = True
    handler.state.peer.data.update({
        "exporter-started": "True",
        "pgbackrest-exporter-started": "True",
    })
    event = Mock()
    with (
        patch.object(handler.workload, "is_patroni_running", return_value=True),
        patch.object(handler.charm, "update_config"),
    ):
        handler._on_peer_relation_changed(event)
    handler.patroni_manager.start_patroni.assert_not_called()
    # The rest of the flow is unaffected: the member gate passes, no defer.
    event.defer.assert_not_called()


def test_on_peer_relation_changed_starts_patroni_when_not_running(handler):
    """Patroni not running: the hook still starts it (first peer data arrival)."""
    membership = _mock_membership(handler)
    handler.patroni_manager = Mock()
    handler.patroni_manager.member_started = False
    event = Mock()
    with (
        patch.object(handler.workload, "is_patroni_running", return_value=False),
        patch.object(handler.charm, "update_config"),
    ):
        handler._on_peer_relation_changed(event)
    handler.patroni_manager.start_patroni.assert_called_once()
    membership.set_unit_status.assert_called_once_with(
        WaitingStatus("awaiting for member to start")
    )
    event.defer.assert_called_once()


# -- config-changed ------------------------------------------------------------


def test_on_config_changed_defers_when_cluster_not_initialised(handler):
    event = Mock()
    with patch.object(handler.membership_manager, "update_endpoint_addresses") as update_addr:
        handler._on_config_changed(event)
    update_addr.assert_called_once()
    event.defer.assert_called_once()


def test_on_config_changed_defers_when_refresh_in_progress(handler):
    _seed_initialised(handler)
    handler.refresh_manager.refresh = Mock(in_progress=True)
    event = Mock()
    with patch.object(handler.membership_manager, "update_endpoint_addresses"):
        handler._on_config_changed(event)
    event.defer.assert_called_once()


def test_on_config_changed_member_ip_change_updates_async_data(handler):
    _seed_initialised(handler)
    event = Mock()
    with (
        patch.object(handler.membership_manager, "update_endpoint_addresses"),
        patch.object(handler.membership_manager, "update_member_ip", return_value=True),
        patch.object(handler.config_manager, "validate_config_options") as validate,
        patch.object(handler.charm, "update_config"),
    ):
        handler._on_config_changed(event)
    handler.charm.async_replication.update_async_replication_data.assert_called_once()
    validate.assert_not_called()


def test_on_config_changed_blocks_on_invalid_option(handler, harness):
    _seed_initialised(handler)
    event = Mock()
    with (
        patch.object(handler.membership_manager, "update_endpoint_addresses"),
        patch.object(handler.membership_manager, "update_member_ip", return_value=False),
        patch.object(
            handler.config_manager, "validate_config_options", side_effect=ValueError("bad")
        ),
        patch.object(handler.charm, "update_config"),
    ):
        handler._on_config_changed(event)
    records = handler.state.statuses.get("unit", handler.config_manager.name).root
    assert any(
        r.status == "blocked" and r.message == "Configuration Error. Please check the logs"
        for r in records
    )


def test_on_config_changed_clears_stale_configuration_error(handler, harness):
    _seed_initialised(handler)
    harness.model.unit.status = BlockedStatus("Configuration Error. Please check the logs")
    event = Mock()
    with (
        patch.object(handler.membership_manager, "update_endpoint_addresses"),
        patch.object(handler.membership_manager, "update_member_ip", return_value=False),
        patch.object(handler.config_manager, "validate_config_options"),
        patch.object(handler.charm, "update_config"),
        patch.object(handler.charm.logical_replication, "apply_changed_config", return_value=True),
        patch.object(handler.state.model.unit, "is_leader", return_value=False),
    ):
        handler._on_config_changed(event)
    assert harness.model.unit.status.name == "active"


def test_on_config_changed_leader_runs_extensions_and_rotation(handler):
    _seed_initialised(handler)
    handler.state.config = Mock()
    handler.state.config.system_users = "secret:1"
    event = Mock()
    with (
        patch.object(handler.membership_manager, "update_endpoint_addresses"),
        patch.object(handler.membership_manager, "update_member_ip", return_value=False),
        patch.object(handler.config_manager, "validate_config_options"),
        patch.object(handler.charm, "update_config"),
        patch.object(handler.charm.logical_replication, "apply_changed_config", return_value=True),
        patch.object(handler.state.model.unit, "is_leader", return_value=True),
        patch.object(handler.config_manager, "reconcile_extensions") as reconcile,
        patch.object(
            handler.cluster_manager,
            "rotate_system_user_passwords",
            side_effect=PostgreSQLUpdateUserPasswordError("x"),
        ),
    ):
        handler._on_config_changed(event)
    reconcile.assert_called_once()
    event.defer.assert_called_once()
