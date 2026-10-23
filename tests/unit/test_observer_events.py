# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the observer events handler (VM)."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.events.observer import ObserverEventsHandler
from single_kernel_postgresql.managers.cluster_membership import COMPLETED, DEFER


def _handler():
    charm = Mock()
    charm.state.substrate = Substrates.VM
    handler = ObserverEventsHandler(
        charm=charm,
        workload=MagicMock(),
        state=charm.state,
        async_replication_manager=Mock(),
        database_manager=Mock(),
        patroni_manager=Mock(),
        raft_manager=Mock(),
        tls_manager=Mock(),
        watcher_handler=Mock(),
    )
    return charm, handler


def test_raft_reconnect_is_not_observed_by_the_observer_handler():
    """raft_reconnect has exactly one registration (postgresql events handler)."""
    _, handler = _handler()
    assert not hasattr(handler, "_on_raft_reconnect")


def test_on_databases_change_keys_the_timestamp_on_the_substrate(substrate):
    """The K8s charm's authorisation-rules flow keys on pg_hba_needs_update_timestamp."""
    charm, handler = _handler()
    charm.state.substrate = Substrates.K8S if substrate == "k8s" else Substrates.VM

    handler._on_databases_change(MagicMock())

    charm.update_config.assert_called_once_with()
    charm.state.peer.data.update.assert_called_once()
    (data,) = charm.state.peer.data.update.call_args.args
    # The VM charm keys on timestamp, the K8s charm on pg_hba_needs_update_timestamp.
    key = "pg_hba_needs_update_timestamp" if substrate == "k8s" else "timestamp"
    assert set(data) == {key}
    datetime.fromisoformat(data[key])


def test_cluster_topology_change_gates_on_the_patroni_primary_endpoint():
    """No endpoint updates run while the Patroni primary is unreachable."""
    charm, handler = _handler()
    # The k8s-service state.primary_endpoint is never falsy on VM; the gate
    # must consult the Patroni-resolved endpoint instead.
    charm.state.primary_endpoint = "postgresql-primary.test-model.svc.cluster.local"
    charm.primary_endpoint = None
    handler._on_cluster_topology_change(MagicMock())
    handler.database_manager.update_endpoints.assert_not_called()


def test_cluster_topology_change_updates_endpoints_with_a_primary():
    charm, handler = _handler()
    charm.state.primary_endpoint = "postgresql-primary.test-model.svc.cluster.local"
    charm.primary_endpoint = "10.0.0.5"
    charm.state.peer.is_app_leader = True
    handler._on_cluster_topology_change(MagicMock())
    handler.database_manager.update_endpoints.assert_called_once()
    handler.charm.set_primary_status_message.assert_called_once()
    handler.async_replication_manager.update_async_replication_data.assert_called_once()


def test_raft_reconnect_observed_once_on_vm(harness, substrate):
    if substrate == Substrates.K8S:
        pytest.skip("VM flow")
    """Emitting raft_reconnect runs the membership flow exactly once."""
    with patch.object(harness.charm.membership_manager, "raft_reconnect") as reconnect:
        harness.charm.on.raft_reconnect.emit()
    assert reconnect.call_count == 1


def test_on_raft_reconnect_defers_on_defer(harness, substrate):
    if substrate == Substrates.K8S:
        pytest.skip("VM flow")
    """The glue defers the event when the membership flow reports DEFER."""
    event = MagicMock()
    with patch.object(harness.charm.membership_manager, "raft_reconnect", return_value=DEFER):
        harness.charm.postgresql_events_handler._on_raft_reconnect(event)
    event.defer.assert_called_once()


def test_on_raft_reconnect_no_defer_on_completed(harness, substrate):
    if substrate == Substrates.K8S:
        pytest.skip("VM flow")
    """A COMPLETED flow result does not defer the event."""
    event = MagicMock()
    with patch.object(harness.charm.membership_manager, "raft_reconnect", return_value=COMPLETED):
        harness.charm.postgresql_events_handler._on_raft_reconnect(event)
    event.defer.assert_not_called()
