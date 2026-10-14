# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the observer events handler (VM)."""

from unittest.mock import MagicMock, Mock

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.events.observer import ObserverEventsHandler


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
