# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Observer Event Handler."""

import logging
from typing import TYPE_CHECKING

from ops import CharmEvents, EventBase, EventSource, Object

from single_kernel_postgresql.config.enums import Substrates

if TYPE_CHECKING:
    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class ClusterTopologyChangeEvent(EventBase):
    """A custom event for cluster topology changes."""


class DatabasesChangeEvent(EventBase):
    """A custom event for databases changes."""


class RaftReconnectEvent(EventBase):
    """A custom event for databases changes."""


class ClusterTopologyChangeCharmEvents(CharmEvents):
    """A CharmEvents extension for cluster topology changes.

    Includes :class:`ClusterTopologyChangeEvent` in those that can be handled.
    """

    cluster_topology_change = EventSource(ClusterTopologyChangeEvent)
    databases_change = EventSource(DatabasesChangeEvent)
    raft_reconnect = EventSource(RaftReconnectEvent)


class Observer(Object):
    """Handler for observer script events."""

    def __init__(self, charm, workload: "BaseWorkload", state: "CharmState"):
        super().__init__(charm, "observer")
        self.charm = charm
        self.state = state
        self.workload = workload

        if self.state.substrate == Substrates.VM:
            pass
        else:
            pass
