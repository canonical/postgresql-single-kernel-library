# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
"""Observer Event Handler."""

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from ops import CharmEvents, EventBase, EventSource, Object

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import RAFT_PARTNER_PREFIX, RAFT_PORT

if TYPE_CHECKING:
    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm
    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.events.watcher import WatcherEventsHandler
    from single_kernel_postgresql.managers.async_replication import AsyncReplicationManager
    from single_kernel_postgresql.managers.database import DatabaseManager
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


class ObserverEventsHandler(Object):
    """Handler for observer script events."""

    def __init__(
        self,
        charm: "AbstractPostgreSQLCharm",
        workload: "BaseWorkload",
        state: "CharmState",
        async_replication_manager: "AsyncReplicationManager",
        database_manager: "DatabaseManager",
        watcher_handler: "WatcherEventsHandler",
    ):
        super().__init__(charm, "observer")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.watcher_handler = watcher_handler
        self.async_replication_manager = async_replication_manager
        self.database_manager = database_manager

        if self.state.substrate == Substrates.VM:
            self.framework.observe(
                self.on.cluster_topology_change, self._on_cluster_topology_change
            )
            self.framework.observe(self.on.raft_reconnect, self._on_raft_reconnect)
            self.framework.observe(self.on.databases_change, self._on_databases_change)
        else:
            pass

    def _on_databases_change(self, _):
        """Handle databases change event."""
        self.charm.update_config()
        logger.debug("databases changed")
        timestamp = datetime.now()
        self.state.peer.data.update({"timestamp": str(timestamp)})
        logger.debug(f"authorisation rules changed at {timestamp}")

    def _on_cluster_topology_change(self, _):
        """Updates endpoints and (optionally) certificates when the cluster topology changes."""
        logger.info("Cluster topology changed")
        if self.state.primary_endpoint:
            self.database_manager.update_endpoints()
            self.charm.set_primary_status_message()
            self.async_replication_manager.update_async_replication_data()

    def _on_raft_reconnect(self, _) -> None:
        raft_status = self._patroni.get_raft_status()
        logger.debug(f"Local raft status: {raft_status}")
        if (
            not raft_status
            or not self.state.unit_ip
            or not self.state.application.is_cluster_initialised
            or self.state.unit_ip not in self.state.application.members_ips
            or self.state.has_raft_keys()
            or (
                not self.state.application.members_ips
                and not self.watcher_handler.watcher_raft_address
            )
        ):
            return

        if all(
            raft_status[partner] == 2
            for partner in raft_status
            if partner.startswith(RAFT_PARTNER_PREFIX)
        ):
            logger.debug("All raft members are active.")
            return

        logger.info("Potentially stuck Raft connection detected. Re-adding Raft member.")
        local_addr = f"{self.state.unit_ip}:{RAFT_PORT}"
        remote_addr = (
            watcher_addr
            if (watcher_addr := self.watcher_handler.watcher_raft_address)
            and self.watcher_handler.is_active
            else f"{next(member for member in self.state.application.members_ips if member != self.state.unit_ip)}:{RAFT_PORT}"
        )
        try:
            self._patroni.remove_raft_member(
                local_addr, remote_address=remote_addr, set_raft_flags=False
            )
        except Exception:
            logger.exception("Unable to remove Raft member")
            return
        try:
            self._patroni.add_raft_member(local_addr, remote_address=remote_addr)
        except Exception:
            logger.exception("Unable to add Raft member")
            return
