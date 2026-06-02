#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""State objects for database-peers relation."""
from single_kernel_postgresql.core.relation_state import RelationState
from ops import Relation, Unit, Application
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import DataPeerUnitData, DataPeerData
from ops import BlockedStatus


class PostgreSQLPeer(RelationState):
    """State/Relation data collection for a PostgreSQL unit"""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerUnitData,
        component: Unit,
    ):
        """Initialize the PostgreSQLPeer object."""
        super().__init__(relation, data_interface, component)
        self.data_interface = data_interface
        self.unit = component

    def get_secret(self, key: str) -> str | None:
        """Get the secret value for 'key' from the peer relation data."""
        if not self.relation:
            return None
        return self.data_interface.get_secret(self.relation.id, key)

    @property
    def is_app_leader(self) -> bool:
        """Check if the current unit is the leader of the application."""
        return self.unit.is_leader()

    @property
    def is_blocked(self) -> bool:
        """Returns whether the unit is in a blocked state."""
        return isinstance(self.unit.status, BlockedStatus)




class PostgreSQLApplication(RelationState):
    """An PostgreSQL Application is the peer application state.

    This class defines state/relation data for a single PostgreSQL application.
    """

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
    ):
        """Initialize the PostgreSQLApplication object."""
        super().__init__(relation, data_interface, component)
        self.app = component
        self.data_interface = data_interface

    def get_secret(self, key: str) -> str | None:
        """Get the secret value for 'key' from the peer relation data."""
        if not self.relation:
            return None
        return self.data_interface.get_secret(self.relation.id, key)