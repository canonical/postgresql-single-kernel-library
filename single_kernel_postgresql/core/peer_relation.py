#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""State objects for database-peers relation."""
import json
from ops import Application, Relation, Unit

from single_kernel_postgresql.config.literals import PATRONI_PASSWORD_KEY, REPLICATION_PASSWORD_KEY, USER_PASSWORD_KEY
from single_kernel_postgresql.core.relation_state import RelationState
from ops import Relation, Unit, Application
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import DataPeerUnitData, DataPeerData
from ops import BlockedStatus


class PostgreSQLPeer(RelationState):
    """State/Relation data collection for a PostgreSQL unit."""

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

    def set_secret(self, key: str, value: str) -> None:
        """Set the secret value for 'key' in the peer relation data."""
        if not self.relation:
            return
        self.data_interface.set_secret(self.relation.id, key, value)

    def remove_secret(self, key: str) -> None:
        """Remove the secret value for 'key' from the peer relation data."""
        if not self.relation:
            return
        self.data_interface.delete_relation_data(self.relation.id, [key])

    @property
    def is_app_leader(self) -> bool:
        """Check if the current unit is the leader of the application."""
        return self.unit.is_leader()

    @property
    def is_blocked(self) -> bool:
        """Returns whether the unit is in a blocked state."""
        return isinstance(self.unit.status, BlockedStatus)

    @property
    def internal_cert(self) -> str | None:
        """Get internal certificate.

        Returns:
            The internal certificate from the peer relation or None if it has not yet been set by the leader.
        """
        return self.get_secret("internal-cert")

    @property
    def ip(self) -> str | None:
        """Get the unit's IP address from the peer relation data."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("ip", "")

    @ip.setter
    def ip(self, value: str | None) -> None:
        """Set the unit's IP address in the peer relation data."""
        if not self.relation:
            return
        if value:
            self.relation.data[self.unit]["ip"] = value

    @property
    def member_name(self) -> str:
        """Get the member name for this unit."""
        return self.unit.name.replace("/", "-")



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


    @property
    def replication_password(self) -> str | None:
        """Get replication user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(REPLICATION_PASSWORD_KEY)

    @property
    def user_password(self) -> str | None:
        """Get operator user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(USER_PASSWORD_KEY)

    @property
    def patroni_password(self) -> str | None:
        """Get Patroni REST API password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(PATRONI_PASSWORD_KEY)

    @property
    def internal_ca(self) -> str | None:
        """Get internal CA.

        Returns:
            The internal CA from the peer relation or None if it has not yet been set by the leader.
        """
        return self.get_secret("internal-ca")

    @property
    def cluster_name(self) -> str:
        """Get cluster name.

        Returns:
            The cluster name, which is the same as the application name.
        """
        return self.app.name

    @property
    def planned_units(self) -> int:
        """Get the number of planned units for the application."""
        return self.app.planned_units()

    @property
    def members_ips(self) -> set[str]:
        """Returns the list of IPs addresses of the current members of the cluster."""
        if not self.relation:
            return set()
        return set(json.loads(self.relation.data[self.app].get("members_ips", "[]")))



    def get_secret(self, key: str) -> str | None:
        """Get the secret value for 'key' from the peer relation data."""
        if not self.relation:
            return None
        return self.data_interface.get_secret(self.relation.id, key)

    def set_secret(self, key: str, value: str) -> None:
        """Set the secret value for 'key' in the peer relation data."""
        if not self.relation:
            return
        self.data_interface.set_secret(self.relation.id, key, value)

    def remove_secret(self, key: str) -> None:
        """Remove the secret value for 'key' from the peer relation data."""
        if not self.relation:
            return
        self.data_interface.delete_relation_data(self.relation.id, [key])
