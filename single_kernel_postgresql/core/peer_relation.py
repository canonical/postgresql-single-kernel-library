#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""State objects for database-peers relation."""

import json
from collections.abc import MutableMapping
from functools import cached_property

from ops import Application, BlockedStatus, Relation, Unit

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    MONITORING_PASSWORD_KEY,
    PATRONI_PASSWORD_KEY,
    RAFT_PASSWORD_KEY,
    REPLICATION_PASSWORD_KEY,
    REWIND_PASSWORD_KEY,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.core.relation_state import RelationState
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DataPeerData,
    DataPeerUnitData,
)


class PostgreSQLPeer(RelationState):
    """State/Relation data collection for a PostgreSQL unit."""

    data_interface: DataPeerUnitData
    unit: Unit

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

    def _get_unit_field(self, key: str) -> str | None:
        """Get a plain (non-secret) field from this unit's databag."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get(key)

    def _set_unit_field(self, key: str, value: str) -> None:
        """Set a plain (non-secret) field in this unit's databag."""
        if not self.relation:
            return
        self.relation.data[self.unit][key] = value

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
    def internal_key(self) -> str | None:
        """Get internal private key.

        Returns:
            The internal private key from the peer relation or None if it has not yet been set by the leader.
        """
        return self.get_secret("internal-key")

    @internal_cert.setter
    def internal_cert(self, value: str) -> None:
        """Set internal certificate in the peer relation."""
        self.set_secret("internal-cert", value)

    @internal_key.setter
    def internal_key(self, value: str) -> None:
        """Set internal private key in the peer relation."""
        self.set_secret("internal-key", value)

    @property
    def current_ca(self) -> str | None:
        """Current peer CA (unit secret); part of the peer CA bundle."""
        return self.get_secret("current-ca")

    @current_ca.setter
    def current_ca(self, value: str) -> None:
        self.set_secret("current-ca", value)

    @property
    def old_ca(self) -> str | None:
        """Previous peer CA (unit secret); retained for the rotation window."""
        return self.get_secret("old-ca")

    @old_ca.setter
    def old_ca(self, value: str) -> None:
        self.set_secret("old-ca", value)

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

    @property
    def unit_name(self) -> str:
        """Get the unit name."""
        return self.unit.name

    @property
    def unit_id(self) -> str:
        """Get the unit id."""
        return self.unit.name.split("/")[1]

    @property
    def patroni_on_failure_condition_override(self) -> str | None:
        """Get the on-failure condition override for patroni from the peer relation data."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("patroni-on-failure-condition-override", None)

    @property
    def database_peers_address(self) -> str | None:
        """Get the address to be used for database peers communication."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("database-peers-address", None)

    @property
    def database_address(self) -> str | None:
        """Get the client-facing database endpoint address."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("database-address", None)

    @property
    def replication_address(self) -> str | None:
        """Get the address to be used for replication communication."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("replication-address", None)

    @property
    def replication_offer_address(self) -> str | None:
        """Get the address to be used for replication communication in case of replication offer."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("replication-offer-address", None)

    @property
    def private_address(self) -> str | None:
        """Get the private address of the unit."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("private-address", None)

    @property
    def peer_addresses(self) -> set[str]:
        """Set of peer unit addresses (database, replication, and replication-offer)."""
        peer_addrs = set()
        if addr := self.database_peers_address:
            peer_addrs.add(addr)
        if addr := self.replication_address:
            peer_addrs.add(addr)
        if addr := self.replication_offer_address:
            peer_addrs.add(addr)
        if addr := (self.ip or self.private_address):
            peer_addrs.add(addr)
        return peer_addrs

    @property
    def is_unit_departing(self) -> bool:
        """Returns whether the unit is departing."""
        if not self.relation:
            return False
        return "departing" in self.relation.data[self.unit]

    @property
    def is_unit_stopped(self) -> bool:
        """Returns whether the unit is stopped."""
        if not self.relation:
            return False
        return "stopped" in self.relation.data[self.unit]

    @property
    def is_connectivity_enabled(self) -> bool:
        """Return whether this unit can be connected externally."""
        if not self.relation:
            return True
        return self.relation.data[self.unit].get("connectivity", "on") == "on"

    @property
    def stanza(self) -> str | None:
        """Get the pgBackRest stanza name set by this unit when it is the primary.

        The leader publishes the stanza on the application databag; a primary that
        is not the leader publishes it here until the leader copies it across.
        """
        return self._get_unit_field("stanza")

    @stanza.setter
    def stanza(self, value: str) -> None:
        """Set the pgBackRest stanza name in the peer relation data."""
        self._set_unit_field("stanza", value)

    @property
    def s3_initialization_done(self) -> str | None:
        """Get the flag marking this unit as done with the S3 initialization sequence."""
        return self._get_unit_field("s3-initialization-done")

    @s3_initialization_done.setter
    def s3_initialization_done(self, value: str) -> None:
        """Set the S3 initialization completion flag in the peer relation data."""
        self._set_unit_field("s3-initialization-done", value)

    @property
    def s3_initialization_block_message(self) -> str | None:
        """Get the block message the S3 initialization sequence failed with on this unit."""
        return self._get_unit_field("s3-initialization-block-message")

    @s3_initialization_block_message.setter
    def s3_initialization_block_message(self, value: str) -> None:
        """Set the S3 initialization block message in the peer relation data."""
        self._set_unit_field("s3-initialization-block-message", value)

    @property
    def last_pitr_fail_id(self) -> str | None:
        """Get the last Patroni PITR failure seen, used to detect a repeated failure."""
        return self._get_unit_field("last_pitr_fail_id")

    @last_pitr_fail_id.setter
    def last_pitr_fail_id(self, value: str) -> None:
        """Set the last Patroni PITR failure in the peer relation data."""
        self._set_unit_field("last_pitr_fail_id", value)

    @property
    def rotate_logs_pid(self) -> str | None:
        """Get the PID of the log-rotation process this unit spawned (machines only)."""
        return self._get_unit_field("rotate-logs-pid")

    @rotate_logs_pid.setter
    def rotate_logs_pid(self, value: str) -> None:
        """Set the PID of the log-rotation process in the peer relation data."""
        self._set_unit_field("rotate-logs-pid", value)

    @property
    def config_hash(self) -> str | None:
        """Get the last-applied PostgreSQL config hash from the peer relation data."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("config_hash")

    @config_hash.setter
    def config_hash(self, value: str) -> None:
        """Set the last-applied PostgreSQL config hash in the peer relation data."""
        if not self.relation:
            return
        self.relation.data[self.unit]["config_hash"] = value

    @property
    def user_hash(self) -> str | None:
        """Get the last-applied users hash from the peer relation data."""
        if not self.relation:
            return None
        return self.relation.data[self.unit].get("user_hash")

    @user_hash.setter
    def user_hash(self, value: str) -> None:
        """Set the last-applied users hash in the peer relation data."""
        if not self.relation:
            return
        self.relation.data[self.unit]["user_hash"] = value

    @property
    def tls(self) -> bool:
        """Get the last-rendered TLS flag from the peer relation data."""
        if not self.relation:
            return False
        return self.relation.data[self.unit].get("tls") == "enabled"

    @tls.setter
    def tls(self, value: bool) -> None:
        """Set the last-rendered TLS flag in the peer relation data."""
        if not self.relation:
            return
        self.relation.data[self.unit]["tls"] = "enabled" if value else ""

    @cached_property
    def data(self) -> MutableMapping[str, str]:
        """Escape hatch method to access the peer data directly."""
        if not self.relation:
            return {}
        return self.relation.data[self.unit]

    @property
    def peer_addresses_no_ip(self) -> set[str]:
        """Peer addresses excluding the ``ip`` databag key (original K8s charm behavior).

        The K8s charm never wrote ``ip`` into the operator peer-cert SANs; it relied on
        ``database-peers-address`` + ``replication-address`` + ``replication-offer-address``
        + ``private-address``. The VM charm additionally included ``ip``. This property
        exposes the K8s-shaped set so :class:`CharmState` can pick the right one per
        substrate without the peer object needing to know the substrate.
        """
        peer_addrs: set[str] = set()
        if addr := self.database_peers_address:
            peer_addrs.add(addr)
        if addr := self.replication_address:
            peer_addrs.add(addr)
        if addr := self.replication_offer_address:
            peer_addrs.add(addr)
        if addr := self.private_address:
            peer_addrs.add(addr)
        return peer_addrs


class PostgreSQLApplication(RelationState):
    """An PostgreSQL Application is the peer application state.

    This class defines state/relation data for a single PostgreSQL application.
    """

    data_interface: DataPeerData
    app: Application

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerData,
        component: Application,
        substrate: Substrates,
    ):
        """Initialize the PostgreSQLApplication object."""
        super().__init__(relation, data_interface, component)
        self.app = component
        self.data_interface = data_interface
        self.substrate = substrate

    @property
    def replication_password(self) -> str | None:
        """Get replication user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(REPLICATION_PASSWORD_KEY)

    @property
    def monitoring_password(self) -> str | None:
        """Get monitoring user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(MONITORING_PASSWORD_KEY)

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

    # rewind-password
    @property
    def rewind_password(self) -> str | None:
        """Get rewind user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(REWIND_PASSWORD_KEY)

    @property
    def raft_password(self) -> str | None:
        """Get raft user password.

        Returns:
            The password from the peer relation or None if the
            password has not yet been set by the leader.
        """
        return self.get_secret(RAFT_PASSWORD_KEY)

    @property
    def internal_ca(self) -> str | None:
        """Get internal CA.

        Returns:
            The internal CA from the peer relation or None if it has not yet been set by the leader.
        """
        return self.get_secret("internal-ca")

    @property
    def internal_ca_key(self) -> str | None:
        """Get internal CA private key.

        Returns:
            The internal CA private key from the peer relation or None if it has not yet been set by the leader.
        """
        return self.get_secret("internal-ca-key")

    @property
    def cluster_name(self) -> str:
        """Get cluster name.

        Returns:
            The cluster name, which is the same as the application name.
        """
        if self.substrate == Substrates.K8S:
            return f"patroni-{self.app.name}"
        return self.app.name

    @cached_property
    def planned_units(self) -> int:
        """Get the number of planned units for the application."""
        return self.app.planned_units()

    @property
    def members_ips(self) -> set[str]:
        """Returns the list of IPs addresses of the current members of the cluster."""
        if not self.relation:
            return set()
        return set(json.loads(self.relation.data[self.app].get("members_ips", "[]")))

    @property
    def endpoints(self) -> set[str]:
        """Returns the list of endpoints of the current members of the cluster."""
        if not self.relation:
            return set()
        return set(json.loads(self.relation.data[self.app].get("endpoints", "[]")))

    @property
    def is_cluster_initialised(self) -> bool:
        """Returns whether the cluster is already initialised."""
        if not self.relation:
            return False
        return "cluster_initialised" in self.relation.data[self.app]

    @property
    def is_cluster_restoring_backup(self) -> bool:
        """Returns whether the cluster is restoring a backup."""
        return self.restoring_backup is not None

    @property
    def is_cluster_restoring_to_time(self) -> bool:
        """Returns whether the cluster is restoring a backup to a specific time."""
        return self.restore_to_time is not None

    @property
    def is_ldap_charm_related(self) -> bool:
        """Return whether this unit has an LDAP charm related."""
        if not self.relation:
            return False
        return self.relation.data[self.app].get("ldap_enabled", "False") == "True"

    @property
    def is_ldap_enabled(self) -> bool:
        """Return whether this unit has LDAP enabled."""
        return self.is_ldap_charm_related and self.is_cluster_initialised

    @property
    def user_hash(self) -> str | None:
        """Get the last-applied users hash from the peer relation data."""
        if not self.relation:
            return None
        return self.relation.data[self.app].get("user_hash")

    @user_hash.setter
    def user_hash(self, value: str) -> None:
        """Set the last-applied users hash in the peer relation data."""
        if not self.relation:
            return
        self.relation.data[self.app]["user_hash"] = value

    @property
    def stanza(self) -> str | None:
        """Get the pgBackRest stanza name published by the leader."""
        return self._get_app_field("stanza")

    @stanza.setter
    def stanza(self, value: str) -> None:
        """Set the pgBackRest stanza name in the peer relation data."""
        self._set_app_field("stanza", value)

    @property
    def restore_stanza(self) -> str | None:
        """Get the stanza a restore reads the backup from.

        A restore may point at a stanza belonging to a different cluster, so this
        is tracked separately from the stanza the cluster archives to.
        """
        return self._get_app_field("restore-stanza")

    @restore_stanza.setter
    def restore_stanza(self, value: str) -> None:
        """Set the stanza a restore reads the backup from."""
        self._set_app_field("restore-stanza", value)

    @property
    def restoring_backup(self) -> str | None:
        """Get the pgBackRest label of the backup being restored."""
        return self._get_app_field("restoring-backup")

    @restoring_backup.setter
    def restoring_backup(self, value: str) -> None:
        """Set the pgBackRest label of the backup being restored."""
        self._set_app_field("restoring-backup", value)

    @property
    def restore_to_time(self) -> str | None:
        """Get the point-in-time-recovery target, or ``latest`` to replay every WAL."""
        return self._get_app_field("restore-to-time")

    @restore_to_time.setter
    def restore_to_time(self, value: str) -> None:
        """Set the point-in-time-recovery target."""
        self._set_app_field("restore-to-time", value)

    @property
    def restore_timeline(self) -> str | None:
        """Get the timeline a point-in-time restore recovers along."""
        return self._get_app_field("restore-timeline")

    @restore_timeline.setter
    def restore_timeline(self, value: str) -> None:
        """Set the timeline a point-in-time restore recovers along."""
        self._set_app_field("restore-timeline", value)

    @property
    def s3_initialization_start(self) -> str | None:
        """Get the timestamp at which the leader started the S3 initialization sequence."""
        return self._get_app_field("s3-initialization-start")

    @s3_initialization_start.setter
    def s3_initialization_start(self, value: str) -> None:
        """Set the timestamp at which the leader started the S3 initialization sequence."""
        self._set_app_field("s3-initialization-start", value)

    @property
    def s3_initialization_done(self) -> str | None:
        """Get the flag the leader raises once it has adopted the primary's stanza fields."""
        return self._get_app_field("s3-initialization-done")

    @s3_initialization_done.setter
    def s3_initialization_done(self, value: str) -> None:
        """Set the S3 initialization completion flag in the peer relation data."""
        self._set_app_field("s3-initialization-done", value)

    @property
    def s3_initialization_block_message(self) -> str | None:
        """Get the block message the S3 initialization sequence failed with."""
        return self._get_app_field("s3-initialization-block-message")

    @s3_initialization_block_message.setter
    def s3_initialization_block_message(self, value: str) -> None:
        """Set the S3 initialization block message in the peer relation data."""
        self._set_app_field("s3-initialization-block-message", value)

    def _get_app_field(self, key: str) -> str | None:
        """Get a plain (non-secret) field from the application databag."""
        if not self.relation:
            return None
        return self.relation.data[self.app].get(key)

    def _set_app_field(self, key: str, value: str) -> None:
        """Set a plain (non-secret) field in the application databag."""
        if not self.relation:
            return
        self.relation.data[self.app][key] = value

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

    @cached_property
    def data(self) -> MutableMapping[str, str]:
        """Escape hatch method to access the peer data directly."""
        if not self.relation:
            return {}
        return self.relation.data[self.app]
