#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Watcher events handler — owns the ``watcher-offer`` relation for stereo mode.

Ported from the PostgreSQL VM charm's watcher relation module. This module
handles the relation between the PostgreSQL charm and a watcher/witness charm
that participates in the Raft consensus for stereo mode (2-node PostgreSQL
clusters): the watcher provides quorum without storing data, enabling automatic
failover when one of the two PostgreSQL nodes becomes unavailable.

The residual VM RAFT operations, the async-replication primary check and the
charm_refresh object stay behind charm-side bridges until their own migration
phases.
"""

import contextlib
import json
import logging
import os
from functools import cached_property

from ops import (
    Object,
    Relation,
    Secret,
    SecretNotFoundError,
)
from pysyncobj.utility import TcpUtility

from single_kernel_postgresql.config.literals import (
    RAFT_PARTNER_PREFIX,
    RAFT_PASSWORD_KEY,
    RAFT_PORT,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
    WATCHER_OFFER_RELATION,
    WATCHER_PASSWORD_KEY,
    WATCHER_SECRET_LABEL,
    WATCHER_USER,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils import new_password
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class WatcherEventsHandler(Object):
    """Handles the watcher relation for stereo mode support.

    ``charm`` is deliberately untyped, as in the TLS handler: the production
    charms do not derive from :class:`AbstractPostgreSQLCharm` until the cutover
    phase, and they are the callers that construct this.
    """

    def __init__(
        self,
        charm,
        state: CharmState,
        workload: BaseWorkload,
        tls_manager: TLSManager,
    ):
        """Initialize the watcher relation handler."""
        super().__init__(charm, WATCHER_OFFER_RELATION)
        self.charm = charm
        self.state = state
        self.workload = workload
        self.tls_manager = tls_manager

    @cached_property
    def _relation(self) -> Relation | None:
        """Return the watcher relation if it exists."""
        return self.model.get_relation(WATCHER_OFFER_RELATION)

    @property
    def is_watcher_connected(self) -> bool:
        """Check if a watcher is connected to this cluster.

        Returns:
            True if a watcher is connected, False otherwise.
        """
        try:
            syncobj_util = TcpUtility(password=self.state.application.raft_password, timeout=3)
            raft_status = syncobj_util.executeCommand(f"127.0.0.1:{RAFT_PORT}", ["status"])
            if raft_status:
                # Check if watcher is in the partner_node_status entries
                member_key = f"{RAFT_PARTNER_PREFIX}{self.watcher_raft_address}"
                return member_key in raft_status
        except Exception as e:
            logger.debug(f"Error checking Raft membership: {e}")
        return False

    def enable_watcher(self) -> None:
        """Clear up disable flag."""
        if not self._relation or not self.charm.unit.is_leader():
            return None

        self._relation.data[self.charm.app].pop("disable-watcher", None)
        self.update_watcher_secret()

    def disable_watcher(self) -> None:
        """Inform watcher to stop service."""
        if not self._relation or not self.charm.unit.is_leader():
            return None

        self._relation.data[self.charm.app].update({"disable-watcher": "True"})
        try:
            if self.watcher_raft_address:
                self.charm.remove_raft_member(self.watcher_raft_address)
        except Exception as e:
            logger.warning(f"Error remove Raft watcher: {e}")

    @cached_property
    def is_active(self) -> bool:
        """Check if the watcher should be added to peers."""
        if not self._relation:
            return False

        return self._relation.data[self._relation.app].get("raft-status") == "connected"

    @cached_property
    def watcher_raft_address(self) -> str | None:
        """Return the watcher's Raft address for inclusion in partner_addrs.

        Returns:
            The watcher's Raft address (ip:port), or None if not available.
        """
        if not self._relation:
            return None

        unit_address = None
        port = None
        # Get the watcher unit address from the relation data
        for unit in self._relation.units:
            if unit_address := self._relation.data[unit].get("unit-address"):
                break
        port_str = self._relation.data[self._relation.app].get("watcher-raft-port")
        if port_str:
            try:
                port = int(port_str)
            except ValueError:
                logger.warning(f"Invalid watcher-raft-port value: {port_str}")

        if unit_address and port is not None:
            return f"{unit_address}:{port}"
        return None

    def _ensure_watcher_user(self) -> str | None:
        """Ensure the watcher PostgreSQL user exists for health checks.

        Creates the watcher user if it doesn't exist, and updates the watcher
        secret with the password so the watcher charm can authenticate.

        Returns:
            The watcher password, or None if user creation failed.
        """
        if not self.state.application.is_cluster_initialised:
            logger.debug("Cluster not initialized, cannot create watcher user")
            return None

        try:
            users = self.charm.postgresql.list_users()
            if WATCHER_USER in users:
                logger.debug(f"User {WATCHER_USER} already exists")
                # Get existing password from secret if available
                try:
                    secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
                    content = secret.get_content(refresh=True)
                    existing_pw = content.get(WATCHER_PASSWORD_KEY)
                    if existing_pw:
                        return existing_pw
                    # Password not in secret — fall through to regenerate
                except SecretNotFoundError:
                    # Secret doesn't exist yet, will be created below with new password
                    pass

            # Generate a password for the watcher user
            watcher_password = new_password()

            # Create the watcher user (minimal privileges - only needs to connect and run SELECT 1)
            if WATCHER_USER not in users:
                logger.info(f"Creating PostgreSQL user: {WATCHER_USER}")
                self.charm.postgresql.create_user(WATCHER_USER, watcher_password)
            else:
                # User exists but we don't have the password, update it
                logger.info(f"Updating password for PostgreSQL user: {WATCHER_USER}")
                self.charm.postgresql.update_user_password(WATCHER_USER, watcher_password)

            # Grant connect privilege on postgres database (for health checks)
            self.charm.postgresql.grant_database_privileges_to_user(
                WATCHER_USER, "postgres", ["connect"]
            )

            # Update the secret to include the watcher password
            self._update_watcher_secret_with_password(watcher_password)

            return watcher_password

        except Exception as e:
            logger.error(f"Failed to ensure watcher user: {e}")
            return None

    def _update_watcher_secret_with_password(self, watcher_password: str) -> None:
        """Update the watcher secret to include the watcher password.

        Args:
            watcher_password: The password for the watcher PostgreSQL user.
        """
        try:
            secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
            content = secret.get_content(refresh=True)
            content[WATCHER_PASSWORD_KEY] = watcher_password
            secret.set_content(content)
            logger.info("Updated watcher secret with watcher password")
        except SecretNotFoundError:
            logger.warning(
                "Watcher secret not found, password change cannot be propagated to watcher. "
                "It will be synced on next relation-changed event."
            )
        except Exception as e:
            logger.error(f"Failed to update watcher secret with password: {e}")

    def _get_existing_watcher_password(self) -> str | None:
        """Get the watcher password from an existing secret if available."""
        try:
            secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
            content = secret.get_content(refresh=True)
            return content.get(WATCHER_PASSWORD_KEY)
        except SecretNotFoundError:
            return None
        except Exception as e:
            logger.debug(f"Failed to get existing watcher password: {e}")
            return None

    def _get_or_create_watcher_secret(self, watcher_password: str | None = None) -> Secret | None:
        """Get or create the secret for sharing Raft credentials with the watcher.

        Args:
            watcher_password: Optional watcher password to include in the secret.

        Returns:
            The Juju secret containing Raft password, or None if creation failed.
        """
        try:
            secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
            logger.debug("Found existing watcher secret")
            return secret
        except SecretNotFoundError:
            logger.debug("No existing watcher secret found, creating new one")

        # Get the Raft password from the internal secret
        try:
            raft_password = self.state.application.raft_password
        except Exception as e:
            logger.warning(f"Error getting raft_password: {e}")
            raft_password = None

        if not raft_password:
            logger.warning("Raft password not available, cannot create secret")
            return None

        # Create a new secret with the Raft password (and watcher password if available)
        try:
            content = {RAFT_PASSWORD_KEY: raft_password}
            # Include watcher password if provided, or look it up from existing secret
            watcher_pw = watcher_password or self._get_existing_watcher_password()
            if watcher_pw:
                content[WATCHER_PASSWORD_KEY] = watcher_pw
            secret = self.charm.model.app.add_secret(
                content=content,
                label=WATCHER_SECRET_LABEL,
            )
            logger.info("Created watcher secret")
            return secret
        except Exception as e:
            logger.error(f"Failed to create watcher secret: {e}")
            return None

    def _update_relation_data(self, relation: Relation) -> None:
        """Update the relation data with cluster information.

        Args:
            relation: The watcher relation.
        """
        if not self.charm.unit.is_leader():
            return

        # Get the secret ID for sharing
        try:
            secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
            secret_id = secret.id
            if not secret_id:
                # When a secret is retrieved by label, the ops library may lazily load the ID.
                # Calling get_info() forces it to resolve.
                secret_id = secret.get_info().id
            if secret_id is None:
                logger.warning("Watcher secret has no ID")
                return
            # Ensure the secret is granted to the watcher relation (handles
            # cases where the secret was recreated after initial relation_joined)
            with contextlib.suppress(Exception):
                secret.grant(relation)
        except SecretNotFoundError:
            logger.warning("Watcher secret not found")
            return
        except Exception as e:
            logger.error(f"Error getting secret: {e}")
            return

        # Collect PostgreSQL unit endpoints using fresh IPs from unit relation data.
        # units_ips reads directly from unit relation data (always fresh), while
        # peer_members_ips reads from app peer data (may be stale after network disruptions).
        pg_endpoints: list[str] = sorted(self.state.units_ips)
        if not pg_endpoints:
            logger.warning("No PostgreSQL endpoints available")
            return

        # Update relation data
        relation.data[self.charm.app].update({
            "cluster-name": self.state.application.cluster_name,
            "raft-secret-id": secret_id,
            "raft-partner-addrs": json.dumps(pg_endpoints),
            "raft-port": str(RAFT_PORT),
            "patroni-cas": self.tls_manager.get_peer_ca_bundle(),
            "standby-clusters": json.dumps(self._get_standby_clusters()),
            "tls-enabled": "true" if self.is_tls_enabled else "false",
        })
        self.update_watcher_secret()

        # Also share this unit's per-unit data.
        self.update_unit_address(relation)

    @property
    def is_tls_enabled(self) -> bool:
        """Return whether TLS is enabled."""
        return all(self.tls_manager.get_client_tls_files())

    def update_unit_address(self, relation: Relation | None = None) -> None:
        """Update this unit's address in the watcher relation.

        Called when the unit's IP changes (e.g., after network isolation).
        This updates unit-specific data in the relation, not application data.
        Can be called by any unit, not just the leader.
        """
        if relation is None:
            relation = self._relation

        if not relation:
            return

        if not (unit_ip := self.state.unit_ip):
            return

        relation.data[self.charm.unit]["version"] = self.workload.get_postgresql_version()
        if refresh := getattr(self.charm, "refresh", None):
            relation.data[self.charm.unit]["snap"] = refresh.pinned_snap_revision
        current_address = relation.data[self.charm.unit].get("unit-address")
        if current_address != unit_ip:
            logger.info(
                f"Updating unit-address in watcher relation from {current_address} to {unit_ip}"
            )
            relation.data[self.charm.unit]["unit-address"] = unit_ip

        unit_az = os.environ.get("JUJU_AVAILABILITY_ZONE")
        current_az = relation.data[self.charm.unit].get("unit-az")
        if unit_az and current_az != unit_az:
            relation.data[self.charm.unit]["unit-az"] = unit_az

    def update_endpoints(self) -> None:
        """Update the watcher with current cluster endpoints.

        Called when cluster membership changes (peer joins/departs).
        Also dynamically adds new PostgreSQL peers to the running Raft cluster.
        """
        if relation := self._relation:
            if self.charm.unit.is_leader():
                self._update_relation_data(relation)
            self.update_unit_address(relation)

    def _get_standby_clusters(self) -> list[str]:
        """Return the names of related standby clusters."""
        standby_clusters = []
        for relation in [
            self.model.get_relation(REPLICATION_OFFER_RELATION),
            self.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if relation is None:
                continue
            # We are interested in the other side's application name
            if relation.app and self.charm.is_primary_cluster():
                standby_clusters.append(relation.app.name)
        return sorted(set(standby_clusters))

    def update_watcher_secret(self) -> None:
        """Update the watcher secret with current Raft password.

        Called when credentials are rotated. Preserves existing secret content
        (e.g., watcher-password) while updating the Raft password.
        """
        if not self.charm.unit.is_leader():
            return

        try:
            if raft_password := self.state.application.raft_password:
                secret = self.charm.model.get_secret(label=WATCHER_SECRET_LABEL)
                content = secret.get_content(refresh=True)
                if content.get(RAFT_PASSWORD_KEY) != raft_password:
                    content[RAFT_PASSWORD_KEY] = raft_password
                    secret.set_content(content)
                    logger.info("Updated watcher secret with new Raft password")
        except SecretNotFoundError:
            logger.debug("Watcher secret not found, nothing to update")
