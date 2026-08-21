#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Client-relation events handler — owns the client-relation observers."""

import logging

from ops import BlockedStatus, Object, RelationBrokenEvent, RelationDepartedEvent

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import DATABASE, DATABASE_PORT
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequestedEvent,
)
from single_kernel_postgresql.managers.database import DatabaseManager, DatabaseRequest
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQLCreateDatabaseError,
    PostgreSQLCreateUserError,
)

logger = logging.getLogger(__name__)


class DatabaseEventsHandler(Object):
    """Handles the ``database`` (``postgresql_client``) provider relation.

    Owns the three observers the client relation needs and drives the
    :class:`~single_kernel_postgresql.managers.database.DatabaseManager` the charm
    constructs, mirroring how the postgresql events handler receives its managers.

    Each handler keeps its guard and its work in one observer: ``defer()`` is
    per-observer, so splitting the readiness check from the action would let a deferred
    action retry alone against state its guard never re-checked.

    ``charm`` is deliberately untyped, as in the TLS handler: the production charms do
    not derive from :class:`AbstractPostgreSQLCharm` until the cutover phase, and they
    are the callers that construct this.
    """

    def __init__(
        self,
        charm,
        state: CharmState,
        database_manager: DatabaseManager,
        patroni_manager: PatroniManager,
        tls_manager: TLSManager,
        relation_name: str = DATABASE,
    ) -> None:
        super().__init__(charm, key="database")
        self.charm = charm
        self.state = state
        self.relation_name = relation_name
        # The charm is the composition root: it constructs the manager and the provider
        # interface; this handler owns the observers and the guard/defer decisions.
        self.manager = database_manager
        self.database_provides = database_manager.database_provides
        # Held for the readiness guards and the endpoint-input gathers: the manager
        # takes no peer-manager references (akram09's review on the mappings PR).
        self.patroni_manager = patroni_manager
        self.tls_manager = tls_manager

        self.framework.observe(
            charm.on[relation_name].relation_departed, self._on_relation_departed
        )
        self.framework.observe(charm.on[relation_name].relation_broken, self._on_relation_broken)
        self.framework.observe(
            self.database_provides.on.database_requested, self._on_database_requested
        )

    def _ready_for_request(self) -> bool:
        """Whether the cluster can serve a new client request right now."""
        if not self.state.application.is_cluster_initialised:
            return False
        if self.state.substrate == Substrates.K8S:
            return self.patroni_manager.primary_endpoint_ready
        return bool(self.patroni_manager.member_started and self.charm.primary_endpoint)

    def _ready_for_removal(self) -> bool:
        """Whether the cluster can serve a relation removal right now.

        K8s settles for a started member here where a request additionally waits on the
        primary endpoint being reachable.
        """
        if not self.state.application.is_cluster_initialised:
            return False
        if self.state.substrate == Substrates.K8S:
            return self.patroni_manager.member_started
        return bool(self.patroni_manager.member_started and self.charm.primary_endpoint)

    def _on_database_requested(self, event: DatabaseRequestedEvent) -> None:
        """Generate password and handle user and database creation for the related app."""
        # The provider library only emits this on the leader.
        if not self.state.peer.is_app_leader:
            return

        if not self._ready_for_request():
            logger.debug(
                "Deferring on_database_requested: %s",
                "Cluster must be initialized before database can be requested"
                if self.state.substrate == Substrates.K8S
                else "cluster not initialized, Patroni not started or primary endpoint not available",
            )
            event.defer()
            return

        request = DatabaseRequest(
            relation_id=event.relation.id,
            database=event.database or "",
            extra_user_roles=event.extra_user_roles,
            prefix_matching=event.prefix_matching,
            requested_entity_secret_content=event.requested_entity_secret_content,
        )
        postgresql = self.charm.postgresql

        if not (creds := self.manager.get_credentials(postgresql, request)):
            return
        user, password = creds

        if not (databases_setup := self.manager.collect_databases(postgresql, user, request)):
            return
        database, databases = databases_setup

        self.manager.update_username_mapping(event.relation.id, user)
        self.charm.update_config()
        if not self.manager.are_units_in_sync():
            logger.debug("Not all units have synced configuration")
            event.defer()
            return

        extra_user_roles = self.manager.build_extra_user_roles(event.extra_user_roles)

        try:
            self.manager.create_relation_user_and_database(
                postgresql, user, password, database, databases, extra_user_roles
            )
            self.database_provides.set_credentials(event.relation.id, user, password)
            self._publish_k8s_inline_endpoints(event.relation.id, user, password, database)
            self.database_provides.set_version(
                event.relation.id, postgresql.get_postgresql_version()
            )
            self.database_provides.set_database(event.relation.id, database)
            if self.state.substrate == Substrates.K8S:
                # K8s refreshed every relation from the request handler.
                self.manager.update_endpoints(
                    client_tls_files=self.tls_manager.get_client_tls_files()
                )
            else:
                self.manager.update_endpoints(
                    event.relation.id,
                    online_members=self.patroni_manager.online_cluster_members(),
                    client_tls_files=self.tls_manager.get_client_tls_files(),
                )
            self.manager.update_unit_status(postgresql, event.relation)
            self.charm.update_config()
        except self.manager.request_errors as e:
            if self.state.substrate == Substrates.K8S:
                logger.exception(e)
            self.charm.set_unit_status(
                BlockedStatus(
                    e.message
                    if isinstance(e, PostgreSQLCreateDatabaseError | PostgreSQLCreateUserError)
                    and e.message is not None
                    # The clearer of the two charms' wordings, now shared by both.
                    else f"Failed to initialize {self.relation_name} relation"
                )
            )
            return

    def _publish_k8s_inline_endpoints(
        self, relation_id: int, user: str, password: str, database: str
    ) -> None:
        """Publish the endpoint/URI/TLS fields the K8s charm wrote inside the request handler.

        ``update_endpoints`` writes the same values moments later, so this is redundant;
        it is kept so the port does not change K8s databag write ordering.
        """
        if self.state.substrate != Substrates.K8S:
            return
        primary = f"{self.state.primary_endpoint}:{DATABASE_PORT}"
        self.database_provides.set_endpoints(relation_id, primary)
        self.database_provides.set_uris(
            relation_id, f"postgresql://{user}:{password}@{primary}/{database}"
        )
        client_tls_files = self.tls_manager.get_client_tls_files()
        self.database_provides.set_tls(relation_id, "True" if all(client_tls_files) else "False")
        if all(client_tls_files):
            self.database_provides.set_tls_ca(relation_id, client_tls_files[1] or "")

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Set a flag to avoid deleting database users when not wanted."""
        # Set a flag to avoid deleting database users when this unit
        # is removed and receives relation broken events from related applications.
        # This is needed because of https://bugs.launchpad.net/juju/+bug/1979811.
        if event.departing_unit == self.state.model.unit and self.state.peer_relation:
            self.state.peer.data.update({"departing": "True"})

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Remove the user created for this relation."""
        if not self.state.peer_relation or not self._ready_for_removal():
            logger.debug(
                "Deferring on_relation_broken: %s",
                "Cluster must be initialized before user can be deleted"
                if self.state.substrate == Substrates.K8S
                else "Cluster must be initialized and primary available before user can be deleted",
            )
            event.defer()
            return

        postgresql = self.charm.postgresql
        self.manager.update_unit_status(postgresql, event.relation)

        if self.state.peer.is_unit_departing:
            logger.debug("Early exit on_relation_broken: Skipping departing unit")
            return

        if not self.state.peer.is_app_leader:
            user = self.manager.get_username_mapping().get(
                str(event.relation.id), self.manager.relation_username(event.relation.id)
            )
            if user in postgresql.list_users():
                logger.debug("Deferring on_relation_broken: user was not deleted yet")
                event.defer()
            else:
                self.charm.update_config()
            return

        self.manager.delete_relation_user(postgresql, event.relation.id)
        self.charm.update_config()
