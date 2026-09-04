#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Async Replication manager.

Owns the async-replication data plane: promoted-cluster-counter resolution, the
primary/standby endpoint getters, the shared-cluster secret handling, and the
primary-cluster-data publication. The events handler owns the observers and the
promotion/standby lifecycle flows.

Ported from the PostgreSQL VM and K8s charms' async replication module, including the
dead-datacenter recovery changes (DPE-10203): relation databag reads tolerate ModelError
from a force-removed cross-model relation, the shared secret is referenced by id instead
of label, and a stale promoted-cluster-counter is cleared on update-status and before the
create-replication/promote actions.
"""

import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from ops import Application, ModelError, Relation, Secret, SecretNotFoundError, Unit
from tenacity import RetryError

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    ASYNC_SHARED_SECRET_ID_KEY,
    PEER_RELATION,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.workload.base import BaseWorkload

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


READ_ONLY_MODE_BLOCKING_MESSAGE = "Standalone read-only cluster"


class AsyncReplicationError(Exception):
    """Exception class for Async replication."""


def _safe_databag_get(
    databag: Mapping[str, str], key: str, default: str | None = None
) -> str | None:
    """Read a relation databag key, treating an unreadable databag as key-absent.

    A force-removed dead DC leaves the remote databag raising ModelError on read
    (DPE-10203); callers must behave as if the key is unset.
    """
    try:
        return databag.get(key, default)
    except ModelError:
        return default


class AsyncReplicationManager(BaseManager):
    """Defines the async-replication management logic."""

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        patroni_manager: PatroniManager,
        update_config: "Callable[[], bool]",
    ):
        """Constructor.

        Args:
            state: the charm state.
            workload: the substrate workload.
            patroni_manager: the Patroni API manager, for the sync-standby endpoint lookup.
            update_config: the charm's config re-render bridge, used to reconcile after
                clearing a stale promotion.
        """
        super().__init__(state, workload, "async_replication_manager")
        self.patroni_manager = patroni_manager
        self.update_config = update_config

    @property
    def _relation(self) -> Relation | None:
        """Return the usable async-replication relation, or None.

        A relation whose databags are unreadable is treated as absent — the dying
        cross-model relation left by a force-removed dead DC reads as "permission
        denied" on every databag (DPE-10203). A cheap own-unit read probes for that.
        """
        for relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if relation is None:
                continue
            try:
                relation.data[self.state.model.unit].get("unit-address")
            except ModelError:
                continue
            return relation
        return None

    @property
    def _unit_ip(self) -> str:
        """Return this unit IP address for the replication relation."""
        if not self._relation:
            raise AsyncReplicationError("No relation to get IP for")

        if self.state.substrate == Substrates.K8S:
            return self._get_unit_ip()
        if self._relation.name == REPLICATION_OFFER_RELATION:
            ip = self.state.replication_offer_ip
        else:
            ip = self.state.replication_consumer_ip

        if not ip:
            raise AsyncReplicationError(f"No IP set for {self._relation.name}")
        return ip

    def _get_unit_ip(self) -> str:
        """Reads some files to quickly figure out its own pod IP.

        It should work for any Ubuntu-based image
        """
        with open("/etc/hosts") as f:
            hosts = f.read()
        with open("/etc/hostname") as f:
            hostname = f.read().replace("\n", "")
        line = next(ln for ln in hosts.split("\n") if ln.find(hostname) >= 0)
        return line.split("\t")[0]

    def get_all_primary_cluster_endpoints(self) -> list[str]:
        """Return all the primary cluster endpoints from the standby cluster."""
        if not (relation := self._relation):
            raise AsyncReplicationError("No relation in get all primary endpoints")

        primary_cluster = self.get_primary_cluster()
        # List the primary endpoints only for the standby cluster.
        if relation is None or primary_cluster is None or self.state.model.app == primary_cluster:
            return []
        return self._remote_unit_addresses()

    def get_highest_promoted_cluster_counter_value(self) -> str:
        """Return the highest promoted cluster counter."""
        promoted_cluster_counter = "0"
        for async_relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if async_relation is None:
                continue
            for databag in [
                async_relation.data[async_relation.app],
                self.state.application.data,
            ]:
                try:
                    relation_promoted_cluster_counter = databag.get(
                        "promoted-cluster-counter", "0"
                    )
                except ModelError:
                    # A force-removed dead DC leaves its databag unreadable; skip the
                    # peer instead of crashing the hook (DPE-10203).
                    continue
                if int(relation_promoted_cluster_counter) > int(promoted_cluster_counter):
                    promoted_cluster_counter = relation_promoted_cluster_counter
        return promoted_cluster_counter

    def get_partner_addresses(self) -> list[str]:
        """Return the partner addresses."""
        try:
            primary_cluster = self.get_primary_cluster()
        except RetryError:
            logger.debug("Handling get primary cluster RetryError on get_partner_addresses()")
            primary_cluster = None

        if (
            primary_cluster is None
            or self.state.model.app == primary_cluster
            or not self.state.model.unit.is_leader()
            or self.state.peer.data.get("unit-promoted-cluster-counter")
            == self.get_highest_promoted_cluster_counter_value()
        ) and (peer_members := self.state.peer_members_ips):
            sorted_partners = sorted(peer_members)
            logger.debug(f"Partner addresses: {sorted_partners}")
            return list(sorted_partners)

        logger.debug("Partner addresses: []")
        return []

    def get_primary_cluster(self) -> Application | None:
        """Return the primary cluster."""
        primary_cluster = None
        promoted_cluster_counter = "0"
        for async_relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if async_relation is None:
                continue
            for app, relation_data in {
                async_relation.app: async_relation.data,
                self.state.model.app: self.state.peer_relation.data
                if self.state.peer_relation
                else {},
            }.items():
                if app is None or relation_data is None:
                    continue
                databag = relation_data[app]
                try:
                    relation_promoted_cluster_counter = databag.get(
                        "promoted-cluster-counter", "0"
                    )
                except ModelError:
                    # A force-removed dead DC leaves its databag unreadable; skip the
                    # peer so reconciliation still runs (DPE-10203).
                    continue
                if int(relation_promoted_cluster_counter) > int(promoted_cluster_counter):
                    promoted_cluster_counter = relation_promoted_cluster_counter
                    primary_cluster = app
        return primary_cluster

    def get_primary_cluster_endpoint(self) -> str | None:
        """Return the primary cluster endpoint."""
        primary_cluster = self.get_primary_cluster()
        if primary_cluster is None or self.state.model.app == primary_cluster:
            return None
        relation = self._relation
        if relation is None:
            return None
        primary_cluster_data = _safe_databag_get(
            relation.data[relation.app], "primary-cluster-data"
        )
        if primary_cluster_data is None:
            return None
        return json.loads(primary_cluster_data).get("endpoint")

    def _get_secret(self) -> Secret | None:
        """Return async replication necessary secrets."""
        app_secret = self.state.model.get_secret(
            label=f"{PEER_RELATION}.{self.state.model.app.name}.app"
        )
        content = app_secret.peek_content()

        # Filter out unnecessary secrets.
        shared_content = dict(filter(lambda x: "password" in x[0], content.items()))

        # The owner references its secret purely by the id persisted in app peer data —
        # no label. Owning under a label risks colliding with a stale consumer alias Juju
        # keeps reserved after a dead-DC teardown ("secret with label already exists"),
        # and a label lookup cannot survive the secret's own id churn (DPE-10203).
        secret_id = self.state.application.data.get(ASYNC_SHARED_SECRET_ID_KEY)
        if not secret_id:
            # Migration from the legacy charm (which owned the secret under a label):
            # this cluster's own relation data still publishes the last-known id. Adopt
            # that secret instead of creating a second one — an id switch would wedge
            # any consumer still running label-attaching code, since Juju refuses to
            # rebind a consumer label to a new secret id.
            secret_id = self._own_published_secret_id()
        if secret_id:
            try:
                secret = self.state.model.get_secret(id=secret_id)
            except SecretNotFoundError:
                logger.debug("Persisted async-replication secret is gone; recreating")
            else:
                if secret.peek_content() != shared_content:
                    logger.info("Updating outdated secret content")
                    secret.set_content(shared_content)
                # Persist the id (covers the migration path, where the id came from
                # this cluster's own relation data rather than peer data).
                self.state.application.data.update({ASYNC_SHARED_SECRET_ID_KEY: secret.id})  # type: ignore
                return secret

        if self.state.model.unit.is_leader():
            secret = self.state.model.app.add_secret(content=shared_content)
            self.state.application.data.update({ASYNC_SHARED_SECRET_ID_KEY: secret.id})  # type: ignore
            return secret
        return None

    def _own_published_secret_id(self) -> str | None:
        """Return the secret id this cluster last published, from its own relation data."""
        for relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if relation is None:
                continue
            try:
                primary_cluster_data = _safe_databag_get(
                    relation.data[self.state.model.app], "primary-cluster-data"
                )
            except ModelError:
                continue
            if primary_cluster_data is None:
                continue
            if secret_id := json.loads(primary_cluster_data).get("secret-id"):
                return secret_id
        return None

    def get_standby_endpoints(self) -> list[str]:
        """Return the standby endpoints."""
        if not (relation := self._relation):
            return []

        primary_cluster = self.get_primary_cluster()
        # List the standby endpoints only for the primary cluster.
        if relation is None or primary_cluster is None or self.state.model.app != primary_cluster:
            return []
        return self._remote_unit_addresses()

    def _remote_unit_addresses(self) -> list[str]:
        """Return unit addresses published across both async relations.

        Skips units whose databag is unreadable — a dead-DC teardown leaves the dying
        cross-model relation's unit databags raising ModelError on read (DPE-10203).
        """
        addresses = []
        for relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if relation is None:
                continue
            for unit in relation.units:
                address = _safe_databag_get(relation.data[unit], "unit-address")
                if address is not None:
                    addresses.append(address)
        return addresses

    def is_following_promoted_cluster(self) -> bool:
        """Return True if this unit is following the promoted cluster."""
        if self.get_primary_cluster() is None:
            return False
        return (
            self.state.peer.data.get("unit-promoted-cluster-counter")
            == self.get_highest_promoted_cluster_counter_value()
        )

    def is_primary_cluster(self) -> bool:
        """Return whether this application is the primary cluster."""
        return self.state.model.app == self.get_primary_cluster()

    def clear_stale_promotion(self) -> None:
        """Clear a promoted-cluster-counter left over from a removed async relation.

        A force-removed dead offerer never delivers ``relation-broken``, leaving the
        counter behind; on a new async relation it would wrongly mark this app as the
        primary and block ``create-replication`` (DPE-10203).
        """
        if not self.state.model.unit.is_leader():
            return
        counter = self.state.application.data.get("promoted-cluster-counter")
        # Empty -> standby/clean (nothing promoted). "0" -> a standby already in read-only mode
        # (set by _on_async_relation_broken); leave it. A positive counter means this cluster
        # was promoted -> revert it to a standalone primary unless a live relation still
        # records that promotion. Deciding this from relation/peer data alone (no Patroni call)
        # is deliberate: after a dead-DC promote Patroni is frequently unreachable, which is
        # exactly when this must still run.
        if not counter or counter == "0":
            return
        # A promotion writes the counter to both the async relation it was promoted under and
        # the peers databag, so a counter mirrored on a current relation is a live replication
        # and is managed by the relation lifecycle. The recovery sequence forms a *new* offer
        # relation before running create-replication, and that relation carries no mirror —
        # the counter left by the dead relation is stale exactly then and must clear even
        # though a relation now exists (DPE-10203).
        for relation in [
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
        ]:
            if relation is None:
                continue
            try:
                if relation.data[self.state.model.app].get("promoted-cluster-counter") == counter:
                    return
            except ModelError:
                # A dying relation whose databags are unreadable cannot vouch for the
                # counter either: the promotion's relation is gone for all purposes.
                continue
        logger.info(
            "Clearing stale promoted-cluster-counter %s (no live async relation records it)",
            counter,
        )
        self.state.application.data.update({"promoted-cluster-counter": ""})
        self.update_config()

    @property
    def _primary_cluster_endpoint(self) -> str | None:
        """Return the endpoint from one of the sync-standbys, or from the primary if there is no sync-standby."""
        sync_standby_names = self.patroni_manager.get_sync_standby_names()
        if len(sync_standby_names) > 0:
            unit = self.state.model.get_unit(sync_standby_names[0])
            return self._get_unit_address(unit)
        return self._get_unit_address(self.state.model.unit)

    def _get_unit_address(self, unit: Unit) -> str | None:
        """Return the address the given peer unit published for the async relation."""
        if self.state.substrate == Substrates.K8S:
            # The K8s charm resolves a peer through the peer databag private address.
            if unit == self.state.model.unit:
                return self.state.unit_ip
            if self.state.peer_relation:
                return self.state.peer_relation.data[unit].get("private-address")
            return None
        return self.state.unit_database_address(unit, self._relation.name)  # type: ignore

    def _remote_secret_id(self) -> str | None:
        """Return the shared secret id published by the primary cluster, or None."""
        relation = self._relation
        if relation is None:
            return None
        primary_cluster_info = relation.data[relation.app].get("primary-cluster-data")
        if primary_cluster_info is None:
            return None
        return json.loads(primary_cluster_info).get("secret-id")

    def _update_internal_secret(self) -> bool:
        # Update the secrets between the clusters. Reference the secret purely by the id published
        # in relation data — never by label — so no consumer-side alias is registered (DPE-10203).
        secret_id = self._remote_secret_id()
        if secret_id is None:
            return False
        try:
            secret = self.state.model.get_secret(id=secret_id)
        except SecretNotFoundError:
            return False
        credentials = secret.peek_content()
        for key, password in credentials.items():
            user = key.split("-password")[0]
            self.state.set_secret(APP_SCOPE, key, password)
            logger.debug("Synced %s password", user)
        return True

    def _update_primary_cluster_data(
        self,
        promoted_cluster_counter: int | None = None,
        system_identifier: str | None = None,
    ) -> None:
        """Update the primary cluster data."""
        async_relation = self._relation

        if promoted_cluster_counter is not None:
            for relation in [async_relation, self.state.peer_relation]:
                relation.data[self.state.model.app].update({  # type: ignore
                    "promoted-cluster-counter": str(promoted_cluster_counter)
                })

        # Update the data in the relation.
        primary_cluster_data = {"endpoint": self._primary_cluster_endpoint}

        # Retrieve the secrets that will be shared between the clusters.
        if async_relation.name == REPLICATION_OFFER_RELATION:  # type: ignore
            secret = self._get_secret()
            if secret is not None:
                secret.grant(async_relation)  # type: ignore
                primary_cluster_data["secret-id"] = secret.id

        if system_identifier is not None:
            primary_cluster_data["system-id"] = system_identifier

        async_relation.data[self.state.model.app]["primary-cluster-data"] = json.dumps(  # type: ignore
            primary_cluster_data
        )

    def update_async_replication_data(self) -> None:
        """Updates the async-replication data, if the unit is the leader.

        This is used to update the standby units with the new primary information.
        """
        relation = self._relation
        if relation is None:
            return
        relation.data[self.state.model.unit].update({"unit-address": self._unit_ip})
        if self.is_primary_cluster() and self.state.model.unit.is_leader():
            self._update_primary_cluster_data()
