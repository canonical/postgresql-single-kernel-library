#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Async Replication manager.

Owns the async-replication data plane (promoted-cluster-counter resolution, the
primary/standby endpoint getters, the shared-cluster secret handling, and the
primary-cluster-data publication) and the promotion/standby lifecycle flows (action
guards, cluster reconfiguration, stop, pgdata reset, start). The events handler owns
the observers and the public surface the charms consume.

Ported from the PostgreSQL VM and K8s charms' async replication module, including the
dead-datacenter recovery changes (DPE-10203): relation databag reads tolerate ModelError
from a force-removed cross-model relation, the shared secret is referenced by id instead
of label, and a stale promoted-cluster-counter is cleared before the create-replication
and promote actions.
"""

import contextlib
import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, cast

from ops import (
    ActionEvent,
    Application,
    MaintenanceStatus,
    ModelError,
    Relation,
    RelationChangedEvent,
    Secret,
    SecretNotFoundError,
    StatusBase,
    Unit,
    WaitingStatus,
)
from tenacity import RetryError, Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from single_kernel_postgresql.compat.postgresql import PostgreSQLBaseError
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import (
    ClusterNotPromotedError,
    NotReadyError,
    StandbyClusterAlreadyPromotedError,
)
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    ASYNC_SHARED_SECRET_ID_KEY,
    K8S_POSTGRESQL_SERVICE_NAME,
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

    from single_kernel_postgresql.managers.k8s import K8sManager
    from single_kernel_postgresql.workload.k8s import K8sWorkload
    from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


READ_ONLY_MODE_BLOCKING_MESSAGE = "Standalone read-only cluster"


class AsyncReplicationError(PostgreSQLBaseError):
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


class AsyncReplicationWatcher(Protocol):
    """The substrate-provided watcher bridge (only the VM charm has a watcher)."""

    def enable_watcher(self) -> None:
        """Enable the watcher."""
        ...

    def update_endpoints(self) -> None:
        """Update the watcher endpoints."""
        ...

    def disable_watcher(self) -> None:
        """Disable the watcher."""
        ...


class AsyncReplicationManager(BaseManager):
    """Defines the async-replication management logic."""

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        patroni_manager: PatroniManager,
        update_config: "Callable[[], bool]",
        set_unit_status: "Callable[[StatusBase], None]",
        set_primary_status_message: "Callable[[], None]",
        set_app_status: "Callable[[], None]",
        create_pgdata: "Callable[[], None]",
        fix_leader_annotation: "Callable[[], bool]",
        re_emit_relation_changed: "Callable[[], None]",
        k8s_manager: "K8sManager | None" = None,
        watcher: "AsyncReplicationWatcher | None" = None,
    ):
        """Constructor.

        Args:
            state: the charm state.
            workload: the substrate workload.
            patroni_manager: the Patroni API manager, for the sync-standby endpoint lookup.
            update_config: the charm's config re-render bridge, used to reconcile after
                clearing a stale promotion.
            set_unit_status: the charm's unit-status bridge, carrying the production
                charms' refresh-priority semantics.
            set_primary_status_message: the charm's primary status-message bridge.
            set_app_status: the bridge recomputing the async-replication app status.
            create_pgdata: the K8s pgdata-recreation bridge.
            fix_leader_annotation: the K8s leader-annotation bridge.
            re_emit_relation_changed: the bridge re-emitting the async relation-changed
                event through the handler (framework plumbing stays event-side).
            k8s_manager: the lightkube resource manager (K8s substrate only).
            watcher: the VM watcher bridge, where one is configured.
        """
        super().__init__(state, workload, "async_replication_manager")
        self.patroni_manager = patroni_manager
        self.update_config = update_config
        self.set_unit_status = set_unit_status
        self.set_primary_status_message = set_primary_status_message
        self.set_app_status = set_app_status
        self.create_pgdata = create_pgdata
        self.fix_leader_annotation = fix_leader_annotation
        self.re_emit_relation_changed = re_emit_relation_changed
        self.k8s_manager = k8s_manager
        self.watcher = watcher

    @property
    def async_relation(self) -> Relation | None:
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
    def unit_ip(self) -> str:
        """Return this unit IP address for the replication relation."""
        if not self.async_relation:
            raise AsyncReplicationError("No relation to get IP for")

        if self.state.substrate == Substrates.K8S:
            return self._get_unit_ip()
        if self.async_relation.name == REPLICATION_OFFER_RELATION:
            ip = self.state.replication_offer_ip
        else:
            ip = self.state.replication_consumer_ip

        if not ip:
            raise AsyncReplicationError(f"No IP set for {self.async_relation.name}")
        return ip

    def _get_unit_ip(self) -> str:
        """Return this unit's pod IP, resolved from the pod filesystem (K8s only)."""
        return cast("K8sWorkload", self.workload).get_unit_ip_from_hosts()

    def get_all_primary_cluster_endpoints(self) -> list[str]:
        """Return all the primary cluster endpoints from the standby cluster."""
        if not (relation := self.async_relation):
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
            for app, databag in [
                (async_relation.app, async_relation.data[async_relation.app]),
                (self.state.model.app, self.state.application.data),
            ]:
                if app is None:
                    continue
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
        relation = self.async_relation
        if relation is None:
            return None
        primary_cluster_data = _safe_databag_get(
            relation.data[relation.app], "primary-cluster-data"
        )
        if primary_cluster_data is None:
            return None
        return json.loads(primary_cluster_data).get("endpoint")

    def get_shared_secret(self) -> Secret | None:
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
        if not (relation := self.async_relation):
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
    def primary_cluster_endpoint(self) -> str | None:
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
        return self.state.unit_database_address(unit, self.async_relation.name)  # type: ignore

    def remote_secret_id(self) -> str | None:
        """Return the shared secret id published by the primary cluster, or None."""
        relation = self.async_relation
        if relation is None:
            return None
        primary_cluster_info = _safe_databag_get(
            relation.data[relation.app], "primary-cluster-data"
        )
        if primary_cluster_info is None:
            return None
        return json.loads(primary_cluster_info).get("secret-id")

    def _update_internal_secret(self) -> bool:
        # Update the secrets between the clusters. Reference the secret purely by the id published
        # in relation data — never by label — so no consumer-side alias is registered (DPE-10203).
        secret_id = self.remote_secret_id()
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

    def update_primary_cluster_data(
        self,
        promoted_cluster_counter: int | None = None,
        system_identifier: str | None = None,
    ) -> None:
        """Update the primary cluster data."""
        async_relation = self.async_relation

        if promoted_cluster_counter is not None:
            for relation in [async_relation, self.state.peer_relation]:
                relation.data[self.state.model.app].update({  # type: ignore
                    "promoted-cluster-counter": str(promoted_cluster_counter)
                })

        # Update the data in the relation.
        primary_cluster_data = {"endpoint": self.primary_cluster_endpoint}

        # Retrieve the secrets that will be shared between the clusters.
        if async_relation.name == REPLICATION_OFFER_RELATION:  # type: ignore
            secret = self.get_shared_secret()
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
        relation = self.async_relation
        if relation is None:
            return
        relation.data[self.state.model.unit].update({"unit-address": self.unit_ip})
        if self.is_primary_cluster() and self.state.model.unit.is_leader():
            self.update_primary_cluster_data()

    # -- Promotion flow (actions)

    def _can_promote_cluster(self, event: ActionEvent) -> bool:
        """Check if the cluster can be promoted."""
        if not self.state.application.is_cluster_initialised:
            event.fail("Cluster not initialised yet.")
            return False

        # Check if there is a relation. If not, see if there is a standby leader. If so promote it to leader. If not,
        # fail the action telling that there is no relation and no standby leader.
        relation = self.async_relation
        if relation is None:
            standby_leader = self.patroni_manager.get_standby_leader()
            if standby_leader is not None:
                try:
                    self.patroni_manager.promote_standby_cluster()
                    if self.state.model.app.status.message == READ_ONLY_MODE_BLOCKING_MESSAGE:
                        self.state.application.data.update({"promoted-cluster-counter": ""})
                        self.set_app_status()
                        self.set_primary_status_message()
                except (StandbyClusterAlreadyPromotedError, ClusterNotPromotedError) as e:
                    event.fail(str(e))
                return False
            event.fail("No relation and no standby leader found.")
            return False

        # Check if this cluster is already the primary cluster. If so, fail the action telling that it's already
        # the primary cluster.
        primary_cluster = self.get_primary_cluster()
        if self.state.model.app == primary_cluster:
            event.fail("This cluster is already the primary cluster.")
            return False

        return self._handle_forceful_promotion(event)

    def _handle_forceful_promotion(self, event: ActionEvent) -> bool:
        if not event.params.get("force"):
            all_primary_cluster_endpoints = self.get_all_primary_cluster_endpoints()
            if len(all_primary_cluster_endpoints) > 0:
                primary_cluster_reachable = False
                try:
                    primary = self.patroni_manager.get_primary(
                        alternative_endpoints=all_primary_cluster_endpoints
                    )
                    if primary is not None:
                        primary_cluster_reachable = True
                except RetryError:
                    pass
                if not primary_cluster_reachable:
                    event.fail(
                        f"{self.async_relation.app.name} isn't reachable. Pass `force=true` to promote anyway."  # type: ignore
                    )
                    return False
        else:
            logger.warning(
                "Forcing promotion of %s to primary cluster due to `force=true`.",
                self.state.model.app.name,
            )
        return True

    def _handle_replication_change(self, event: ActionEvent) -> bool:
        k8s = self.state.substrate == Substrates.K8S
        if not self._can_promote_cluster(event):
            return False

        relation = self.async_relation
        if relation is None:
            event.fail("Replication relation not found")
            return False

        # Ensure the relation has at least one remote unit before trying to process unit data.
        remote_units = [unit for unit in relation.units if unit.app == relation.app]
        addresses_message = (
            "All units from the other cluster must publish their pod addresses in the relation data."
            if k8s
            else "All units from the other cluster must publish their unit addresses in the relation data."
        )
        if len(remote_units) == 0:
            event.fail(addresses_message)
            return False

        # Check if all units from the other cluster published their IPs in the relation data.
        # If not, fail the action telling that all units must publish their pod addresses in the
        # relation data.
        for unit in remote_units:
            if _safe_databag_get(relation.data[unit], "unit-address") is None:
                event.fail(addresses_message)
                return False

        system_identifier, error = self.workload.get_system_identifier()
        if error is not None:
            logger.exception(error)
            event.fail("Failed to get system identifier")
            return False

        # Increment the current cluster counter in this application side based on the highest counter value.
        promoted_cluster_counter = int(self.get_highest_promoted_cluster_counter_value())
        promoted_cluster_counter += 1
        logger.debug("Promoted cluster counter: %s", promoted_cluster_counter)

        self.update_primary_cluster_data(promoted_cluster_counter, system_identifier)

        if k8s:
            # Emit an async replication changed event for this unit (to promote this cluster before demoting the
            # other if this one is a standby cluster, which is needed to correctly set up the async replication
            # when performing a switchover).
            self.re_emit_relation_changed()

        return True

    def promote_to_primary(self, event: ActionEvent) -> None:
        """Promote this cluster to the primary cluster."""
        # Same stale-counter exposure as create-replication: a counter orphaned by a
        # teardown without events would mask the "no primary" condition below.
        self.clear_stale_promotion()
        if (
            self.state.model.app.status.message != READ_ONLY_MODE_BLOCKING_MESSAGE
            and self.get_primary_cluster() is None
        ):
            event.fail(
                "No primary cluster found. Run `create-replication` action in the cluster where the offer was created."
            )
            return

        if not self._handle_replication_change(event):
            return

        # Set the status. The VM charm reuses the create-replication message; the K8s
        # charm reports the promotion explicitly.
        message = (
            "Promoting cluster..."
            if self.state.substrate == Substrates.K8S
            else "Creating replication..."
        )
        self.set_unit_status(MaintenanceStatus(message))

    # -- Standby lifecycle (relation-changed flow)

    def _wait_for_all_units_stopped(self, event: RelationChangedEvent) -> bool:
        """Wait until all units stopped; True when the event is deferred."""
        peers = self.state.peer_relation.units if self.state.peer_relation else []
        if not (
            self.state.peer.is_unit_stopped or self.is_following_promoted_cluster()
        ) or not all(
            "stopped" in self.state.peer_relation.data[unit]  # type: ignore
            or self.state.peer_relation.data[unit].get("unit-promoted-cluster-counter")  # type: ignore
            == self.get_highest_promoted_cluster_counter_value()
            for unit in peers
        ):
            self.set_unit_status(
                WaitingStatus("Waiting for the database to be stopped in all units")
            )
            logger.debug("Deferring on_async_relation_changed: not all units stopped.")
            event.defer()
            return True
        return False

    def _publish_stop_marker(self, event: RelationChangedEvent) -> None:
        """Publish the stop marker into the relation databag (VM only).

        The demoted cluster's primary-side pre-check compares this against the highest
        promoted-cluster-counter to decide the other cluster is down.
        """
        if self.state.substrate == Substrates.VM:
            event.relation.data[self.state.model.unit]["stopped"] = (
                self.get_highest_promoted_cluster_counter_value()
            )

    def _handle_late_joiner(self, event: RelationChangedEvent) -> bool:
        """Handle a non-leader unit joining an existing standby cluster.

        Returns True when the relation-changed handling must stop.
        """
        if self.state.substrate == Substrates.K8S:
            # If the database is already running (i.e., we're a late joiner that completed
            # setup), just return early - the unit is already part of the standby cluster.
            if self.patroni_manager.member_started:
                logger.debug("Early exit on_async_relation_changed: following promoted cluster.")
                return True
            # Database not running - clear pgdata if needed so Patroni can run pg_basebackup.
            # Only clear once, tracked by standby-pgdata-cleared flag.
            if self.state.peer.data.get("standby-pgdata-cleared") != "True":
                self._clear_pgdata()
                self.state.peer.data.update({"standby-pgdata-cleared": "True"})
            return False
        logger.debug("Early exit on_async_relation_changed: following promoted cluster.")
        self.update_config()
        return True

    def _start_standby_database(self, event: RelationChangedEvent) -> bool:
        """Update the configuration and start the database; True when the event is deferred."""
        if self.state.substrate == Substrates.K8S:
            if not cast("K8sWorkload", self.workload).postgresql_service_registered():
                logger.debug("Early exit on_async_relation_changed: container hasn't started yet.")
                event.defer()
                return True
            # Update the asynchronous replication configuration and start the database.
            self.update_config()
            self.workload.start_service(K8S_POSTGRESQL_SERVICE_NAME)
        else:
            # Update the asynchronous replication configuration and start the database.
            self.update_config()
            if not self.patroni_manager.start_patroni():
                raise AsyncReplicationError("Failed to start patroni service.")
        return False

    def _configure_primary_cluster(
        self, primary_cluster: Application, event: RelationChangedEvent
    ) -> bool:
        """Configure the primary cluster."""
        k8s = self.state.substrate == Substrates.K8S
        if self.state.model.app == primary_cluster:
            if not k8s:
                # The VM charm waits for the other cluster to stop before reconfiguring.
                counter = self.get_highest_promoted_cluster_counter_value()
                if not all(
                    _safe_databag_get(event.relation.data[unit], "stopped") == counter
                    for unit in event.relation.units
                    if unit.app == event.relation.app
                ):
                    logger.info("Other cluster not yet down.")
                    event.defer()
                    return True
            self.update_config()
            if self.is_primary_cluster() and self.state.model.unit.is_leader():
                self.update_primary_cluster_data()
                # If this is a standby cluster, remove the information from DCS to make it
                # a normal cluster.
                if self.patroni_manager.get_standby_leader() is not None:
                    self.patroni_manager.promote_standby_cluster()
                    try:
                        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                            with attempt:
                                if self.state.model.unit.name != self.patroni_manager.get_primary(
                                    unit_name_pattern=True
                                ):
                                    raise ClusterNotPromotedError()
                    except RetryError:
                        logger.debug(
                            "Deferring on_async_relation_changed: standby cluster not promoted yet."
                        )
                        event.defer()
                        return True
            self.state.peer.data.update({
                "unit-promoted-cluster-counter": self.get_highest_promoted_cluster_counter_value()
            })
            self.set_primary_status_message()
            return True
        return False

    def _configure_standby_cluster(self, event: RelationChangedEvent) -> bool:
        """Configure the standby cluster."""
        k8s = self.state.substrate == Substrates.K8S
        if not (relation := self.async_relation):
            raise AsyncReplicationError("No relation in configure standby cluster")

        if relation.name == REPLICATION_CONSUMER_RELATION and not (self._update_internal_secret()):
            logger.debug("Secret not found, deferring event")
            event.defer()
            return False
        system_identifier, error = self.workload.get_system_identifier()
        if error is not None:
            raise AsyncReplicationError(error)
        if system_identifier != _safe_databag_get(relation.data[relation.app], "system-id"):
            # Store current data in a tar.gz file.
            logger.info(
                "Creating backup of pgdata folder" if k8s else "Creating backup of data folder"
            )
            filename = self.workload.create_data_backup_tarball()
            logger.warning("Please review the backup file %s and handle its removal", filename)
        if k8s:
            # Remove the Kubernetes resources left by the previous cluster.
            if self.k8s_manager is not None:
                self.k8s_manager.delete_patroni_cluster_resources()
        else:
            self.state.application.data["suppress-oversee-users"] = "true"
        return True

    def _stop_database(self, event: RelationChangedEvent) -> bool:
        """Stop the database."""
        k8s = self.state.substrate == Substrates.K8S
        if not self.state.peer.is_unit_stopped and not (self.is_following_promoted_cluster()):
            if not self.state.model.unit.is_leader() and not self.workload.exists(
                self.workload.paths.data
            ):
                logger.debug("Early exit on_async_relation_changed: following promoted cluster.")
                return False

            if k8s:
                self.workload.stop()
            elif not self._stop_patroni_with_retries(event):
                return False

            if self.state.model.unit.is_leader():
                # Remove the "cluster_initialised" flag to avoid self-healing in the update status hook.
                self.state.application.data.update({"cluster_initialised": ""})
                if not self._configure_standby_cluster(event):
                    return False

                if k8s:
                    # Only the leader clears pgdata here. Non-leaders will clear pgdata
                    # after the standby leader has started (in _wait_for_standby_leader)
                    # to avoid system ID mismatch issues.
                    self._clear_pgdata()

            if not k8s:
                # The VM charm clears pgdata and the raft state on every unit, so each
                # one re-initialises from the new primary.
                self._reinitialise_pgdata()

            self.state.peer.data.update({"stopped": "True"})
        return True

    def _stop_patroni_with_retries(self, event: RelationChangedEvent) -> bool:
        """Stop Patroni, retrying a few times; True when the event is deferred."""
        if self.watcher is not None:
            self.watcher.disable_watcher()

        try:
            for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3)):
                with attempt:
                    if not self.patroni_manager.stop_patroni():
                        raise AsyncReplicationError("Failed to stop patroni service.")
        except RetryError:
            logger.debug("Deferring on_async_relation_changed: patroni hasn't stopped yet.")
            event.defer()
            return False
        return True

    def _reinitialise_pgdata(self) -> None:
        """Remove and recreate the data folder to enable replication (VM only)."""
        # Remove and recreate the data folder to enable replication of the data from the
        # primary cluster.
        logger.info("Removing and recreating data folder")
        self.workload.clear_data_directories()

        # Remove previous cluster information to make it possible to initialise a new
        # cluster.
        logger.info("Removing previous cluster information")
        cast("VMWorkload", self.workload).remove_raft_state()

    def _clear_pgdata(self) -> None:
        """Remove and recreate the pgdata folder to enable replication (K8s only)."""
        # Note: the workload clears the real pgdata path instead of the Debian
        # compatibility symlink (/var/lib/postgresql/16/main), because find doesn't
        # follow symlinks by default.
        self.workload.clear_data_directories()
        self.create_pgdata()

    def _handle_database_start(self, event: RelationChangedEvent) -> None:
        """Handle the database start in the standby cluster."""
        k8s = self.state.substrate == Substrates.K8S
        try:
            if self.patroni_manager.member_started:
                # If the database is started, update the databag in a way the unit is marked as configured
                # for async replication.
                self.state.peer.data.update({
                    "stopped": "",
                    **({"standby-pgdata-cleared": ""} if k8s else {}),
                    "unit-promoted-cluster-counter": self.get_highest_promoted_cluster_counter_value(),
                })

                if self.state.model.unit.is_leader() and self._handle_leader_database_start(event):
                    return

                self.set_primary_status_message()
            elif not self.state.model.unit.is_leader():
                if not k8s:
                    with contextlib.suppress(RetryError):
                        self.patroni_manager.reload_patroni_configuration()
                raise NotReadyError()
            else:
                if k8s:
                    # If the standby leader fails to start, fix the leader annotation and defer the event.
                    self.fix_leader_annotation()
                self.set_unit_status(
                    WaitingStatus("Still starting the database in the standby leader")
                )
                event.defer()
        except NotReadyError:
            self.set_unit_status(WaitingStatus("Waiting for the database to start"))
            logger.debug("Deferring on_async_relation_changed: database hasn't started yet.")
            event.defer()

    def _handle_leader_database_start(self, event: RelationChangedEvent) -> bool:
        """Leader-side handling after the database started; True when the event is deferred."""
        k8s = self.state.substrate == Substrates.K8S
        peers = self.state.peer_relation.units if self.state.peer_relation else []
        if not k8s:
            self.update_config()
        if all(
            self.state.peer_relation.data[unit].get("unit-promoted-cluster-counter")  # type: ignore
            == self.get_highest_promoted_cluster_counter_value()
            for unit in {*peers, self.state.model.unit}
        ):
            self.state.application.data.update({"cluster_initialised": "True"})
            if self.watcher is not None:
                self.watcher.enable_watcher()
        elif self.is_following_promoted_cluster():
            self.set_unit_status(
                WaitingStatus("Waiting for the database to be started in all units")
            )
            event.defer()
            return True
        return False

    def _wait_for_standby_leader(self, event: RelationChangedEvent) -> bool:
        """Wait for the standby leader to be up and running."""
        k8s = self.state.substrate == Substrates.K8S
        try:
            standby_leader = self.patroni_manager.get_standby_leader(check_whether_is_running=True)
        except RetryError:
            standby_leader = None
        if not self.state.model.unit.is_leader() and standby_leader is None:
            if not k8s and self.patroni_manager.is_member_isolated:
                self.patroni_manager.restart_patroni()
                self.set_unit_status(WaitingStatus("Restarting Patroni to rejoin the cluster"))
                logger.debug(
                    "Deferring on_async_relation_changed: restarting Patroni to rejoin the cluster."
                )
                event.defer()
                return True
            self.set_unit_status(
                WaitingStatus("Waiting for the standby leader start the database")
            )
            logger.debug("Deferring on_async_relation_changed: standby leader hasn't started yet.")
            event.defer()
            return True

        # For non-leader units, clear pgdata once the standby leader is confirmed running
        # (K8s only). This ensures replicas get the correct system ID from the standby
        # leader. Only clear pgdata once - use a flag to track if we've already done it.
        if (
            k8s
            and not self.state.model.unit.is_leader()
            and self.state.peer.data.get("standby-pgdata-cleared") != "True"
        ):
            self._clear_pgdata()
            self.state.peer.data.update({"standby-pgdata-cleared": "True"})

        return False
