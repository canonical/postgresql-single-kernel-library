#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Async Replication events handler.

Owns the observers for the ``replication-offer``/``replication`` relations, the
create-replication and promote flows, and the standby lifecycle (stop, pgdata reset,
start) that moves a cluster between the primary and standby roles. The data plane
(counters, endpoints, secrets, primary-cluster-data publication) lives in the
:class:`~single_kernel_postgresql.managers.async_replication.AsyncReplicationManager`.

Ported from the PostgreSQL VM and K8s charms' async replication module. Substrate
divergences are kept: the VM charm observes relation_joined while the K8s charm observes
relation_created, the VM flows drive Patroni while the K8s flows drive the pebble
service, and only the K8s flows track the ``standby-pgdata-cleared`` flag. The
dead-datacenter recovery changes (DPE-10203) apply to both substrates.
"""

import contextlib
import json
import logging
from typing import TYPE_CHECKING, Protocol, cast

from ops import (
    ActionEvent,
    ActiveStatus,
    Application,
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    Object,
    RelationChangedEvent,
    RelationDepartedEvent,
    SecretChangedEvent,
    WaitingStatus,
)
from tenacity import RetryError, Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import (
    ClusterNotPromotedError,
    DeployedWithoutTrustError,
    NotReadyError,
    StandbyClusterAlreadyPromotedError,
)
from single_kernel_postgresql.config.literals import (
    PEER_RELATION,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.async_replication import (
    READ_ONLY_MODE_BLOCKING_MESSAGE,
    AsyncReplicationError,
    AsyncReplicationManager,
    _safe_databag_get,
)
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


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


if TYPE_CHECKING:
    # Substrate-only seams are injected; they must never enter the other substrate's
    # import graph, hence the type-checking-only imports.
    from single_kernel_postgresql.workload.k8s import K8sWorkload
    from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


def _same_secret_id(a: str | None, b: str | None) -> bool:
    """Whether two Juju secret ids refer to the same secret.

    Juju/ops may render an id as ``secret:<key>`` or ``secret://<uuid>/<key>``; compare on the
    trailing key so a format difference doesn't mask a real match.
    """
    if not a or not b:
        return False
    return a.rsplit("/", 1)[-1].split(":")[-1] == b.rsplit("/", 1)[-1].split(":")[-1]


class PostgreSQLAsyncReplication(Object):
    """Defines the async-replication management logic."""

    def __init__(
        self,
        charm,
        state: CharmState,
        manager: AsyncReplicationManager,
        patroni_manager: PatroniManager,
        workload: BaseWorkload,
        watcher: AsyncReplicationWatcher | None = None,
        k8s_manager=None,
    ):
        """Constructor.

        The charm is the composition root: it constructs the manager and the substrate
        bridges and hands them over, like the other events handlers.

        Args:
            charm: the charm using the handler.
            state: the charm state.
            manager: the async-replication data-plane manager.
            patroni_manager: the Patroni API manager.
            workload: the substrate workload.
            watcher: the VM watcher relation bridge; the K8s charm has no watcher.
            k8s_manager: the K8s API manager; only the K8s charm injects it.
        """
        super().__init__(charm, "postgresql")
        self.charm = charm
        self.state = state
        self.manager = manager
        self.patroni_manager = patroni_manager
        self.workload = workload
        self.watcher = watcher
        self.k8s_manager = k8s_manager

        k8s = self.state.substrate == Substrates.K8S
        # Departure and change events are the same on both substrates; the VM charm
        # observes relation_joined while the K8s charm observes relation_created.
        join_event_name = "relation_created" if k8s else "relation_joined"
        for relation_name in [REPLICATION_OFFER_RELATION, REPLICATION_CONSUMER_RELATION]:
            self.framework.observe(
                getattr(self.charm.on[relation_name], join_event_name),
                self._on_async_relation_joined,
            )
            self.framework.observe(
                self.charm.on[relation_name].relation_changed,
                self._on_async_relation_changed,
            )
            self.framework.observe(
                self.charm.on[relation_name].relation_departed,
                self._on_async_relation_departed,
            )
            self.framework.observe(
                self.charm.on[relation_name].relation_broken,
                self._on_async_relation_broken,
            )

        # Actions
        self.framework.observe(
            self.charm.on.create_replication_action, self._on_create_replication
        )

        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)

    @property
    def _relation(self):
        """Return the usable async-replication relation, or None."""
        return self.manager._relation

    # -- Public surface the charms consume

    def get_primary_cluster(self) -> Application | None:
        """Return the primary cluster."""
        return self.manager.get_primary_cluster()

    def get_primary_cluster_endpoint(self) -> str | None:
        """Return the primary cluster endpoint."""
        return self.manager.get_primary_cluster_endpoint()

    def get_all_primary_cluster_endpoints(self) -> list[str]:
        """Return all the primary cluster endpoints from the standby cluster."""
        return self.manager.get_all_primary_cluster_endpoints()

    def get_standby_endpoints(self) -> list[str]:
        """Return the standby endpoints."""
        return self.manager.get_standby_endpoints()

    def get_partner_addresses(self) -> list[str]:
        """Return the partner addresses."""
        return self.manager.get_partner_addresses()

    def is_primary_cluster(self) -> bool:
        """Return whether this application is the primary cluster."""
        return self.manager.is_primary_cluster()

    def update_async_replication_data(self) -> None:
        """Update the async-replication data (unit address and primary cluster data)."""
        self.manager.update_async_replication_data()

    def clear_stale_promotion(self) -> None:
        """Clear a promoted-cluster-counter left over from a removed async relation."""
        self.manager.clear_stale_promotion()

    def set_app_status(self) -> None:
        """Set the app status."""
        if self.state.peer_relation is None:
            return
        if self.state.application.data.get("promoted-cluster-counter") == "0":
            self.charm.set_app_status(BlockedStatus(READ_ONLY_MODE_BLOCKING_MESSAGE))
            return
        if self.manager._relation is None:
            self.charm.set_app_status(ActiveStatus())
            return
        primary_cluster = self.manager.get_primary_cluster()
        if primary_cluster is None:
            self.charm.set_app_status(ActiveStatus())
        else:
            self.charm.set_app_status(
                ActiveStatus("Primary" if self.state.model.app == primary_cluster else "Standby")
            )

    def handle_read_only_mode(self) -> None:
        """Handle read-only mode (standby cluster that lost the relation with the primary cluster)."""
        if not isinstance(self.state.model.unit.status, BlockedStatus):
            self.charm.set_primary_status_message()

        if self.state.model.unit.is_leader():
            self.set_app_status()

    # -- Actions

    def _on_create_replication(self, event: ActionEvent) -> None:
        """Set up asynchronous replication between two clusters."""
        # A dead-DC teardown whose relation-broken never fired leaves the promoted-
        # cluster-counter orphaned in peer data; clear it before the guard reads it,
        # or create-replication reports "There is already a replication set up."
        # until an update-status cycle happens to reconcile (DPE-10203).
        self.manager.clear_stale_promotion()
        if self.manager.get_primary_cluster() is not None:
            event.fail("There is already a replication set up.")
            return

        if self.manager._relation.name == REPLICATION_CONSUMER_RELATION:  # type: ignore
            event.fail("This action must be run in the cluster where the offer was created.")
            return

        if not self._handle_replication_change(event):
            return

        # Set the replication name in the relation data.
        self.manager._relation.data[self.state.model.app].update(  # type: ignore
            {"name": event.params["name"]}
        )

        # Set the status.
        self.charm.set_unit_status(MaintenanceStatus("Creating replication..."))

    def _on_async_relation_joined(self, _) -> None:
        """Publish this unit address in the relation data."""
        # store unit address in relation data
        self.manager._relation.data[self.state.model.unit].update(  # type: ignore
            {"unit-address": self.manager._unit_ip}
        )

        # Set the counter for new units.
        highest_promoted_cluster_counter = (
            self.manager.get_highest_promoted_cluster_counter_value()
        )
        if highest_promoted_cluster_counter != "0":
            self.state.peer.data.update({
                "unit-promoted-cluster-counter": highest_promoted_cluster_counter
            })

        if self.watcher is not None and self.state.model.unit.is_leader():
            self.watcher.update_endpoints()

    def _on_async_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Set a flag to avoid setting a wrong status message on relation broken event handler."""
        # This is needed because of https://bugs.launchpad.net/juju/+bug/1979811.
        if event.departing_unit == self.state.model.unit and self.state.peer_relation is not None:
            self.state.peer.data.update({"departing": "True"})

    def _on_async_relation_broken(self, _) -> None:
        k8s = self.state.substrate == Substrates.K8S
        if self.state.peer_relation is None or self.state.peer.is_unit_departing:
            logger.debug("Early exit on_async_relation_broken: Skipping departing unit.")
            return

        self.state.peer.data.update({
            "stopped": "",
            **({"standby-pgdata-cleared": ""} if k8s else {}),
            "unit-promoted-cluster-counter": "",
        })

        # A force-removed dead offerer can make the standby check fail transiently;
        # crashing here would wedge the unit before the counter is cleared, so treat
        # the cluster as primary (DPE-10203 / Issue B).
        try:
            is_standby = self.patroni_manager.get_standby_leader() is not None
        except Exception as e:
            logger.warning(
                "get_standby_leader unavailable during teardown, assuming primary: %s", e
            )
            is_standby = False

        # If this is the standby cluster, set 0 in the "promoted-cluster-counter" field to set
        # the cluster in read-only mode message also in the other units.
        if is_standby:
            if self.state.model.unit.is_leader():
                self.state.application.data.update({"promoted-cluster-counter": "0"})
                self.set_app_status()
        else:
            if self.state.model.unit.is_leader():
                self.state.application.data.update({"promoted-cluster-counter": ""})
            try:
                self.charm.update_config()
            except (DeployedWithoutTrustError, RetryError, ModelError) as e:
                logger.warning("update_config failed during teardown (continuing): %s", e)

        if self.watcher is not None and self.state.model.unit.is_leader():
            try:
                self.watcher.update_endpoints()
            except (ModelError, RetryError) as e:
                logger.warning(
                    "watcher endpoint update failed during teardown (continuing): %s", e
                )

    def _on_async_relation_changed(self, event: RelationChangedEvent) -> None:
        """Update the Patroni configuration if one of the clusters was already promoted."""
        if self.state.model.unit.is_leader():
            self.set_app_status()
            if self.watcher is not None:
                self.watcher.update_endpoints()

        primary_cluster = self.manager.get_primary_cluster()
        logger.debug("Primary cluster: %s", primary_cluster)
        if primary_cluster is None:
            logger.debug("Early exit on_async_relation_changed: No primary cluster found.")
            return

        if self._configure_primary_cluster(primary_cluster, event):
            return

        # Return if this is a new unit joining an existing standby cluster.
        if (
            not self.state.model.unit.is_leader()
            and self.manager.is_following_promoted_cluster()
            and self._handle_late_joiner(event)
        ):
            return

        if not self._stop_database(event):
            return
        self._publish_stop_marker(event)

        if self._wait_for_all_units_stopped(event):
            return

        if self._wait_for_standby_leader(event):
            return

        if self._start_standby_database(event):
            return

        self._handle_database_start(event)
    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        """Update the internal secret when the relation secret changes."""
        relation = self.manager._relation
        if relation is None:
            logger.debug("Early exit on_secret_changed: No relation found.")
            return

        if (
            relation.name == REPLICATION_OFFER_RELATION
            and event.secret.label == f"{PEER_RELATION}.{self.state.model.app.name}.app"
        ):
            logger.info("Internal secret changed, updating relation secret")
            if not (secret := self.manager._get_secret()):
                logger.debug("Defer on_secret_changed: Secret not created yet")
                event.defer()
                return
            secret.grant(relation)
            primary_cluster_data = {
                "endpoint": self.manager._primary_cluster_endpoint,
                "secret-id": secret.id,
            }
            relation.data[self.state.model.app]["primary-cluster-data"] = json.dumps(
                primary_cluster_data
            )
            return

        if relation.name == REPLICATION_CONSUMER_RELATION and _same_secret_id(
            event.secret.id, self.manager._remote_secret_id()
        ):
            logger.info("Relation secret changed, updating internal secret")
            if not self.manager._update_internal_secret():
                logger.debug("Secret not found, deferring event")
                event.defer()

