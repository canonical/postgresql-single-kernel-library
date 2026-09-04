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

    def _can_promote_cluster(self, event: ActionEvent) -> bool:
        """Check if the cluster can be promoted."""
        if not self.state.application.is_cluster_initialised:
            event.fail("Cluster not initialised yet.")
            return False

        # Check if there is a relation. If not, see if there is a standby leader. If so promote it to leader. If not,
        # fail the action telling that there is no relation and no standby leader.
        relation = self.manager._relation
        if relation is None:
            standby_leader = self.patroni_manager.get_standby_leader()
            if standby_leader is not None:
                try:
                    self.patroni_manager.promote_standby_cluster()
                    if self.state.model.app.status.message == READ_ONLY_MODE_BLOCKING_MESSAGE:
                        self.state.application.data.update({"promoted-cluster-counter": ""})
                        self.set_app_status()
                        self.charm.set_primary_status_message()
                except (StandbyClusterAlreadyPromotedError, ClusterNotPromotedError) as e:
                    event.fail(str(e))
                return False
            event.fail("No relation and no standby leader found.")
            return False

        # Check if this cluster is already the primary cluster. If so, fail the action telling that it's already
        # the primary cluster.
        primary_cluster = self.manager.get_primary_cluster()
        if self.state.model.app == primary_cluster:
            event.fail("This cluster is already the primary cluster.")
            return False

        return self._handle_forceful_promotion(event)

    def _handle_forceful_promotion(self, event: ActionEvent) -> bool:
        if not event.params.get("force"):
            all_primary_cluster_endpoints = self.manager.get_all_primary_cluster_endpoints()
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
                        f"{self.manager._relation.app.name} isn't reachable. Pass `force=true` to promote anyway."  # type: ignore
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

        relation = self.manager._relation
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
        promoted_cluster_counter = int(self.manager.get_highest_promoted_cluster_counter_value())
        promoted_cluster_counter += 1
        logger.debug("Promoted cluster counter: %s", promoted_cluster_counter)

        self.manager._update_primary_cluster_data(promoted_cluster_counter, system_identifier)

        if k8s:
            # Emit an async replication changed event for this unit (to promote this cluster before demoting the
            # other if this one is a standby cluster, which is needed to correctly set up the async replication
            # when performing a switchover).
            self._re_emit_async_relation_changed_event()

        return True

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

    def promote_to_primary(self, event: ActionEvent) -> None:
        """Promote this cluster to the primary cluster."""
        # Same stale-counter exposure as create-replication: a counter orphaned by a
        # teardown without events would mask the "no primary" condition below.
        self.manager.clear_stale_promotion()
        if (
            self.state.model.app.status.message != READ_ONLY_MODE_BLOCKING_MESSAGE
            and self.manager.get_primary_cluster() is None
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
        self.charm.set_unit_status(MaintenanceStatus(message))

    # -- Relation lifecycle

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

    def _wait_for_all_units_stopped(self, event: RelationChangedEvent) -> bool:
        """Wait until all units stopped; True when the event is deferred."""
        peers = self.state.peer_relation.units if self.state.peer_relation else []
        if not (
            self.state.peer.is_unit_stopped or self.manager.is_following_promoted_cluster()
        ) or not all(
            "stopped" in self.state.peer_relation.data[unit]  # type: ignore
            or self.state.peer_relation.data[unit].get("unit-promoted-cluster-counter")  # type: ignore
            == self.manager.get_highest_promoted_cluster_counter_value()
            for unit in peers
        ):
            self.charm.set_unit_status(
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
                self.manager.get_highest_promoted_cluster_counter_value()
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
        self.charm.update_config()
        return True

    def _start_standby_database(self, event: RelationChangedEvent) -> bool:
        """Update the configuration and start the database; True when the event is deferred."""
        if self.state.substrate == Substrates.K8S:
            if not cast("K8sWorkload", self.workload).postgresql_service_registered():
                logger.debug("Early exit on_async_relation_changed: container hasn't started yet.")
                event.defer()
                return True
            # Update the asynchronous replication configuration and start the database.
            self.charm.update_config()
            self.workload.start_service()
        else:
            # Update the asynchronous replication configuration and start the database.
            self.charm.update_config()
            if not self.patroni_manager.start_patroni():
                raise Exception("Failed to start patroni service.")
        return False

    def _configure_primary_cluster(
        self, primary_cluster: Application, event: RelationChangedEvent
    ) -> bool:
        """Configure the primary cluster."""
        k8s = self.state.substrate == Substrates.K8S
        if self.state.model.app == primary_cluster:
            if not k8s:
                # The VM charm waits for the other cluster to stop before reconfiguring.
                counter = self.manager.get_highest_promoted_cluster_counter_value()
                if not all(
                    _safe_databag_get(event.relation.data[unit], "stopped") == counter
                    for unit in event.relation.units
                    if unit.app == event.relation.app
                ):
                    logger.info("Other cluster not yet down.")
                    event.defer()
                    return True
            self.charm.update_config()
            if self.manager.is_primary_cluster() and self.state.model.unit.is_leader():
                self.manager._update_primary_cluster_data()
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
                "unit-promoted-cluster-counter": self.manager.get_highest_promoted_cluster_counter_value()
            })
            self.charm.set_primary_status_message()
            return True
        return False

    def _configure_standby_cluster(self, event: RelationChangedEvent) -> bool:
        """Configure the standby cluster."""
        k8s = self.state.substrate == Substrates.K8S
        if not (relation := self.manager._relation):
            raise AsyncReplicationError("No relation in configure standby cluster")

        if relation.name == REPLICATION_CONSUMER_RELATION and not (
            self.manager._update_internal_secret()
        ):
            logger.debug("Secret not found, deferring event")
            event.defer()
            return False
        system_identifier, error = self.workload.get_system_identifier()
        if error is not None:
            raise Exception(error)
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
        if not self.state.peer.is_unit_stopped and not (
            self.manager.is_following_promoted_cluster()
        ):
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
                        raise Exception("Failed to stop patroni service.")
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
        self.charm.create_pgdata()

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
                    "unit-promoted-cluster-counter": self.manager.get_highest_promoted_cluster_counter_value(),
                })

                if self.state.model.unit.is_leader() and self._handle_leader_database_start(event):
                    return

                self.charm.set_primary_status_message()
            elif not self.state.model.unit.is_leader():
                if not k8s:
                    with contextlib.suppress(RetryError):
                        self.patroni_manager.reload_patroni_configuration()
                raise NotReadyError()
            else:
                if k8s:
                    # If the standby leader fails to start, fix the leader annotation and defer the event.
                    self.charm.fix_leader_annotation()
                self.charm.set_unit_status(
                    WaitingStatus("Still starting the database in the standby leader")
                )
                event.defer()
        except NotReadyError:
            self.charm.set_unit_status(WaitingStatus("Waiting for the database to start"))
            logger.debug("Deferring on_async_relation_changed: database hasn't started yet.")
            event.defer()

    def _handle_leader_database_start(self, event: RelationChangedEvent) -> bool:
        """Leader-side handling after the database started; True when the event is deferred."""
        k8s = self.state.substrate == Substrates.K8S
        peers = self.state.peer_relation.units if self.state.peer_relation else []
        if not k8s:
            self.charm.update_config()
        if all(
            self.state.peer_relation.data[unit].get("unit-promoted-cluster-counter")  # type: ignore
            == self.manager.get_highest_promoted_cluster_counter_value()
            for unit in {*peers, self.state.model.unit}
        ):
            self.state.application.data.update({"cluster_initialised": "True"})
            if self.watcher is not None:
                self.watcher.enable_watcher()
        elif self.manager.is_following_promoted_cluster():
            self.charm.set_unit_status(
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
                self.charm.set_unit_status(
                    WaitingStatus("Restarting Patroni to rejoin the cluster")
                )
                logger.debug(
                    "Deferring on_async_relation_changed: restarting Patroni to rejoin the cluster."
                )
                event.defer()
                return True
            self.charm.set_unit_status(
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

    # -- Secrets

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

    def _re_emit_async_relation_changed_event(self) -> None:
        """Re-emit the async relation changed event."""
        if relation := self.manager._relation:
            relation_unit = next(
                (unit for unit in relation.units if unit.app == relation.app), None
            )
            if relation_unit is None:
                logger.debug(
                    "Skipping re-emitting relation-changed event: no related units found yet."
                )
                return
            getattr(self.charm.on, f"{relation.name.replace('-', '_')}_relation_changed").emit(
                relation,
                app=relation.app,
                unit=relation_unit,
            )
