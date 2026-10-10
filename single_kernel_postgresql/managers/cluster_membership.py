#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cluster membership manager.

Owns the VM cluster-membership orchestration and the RAFT recovery state
machine, ported from the VM charm's peer-relation handlers.
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from ops import MaintenanceStatus, WaitingStatus
from tenacity import RetryError

from single_kernel_postgresql.config.exceptions import NotReadyError, RemoveRaftMemberFailedError
from single_kernel_postgresql.config.literals import (
    DATABASE,
    PEER_RELATION,
    PRIMARY_NOT_REACHABLE_MESSAGE,
    RAFT_PARTNER_PREFIX,
    RAFT_PORT,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
)
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import label2name

if TYPE_CHECKING:
    from ops.model import Relation

    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.managers.async_replication import AsyncReplicationManager
    from single_kernel_postgresql.managers.raft import RaftManager
    from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)

# Return protocol for flows whose defer decision stays with the event handler.
DEFER = "defer"
COMPLETED = "completed"
WAITING = "waiting"
SKIPPED = "skipped"


class ClusterMembershipManager(BaseManager):
    """Reconfigure the cluster members and recover from a lost RAFT quorum."""

    def __init__(
        self,
        state: "CharmState",
        workload: "BaseWorkload",
        patroni_manager,
        update_config: Callable[..., bool],
        set_unit_status: Callable,
        watcher_handler,
        async_replication_manager: "AsyncReplicationManager",
        update_relation_endpoints: Callable[[], None],
        raft_manager: "RaftManager",
        update_certificate: Callable[[], None] | None = None,
        set_primary_status_message: Callable[[], None] | None = None,
    ):
        super().__init__(state, workload, "cluster_membership_manager")
        self.patroni_manager = patroni_manager
        self.update_config = update_config
        self.set_unit_status = set_unit_status
        self.watcher_handler = watcher_handler
        self.async_replication_manager = async_replication_manager
        self.update_relation_endpoints = update_relation_endpoints
        self.raft_manager = raft_manager
        # Late-bound bridges (wired by the charm class when the target lands).
        self.update_certificate = update_certificate
        self.set_primary_status_message = set_primary_status_message

    # -- Hosts and IP bookkeeping ----------------------------------------------------

    def _peer_relation(self) -> "Relation | None":
        return self.state.model.get_relation(PEER_RELATION)

    def hosts(self) -> set[str]:
        """The current Juju hosts with '-' instead of '/' (port of the charm's _hosts)."""
        hosts = [self.state.model.unit.name.replace("/", "-")]
        if relation := self._peer_relation():
            hosts.extend(unit.name.replace("/", "-") for unit in relation.units)
        return set(hosts)

    def get_unit_ip(self, unit, relation_name: str = PEER_RELATION) -> str | None:
        """Get the IP address of a specific unit from its peer databag."""
        try:
            if relation := self._peer_relation():
                return relation.data[unit].get(f"{relation_name}-address")
        except KeyError:
            return None
        return None

    def get_ips_to_remove(self) -> set[str]:
        """List the IPs that were part of the cluster but departed."""
        return self.state.application.members_ips - self.units_ips()

    def units_ips(self) -> set[str]:
        """The current Juju peers' IPs (port of the charm's _units_ips)."""
        addresses = set()
        if self.state.unit_ip:
            addresses.add(self.state.unit_ip)
        for peer in self.state.application_peers:
            if ip := peer.data.get(f"{PEER_RELATION}-address"):
                addresses.add(ip)
        return addresses

    def add_member_ip(self, ip: str) -> None:
        """Add one IP to the members list (leader only, port of _add_to_members_ips)."""
        if not self.state.model.unit.is_leader():
            return
        self.state.application.add_member_ip(ip)

    def remove_member_ip(self, ip: str) -> None:
        """Remove one IP from the members list (leader only, port of _remove_from_members_ips)."""
        if not self.state.model.unit.is_leader():
            return
        self.state.application.remove_member_ip(ip)

    def updated_synchronous_node_count(self) -> bool:
        """Try to update the synchronous_node_count configuration (port)."""
        try:
            self.patroni_manager.update_synchronous_node_count()
            return True
        except RetryError:
            logger.debug("Unable to set synchronous_node_count")
            return False

    def update_endpoint_addresses(self) -> None:
        """Update ip addresses for relation endpoints on the unit peer databag (port)."""
        logger.debug("Updating relation endpoints addresses")
        updates: dict[str, str] = {}
        for key, val in (
            (f"{PEER_RELATION}-address", self.state.unit_ip),
            (f"{DATABASE}-address", self.state.database_ip),
            (f"{REPLICATION_OFFER_RELATION}-address", self.state.replication_offer_ip),
            (f"{REPLICATION_CONSUMER_RELATION}-address", self.state.replication_consumer_ip),
        ):
            if val:
                updates[key] = val
        self.state.peer.data.update(updates)
        self.watcher_handler.update_endpoints()

    def update_member_ip(self) -> bool:
        """Update the member IP in the unit databag (port).

        Returns:
            Whether the IP was updated.
        """
        self.update_endpoint_addresses()
        # Stop Patroni (and update the member IP) if it was previously isolated
        # from the cluster network. Patroni will start back when its IP address
        # is updated in all the units through the peer-relation-changed event.
        stored_ip = self.state.peer.data.get("ip")
        current_ip = self.state.unit_ip
        if stored_ip is None:
            self.state.peer.data.update({"ip": current_ip})  # ty: ignore[no-matching-overload]
            return False
        elif current_ip != stored_ip:
            logger.info(f"ip changed from {stored_ip} to {current_ip}")
            self.state.peer.data.update(  # ty: ignore[no-matching-overload]
                {"ip-to-remove": stored_ip, "ip": current_ip}
            )
            self.patroni_manager.stop_patroni()
            if self.update_certificate:
                self.update_certificate()
            # Update the watcher relation: unit address for all units, endpoints
            # only for the leader.
            self.watcher_handler.update_unit_address()
            if self.state.model.unit.is_leader():
                self.watcher_handler.update_endpoints()
            return True
        else:
            self.state.peer.data.update({"ip-to-remove": ""})
            return False

    # -- Membership flows -------------------------------------------------------------

    def departed_early_exit(self, event) -> bool:
        """Whether the departed event must be skipped (port of the early-exit check)."""
        if not event.departing_unit:
            logger.debug("Early exit on_peer_relation_departed: No departing unit")
            return True
        if event.departing_unit == self.state.model.unit:
            logger.debug("Early exit on_peer_relation_departed: Skipping departing unit")
            return True
        if self.state.has_raft_keys():
            logger.debug("Early exit on_peer_relation_departed: Raft recovery in progress")
            return True
        return False

    def remove_departing_raft_member(self, event) -> str:
        """Remove the departing member from the raft cluster.

        Returns:
            "defer" when the removal failed (the handler must defer), "skip"
            when the member IP could not be resolved, "ok" otherwise.
        """
        try:
            departing_member = event.departing_unit.name.replace("/", "-")
            if member_ip := self.patroni_manager.get_member_ip(departing_member):
                self.raft_manager.remove_raft_member(f"{member_ip}:{RAFT_PORT}")
        except RemoveRaftMemberFailedError:
            logger.debug(
                "Deferring on_peer_relation_departed: Failed to remove member from raft cluster"
            )
            return DEFER
        except RetryError:
            unit = event.departing_unit.name if event.departing_unit else None
            logger.warning(f"Early exit on_peer_relation_departed: Cannot get {unit} member IP")
            return SKIPPED
        return COMPLETED

    def remove_departed_members(self) -> str:
        """Remove departed members from the cluster, one at a time (leader side).

        Returns:
            "defer" when the reconfiguration must be retried later, "waiting"
            when the primary is not reachable (the unit status was set),
            "completed" otherwise.
        """
        # Allow the leader to update the cluster members.
        if not self.state.model.unit.is_leader():
            return SKIPPED
        if (
            not self.state.application.is_cluster_initialised
            or not self.updated_synchronous_node_count()
        ):
            logger.debug("Deferring on_peer_relation_departed: cluster not initialized")
            return DEFER
        for member_ip in self.get_ips_to_remove():
            # Check that all members are ready before removing the unit.
            if not self.patroni_manager.are_all_members_ready():
                logger.info("Deferring reconfigure: another member doing sync right now")
                return DEFER
            # Update the list of the current members.
            self.remove_member_ip(member_ip)
            self.update_config()
            if self.state.primary_endpoint:
                self.update_relation_endpoints()
            else:
                self.set_unit_status(WaitingStatus(PRIMARY_NOT_REACHABLE_MESSAGE))
                return WAITING
        return COMPLETED

    def add_cluster_member(self, member: str) -> None:
        """Add a member to the cluster if all members are ready (port).

        Raises:
            NotReadyError: if either the new member or the current members are not ready.
        """
        unit = self.state.model.get_unit(label2name(member))
        if member_ip := self.get_unit_ip(unit):
            if not self.patroni_manager.are_all_members_ready():
                logger.info("not all members are ready")
                raise NotReadyError("not all members are ready")
            # Add the member to the list that should be updated in each other member.
            self.add_member_ip(member_ip)
            # Update the Patroni configuration file.
            try:
                self.update_config()
            except RetryError:
                self.set_unit_status(MaintenanceStatus("cluster member update failed, retrying"))
        else:
            self.set_unit_status(WaitingStatus("waiting for peer IP"))

    def add_members(self, event) -> None:
        """Add new cluster members (port).

        The event is deferred by the caller when one of the current units is
        copying data from the primary, to avoid multiple units copying data at
        the same time.

        Raises:
            NotReadyError: when a member is not ready (caller defers).
            RetryError: when the cluster members could not be retrieved.
        """
        # Compare the Patroni cluster members with the Juju hosts to avoid
        # unnecessary reconfiguration.
        if self.patroni_manager.cluster_members == self.hosts() and self.state.units_ips <= (
            self.state.application.members_ips
        ):
            logger.debug("Early exit add_members: Patroni members equal Juju hosts")
            return
        logger.info("Reconfiguring cluster")
        self.set_unit_status(MaintenanceStatus("reconfiguring cluster"))
        for member in self.hosts() - self.patroni_manager.cluster_members:
            logger.debug("Adding %s to cluster", member)
            self.add_cluster_member(member)
        if missing_ips := self.state.units_ips - self.state.application.members_ips:
            for ip in missing_ips:
                logger.info("Adding new IP %s to the members list", ip)
                self.add_member_ip(ip)
            self.update_config()
        self.patroni_manager.update_synchronous_node_count()

    def reconfigure_cluster(self, event) -> bool:
        """Reconfigure the cluster by adding and removing member IPs (port).

        Returns:
            Whether it was possible to reconfigure the cluster.
        """
        # Remove departing units when the leader changes.
        if (
            self.state.application.is_cluster_initialised
            and not self.raft_manager.cleanup_raft_cluster()
        ):
            logger.debug("Deferring on_peer_relation_changed: failed to remove raft member")
            return False
        try:
            self.add_members(event)
        except Exception:
            logger.debug("Deferring on_peer_relation_changed: Unable to add members")
            return False
        return True

    # -- RAFT recovery state machine (port of the charm's stuck-raft family) ---------

    def has_raft_keys(self) -> bool:
        """Checks for the presence of raft recovery keys in peer data."""
        return self.state.has_raft_keys()

    def stuck_cluster_check(self) -> None:
        """Check for a stuck raft cluster and select a reinit candidate if safe."""
        raft_stuck = False
        all_units_stuck = True
        candidate = self.state.application.data.get("raft_selected_candidate")
        candidate_unit: str | None = None
        for peer in self.state.application_peers:
            if "raft_stuck" in peer.data:
                raft_stuck = True
            else:
                all_units_stuck = False
            if not candidate and not candidate_unit and "raft_candidate" in peer.data:
                candidate_unit = peer.unit.name
        if not raft_stuck:
            return
        if not all_units_stuck:
            logger.warning("Stuck raft not yet detected on all units")
            return
        if not candidate and not candidate_unit:
            logger.warning("Stuck raft has no candidate")
            return
        if "raft_selected_candidate" not in self.state.application.data and candidate_unit:
            logger.info(f"{candidate_unit} selected for new raft leader")
            self.state.application.data["raft_selected_candidate"] = candidate_unit

    def stuck_cluster_rejoin(self) -> None:
        """Reconnect the cluster to a new raft leader."""
        primary = None
        for peer in self.state.application_peers:
            if "raft_primary" in peer.data:
                primary = peer
                break
        if primary and "raft_reset_primary" not in self.state.application.data:
            logger.info("Updating the primary endpoint")
            self.state.application.data.pop("members_ips", None)
            for peer in self.state.application_peers:
                if ip := peer.data.get(f"{PEER_RELATION}-address"):
                    self.add_member_ip(ip)
            if self.state.unit_ip:
                self.add_member_ip(self.state.unit_ip)
            self.state.application.data["raft_reset_primary"] = "True"
            self.update_relation_endpoints()
        if (
            "raft_rejoin" not in self.state.application.data
            and "raft_followers_stopped" in self.state.application.data
            and "raft_reset_primary" in self.state.application.data
        ):
            logger.info("Notify units they can rejoin")
            self.state.application.data["raft_rejoin"] = "True"

    def stuck_cluster_stopped_check(self) -> None:
        """Check that the cluster is stopped."""
        if "raft_followers_stopped" in self.state.application.data:
            return
        for peer in self.state.application_peers:
            if "raft_stopped" not in peer.data:
                return
        logger.info("Cluster is shut down")
        self.state.application.data["raft_followers_stopped"] = "True"

    def stuck_cluster_cleanup(self) -> None:
        """Clean the raft app-data flags once every unit dropped its flags."""
        for peer in self.state.application_peers:
            if any(flag.startswith("raft_") for flag in peer.data):
                return
        logger.info("Cleaning up raft app data")
        for flag in (
            "raft_rejoin",
            "raft_reset_primary",
            "raft_selected_candidate",
            "raft_followers_stopped",
        ):
            self.state.application.data.pop(flag, None)

    def raft_reinitialisation(self) -> None:
        """Handle raft cluster loss of quorum (port of the charm's _raft_reinitialisation)."""
        app_data = self.state.application.data
        if "raft_rejoin" not in app_data:
            if self.state.model.unit.is_leader():
                self.stuck_cluster_check()
            if not self._reinit_selected_candidate():
                return
            if self.state.model.unit.is_leader():
                self.stuck_cluster_stopped_check()
            self._reinit_promote_candidate()
            if self.state.model.unit.is_leader():
                self.stuck_cluster_rejoin()
        if "raft_rejoin" in app_data:
            self._reinit_rejoin_cleanup()

    def _reinit_selected_candidate(self) -> bool:
        """Stop Patroni and drop the raft data for the selected candidate's unit.

        Returns:
            False when the RAFT watcher has not disconnected yet (the caller
            must wait for the next event).
        """
        candidate = self.state.application.data.get("raft_selected_candidate")
        if candidate and "raft_stopped" not in self.state.peer.data:
            self.state.peer.data.pop("raft_stuck", None)
            self.state.peer.data.pop("raft_candidate", None)
            self.raft_manager.remove_raft_data()
            logger.info(f"Stopping {self.state.model.unit.name}")
            self.state.peer.data["raft_stopped"] = "True"
            self.watcher_handler.disable_watcher()
            if self.watcher_handler.is_active:
                logger.info("waiting for RAFT watcher to disconnect.")
                return False
        return True

    def _reinit_promote_candidate(self) -> None:
        """Reinitialise this unit as the new raft primary when it is the candidate."""
        if (
            self.state.application.data.get("raft_selected_candidate")
            == self.state.model.unit.name
            and "raft_primary" not in self.state.peer.data
            and "raft_followers_stopped" in self.state.application.data
        ):
            self.set_unit_status(MaintenanceStatus("Reinitialising raft"))
            logger.info(f"Reinitialising {self.state.model.unit.name} as primary")
            self.raft_manager.reinitialise_raft_data()
            self.state.peer.data["raft_primary"] = "True"

    def _reinit_rejoin_cleanup(self) -> None:
        """Rejoin the cluster and clean the recovery flags on the leader."""
        logger.info("Cleaning up raft unit data")
        self.state.peer.data.pop("raft_primary", None)
        self.state.peer.data.pop("raft_stopped", None)
        self.update_config()
        self.patroni_manager.start_patroni()
        if self.set_primary_status_message:
            self.set_primary_status_message()
        if self.state.model.unit.is_leader():
            self.stuck_cluster_cleanup()

    def raft_reconnect(self, _) -> None:
        """Re-add this unit's raft member after a potential stuck connection (port)."""
        raft_status = self.raft_manager.get_raft_status()
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
            self.raft_manager.remove_raft_member(
                local_addr, remote_address=remote_addr, set_raft_flags=False
            )
        except Exception:
            logger.exception("Unable to remove Raft member")
            return
        try:
            self.raft_manager.add_raft_member(local_addr, remote_address=remote_addr)
        except Exception:
            logger.exception("Unable to add Raft member")
            return
