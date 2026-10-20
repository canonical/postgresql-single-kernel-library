#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for General PostgreSQL charm events."""

import logging
import os
import subprocess
from contextlib import suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import charm_refresh
import psycopg2

# Platform specific imports
with suppress(ImportError):
    from charmlibs import snap
from ops import (
    ActiveStatus,
    BlockedStatus,
    InstallEvent,
    JujuVersion,
    LeaderElectedEvent,
    MaintenanceStatus,
    ModelError,
    Object,
    SecretNotFoundError,
    StartEvent,
    WaitingStatus,
    WorkloadEvent,
)
from tenacity import RetryError, Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import (
    CannotConnectError,
    RaftRecoveryTimeoutError,
    SettingSystemPasswordError,
    StorageUnavailableError,
    SwitchoverFailedError,
    SwitchoverNotSyncError,
)
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    CONFIGURATION_ERROR_MESSAGE,
    EXTENSION_OBJECT_MESSAGE,
    LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS,
    MONITORING_PASSWORD_KEY,
    PATRONI_PASSWORD_KEY,
    PEER_RELATION,
    PRIMARY_NOT_REACHABLE_MESSAGE,
    RAFT_PASSWORD_KEY,
    REPLICATION_PASSWORD_KEY,
    REWIND_PASSWORD_KEY,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses, PatroniStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.managers.cluster_membership import (
    COMPLETED,
    DEFER,
    SKIPPED,
    ClusterMembershipManager,
)
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.refresh import RefreshManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils import new_password
from single_kernel_postgresql.utils.backup import (
    S3_BLOCK_MESSAGES,
    parse_backup_id,
)
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQLCreatePredefinedRolesError,
    PostgreSQLCreateUserError,
    PostgreSQLGetCurrentTimelineError,
    PostgreSQLGrantDatabasePrivilegesToUserError,
    PostgreSQLListUsersError,
    PostgreSQLUpdateUserPasswordError,
)
from single_kernel_postgresql.workload.base import BaseWorkload, PebbleLayerSpec
from single_kernel_postgresql.workload.vm import VMWorkload

if TYPE_CHECKING:
    from collections.abc import Callable

    from ops import (
        ConfigChangedEvent,
        EventBase,
        HookEvent,
        RelationDepartedEvent,
    )

    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm
    from single_kernel_postgresql.managers.async_replication import AsyncReplicationManager
    from single_kernel_postgresql.managers.backup import BackupManager
    from single_kernel_postgresql.managers.database import DatabaseManager
    from single_kernel_postgresql.managers.observer import ObserverManager
    from single_kernel_postgresql.managers.restore import RestoreManager
    from single_kernel_postgresql.utils.postgresql import PostgreSQL as PostgreSQLClient

logger = logging.getLogger(__name__)


class PostgreSQLEventsHandler(Object):
    """Class implementing PostgreSQL Charm events handling."""

    def __init__(
        self,
        charm: "AbstractPostgreSQLCharm",
        workload: BaseWorkload,
        state: CharmState,
        cluster_manager: ClusterManager,
        tls_manager: TLSManager,
        config_manager: ConfigManager,
        patroni_manager: PatroniManager,
        refresh_manager: RefreshManager,
        membership_manager: "ClusterMembershipManager",
        backup_manager: "BackupManager",
        restore_manager: "RestoreManager",
        observer_manager: "ObserverManager",
        async_replication_manager: "AsyncReplicationManager",
        database_manager: "DatabaseManager",
        postgresql: "Callable[[], PostgreSQLClient]",
    ) -> None:
        super().__init__(charm, key="postgresql_events")
        self.charm = charm
        self.workload = workload
        self.state = state
        self.cluster_manager = cluster_manager
        self.config_manager = config_manager
        self.tls_manager = tls_manager
        self.patroni_manager = patroni_manager
        self.refresh_manager = refresh_manager
        self.membership_manager = membership_manager
        self.backup_manager = backup_manager
        self.restore_manager = restore_manager
        self.observer_manager = observer_manager
        self.async_replication_manager = async_replication_manager
        self.database_manager = database_manager
        self.postgresql = postgresql

        # Charm events
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.start, self._on_start)
        self.framework.observe(self.charm.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.update_status, self._on_update_status)
        # The charm re-runs the full peer-relation flow on secret changes too, before
        # the rotation observer.
        self.framework.observe(self.charm.on.secret_changed, self._on_peer_relation_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        self.framework.observe(self.charm.on.secret_remove, self._on_secret_remove)
        self.framework.observe(self.charm.on.get_primary_action, self._on_get_primary)
        self.framework.observe(
            self.charm.on.promote_to_primary_action, self._on_promote_to_primary
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_peer_relation_changed
        )
        if self.state.substrate == Substrates.K8S:
            self.framework.observe(
                self.charm.on.postgresql_pebble_ready, self._on_postgresql_pebble_ready
            )
        if self.state.substrate == Substrates.VM:
            self.framework.observe(
                getattr(
                    self.charm.on,
                    f"{PEER_RELATION.replace('-', '_')}_relation_departed",
                ),
                self._on_peer_relation_departed,
            )
            self.framework.observe(self.charm.on.raft_reconnect, self._on_raft_reconnect)
            for storage_name in self.charm.meta.storages:
                self.framework.observe(
                    self.charm.on[storage_name].storage_detaching, self._on_storage_detaching
                )
            self.framework.observe(self.charm.on.remove, self._on_remove)

    def _on_install(self, event: InstallEvent) -> None:
        """Install prerequisites for the application."""
        logger.debug("Install start time: %s", datetime.now())
        if self.charm.substrate == Substrates.VM and isinstance(self.workload, VMWorkload):
            self._check_detached_storage(self.workload)

        self.state.add_status_if_not_present(
            GeneralStatuses.MAINTAINENANCE_INSTALLING.value,
            scope="unit",
            component=self.cluster_manager.name,
        )
        self.cluster_manager.install_workload()

        self.state.remove_status_if_present(
            GeneralStatuses.MAINTAINENANCE_INSTALLING.value,
            scope="unit",
            component=self.cluster_manager.name,
        )
        self.state.add_status_if_not_present(
            GeneralStatuses.WAITING_POSTGRESQL_START.value,
            scope="unit",
            component=self.cluster_manager.name,
        )

    def _on_start(self, event: StartEvent) -> None:
        """Event handler for start event."""
        if not self._can_start(event):
            return

        try:
            postgres_password = self.state.application.user_password
        except ModelError:
            logger.debug("_on_start: secrets not yet available")
            postgres_password = None
        # If the leader was not elected (and the needed passwords were not generated yet),
        # the cluster cannot be bootstrapped yet.
        if not postgres_password or not self.state.application.replication_password:
            logger.info("leader not elected and/or passwords not yet generated")
            event.defer()
            return

        if not self.state.application.internal_ca:
            logger.info("leader not elected and/or internal CA not yet generated")
            event.defer()
            return
        self.tls_manager.configure_internal_peer_cert()

        self.cluster_manager.expose_ip_and_port()

        self._start_primary(event)

    def _on_postgresql_pebble_ready(self, event: WorkloadEvent) -> None:
        """Event handler for PostgreSQL container on PebbleReadyEvent."""
        # Safeguard against starting while refreshing.
        if self.refresh_manager.refresh is None:
            logger.warning("Warning on_postgresql_pebble_ready: Refresh could be in progress")
        elif (
            self.refresh_manager.refresh.in_progress
            and not self.refresh_manager.refresh.workload_allowed_to_start
        ):
            logger.debug("Defer on_postgresql_pebble_ready: Refresh in progress")
            event.defer()
            return
        if self.state.endpoint in self.state.endpoints:
            # TODO: Fix pod by adding services
            pass

        # TODO: move this code to an "_update_layer" method in order to also utilize it in
        # config-changed hook.
        # Get the postgresql container so we can configure/manipulate it.
        container = event.workload
        if not container.can_connect():
            logger.debug(
                "Defer on_postgresql_pebble_ready: Waiting for container to become available"
            )
            event.defer()
            return
        # Create the PostgreSQL data directory. This is needed on cloud environments
        # where the volume is mounted with more restrictive permissions.
        # TODO: Create pgdata

        if not self.state.application.internal_ca:
            logger.info("leader not elected and/or internal CA not yet generated")
            event.defer()
            return
        if not self.state.peer.internal_cert:
            self.tls_manager.configure_internal_peer_cert()

        # Start the database service
        self.workload.update_pebble_layers(PebbleLayerSpec.from_state(self.state))

        # Assert the member is up and running before marking it as initialised.
        if not self.patroni_manager.member_started:
            logger.debug("Deferring on_pebble_ready: awaiting for member to start")
            event.defer()
            return

    def _on_leader_elected(self, event: LeaderElectedEvent) -> None:
        """Event handler for leader elected event (port of the VM charm's flow)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s leader-elected flow is ported by the K8s slice; keep the
            # previously-ported behavior until then.
            try:
                self.cluster_manager.configure_system_passwords()
            except SettingSystemPasswordError:
                self.state.add_status_if_not_present(
                    GeneralStatuses.FAILED_SETTING_PASSWORDS.value,
                    scope="unit",
                    component=self.cluster_manager.name,
                )
                event.defer()
                return

            self.tls_manager.configure_internal_peer_ca()

            # Render the Patroni configuration — required for bootstrap (the real charm
            # renders on leader-elected); route through the charm bridge for the per-user
            # pg_hba map the real charm passes.
            if self.charm.substrate == Substrates.VM:
                self.charm.update_config()
            return

        # Consider configured system user passwords.
        system_user_passwords = self._ensure_system_passwords(event)

        # The leader sets the needed passwords if they weren't set before.
        for key in (
            USER_PASSWORD_KEY,
            REPLICATION_PASSWORD_KEY,
            REWIND_PASSWORD_KEY,
            MONITORING_PASSWORD_KEY,
            RAFT_PASSWORD_KEY,
            PATRONI_PASSWORD_KEY,
        ):
            if self.state.get_secret(APP_SCOPE, key) is None:
                if key in system_user_passwords:
                    # Use provided passwords for system-users if available.
                    self.state.set_secret(APP_SCOPE, key, system_user_passwords[key])
                    logger.info(f"Using configured password for {key}")
                else:
                    # Generate a password for this user if not provided.
                    self.state.set_secret(APP_SCOPE, key, new_password())
                    logger.info(f"Generated new password for {key}")

        if self.state.has_raft_keys():
            try:
                self.membership_manager.raft_reinitialisation()
            except RaftRecoveryTimeoutError as e:
                logger.warning("Deferring on_leader_elected: %s", e)
                event.defer()
            return

        self._update_leader_cluster_members(event)

        self._update_leader_endpoints()

    def _update_leader_endpoints(self) -> None:
        """Generate the internal CA, render, and refresh the endpoints (port)."""
        if not self.state.get_secret(APP_SCOPE, "internal-ca"):
            self.tls_manager.generate_internal_peer_ca()
        self.charm.update_config()

        # Don't update connection endpoints the first time this event runs for
        # this application because there are no primary and replicas yet.
        if not self.state.application.is_cluster_initialised:
            logger.debug("Early exit on_leader_elected: Cluster not initialized")
            return

        # Only update the connection endpoints if there is a primary.
        # A cluster can have all members as replicas for some time after
        # a failed switchover, so wait until the primary is elected.
        if self.charm.primary_endpoint:
            self.membership_manager.update_relation_endpoints()
            self.async_replication_manager.update_async_replication_data()
        else:
            self.membership_manager.set_unit_status(WaitingStatus(PRIMARY_NOT_REACHABLE_MESSAGE))

    def _ensure_system_passwords(self, event) -> dict:
        """Collect the configured system-user passwords from their secret (port).

        Defers the event (and returns an empty mapping) when the secret cannot
        be read, so every user still gets a generated password.
        """
        system_user_passwords = {}
        if admin_secret_id := self.state.config.system_users:
            try:
                system_user_passwords = self.state.get_secret_from_id(admin_secret_id)
            except (ModelError, SecretNotFoundError) as e:
                # Only display the error but don't return to make sure all users
                # have passwords.
                logger.error(f"Error setting internal passwords: {e}")
                self.membership_manager.set_unit_status(
                    BlockedStatus("Password setting for system users failed.")
                )
                event.defer()
        return system_user_passwords

    def _update_leader_cluster_members(self, event) -> None:
        """Reconcile the cluster members on leader election (port)."""
        # Update the list of the current PostgreSQL hosts when a new leader is elected.
        # Add this unit to the list of cluster members (the cluster should start
        # with only this member).
        if (
            self.state.unit_ip
            and self.state.unit_ip not in self.membership_manager.state.application.members_ips
        ):
            self.membership_manager.add_member_ip(self.state.unit_ip)

        # Remove departing units when the leader changes.
        for ip in self.membership_manager.get_ips_to_remove():
            logger.info("Removing %s from the cluster", ip)
            self.membership_manager.remove_member_ip(ip)

        if not self.membership_manager.reconfigure_cluster(event):
            logger.debug("On leader elected failed to reconfigure cluster.")

    def _can_start(self, event: StartEvent) -> bool:
        """Returns whether the workload can be started on this unit."""
        if self.charm.substrate == Substrates.VM and isinstance(self.workload, VMWorkload):
            self._check_detached_storage(self.workload)

        # Safeguard against starting while refreshing.
        if self.refresh_manager.refresh is None:
            logger.warning("Warning on_start: Refresh could be in progress")
        elif self.refresh_manager.refresh.in_progress:
            # TODO: we should probably start workload if scale up while refresh in progress
            logger.debug("Defer on_start: Refresh in progress")
            event.defer()
            return False

        if self.state.has_raft_keys():
            logger.debug("Early exit on_start: Raft recovery in process")
            return False

        # Doesn't try to bootstrap the cluster if it's in a blocked state
        # caused, for example, because a failed installation of packages.
        if self.state.peer.is_blocked_status:
            logger.debug("Early exit on_start: Unit blocked")
            return False

        return True

    def _on_start(self, event: StartEvent) -> None:
        """Handle the start event (port of the VM charm's flow)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s start flow is ported by the K8s slice.
            self._start_primary(event)
            return
        if not self._can_start(event):
            return

        try:
            postgres_password = self.state.application.user_password
        except ModelError:
            logger.debug("_on_start: secrets not yet available")
            postgres_password = None
        # If the leader was not elected (and the needed passwords were not generated yet),
        # the cluster cannot be bootstrapped yet.
        if not postgres_password or not self.state.application.replication_password:
            logger.info("leader not elected and/or passwords not yet generated")
            self.membership_manager.set_unit_status(WaitingStatus("awaiting passwords generation"))
            event.defer()
            return

        if not self.state.get_secret(APP_SCOPE, "internal-ca"):
            logger.info("leader not elected and/or internal CA not yet generated")
            event.defer()
            return
        self.tls_manager.configure_internal_peer_cert()

        self.membership_manager.update_member_ip()

        self.workload.ensure_storage_layout()

        # Open port.
        try:
            self.state.model.unit.open_port("tcp", 5432)
        except ModelError:
            logger.exception("failed to open port")

        self.observer_manager.start_raft_observer()
        # Only the leader can bootstrap the cluster.
        # On replicas, only prepare for starting the instance later.
        if not self.state.model.unit.is_leader():
            self._start_replica(event)
            self._restart_services_after_reboot()
            return

        # Bootstrap the cluster in the leader unit.
        self._start_primary(event)
        self._restart_services_after_reboot()

    def _start_primary(self, event: StartEvent) -> None:
        """Bootstrap the cluster (port of the VM charm's flow)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s bootstrap flow is ported by the K8s slice.
            self.config_manager.configure_patroni_on_unit()
            if not self.patroni_manager.start_patroni():
                self.state.add_status_if_not_present(
                    PatroniStatuses.FAILLED_STARTING_PATRONI.value,
                    scope="unit",
                    component=self.patroni_manager.name,
                )
            self.state.application.data["cluster_initialised"] = "True"
            return
        # Set some information needed by Patroni to bootstrap the cluster.
        if not self.patroni_manager.bootstrap_cluster():
            self.membership_manager.set_unit_status(BlockedStatus("failed to start Patroni"))
            return

        # Assert the member is up and running before marking it as initialised.
        if not self.patroni_manager.member_started:
            logger.debug("Deferring on_start: awaiting for member to start")
            self.membership_manager.set_unit_status(WaitingStatus("awaiting for member to start"))
            event.defer()
            return

        if not self.cluster_manager.can_connect_to_postgresql(self.postgresql()):
            logger.debug("Deferring on_start: awaiting for database to start")
            self.membership_manager.set_unit_status(
                WaitingStatus("awaiting for database to start")
            )
            event.defer()
            return

        if not self.charm.primary_endpoint:
            logger.debug("Deferring on_start: awaiting start of the primary")
            self.membership_manager.set_unit_status(WaitingStatus("awaiting start of the primary"))
            event.defer()
            return

        postgresql = self.postgresql()
        if not self._setup_instance_users(postgresql, event):
            return
        self.database_manager.oversee_users(postgresql)

        # Set the flag to enable the replicas to start the Patroni service.
        self.state.application.data["cluster_initialised"] = "True"
        # Flag to know if triggers need to be removed after refresh.
        self.state.application.data["refresh_remove_trigger"] = "True"

        # Clear unit data if this unit became a replica after a failover/switchover.
        self.membership_manager.update_relation_endpoints()

        # Enable/disable PostgreSQL extensions if they were set before the cluster
        # was fully initialised.
        self.config_manager.reconcile_extensions(postgresql)

        logger.debug("Active workload time: %s", datetime.now())
        self.charm.set_primary_status_message()

    def _setup_instance_users(self, postgresql: "PostgreSQLClient", event: StartEvent) -> bool:
        """Provision the pre-defined roles and system users (port of _setup_users).

        Returns False when the failure was handled (status set or event deferred).
        """
        try:
            self.cluster_manager.setup_instance_users(postgresql)
        except PostgreSQLCreatePredefinedRolesError as e:
            logger.exception(e)
            self.membership_manager.set_unit_status(
                BlockedStatus("Failed to create pre-defined roles")
            )
            return False
        except PostgreSQLGrantDatabasePrivilegesToUserError as e:
            logger.exception(e)
            self.membership_manager.set_unit_status(
                BlockedStatus("Failed to grant database privileges to user")
            )
            return False
        except PostgreSQLCreateUserError as e:
            logger.exception(e)
            self.membership_manager.set_unit_status(
                BlockedStatus("Failed to create postgres user")
            )
            return False
        except PostgreSQLListUsersError:
            logger.warning("Deferring on_start: Unable to list users")
            event.defer()
            return False
        return True

    def _start_replica(self, event: StartEvent) -> None:
        """Configure the replica if the cluster was already initialised (port)."""
        if not self.state.application.is_cluster_initialised:
            logger.debug("Deferring on_start: awaiting for cluster to start")
            self.membership_manager.set_unit_status(WaitingStatus("awaiting for cluster to start"))
            event.defer()
            return

        # Member already started, so we can set an ActiveStatus.
        # This can happen after a reboot.
        if self.patroni_manager.member_started:
            self.membership_manager.set_unit_status(ActiveStatus())
            return

        # Configure Patroni in the replica but don't start it yet.
        self.patroni_manager.configure_patroni_on_unit()

    def _restart_services_after_reboot(self) -> None:
        """Restart the Patroni and pgBackRest after a reboot (port)."""
        if self.state.unit_ip in self.membership_manager.state.application.members_ips:
            self.patroni_manager.start_patroni()
            self.backup_manager.start_stop_pgbackrest_service()

    # -- Update status (port of the VM charm's update-status family) ------------------

    def _on_update_status(self, event) -> None:
        """Update the unit status message and users list in the database."""
        if self.state.substrate == Substrates.K8S:
            # The K8s update-status flow is ported by the K8s slice.
            return
        if not self._can_run_on_update_status():
            return

        if (
            self.state.application.data.get("restoring-backup")
            or self.state.application.data.get("restore-to-time")
        ) and not self._was_restore_successful():
            return

        if self._handle_processes_failures():
            return

        self.database_manager.oversee_users(self.postgresql())
        if self.charm.primary_endpoint:
            self.membership_manager.update_relation_endpoints()

        if not self.patroni_manager.member_started and self.patroni_manager.is_member_isolated:
            self.patroni_manager.restart_patroni()
            self.observer_manager.start_observer()
            return

        # Leader-side membership reconcile: if the Patroni cluster is missing
        # members relative to the planned unit count (e.g. a former leader
        # serving a divergent DCS view after a network cut), run the same
        # cleanup_raft_cluster/add_members flow the peer-relation-changed
        # leader path uses. Status stays untouched: the "(degraded)" suffix
        # computed by set_primary_status_message below is the user surface,
        # and the next update-status is the retry.
        if (
            self.state.model.unit.is_leader()
            and len(self.patroni_manager.get_running_cluster_members())
            < self.state.application.planned_units
        ) and not self.membership_manager.reconfigure_cluster(event):
            logger.debug(
                "on_update_status: membership reconcile did not complete; "
                "retrying on the next update-status"
            )
            return

        # Update the sync-standby endpoint in the async replication data.
        self.async_replication_manager.update_async_replication_data()

        # Clear a promoted-cluster-counter orphaned by a dead-DC teardown whose
        # relation-broken never fired (Juju CMR limitation); otherwise a
        # newly-formed async relation re-counts it and create-replication wrongly
        # reports "There is already a replication set up.".
        self.async_replication_manager.clear_stale_promotion()

        self.backup_manager.coordinate_stanza_fields()

        self.charm.set_primary_status_message()

        # Restart the topology observer if it is gone.
        self.observer_manager.start_observer()

        # Keep this unit data current for watcher AZ/IP checks.
        self.charm.watcher_handler.update_unit_address()

        if (
            self.state.model.unit.is_leader()
            and "refresh_remove_trigger" not in self.state.application.data
        ):
            self.postgresql().drop_hba_triggers()
            self.state.application.data["refresh_remove_trigger"] = "True"

    def _can_run_on_update_status(self) -> bool:
        """Gate the update-status work (port)."""
        if not self.state.application.is_cluster_initialised:
            return False
        if self.state.has_raft_keys():
            logger.debug("Early exit on_update_status: Raft recovery in progress")
            return False
        if self.refresh_manager.refresh is None:
            logger.debug("Early exit on_update_status: Refresh could be in progress")
            return False
        if self.refresh_manager.refresh.in_progress:
            logger.debug("Early exit on_update_status: Refresh in progress")
            return False
        if (
            self.state.peer.is_blocked_status
            and self.state.peer.unit.status.message not in S3_BLOCK_MESSAGES
            and self.state.peer.unit.status.message != LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS
            and self.state.peer.unit.status.message
            != self.state.application.data.get("logical-replication-validation-status-message")
            and self.state.peer.unit.status.message
            != self.state.application.data.get("logical-replication-last-block-message")
        ):
            # If the charm was failing to disable a plugin, try again (the user
            # may have removed the objects).
            if self.state.peer.unit.status.message == EXTENSION_OBJECT_MESSAGE:
                self.config_manager.reconcile_extensions(self.postgresql())
            logger.debug("on_update_status early exit: Unit is in Blocked status")
            return False
        return True

    def _was_restore_successful(self) -> bool:
        """Complete a restore: verify, clean the flags, and revalidate S3 (port)."""
        restore_manager = self.restore_manager
        backup_manager = self.backup_manager
        if self.state.application.data.get("restore-to-time") and all(
            restore_manager.is_pitr_failed()
        ):
            logger.error(
                "Restore failed: database service failed to reach point-in-time-recovery target. "
                "You can launch another restore with different parameters"
            )
            restore_manager.log_pitr_last_transaction_time()
            # Direct unit.status writes are wiped by the collect-unit-status
            # replay (which re-adds this manager's "restoring backup"
            # maintenance): persist the blocked through the status machinery.
            restore_manager.block_on_pitr_failure()
            return False

        member_name = self.state.model.unit.name.replace("/", "-")
        if "failed" in self.patroni_manager.get_member_status(member_name):
            logger.error("Restore failed: database service failed to start")
            self.membership_manager.set_unit_status(BlockedStatus("Failed to restore backup"))
            return False

        if not self.patroni_manager.member_started:
            logger.debug("Restore check early exit: Patroni has not started yet")
            return False

        postgresql = self.postgresql()
        try:
            self.cluster_manager.setup_instance_users(postgresql)
            self.database_manager.oversee_users(postgresql)
        except Exception as e:
            logger.exception(e)
            return False

        restoring_backup = self.state.application.data.get("restoring-backup")
        restore_timeline = self.state.application.data.get("restore-timeline")
        restore_to_time = self.state.application.data.get("restore-to-time")
        try:
            current_timeline = postgresql.get_current_timeline()
        except PostgreSQLGetCurrentTimelineError:
            logger.debug("Restore check early exit: can't get current wal timeline")
            return False

        self.config_manager.reconcile_extensions(postgresql)

        # Remove the restoring backup flag and the restore stanza name.
        self.state.application.data.update({
            "restoring-backup": "",
            "restore-stanza": "",
            "restore-to-time": "",
            "restore-timeline": "",
        })
        restore_manager.clear_pitr_failure()
        self.charm.update_config()
        restore_manager.restore_patroni_restart_condition()

        logger.info(
            "Restored"
            f"{f' to {restore_to_time}' if restore_to_time else ''}"
            f"{f' from timeline {restore_timeline}' if restore_timeline and not restoring_backup else ''}"
            f"{f' from backup {parse_backup_id(restoring_backup)[0]}' if restoring_backup else ''}"
            f". Currently tracking the newly created timeline {current_timeline}."
        )

        can_use_s3_repository, validation_message = backup_manager.can_use_s3_repository()
        if not can_use_s3_repository:
            self.state.application.data.update({
                "stanza": "",
                "s3-initialization-start": "",
                "s3-initialization-done": "",
                "s3-initialization-block-message": validation_message,
            })
        return True

    def _handle_processes_failures(self) -> bool:
        """Handle Patroni and PostgreSQL OS process failures (port).

        Returns:
            A bool indicating whether the charm performed any action.
        """
        # Restart the PostgreSQL process if it was frozen (in that case, the
        # Patroni process is running but the PostgreSQL process is not).
        if (
            self.state.unit_ip in self.membership_manager.state.application.members_ips
            and self.patroni_manager.member_inactive
        ):
            data_dir = self.workload.paths.data
            if not data_dir.exists():
                # The data directory is created during bootstrap. If it does not
                # exist yet, the member has not been initialised (e.g.
                # update-status firing before the start event completes), so
                # there is no frozen process to recover.
                logger.debug(
                    "Early exit handle_processes_failures: data directory does not exist yet"
                )
                return False
            data_directory_contents = os.listdir(str(data_dir))
            if len(data_directory_contents) == 1 and data_directory_contents[0] == "pg_wal":
                os.rename(
                    str(data_dir / "pg_wal"),
                    str(data_dir / f"pg_wal-{datetime.now(UTC).isoformat()}"),
                )
                logger.info("PostgreSQL data directory was not empty. Moved pg_wal")
                return True
            try:
                logger.info("restarted PostgreSQL because it was not running")
                self.patroni_manager.restart_patroni()
                self.observer_manager.start_observer()
                return True
            except RetryError:
                logger.error("failed to restart PostgreSQL after checking that it was not running")
                return False
        return False

    # -- Teardown and restart (port of the VM charm's teardown family) ----------------

    def _on_storage_detaching(self, _) -> None:
        """Stop the workload so Juju can unmount the storage on app teardown (port)."""
        # On scale-down the surviving cluster still needs this unit's Patroni to
        # remove it from raft; only stop when the whole app is going away.
        if self.state.application.planned_units > 0:
            return
        self.observer_manager.stop_observer()
        self.backup_manager.stop_log_rotation()
        try:
            # Disable too, so a mid-teardown restart of the unit can't re-enable
            # the services and re-grab the storage mounts before Juju finishes
            # unmounting.
            snap.SnapCache()[charm_refresh.snap_name()].stop(disable=True)
        except Exception:
            logger.exception("Failed to stop charmed-postgresql snap services")

    def _on_remove(self, _) -> None:
        """Remove the charmed-postgresql snap on app teardown (port)."""
        # Juju only unmounts the storages after this hook, and snapd refuses to
        # remove the snap while anything is mounted under its data directories,
        # so unmount them here. The services were already stopped on
        # storage-detaching, and unmounting leaves the storage contents untouched.
        for name, storage in self.charm.meta.storages.items():
            if not storage.location:
                continue
            # `mountpoint` also detects bind mounts (unlike `os.path.ismount`),
            # which is how Juju attaches rootfs storage.
            if subprocess.run(["/usr/bin/mountpoint", "-q", storage.location]).returncode != 0:  # noqa: S603
                continue
            try:
                subprocess.run(  # noqa: S603
                    ["/usr/bin/umount", storage.location],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(
                    "Skipping charmed-postgresql snap removal: failed to unmount %s storage: %s",
                    name,
                    e.stderr,
                )
                return
        try:
            snap.SnapCache()[charm_refresh.snap_name()].ensure(snap.SnapState.Absent)
        except (snap.SnapError, snap.SnapNotFoundError):
            # Don't fail the hook: an error here would block the removal of the unit.
            logger.exception("Failed to remove charmed-postgresql snap")

    def _restart(self, event) -> None:
        """Restart PostgreSQL (the rolling-ops callback, port)."""
        if not self.patroni_manager.are_all_members_ready():
            logger.debug("Early exit _restart: not all members ready yet")
            event.defer()
            return

        try:
            self.patroni_manager.restart_postgresql()
            self.state.peer.data["postgresql_restarted"] = "True"
        except RetryError:
            error_message = "failed to restart PostgreSQL"
            logger.exception(error_message)
            self.membership_manager.set_unit_status(BlockedStatus(error_message))
            return

        try:
            for attempt in Retrying(wait=wait_fixed(3), stop=stop_after_delay(300)):
                with attempt:
                    if not self.cluster_manager.can_connect_to_postgresql(self.postgresql()):
                        raise CannotConnectError()
        except Exception:
            logger.exception("Unable to reconnect to postgresql")

        # Start or stop the pgBackRest TLS server service when TLS certificate change.
        self.backup_manager.start_stop_pgbackrest_service()

    # -- Actions and secret glue (port of the VM charm's misc handlers) ---------------

    def _on_get_primary(self, event) -> None:
        """Get primary instance (port)."""
        try:
            primary = self.patroni_manager.get_primary(unit_name_pattern=True)
            event.set_results({"primary": primary})
        except RetryError as e:
            logger.error(f"failed to get primary with error {e}")

    def _on_promote_to_primary(self, event) -> None:
        """Promote this unit or cluster to primary (port)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s promote flow is ported by the K8s slice.
            return
        if event.params.get("scope") == "cluster":
            self.async_replication_manager.promote_to_primary(event)
        elif event.params.get("scope") == "unit":
            self._promote_primary_unit(event)
        else:
            event.fail("Scope should be either cluster or unit")

    def _promote_primary_unit(self, event) -> None:
        """Handle promote-to-primary for the unit scope (port)."""
        if event.params.get("force"):
            if self.state.has_raft_keys():
                self.state.peer.data.update({"raft_candidate": "True"})
                if self.state.model.unit.is_leader():
                    try:
                        self.membership_manager.raft_reinitialisation()
                    except RaftRecoveryTimeoutError as e:
                        logger.warning("Deferring promote-to-primary: %s", e)
                        event.defer()
                return
            event.fail("Raft is not stuck")
        else:
            if self.state.has_raft_keys():
                event.fail("Raft is stuck. Set force to reinitialise with new primary")
                return
            try:
                member_name = self.state.model.unit.name.replace("/", "-")
                self.patroni_manager.switchover(member_name)
                self.state.peer.data["timestamp"] = str(datetime.now())
                if self.state.model.unit.is_leader():
                    self.database_manager.update_endpoints(**self._client_endpoint_inputs())
                    self.async_replication_manager.update_async_replication_data()
            except SwitchoverNotSyncError:
                event.fail("Unit is not sync standby")
            except SwitchoverFailedError:
                event.fail("Switchover failed or timed out, check the logs for details")

    def _client_endpoint_inputs(self) -> dict:
        """Gather the update_endpoints inputs the manager takes caller-side (port)."""
        return {
            "online_members": (
                self.patroni_manager.online_cluster_members()
                if self.state.model.unit.is_leader()
                else None
            ),
            "client_tls_files": self.tls_manager.get_client_tls_files(),
        }

    def _on_secret_changed(self, event) -> None:
        """Rotate system-user passwords when their secret changes (port)."""
        if self.state.substrate == Substrates.K8S:
            return
        if not self.state.model.unit.is_leader() or not self.membership_manager:
            return
        if (
            admin_secret_id := self.state.config.system_users
        ) and admin_secret_id == event.secret.id:
            try:
                self.cluster_manager.rotate_system_user_passwords(
                    self.postgresql(), admin_secret_id
                )
            except PostgreSQLUpdateUserPasswordError:
                event.defer()

    def _on_secret_remove(self, event) -> None:
        """Work around the juju 3.6.11 secret-revision removal issue (port)."""
        if self.state.model.juju_version < JujuVersion("3.6.11"):
            logger.warning(
                "Skipping secret revision removal due to https://github.com/juju/juju/issues/20782"
            )
            return
        # A secret removal (entire removal, not just a revision removal) causes
        # https://github.com/juju/juju/issues/20794. This check avoids the errors
        # that would happen if we tried to remove the revision in that case (in
        # the revision removal, the label is present).
        if event.secret.label is None:
            logger.debug("Secret with no label cannot be removed")
            return
        logger.debug(f"Removing secret with label {event.secret.label} revision {event.revision}")
        event.remove_revision()

    def _check_detached_storage(self, workload: VMWorkload) -> None:
        """Wait for storage to become available.

        Workaround for lxd containers not getting storage attached on startups.
        """
        cached_status = self.charm.unit.status
        # Check the juju-managed mount (the metadata storage location) rather than the
        # workload's versioned data path, which is a child of the mount and is never a
        # mountpoint itself.
        storage_location = self.charm.meta.storages["data"].location
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(1), reraise=True):
            with attempt:
                if not workload.is_storage_attached(storage_location):
                    logger.error("Data directory not attached.")
                    self.charm.unit.status = WaitingStatus("Data directory not attached")
                    raise StorageUnavailableError()
        self.config_manager._restore_unit_status(cached_status)

    # -- VM peer-relation membership flows (ported from the VM charm) ----------------

    def _on_peer_relation_departed(self, event: "RelationDepartedEvent") -> None:
        """The leader removes the departing units from the cluster members (port)."""
        if self.membership_manager.departed_early_exit(event):
            return
        # Remove the departing member from the raft cluster.
        outcome = self.membership_manager.remove_departing_raft_member(event)
        if outcome == DEFER:
            event.defer()
            return
        if outcome == SKIPPED:
            return
        result = self.membership_manager.remove_departed_members()
        if result == DEFER:
            event.defer()
            return
        if result == COMPLETED:
            self.membership_manager.async_replication_manager.update_async_replication_data()

    def _peer_relation_changed_checks(self, event: "HookEvent") -> bool:
        """Early-exit checks for the peer-relation-changed flow (port)."""
        membership = self.membership_manager
        # Prevents the cluster from being reconfigured before it's bootstrapped in the leader.
        if not membership.state.application.is_cluster_initialised:
            logger.debug("Early exit on_peer_relation_changed: cluster not initialized")
            return False
        # Check whether raft is stuck.
        if membership.has_raft_keys():
            try:
                membership.raft_reinitialisation()
            except RaftRecoveryTimeoutError as e:
                logger.warning("Deferring on_peer_relation_changed: %s", e)
                event.defer()
            logger.debug("Early exit on_peer_relation_changed: stuck raft recovery")
            return False
        # If the unit is the leader, it can reconfigure the cluster.
        if membership.state.model.unit.is_leader() and not membership.reconfigure_cluster(event):
            event.defer()
            return False
        # Don't update this member before it's part of the members list.
        if membership.state.unit_ip not in membership.state.application.members_ips:
            logger.debug("Early exit on_peer_relation_changed: Unit not in the members list")
            return False
        return True

    def _on_peer_relation_changed(self, event: "HookEvent") -> None:
        """Reconfigure cluster members when something changes (substrate-branched)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s flow lands with the K8s slices (their ctor observes this event too).
            return
        if not self._peer_relation_changed_checks(event):
            return
        self._vm_peer_relation_changed(event)

    def _vm_peer_relation_changed(self, event: "HookEvent") -> None:
        """Reconfigure cluster members on VM (port of the charm's _on_peer_relation_changed)."""
        try:
            self.charm.update_config()
        except RetryError:
            self.membership_manager.set_unit_status(
                MaintenanceStatus("cluster member update failed, retrying")
            )
            return
        except ValueError as e:
            self.config_manager.block_on_configuration_error()
            self.membership_manager.set_unit_status(
                BlockedStatus("Configuration Error. Please check the logs")
            )
            logger.error("Invalid configuration: %s", str(e))
            return

        # Config validates: clear any persisted config-error state (self-heal).
        self.config_manager.clear_configuration_error(CONFIGURATION_ERROR_MESSAGE)

        if not self._vm_peer_relation_changed_gates(event):
            return

        # Start Patroni the first time peer data arrives in the relation, but only
        # when it is not already running: an unconditional start on every event
        # enqueues a snap start job that can land inside a deliberate stop of the
        # service, canceling the stop ("Job ... canceled.") or re-activating the
        # stopped service seconds later. The cheap snap running-probe is used here,
        # NOT member_started (which retries over the Patroni API for up to 60s).
        if not self.workload.is_patroni_running():
            self.patroni_manager.start_patroni()

        # Assert the member is up and running before marking the unit as active.
        if not self.patroni_manager.member_started:
            logger.debug("Deferring on_peer_relation_changed: awaiting for member to start")
            self.membership_manager.set_unit_status(WaitingStatus("awaiting for member to start"))
            event.defer()
            return

        # In Raft mode with a watcher, ensure this member is properly registered in
        # the DCS: a new member may be running but not registered if it was added to
        # Raft after starting.
        if (
            self.charm.watcher_handler.is_watcher_connected
            and not self.patroni_manager.is_member_registered_in_cluster()
        ):
            logger.info("Member running but not registered in Raft cluster - restarting Patroni")
            self.patroni_manager.restart_patroni()
            event.defer()
            return

        self._vm_start_stop_pgbackrest_service(event)

        if not self._vm_handle_s3_initialization():
            return

        # Update the watcher relation with fresh peer IPs when peer data changes, so
        # the pg-endpoints stay current when unit IPs change.
        if self.state.model.unit.is_leader():
            self.charm.watcher_handler.update_endpoints()
            self.charm.async_replication.update_async_replication_data()

        self._vm_update_new_unit_status()

    def _vm_peer_relation_changed_gates(self, event: "HookEvent") -> bool:
        """Post-update_config early-exit gates for the VM peer-relation flow (port).

        Returns:
            False when the flow must stop.
        """
        # Should not override a blocked status.
        if isinstance(self.state.peer.unit.status, BlockedStatus):
            logger.debug("on_peer_relation_changed early exit: Unit in blocked status")
            return False

        if (
            self.state.application.is_cluster_restoring_backup
            or self.state.application.is_cluster_restoring_to_time
        ) and not self._was_restore_successful():
            logger.debug("on_peer_relation_changed early exit: Backup restore check failed")
            return False

        return True

    def _vm_start_stop_pgbackrest_service(self, event: "HookEvent") -> None:
        """Start or stop the pgBackRest TLS server service on a TLS change (port)."""
        if not self.backup_manager.start_stop_pgbackrest_service():
            logger.debug(
                "Deferring on_peer_relation_changed: awaiting for TLS server service to start on primary"
            )
            event.defer()
            return

        self.backup_manager.coordinate_stanza_fields()

        if "exporter-started" not in self.state.peer.data:
            self.charm._setup_exporter()
        if "pgbackrest-exporter-started" not in self.state.peer.data:
            self.charm._setup_pgbackrest_exporter()

    def _vm_handle_s3_initialization(self) -> bool:
        """Handle S3 re-initialization after a leader change (port).

        Returns:
            True if processing should continue, False if we should return early.
        """
        # Only executed when the leader is reinitializing the S3 connection due to a
        # leader change.
        if (
            "s3-initialization-start" in self.state.application.data
            and "s3-initialization-done" not in self.state.peer.data
            and self.charm.is_primary
            and not self.backup_manager.initialise_s3_repository()
        ):
            return False

        # Clean up the unit initialization data after a successful sync to the leader.
        if "s3-initialization-done" in self.state.application.data and (
            not self.state.model.unit.is_leader()
        ):
            self.state.peer.data.update({
                "stanza": "",
                "s3-initialization-block-message": "",
                "s3-initialization-done": "",
                "s3-initialization-start": "",
            })

        return True

    def _vm_update_new_unit_status(self) -> None:
        """Update the status of a unit that recently joined the cluster (port)."""
        # Only update the connection endpoints if there is a primary: a cluster can
        # have all members as replicas for some time after a failed switchover, so
        # wait until the primary is elected.
        if self.charm.primary_endpoint:
            self.charm.update_relation_endpoints()
            self.charm.async_replication.handle_read_only_mode()
            # Update the watcher relation with the current cluster endpoints.
            self.charm.watcher_handler.update_endpoints()
        else:
            self.membership_manager.set_unit_status(WaitingStatus(PRIMARY_NOT_REACHABLE_MESSAGE))

    def _on_config_changed(self, event: "ConfigChangedEvent") -> None:
        """Handle configuration changes (substrate-branched)."""
        if self.state.substrate == Substrates.K8S:
            # The K8s charm ports the faithful body; the VM flow lives here.
            self.charm._on_config_changed(event)
            return
        self._vm_config_changed(event)

    def _vm_config_changed(self, event: "ConfigChangedEvent") -> None:
        """Handle configuration changes on VM (port of the charm's _on_config_changed)."""
        if not self._vm_config_changed_gates(event):
            return

        if self.membership_manager.update_member_ip():
            # Update the sync-standby endpoint in the async replication data.
            self.charm.async_replication.update_async_replication_data()
            return

        try:
            self.config_manager.validate_config_options(self.postgresql())
            # Update the config on every run.
            self.charm.update_config()
        except psycopg2.OperationalError:
            logger.debug("Defer on_config_changed: Cannot connect to database")
            event.defer()
            return
        except ValueError as e:
            # Persist through the status machinery: a directly-written blocked
            # status is wiped by the collect handler's all-active recompute on
            # this same hook, so the "Configuration Error" message never
            # becomes observable to anything reading juju status.
            self.config_manager.block_on_configuration_error()
            self.membership_manager.set_unit_status(
                BlockedStatus("Configuration Error. Please check the logs")
            )
            logger.error("Invalid configuration: %s", str(e))
            return
        # Config now validates cleanly: clear any persisted config-error
        # blocked (the collect recompute then reports active).
        self.config_manager.clear_configuration_error(CONFIGURATION_ERROR_MESSAGE)

        if self.state.peer.is_blocked_status and (
            "Configuration Error" in self.state.peer.unit.status.message
        ):
            self.membership_manager.set_unit_status(ActiveStatus())

        # Update the sync-standby endpoint in the async replication data.
        self.charm.async_replication.update_async_replication_data()

        if not self.charm.logical_replication.apply_changed_config(event):
            return

        self._vm_config_changed_leader(event)

    def _vm_config_changed_gates(self, event: "ConfigChangedEvent") -> bool:
        """Pre-work gates for the VM config-changed flow (port).

        Returns:
            False when the flow was deferred and must stop.
        """
        if not self.state.peer:
            logger.debug("Defer on_config_changed: no peer relation")
            event.defer()
            return False
        self.membership_manager.update_endpoint_addresses()

        if not self.state.application.is_cluster_initialised:
            logger.debug("Defer on_config_changed: cluster not initialised yet")
            event.defer()
            return False

        if self.refresh_manager.refresh is None:
            logger.warning("Warning _on_config_changed: Refresh could be in progress")
        elif self.refresh_manager.refresh.in_progress:
            logger.debug("Defer on_config_changed: Refresh in progress")
            event.defer()
            return False

        return True

    def _vm_config_changed_leader(self, event: "ConfigChangedEvent") -> None:
        """Leader tail of the VM config-changed flow: extensions and rotation (port)."""
        if not self.state.model.unit.is_leader():
            return

        self.config_manager.reconcile_extensions(self.postgresql())

        if admin_secret_id := self.state.config.system_users:
            try:
                self.cluster_manager.rotate_system_user_passwords(
                    self.postgresql(), admin_secret_id
                )
            except PostgreSQLUpdateUserPasswordError:
                event.defer()

    def _on_raft_reconnect(self, event: "EventBase") -> None:
        """Re-add this unit's raft member after a stuck connection (port)."""
        if self.membership_manager:
            outcome = self.membership_manager.raft_reconnect(event)
            if outcome == DEFER:
                logger.warning(
                    "on_raft_reconnect: re-adding the Raft member failed within "
                    "the retry budget; deferring"
                )
                event.defer()
