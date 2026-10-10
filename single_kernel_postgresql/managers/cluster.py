#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Cluster Manager.

Responsible for managing cluster-wide operations.
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from ops import ModelError, SecretNotFoundError
from ops.model import BlockedStatus, StatusBase
from tenacity import RetryError, Retrying, stop_after_delay, wait_fixed

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import (
    PostgreSQLCannotConnectError,
    SettingSystemPasswordError,
)
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    BACKUP_USER,
    MONITORING_PASSWORD_KEY,
    MONITORING_USER,
    PATRONI_PASSWORD_KEY,
    RAFT_PASSWORD_KEY,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
    REPLICATION_PASSWORD_KEY,
    REWIND_PASSWORD_KEY,
    SYSTEM_USERS,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import new_password
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUPS,
    ROLE_BACKUP,
    ROLE_STATS,
    PostgreSQLUpdateUserPasswordError,
)
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQL as PostgreSQLClient,
)
from single_kernel_postgresql.workload.base import BaseWorkload

if TYPE_CHECKING:
    from single_kernel_postgresql.managers.async_replication import AsyncReplicationManager
    from single_kernel_postgresql.managers.patroni import PatroniManager

logger = logging.getLogger(__name__)


class ClusterManager(BaseManager):
    """PostgreSQL Cluster Manager.

    This manager is responsible for handling cluster-wide operations.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        patroni_manager: "PatroniManager | None" = None,
        async_replication_manager: "AsyncReplicationManager | None" = None,
        update_config: Callable[[], None] | None = None,
        set_unit_status: Callable[[StatusBase], None] | None = None,
        is_standby_cluster: Callable[[], bool] | None = None,
    ):
        super().__init__(state, workload, "cluster_manager")
        # Optional collaborators for the user-lifecycle flows; the composition
        # root wires them when the charm adopts those flows (conventions: the
        # event handler coordinates, managers never call each other).
        self.patroni_manager = patroni_manager
        self.async_replication_manager = async_replication_manager
        self.update_config = update_config
        self.set_unit_status = set_unit_status
        self.is_standby_cluster = is_standby_cluster or (lambda: False)

    def install_workload(self) -> None:
        """Install the workload."""
        if self.state.substrate == Substrates.VM:
            self.workload.install_snap_package(revision=None)  # type: ignore
            self.workload.create_snap_alias("patronictl")  # type:ignore
            self.workload.create_snap_alias("psql")  # type: ignore
        else:
            logger.debug(
                "No workload installation steps defined for substrate %s", self.state.substrate
            )

    def configure_system_passwords(self) -> None:
        """Configure system user passwords.

        This is called on leader units only to create system passwords
        if not already set.
        """
        # consider configured system user passwords
        raise_error = False
        system_user_passwords = {}
        if admin_secret_id := self.state.config.system_users:
            try:
                system_user_passwords = self.state.get_secret_from_id(secret_id=admin_secret_id)
            except (ModelError, SecretNotFoundError) as e:
                # only display the error but don't return to make sure all users have passwords
                logger.error(f"Error setting internal passwords: {e}")
                raise_error = True

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
                    # use provided passwords for system-users if available
                    self.state.set_secret(APP_SCOPE, key, system_user_passwords[key])
                    logger.info(f"Using configured password for {key}")
                else:
                    # generate a password for this user if not provided
                    self.state.set_secret(APP_SCOPE, key, new_password())
                    logger.info(f"Generated new password for {key}")

        if raise_error:
            raise SettingSystemPasswordError("Failed to set system user passwords.")

    def expose_ip_and_port(self) -> None:
        """Expose the unit's IP and port to the peer relation."""
        self.state.peer.ip = self.state.unit_ip

        # Open port
        try:
            self.state.peer.unit.open_port("tcp", 5432)
        except ModelError:
            logger.exception("failed to open port")

    def can_connect_to_postgresql(
        self, postgresql_client: PostgreSQLClient, retry: bool = True
    ) -> bool:
        """Whether the local PostgreSQL instance is reachable and responding."""
        if not postgresql_client.password or not postgresql_client.current_host:
            return False

        def _check_connection():
            try:
                if not postgresql_client.get_postgresql_timezones():
                    logger.debug("Cannot connect to database (CannotConnectError)")
                    raise PostgreSQLCannotConnectError
            except Exception as e:
                logger.debug("Error occurred while checking connection: %s", e)
                raise PostgreSQLCannotConnectError from e

        try:
            if retry:
                for attempt in Retrying(stop=stop_after_delay(10), wait=wait_fixed(3)):
                    with attempt:
                        _check_connection()
            else:
                _check_connection()
        except (RetryError, PostgreSQLCannotConnectError):
            logger.debug("Cannot connect to database (RetryError)")
            return False

        return True

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        if not self.workload.workload_present:
            return [GeneralStatuses.MAINTAINENANCE_INSTALLING.value]
        if (
            not self.state.application.user_password
            or not self.state.application.replication_password
        ):
            return [GeneralStatuses.WAITING_PASSWORDS_GENERATION.value]

        return [GeneralStatuses.ACTIVE_IDLE.value]

    # -- User lifecycle (ports of the charms' _setup_users / _update_admin_password) --

    def setup_instance_users(self, postgresql_client: PostgreSQLClient) -> None:
        """Provision the predefined roles and system users on a primary cluster.

        Port of the charms' ``_setup_users``: a standby cluster is a read-only
        hot standby (DPE-10284) and skips the write DDL. Overseeing the
        relation users afterwards stays with the event handler (no
        manager-to-manager calls).
        """
        if self.is_standby_cluster():
            logger.debug("Early exit setup_instance_users: standby cluster is read-only")
            return

        postgresql_client.create_predefined_instance_roles()

        # Create the default postgres database user that is needed for some
        # applications (not charms) like Landscape Server.

        # This can be run on a replica if the machines are restarted.
        # For that case, check whether the postgres user already exists.
        users = postgresql_client.list_users()
        # Create the backup user.
        if BACKUP_USER not in users:
            postgresql_client.create_user(
                BACKUP_USER, new_password(), extra_user_roles=[ROLE_BACKUP]
            )
            postgresql_client.grant_database_privileges_to_user(
                BACKUP_USER, "postgres", ["connect"]
            )
        if MONITORING_USER not in users:
            # Create the monitoring user.
            postgresql_client.create_user(
                MONITORING_USER,
                self.state.get_secret(APP_SCOPE, MONITORING_PASSWORD_KEY),
                extra_user_roles=[ROLE_STATS],
            )

        postgresql_client.set_up_database(temp_location=str(self.workload.paths.temp))

        access_groups = postgresql_client.list_access_groups()
        if access_groups != set(ACCESS_GROUPS):
            postgresql_client.create_access_groups()
            postgresql_client.grant_internal_access_group_memberships()

    def rotate_system_user_passwords(
        self, postgresql_client: PostgreSQLClient, admin_secret_id: str
    ) -> None:
        """Rotate changed system-user passwords in the database and secret store.

        Port of the charms' ``_update_admin_password``. The cross-cluster
        primary host is resolved over the replication-offer relation so the
        password update lands on the right cluster.
        """
        if self.patroni_manager is None or self.async_replication_manager is None:
            raise PostgreSQLUpdateUserPasswordError(
                "Failed changing the password: user-rotation collaborators are not wired."
            )
        if not self.patroni_manager.are_all_members_ready():
            # Ensure all members are ready before reloading Patroni configuration to avoid
            # errors e.g. API not responding in one instance because PostgreSQL / Patroni
            # are not ready.
            raise PostgreSQLUpdateUserPasswordError(
                "Failed changing the password: Not all members healthy or finished initial sync."
            )
        other_cluster_primary_ip = self._resolve_other_cluster_primary_ip(
            self.patroni_manager, self.async_replication_manager
        )
        if other_cluster_primary_ip is None:
            return
        updated_passwords = self._collect_changed_passwords(admin_secret_id)
        if updated_passwords is None:
            return
        if not self._apply_password_updates(
            postgresql_client, updated_passwords, other_cluster_primary_ip
        ):
            return
        # Update and reload Patroni configuration in this unit to use the new password.
        # Other units' Patroni configuration is reloaded in the peer-relation-changed event.
        if self.update_config:
            self.update_config()

    def _resolve_other_cluster_primary_ip(
        self,
        patroni_manager: "PatroniManager",
        async_replication_manager: "AsyncReplicationManager",
    ) -> str | None:
        """Resolve the other cluster's primary host over the replication relations.

        Returns None (after blocking the unit) when this unit is on the
        consumer side of the replication relation; an empty string means the
        password update runs on this cluster's own primary.
        """
        replication_offer_relation = self.state.model.get_relation(REPLICATION_OFFER_RELATION)
        if (
            replication_offer_relation is not None
            and not async_replication_manager.is_primary_cluster()
        ):
            other_cluster_endpoints = async_replication_manager.get_all_primary_cluster_endpoints()
            other_cluster_primary = patroni_manager.get_primary(
                alternative_endpoints=other_cluster_endpoints
            )
            return next(
                replication_offer_relation.data[unit].get("ip")
                or replication_offer_relation.data[unit].get("private-address")
                for unit in replication_offer_relation.units
                if unit.name.replace("/", "-") == other_cluster_primary
            )
        if self.state.model.get_relation(REPLICATION_CONSUMER_RELATION) is not None:
            logger.error(
                "Failed changing the password: This can be ran only in the cluster from the "
                "offer side."
            )
            if self.set_unit_status:
                self.set_unit_status(BlockedStatus("Password update for system users failed."))
            return None
        return ""

    def _collect_changed_passwords(self, admin_secret_id: str) -> dict[str, str] | None:
        """Filter the admin secret down to the system users with changed passwords.

        Returns None (after blocking the unit) when the secret is unreadable.
        """
        try:
            updateable_users = list(SYSTEM_USERS)
            # Get the secret content and check each user configured there; only
            # SYSTEM_USERS with changed passwords are processed, all others ignored.
            updated_passwords = self.state.get_secret_from_id(admin_secret_id)
            for user, password in list(updated_passwords.items()):
                if user not in updateable_users:
                    logger.error(
                        f"Can only update system users: {', '.join(updateable_users)} not {user}"
                    )
                    updated_passwords.pop(user)
                    continue
                if password == self.state.get_secret(APP_SCOPE, f"{user}-password"):
                    updated_passwords.pop(user)
            return updated_passwords
        except (ModelError, SecretNotFoundError) as e:
            logger.error(f"Error updating internal passwords: {e}")
            if self.set_unit_status:
                self.set_unit_status(BlockedStatus("Password update for system users failed."))
            return None

    def _apply_password_updates(
        self,
        postgresql_client: PostgreSQLClient,
        updated_passwords: dict[str, str],
        other_cluster_primary_ip: str,
    ) -> bool:
        """Update the changed passwords in the database and the secret store."""
        try:
            for user, password in updated_passwords.items():
                logger.info(f"Updating password for user {user}")
                postgresql_client.update_user_password(
                    user,
                    password,
                    database_host=other_cluster_primary_ip if other_cluster_primary_ip else None,
                )
                # Update the password in the secret store after updating it in the database.
                self.state.set_secret(APP_SCOPE, f"{user}-password", password)
        except PostgreSQLUpdateUserPasswordError as e:
            logger.exception(e)
            if self.set_unit_status:
                self.set_unit_status(BlockedStatus("Password update for system users failed."))
            return False
        return True
