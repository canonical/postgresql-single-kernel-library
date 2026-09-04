#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Refresh Manager.

This module ports the refresh (rolling upgrade) logic from the PostgreSQL VM and K8s
charms' 16/edge branches. It hosts the charm-specific callbacks handed to
``charm_refresh`` and the manager that owns the refresh-aware unit status handling.
"""

import dataclasses
import json
import logging
import pathlib
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

import charm_refresh
import psycopg2
from charm_refresh import CharmVersion, PrecheckFailed
from cryptography.x509 import load_pem_x509_certificate
from cryptography.x509.oid import NameOID
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, StatusBase, WaitingStatus
from tenacity import (
    RetryError,
    Retrying,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import SwitchoverFailedError
from single_kernel_postgresql.config.literals import (
    K8S_CHARM_NAME,
    K8S_OCI_RESOURCE_NAME,
    LAST_REFRESH_UNIT_STATUS_FILE,
    UNIT_SCOPE,
    VM_CHARM_NAME,
    WORKLOAD_NAME,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.workload.base import BaseWorkload

if TYPE_CHECKING:
    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm
    from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


@dataclasses.dataclass(eq=False)
class PostgreSQLRefreshBase(charm_refresh.CharmSpecificCommon):
    """Base class for PostgreSQL refresh operations, shared by both substrates."""

    _charm: "AbstractPostgreSQLCharm"

    @classmethod
    def is_compatible(
        cls,
        *,
        old_charm_version: CharmVersion,
        new_charm_version: CharmVersion,
        old_workload_version: str,
        new_workload_version: str,
    ) -> bool:
        """Checks charm and workload version compatibility."""
        if not super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        ):
            return False

        # Check workload version compatibility
        old_major, old_minor = (int(component) for component in old_workload_version.split("."))
        new_major, new_minor = (int(component) for component in new_workload_version.split("."))
        if old_major != new_major:
            return False
        return new_minor >= old_minor


@dataclasses.dataclass(eq=False)
class PostgreSQLRefreshK8s(PostgreSQLRefreshBase, charm_refresh.CharmSpecificKubernetes):
    """Charm-specific refresh callbacks for the Kubernetes substrate."""

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None:
        """Implement pre-refresh checks after 1 unit refreshed."""
        logger.debug("Running pre-refresh checks")
        if self._charm.patroni_manager.is_creating_backup:
            raise PrecheckFailed("Backup in progress")

        # Check if all units except the highest unit (first to be refreshed) are online.
        running_members = self._charm.patroni_manager.get_running_cluster_members()

        # The highest unit number is planned_units - 1 (e.g., if 3 units, highest is unit 2).
        # Members are named like "postgresql-k8s-0", "postgresql-k8s-1", etc.
        highest_unit_number = self._charm.app.planned_units() - 1

        # Check if all units except the highest unit are online.
        for unit_number in range(self._charm.app.planned_units()):
            member_name = f"{self._charm.app.name}-{unit_number}"
            if unit_number != highest_unit_number and member_name not in running_members:
                raise PrecheckFailed(f"PostgreSQL is not running on unit {unit_number}")

        # Switch primary to last unit to refresh (lowest unit number).
        last_unit_to_refresh = f"{self._charm.app.name}/0"
        if self._charm.patroni_manager.get_primary(unit_name_pattern=True) == last_unit_to_refresh:
            logger.info(
                f"Unit {last_unit_to_refresh} was already primary during pre-refresh check"
            )
        else:
            try:
                self._charm.patroni_manager.switchover(
                    candidate=last_unit_to_refresh,
                    async_cluster=bool(self._charm.get_async_primary_cluster_endpoint()),
                )
            except SwitchoverFailedError as e:
                logger.warning(f"switchover failed with reason: {e}")
                raise PrecheckFailed("Unable to switch primary") from None
            else:
                logger.info(
                    f"Switched primary to unit {last_unit_to_refresh} during pre-refresh check"
                )

    def run_pre_refresh_checks_before_any_units_refreshed(self) -> None:
        """Implement pre-refresh checks before any unit refreshed."""
        if not self._charm.patroni_manager.are_all_members_ready():
            raise PrecheckFailed("PostgreSQL is not running on 1+ units")

        self.run_pre_refresh_checks_after_1_unit_refreshed()


@dataclasses.dataclass(eq=False)
class PostgreSQLRefreshVM(PostgreSQLRefreshBase, charm_refresh.CharmSpecificMachines):
    """Charm-specific refresh callbacks for the machines substrate."""

    def _check_temp_tablespace_objects(self) -> None:
        try:
            connection = self._charm.postgresql._connect_to_database()
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(
                "SELECT count(*) FROM pg_class WHERE reltablespace = "
                "(SELECT oid FROM pg_tablespace WHERE spcname = 'temp');"
            )
            count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            if count > 0:
                raise PrecheckFailed(
                    f"Temp tablespace has {count} active object(s). "
                    "Please ensure no sessions are using temp tables before refreshing."
                )
        except PrecheckFailed:
            raise
        except Exception:
            logger.debug("Unable to check temp tablespace objects", exc_info=True)

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None:
        """Run the temp tablespace pre-refresh check."""
        self._check_temp_tablespace_objects()

    def run_pre_refresh_checks_before_any_units_refreshed(self) -> None:
        """Implement pre-refresh checks before any unit refreshed."""
        for attempt in Retrying(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True):
            with attempt:
                if not self._charm.patroni_manager.are_all_members_ready():
                    raise PrecheckFailed("PostgreSQL is not running on 1+ units")
        if self._charm.patroni_manager.is_creating_backup:
            raise PrecheckFailed("Backup in progress")
        self._check_temp_tablespace_objects()

        # Switch primary to last unit to refresh

        if self._charm.state.peer_relation is None:
            # This should not happen since `charm_refresh.PeerRelationNotReady` should've been
            # raised, so this code would not run
            raise ValueError
        all_units = (
            unit.name for unit in (*self._charm.state.peer_relation.units, self._charm.unit)
        )

        def unit_number(unit_name: str):
            _, number = unit_name.split("/")
            return int(number)

        # Lowest unit number is last to refresh
        last_unit_to_refresh = sorted(all_units, key=unit_number)[0].replace("/", "-")
        if self._charm.patroni_manager.get_primary() == last_unit_to_refresh:
            logger.info(
                f"Unit {last_unit_to_refresh} was already primary during pre-refresh check"
            )
        else:
            try:
                self._charm.patroni_manager.switchover(
                    candidate=last_unit_to_refresh,
                    async_cluster=bool(self._charm.get_async_primary_cluster_endpoint()),
                )
                self._charm.update_relation_endpoints()
            except SwitchoverFailedError as e:
                logger.warning(f"switchover failed with reason: {e}")
                raise PrecheckFailed("Unable to switch primary") from None
            else:
                logger.info(
                    f"Switched primary to unit {last_unit_to_refresh} during pre-refresh check"
                )

    def refresh_snap(
        self, *, snap_name: str, snap_revision: str, refresh: charm_refresh.Machines
    ) -> None:
        """Refresh the PostgreSQL snap."""
        # Update the configuration.
        self._charm.set_unit_status(MaintenanceStatus("updating configuration"), refresh=refresh)
        self._charm.update_config(refresh=refresh)

        # TODO add graceful shutdown before refreshing snap?
        # TODO future improvement: if snap refresh fails (i.e. same snap revision installed) after
        # graceful shutdown, restart workload

        self._charm.set_unit_status(MaintenanceStatus("refreshing the snap"), refresh=refresh)
        cast("VMWorkload", self._charm.workload).install_snap_package(
            revision=snap_revision, refresh=refresh
        )

        self._charm.refresh_manager.post_snap_refresh(refresh)


class RefreshManager(BaseManager):
    """PostgreSQL Refresh Manager.

    Owns the ``charm_refresh`` integration and the refresh-aware unit status handling.
    Status writes route through the priority gate so refresh statuses are never
    overridden, and the collect-unit-status reconciliation keeps the cached refresh
    status in sync with the workload state.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        charm: "AbstractPostgreSQLCharm",
        set_default_status: Callable[[], None],
    ) -> None:
        super().__init__(state, workload, "refresh_manager")
        self._charm = charm
        self.set_default_status = set_default_status
        self.can_set_app_status = True
        self.refresh: charm_refresh.Machines | charm_refresh.Kubernetes | None
        if state.substrate == Substrates.VM:
            try:
                self.refresh = charm_refresh.Machines(
                    PostgreSQLRefreshVM(
                        workload_name=WORKLOAD_NAME, charm_name=VM_CHARM_NAME, _charm=charm
                    )
                )
            except (charm_refresh.UnitTearingDown, charm_refresh.PeerRelationNotReady):
                self.refresh = None
        else:
            try:
                self.refresh = charm_refresh.Kubernetes(
                    PostgreSQLRefreshK8s(
                        workload_name=WORKLOAD_NAME,
                        charm_name=K8S_CHARM_NAME,
                        oci_resource_name=K8S_OCI_RESOURCE_NAME,
                        _charm=charm,
                    )
                )
            except charm_refresh.KubernetesJujuAppNotTrusted:
                self.refresh = None
                self.can_set_app_status = False
            except charm_refresh.PeerRelationNotReady:
                self.refresh = None
            except charm_refresh.UnitTearingDown:
                self._charm.unit.status = MaintenanceStatus("Tearing down")
                sys.exit()
        self.reconcile_refresh_status()

    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: charm_refresh.Machines | charm_refresh.Kubernetes | None = None,
    ) -> None:
        """Set unit status without overriding higher priority refresh status."""
        if refresh is None:
            refresh = self.refresh
        if refresh is not None and refresh.unit_status_higher_priority:
            return
        if (
            isinstance(status, ActiveStatus)
            and refresh is not None
            and (refresh_status := refresh.unit_status_lower_priority())
        ):
            self._charm.unit.status = refresh_status
            pathlib.Path(LAST_REFRESH_UNIT_STATUS_FILE).write_text(
                json.dumps(refresh_status.message)
            )
            return
        self._charm.unit.status = status

    def reconcile_refresh_status(self, _=None) -> None:
        """Reconcile the unit status with the refresh status.

        Workaround for other unit statuses being set in a stateful way (i.e. unable to
        recompute status on every event). The charms observe this on collect-unit-status;
        do not use collect status events elsewhere - otherwise ops will prioritize
        statuses incorrectly.
        """
        if self._charm.unit.is_leader():
            self._charm.set_app_status()

        path = pathlib.Path(LAST_REFRESH_UNIT_STATUS_FILE)
        try:
            last_refresh_unit_status = json.loads(path.read_text())
        except FileNotFoundError:
            last_refresh_unit_status = None
        new_refresh_unit_status = None
        if self.refresh is not None and self.refresh.unit_status_higher_priority:
            self._charm.unit.status = self.refresh.unit_status_higher_priority
            new_refresh_unit_status = self.refresh.unit_status_higher_priority.message
        elif self._charm.unit.status.message == last_refresh_unit_status:
            if self.refresh is not None and (
                refresh_status := self.refresh.unit_status_lower_priority(
                    workload_is_running=self.state.substrate == Substrates.VM
                    or self.workload.is_patroni_running()
                )
            ):
                self._charm.unit.status = refresh_status
                new_refresh_unit_status = refresh_status.message
            else:
                # Clear refresh status from unit status
                self.set_default_status()
        elif (
            isinstance(self._charm.unit.status, ActiveStatus)
            and self.refresh is not None
            and (
                refresh_status := self.refresh.unit_status_lower_priority(
                    workload_is_running=self.state.substrate == Substrates.VM
                    or self.workload.is_patroni_running()
                )
            )
        ):
            self._charm.unit.status = refresh_status
            new_refresh_unit_status = refresh_status.message
        path.write_text(json.dumps(new_refresh_unit_status))

    # -- Substrate refresh resume and post-refresh flows

    def on_init(self) -> None:
        """Resume or prepare the refresh after the charm has been initialized."""
        refresh = self.refresh
        if refresh is None:
            return
        if self.state.substrate == Substrates.VM:
            if not refresh.next_unit_allowed_to_refresh:
                if refresh.in_progress:
                    self.post_snap_refresh(refresh)
                else:
                    self.migrate_temp_tablespace_location()
                    refresh.next_unit_allowed_to_refresh = True
        else:
            if refresh.workload_allowed_to_start and not refresh.next_unit_allowed_to_refresh:
                if refresh.in_progress:
                    self.reconcile()
                else:
                    refresh.next_unit_allowed_to_refresh = True

    def post_snap_refresh(
        self, refresh: charm_refresh.Machines | charm_refresh.Kubernetes
    ) -> None:
        """Start PostgreSQL, check if this app and unit are healthy, and allow next unit to refresh.

        Called after snap refresh.
        """
        self.check_and_update_internal_cert()

        if not self._charm.patroni_manager.start_patroni():
            self.set_unit_status(BlockedStatus("Failed to start PostgreSQL"), refresh=refresh)
            return

        self._charm.post_refresh_side_effects()

        # Wait until the database initialise.
        self.set_unit_status(WaitingStatus("waiting for database initialisation"), refresh=refresh)
        try:
            for attempt in Retrying(stop=stop_after_attempt(30), wait=wait_fixed(10)):
                with attempt:
                    # Check if the member hasn't started or hasn't joined the cluster yet.
                    if (
                        not self._charm.patroni_manager.member_started
                        or self._charm.unit.name.replace("/", "-")
                        not in self._charm.patroni_manager.cluster_members
                        or not self._charm.patroni_manager.is_replication_healthy()
                    ):
                        logger.debug(
                            "Instance not yet back in the cluster."
                            f" Retry {attempt.retry_state.attempt_number}/6"
                        )
                        raise Exception()
        except RetryError:
            logger.debug(
                "Did not allow next unit to refresh: member not ready or not joined the cluster yet"
            )
        else:
            try:
                self._charm.patroni_manager.set_max_timelines_history()
            except Exception:
                logger.warning("Unable to patch in max_timelines_history")
            peer_relation = self._charm.state.peer_relation
            all_units = sorted(
                [self._charm.unit, *(peer_relation.units if peer_relation else [])],
                key=lambda u: int(u.name.split("/")[1]),
            )
            if self._charm.unit == all_units[0]:
                for attempt in Retrying(
                    stop=stop_after_delay(180), wait=wait_fixed(5), reraise=True
                ):
                    with attempt:
                        if not self.migrate_temp_tablespace_location(required=True):
                            raise Exception("Temp tablespace migration not yet complete")
            refresh.next_unit_allowed_to_refresh = True
            self.set_unit_status(ActiveStatus(), refresh=refresh)

    def check_and_update_internal_cert(self) -> None:
        """Check if the internal cert CN matches the unit IP and regenerate if needed."""
        try:
            if (
                (raw_cert := self._charm.state.get_secret(UNIT_SCOPE, "internal-cert"))
                and (cert := load_pem_x509_certificate(raw_cert.encode()))
                and (
                    cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
                    != self._charm.state.unit_ip
                )
            ):
                self._charm.tls_manager.generate_internal_peer_cert()
                self._charm.tls_manager.push_tls_files()
                self._charm.update_config()
        except Exception:
            logger.exception("Unable to check or update internal cert")

    def migrate_temp_tablespace_location(self, *, required: bool = False) -> bool:
        """One-shot migration of the temp tablespace to the versioned directory.

        During a snap upgrade, the post-refresh hook migrates temp data from the
        old non-versioned storage root to the versioned subdirectory. This method
        updates the PostgreSQL catalog entry to match.

        During a snap downgrade (rollback), the pre-refresh hook handles both
        file migration and catalog migration (DROP/CREATE TABLESPACE) back to
        the non-versioned root. This method only handles the forward case.

        DROP TABLESPACE and CREATE TABLESPACE cannot run inside a transaction
        block, so this method avoids using the connection as a context manager
        (which would create one in psycopg2). Instead it uses plain assignments
        and explicit close(), mirroring the pattern in the single_kernel_postgresql
        set_up_database helper.

        Args:
            required: If True (used during upgrade), return False when the
                primary is unavailable so the caller can retry. If False
                (default, used during install), return True to skip gracefully
                when no cluster exists yet.
        """
        if not self._charm.primary_endpoint:
            return not required

        if self._charm.has_async_replication_relation():
            return True

        target_host = self._resolve_primary_host()
        if target_host is None:
            return False

        return self._execute_temp_tablespace_migration(target_host)

    def _resolve_primary_host(self) -> str | None:
        """Wait for Patroni to settle and return the primary host.

        After a snap refresh, Patroni may briefly report this unit as the
        primary before discovering the real cluster topology. Query the
        Patroni API directly (bypassing primary_endpoint, which can return
        stale data from the peer databag) and retry until the primary
        points to a different host or this unit truly is the primary.
        """
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                with attempt:
                    primary = self._charm.patroni_manager.get_primary()
                    if not primary:
                        raise Exception("No primary found yet")
                    target_host = self._charm.patroni_manager.get_member_ip(primary)
                    if not target_host:
                        raise Exception("Primary IP not available yet")
                    if (
                        target_host != self._charm.state.unit_ip
                        or self._charm.patroni_manager.get_primary(unit_name_pattern=True)
                        == self._charm.unit.name
                    ):
                        return target_host
                    raise Exception("Patroni not settled yet")
        except RetryError:
            logger.warning("Patroni did not settle within 60s")
            return None
        return None

    def _execute_temp_tablespace_migration(self, target_host: str) -> bool:
        """Execute the temp tablespace DDL migration on the given host."""
        temp_storage_path = str(self._charm.workload.paths.temp.parent)
        temp_data_dir = str(self._charm.workload.paths.temp)
        connection = None
        cursor = None
        try:
            connection = self._charm.postgresql._connect_to_database(database_host=target_host)
            connection.autocommit = True
            cursor = connection.cursor()

            cursor.execute(
                "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='temp';"
            )
            row = cursor.fetchone()
            if row is None:
                return True

            current_location = row[0]
            if current_location == temp_data_dir:
                return True

            if current_location != temp_storage_path:
                logger.warning(
                    "Skipping temp tablespace migration: unexpected location %s "
                    "(expected %s or %s)",
                    current_location,
                    temp_storage_path,
                    temp_data_dir,
                )
                return True

            logger.info(
                "Migrating temp tablespace location from %s to %s",
                temp_storage_path,
                temp_data_dir,
            )
            cursor.execute("DROP TABLESPACE temp;")
            cursor.execute(f"CREATE TABLESPACE temp LOCATION '{temp_data_dir}';")
            cursor.execute("GRANT CREATE ON TABLESPACE temp TO public;")
            # Flush WAL past the CREATE TABLESPACE record so replicas won't
            # need to replay it during a future rollback (the versioned
            # directory may not exist after the snap's pre-refresh hook).
            cursor.execute("CHECKPOINT;")
        except psycopg2.Error:
            logger.exception("Failed to migrate temp tablespace location")
            try:
                check_conn = self._charm.postgresql._connect_to_database(database_host=target_host)
                check_conn.autocommit = True
                check_cur = check_conn.cursor()
                check_cur.execute(
                    "SELECT count(*) FROM pg_class WHERE reltablespace = "
                    "(SELECT oid FROM pg_tablespace WHERE spcname = 'temp')"
                )
                obj_count = check_cur.fetchone()[0]
                check_cur.close()
                check_conn.close()
                if obj_count > 0:
                    logger.error(
                        "Temp tablespace has %d object(s). "
                        "Please move or drop all objects from the temp tablespace, "
                        "then run 'juju resolved postgresql/<unit-number>' to retry.",
                        obj_count,
                    )
            except Exception:
                logger.debug("Could not query temp tablespace for blocking objects")
            return False
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

        return True

    def reconcile(self) -> None:
        """Reconcile the unit state on refresh."""
        self.set_unit_status(MaintenanceStatus("starting services"))
        self._charm.ensure_pgdata_dirs_and_symlinks()
        self._charm.update_pebble_layers()

        if not self._charm.patroni_manager.member_started:
            logger.debug("Early exit reconcile: Patroni has not started yet")
            return

        if self._charm.unit.is_leader() and not self._charm.patroni_manager.primary_endpoint_ready:
            logger.debug(
                "Early exit reconcile: current unit is leader but primary endpoint is not ready yet"
            )
            return

        self.set_unit_status(WaitingStatus("waiting for database initialisation"))
        try:
            for attempt in Retrying(stop=stop_after_attempt(6), wait=wait_fixed(10)):
                with attempt:
                    if not (
                        self._charm.unit.name.replace("/", "-")
                        in self._charm.patroni_manager.cluster_members
                        and self._charm.patroni_manager.is_replication_healthy()
                    ):
                        logger.error(
                            "Instance not yet back in the cluster or not healthy."
                            f" Retry {attempt.retry_state.attempt_number}/6"
                        )
                        raise Exception
        except RetryError:
            logger.debug("Upgraded unit is not part of the cluster or not healthy")
            self.set_unit_status(
                BlockedStatus("upgrade failed. Check logs for rollback instruction")
            )
        else:
            if self.refresh is not None:
                self.refresh.next_unit_allowed_to_refresh = True
                self.set_unit_status(ActiveStatus())


__all__ = [
    "PostgreSQLRefreshBase",
    "PostgreSQLRefreshK8s",
    "PostgreSQLRefreshVM",
    "RefreshManager",
]
