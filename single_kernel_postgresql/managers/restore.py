# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager of PostgreSQL backup restores via pgBackRest.

``RestoreManager`` holds the restore-side business logic ported from the charms
(``src/backups.py`` on both substrates plus the PITR helpers from the charm
bodies). Validation, timeline/PITR target resolution and the Patroni
reconfiguration handshake return values instead of writing statuses or deferring
events; the events layer (``events/backup.py``) maps those results onto action
results and unit statuses. Repository reads (backups, timelines, nearest
timeline) are consumed from the constructor-injected ``BackupManager``.
"""

import logging
import re
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from lightkube.core.exceptions import ApiError
from ops.pebble import ChangeError, ExecError

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    K8S_POSTGRESQL_SERVICE_NAME,
    ORIGINAL_PATRONI_ON_FAILURE_CONDITION,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
    RESTORE_REPEAT_CAUSE,
)
from single_kernel_postgresql.managers.backup import (
    IsStandbyClusterFunction,
    UpdateConfigFunction,
)
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.utils.backup import (
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    CANNOT_RESTORE_PITR,
    STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE,
    extract_error_message,
    fetch_backup_from_id,
    get_nearest_timeline,
    is_psql_timestamp,
)

if TYPE_CHECKING:
    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.managers.backup import BackupManager
    from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)

# Bootstrap-failure markers scanned in the Patroni logs during a PITR restore.
# The VM charm greps the snap service logs; the K8s charm greps the pebble logs
# of the postgresql service, with a Juju-2 fallback reading the patroni log
# files (whose format carries a different line).
PITR_VM_BOOTSTRAP_FAILURE_PATTERN = (
    r"^([0-9-:TZ]+).*patroni\.exceptions\.PatroniFatalException: Failed to bootstrap cluster$"
)
PITR_K8S_BOOTSTRAP_FAILURE_PATTERN = (
    r"^([0-9-:TZ.]+) \[postgresql] patroni\.exceptions\.PatroniFatalException:"
    r" Failed to bootstrap cluster$"
)
PITR_K8S_JUJU2_BOOTSTRAP_FAILURE_PATTERN = (
    r"^([0-9- :]+) UTC \[[0-9]+\]: INFO: removing initialize key after failed attempt"
    r" to bootstrap the cluster"
)


class RestoreManager(BaseManager):
    """In this class, we manage PostgreSQL backup restores."""

    patroni_manager: PatroniManager

    def __init__(
        self,
        state: "CharmState",
        workload: "BaseWorkload",
        patroni_manager: PatroniManager,
        update_config: UpdateConfigFunction,
        backup_manager: "BackupManager",
        is_standby_cluster: IsStandbyClusterFunction | None = None,
        update_pebble_layers: Callable[[], None] | None = None,
    ):
        """Manager of PostgreSQL backup restores.

        The bridge callables mirror ``BackupManager``: the charm wrapper
        re-rendering the Patroni configuration (``update_config``), the VM-only
        standby-cluster check (``is_standby_cluster``, None on K8s) and, for
        K8s, the pebble-layer refresh that applies the on-failure condition
        override (``update_pebble_layers``).
        """
        super().__init__(state, workload, "restore")
        self.patroni_manager = patroni_manager
        self.update_config = update_config
        self.backup_manager = backup_manager
        self._is_standby_cluster_bridge = is_standby_cluster
        self._update_pebble_layers_bridge = update_pebble_layers

    # -- Substrate-bridged predicates -------------------------------------------

    @property
    def _is_standby_cluster(self) -> bool:
        """Whether this cluster is a standby (read-only) cluster.

        The charm-side async-replication check is injected as a callable; when
        omitted (K8s) the cluster is never a standby cluster.
        """
        return bool(self._is_standby_cluster_bridge and self._is_standby_cluster_bridge())

    # -- PITR failure inspection -------------------------------------------------

    def is_pitr_failed(self) -> tuple[bool, bool]:
        """Check if Patroni service failed to bootstrap cluster during point-in-time-recovery.

        Typically, this means that database service failed to reach point-in-time-recovery target or has been
        supplied with bad PITR parameter. Also, remembers last state and can provide info is it new event, or
        it belongs to previous action. Executes only on current unit.

        Returns:
            Tuple[bool, bool]:
                - Is patroni service failed to bootstrap cluster.
                - Is it new fail, that wasn't observed previously.
        """
        patroni_exceptions = []
        count = 0
        while len(patroni_exceptions) == 0 and count < 10:
            if count > 0:
                time.sleep(3)
            if self.state.substrate == Substrates.VM:
                patroni_logs = self.patroni_manager.patroni_logs(num_lines="all")
                patroni_exceptions = re.findall(
                    PITR_VM_BOOTSTRAP_FAILURE_PATTERN,
                    patroni_logs,
                    re.MULTILINE,
                )
            else:
                patroni_logs, juju2 = self.workload.pitr_bootstrap_failure_logs()
                patroni_exceptions = re.findall(
                    PITR_K8S_JUJU2_BOOTSTRAP_FAILURE_PATTERN
                    if juju2
                    else PITR_K8S_BOOTSTRAP_FAILURE_PATTERN,
                    patroni_logs,
                    re.MULTILINE,
                )
            count += 1

        if len(patroni_exceptions) > 0:
            logger.debug("Failures to bootstrap cluster detected on Patroni service logs")
            old_pitr_fail_id = self.state.peer.data.get("last_pitr_fail_id", None)
            self.state.peer.data["last_pitr_fail_id"] = patroni_exceptions[-1]
            return True, patroni_exceptions[-1] != old_pitr_fail_id

        logger.debug("No failures detected on Patroni service logs")
        return False, False

    def log_pitr_last_transaction_time(self) -> None:
        """Log to user last completed transaction time acquired from postgresql logs."""
        postgresql_logs = self.patroni_manager.last_postgresql_logs()
        log_time = re.findall(
            r"last completed transaction was at log time (.*)$",
            postgresql_logs,
            re.MULTILINE,
        )
        if len(log_time) > 0:
            logger.info(f"Last completed transaction was at {log_time[-1]}")
        else:
            logger.error("Can't tell last completed transaction time")

    # -- Pre-restore checks --------------------------------------------------------

    def _pre_restore_checks(
        self, backup_id: str | None, restore_to_time: str | None
    ) -> tuple[bool, str]:
        """Run some checks before starting the restore.

        Returns:
            a (can_restore, message) tuple; message is the rejection reason when
            the restore cannot start, and empty when it can.
        """
        if self._is_standby_cluster:
            logger.error(f"Restore failed: {STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE}")
            return False, STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE

        are_backup_settings_ok, validation_message = self.backup_manager._are_backup_settings_ok()
        if not are_backup_settings_ok:
            logger.error(f"Restore failed: {validation_message}")
            return False, validation_message

        if not backup_id and restore_to_time is None:
            error_message = (
                "Either backup-id or restore-to-time parameters need to be provided to be able"
                " to do restore"
            )
            logger.error(f"Restore failed: {error_message}")
            return False, error_message

        # Quick check for timestamp format
        if (
            restore_to_time
            and restore_to_time != "latest"
            and not is_psql_timestamp(restore_to_time)
        ):
            error_message = "Bad restore-to-time format"
            logger.error(f"Restore failed: {error_message}")
            return False, error_message

        if self.state.substrate == Substrates.K8S and not self.workload.workload_present:
            error_message = "Workload container not ready yet!"
            logger.error(f"Restore failed: {error_message}")
            return False, error_message

        error_message = self._pre_restore_cluster_checks()
        if error_message:
            return False, error_message

        return True, ""

    def _pre_restore_cluster_checks(self) -> str:
        """Run the cluster-level guards of the pre-restore checks.

        Returns:
            the rejection reason, or an empty string when the cluster state allows
            a restore.
        """
        logger.info("Checking if cluster is in blocked state")
        if self.state.peer.is_blocked and self.state.model.unit.status.message not in [
            ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
            CANNOT_RESTORE_PITR,
        ]:
            error_message = "Cluster or unit is in a blocking state"
            logger.error(f"Restore failed: {error_message}")
            return error_message

        logger.info("Checking that the cluster does not have more than one unit")
        if self.state.application.planned_units > 1:
            error_message = (
                "Unit cannot restore backup as there are more than one unit in the cluster"
            )
            logger.error(f"Restore failed: {error_message}")
            return error_message

        logger.info("Checking that cluster does not have an active async replication relation")
        for relation in [
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION),
            self.state.model.get_relation(REPLICATION_OFFER_RELATION),
        ]:
            if not relation:
                continue
            error_message = "Unit cannot restore backup with an active async replication relation"
            logger.error(f"Restore failed: {error_message}")
            return error_message

        logger.info("Checking that this unit was already elected the leader unit")
        if not self.state.peer.is_app_leader:
            error_message = "Unit cannot restore backup as it was not elected the leader unit yet"
            logger.error(f"Restore failed: {error_message}")
            return error_message

        return ""

    # -- Restore target resolution ---------------------------------------------------

    def _resolve_restore_target(
        self, backup_id: str | None, restore_to_time: str | None
    ) -> tuple[tuple[str, str] | None, bool, str]:
        """Validate the backup id / restore-to-time and resolve the (stanza, timeline).

        Returns:
            (restore_stanza_timeline, is_backup_id_real, error_message). On failure
            restore_stanza_timeline is None and error_message is non-empty.

        Raises:
            ListBackupsError: if pgBackRest fails to enumerate backups or timelines.
        """
        backups = self.backup_manager._list_backups(show_failed=False)
        timelines = self.backup_manager._list_timelines()
        is_backup_id_real = bool(backup_id and backup_id in backups)
        is_backup_id_timeline = bool(
            backup_id and not is_backup_id_real and backup_id in timelines
        )
        if backup_id and not is_backup_id_real and not is_backup_id_timeline:
            return None, False, f"Invalid backup-id: {backup_id}"
        if is_backup_id_timeline and not restore_to_time:
            return None, False, "Cannot restore to the timeline without restore-to-time parameter"
        if is_backup_id_real:
            return backups[backup_id], True, ""
        if is_backup_id_timeline:
            return timelines[backup_id], False, ""

        backups_list = list(backups.values())
        # Faithful copy of the charms' "latest" base-backup check: the
        # ``backups_list[0]`` fallback is only evaluated when there are no timelines; an
        # empty repository (no backups and no timelines) is rejected upstream, so it
        # cannot IndexError here.
        if (
            restore_to_time == "latest"
            and timelines is not None
            and max(timelines.values() or [backups_list[0]]) not in backups_list
        ):
            return None, False, "There is no base backup created from the latest timeline"

        # Reuse the already-fetched lists rather than re-listing (the charms'
        # _get_nearest_timeline re-runs _list_backups | _list_timelines internally).
        # restore_to_time is always non-None here (the no-target case is rejected in
        # _pre_restore_checks); use `or ""` to satisfy the type checker.
        restore_stanza_timeline = get_nearest_timeline(restore_to_time or "", backups | timelines)
        if not restore_stanza_timeline:
            return (
                None,
                False,
                (f"Can't find the nearest timeline before timestamp {restore_to_time} to restore"),
            )
        logger.info(
            f"Chosen timeline {restore_stanza_timeline[1]} as nearest for the"
            f" specified timestamp {restore_to_time}"
        )
        return restore_stanza_timeline, False, ""

    def _fetch_backup_from_id(self, backup_id: str) -> str | None:
        """Fetches backup's pgbackrest label from backup id."""
        return fetch_backup_from_id(
            backup_id, self.backup_manager._list_backups(show_failed=False, parse=False).keys()
        )

    # -- Restore orchestration -------------------------------------------------------

    def restore(
        self,
        restore_stanza_timeline: tuple[str, str],
        backup_id: str | None,
        restore_to_time: str | None,
        is_backup_id_real: bool,
    ) -> tuple[bool, str]:
        """Restore a pgBackRest backup (optionally to a point in time).

        The target must already be resolved and validated with
        _pre_restore_checks and _resolve_restore_target (the events layer calls
        them to fail the action before any service disruption).

        Returns:
            (success, message) tuple; message is "restore started" on success or
            the failure reason otherwise. The restore proceeds asynchronously as
            Patroni bootstraps the restored cluster.
        """
        logger.info("Stopping database service")
        error_message = self._stop_database()
        if error_message:
            logger.error(f"Restore failed: {error_message}")
            return False, error_message

        # Temporarily disabling patroni service auto-restart. This is required as
        # point-in-time-recovery can fail on restore, therefore during cluster
        # bootstrapping process. In this case, we need be able to check patroni
        # service status and logs. Disabling auto-restart feature is essential to
        # prevent wrong status indicated and logs reading race condition (as logs
        # cleared / moved with service restarts).
        if not self._override_patroni_restart_condition(RESTORE_REPEAT_CAUSE):
            # The K8s charm words this after the Pebble on-failure condition it
            # overrides; the VM charm after the systemd restart condition.
            error_message = (
                "Failed to override Patroni on-failure condition"
                if self.state.substrate == Substrates.K8S
                else "Failed to override Patroni restart condition"
            )
            logger.error(f"Restore failed: {error_message}")
            self._restart_database()
            return False, error_message

        error_message = self._remove_cluster_info_before_wiping()
        if error_message:
            logger.error(f"Restore failed: {error_message}")
            self._restart_database()
            return False, error_message

        error_message = self._empty_data_files()
        if error_message:
            logger.error(f"Restore failed: {error_message}")
            self._restart_database()
            return False, error_message

        if self.state.substrate == Substrates.K8S:
            logger.info("Creating PostgreSQL data directory")
            self.workload.init_storage()

        # Mark the cluster as in a restoring backup state and update the Patroni
        # configuration.
        logger.info("Configuring Patroni to restore the backup")
        application = self.state.application
        application.restoring_backup = (
            (self._fetch_backup_from_id(backup_id or "") or "") if is_backup_id_real else ""
        )
        application.restore_stanza = restore_stanza_timeline[0]
        application.restore_timeline = restore_stanza_timeline[1] if restore_to_time else ""
        application.restore_to_time = restore_to_time or ""
        application.s3_initialization_block_message = ""
        self.update_config()

        # Start the database to start the restore process.
        logger.info("Starting the database to start the restore process")
        self._start_database()

        error_message = self._remove_cluster_info_after_start()
        if error_message:
            logger.error(f"Restore failed: {error_message}")
            return False, error_message

        return True, "restore started"

    def _remove_cluster_info_before_wiping(self) -> str | None:
        """Remove previous cluster information before emptying the data directory (K8s).

        The K8s endpoints that track the cluster information, including its id, are
        deleted through the workload seam: this is the same as "patronictl remove
        patroni-<name>", but the latter doesn't work after the database service is
        stopped on Pebble.
        """
        if self.state.substrate != Substrates.K8S:
            return None
        logger.info("Removing previous cluster information")
        try:
            self.workload.remove_cluster_info(self.state.cluster_name, self.state.model_name)
        except ApiError as e:
            # If previous PITR restore was unsuccessful, there are no such endpoints.
            if not self.state.application.is_cluster_restoring_to_time:
                return f"Failed to remove previous cluster information with error: {e!s}"
        return None

    def _empty_data_files(self) -> str | None:
        """Remove the contents of the data directory.

        Returns:
            an error message on failure, None on success.
        """
        try:
            if not self.workload.empty_data_files():
                return "Failed to remove contents of the data directory"
        except ExecError as e:
            return f"Failed to remove contents of the data directory with error: {e!s}"
        return None

    def _remove_cluster_info_after_start(self) -> str | None:
        """Remove previous cluster information after the start (VM).

        The VM charm runs patronictl while the cluster is up, so this happens after
        the start (the K8s charm removes the patroni Endpoints before emptying the
        data directories, see _remove_cluster_info_before_wiping).

        Returns:
            an error message on failure, None on success.
        """
        if self.state.substrate != Substrates.VM:
            return None
        logger.info("Removing previous cluster information")
        result = self.workload.remove_cluster_info(self.state.cluster_name)
        if not result.ok:
            extracted_error = extract_error_message(
                result.stderr, str(self.workload.backup_config.logs_path)
            )
            return f"Failed to remove previous cluster information with error: {extracted_error}"
        return None

    # -- Service control ------------------------------------------------------------

    def _stop_database(self) -> str | None:
        """Stop the database service before performing the restore.

        VM stops the patroni snap service; K8s stops the postgresql pebble service
        by name.

        Returns:
            an error message on failure, None on success.
        """
        if self.state.substrate == Substrates.VM:
            if not self.patroni_manager.stop_patroni():
                return "Failed to stop database service"
            return None
        try:
            self.workload.stop_service(K8S_POSTGRESQL_SERVICE_NAME)
        except ChangeError as e:
            return f"Failed to stop database service with error: {e!s}"
        return None

    def _start_database(self) -> None:
        """Start the database to begin the restore process."""
        if self.state.substrate == Substrates.VM:
            self.patroni_manager.start_patroni()
            return
        self.workload.start_service(K8S_POSTGRESQL_SERVICE_NAME)

    def _restart_database(self) -> None:
        """Remove the restoring backup flag and restart the database (recovery path)."""
        self.state.application.restoring_backup = ""
        self.state.application.restore_to_time = ""
        self.update_config()
        self._start_database()

    # -- Patroni restart-condition override ---------------------------------------------

    def _override_patroni_restart_condition(self, repeat_cause: str | None) -> bool:
        """Temporarily disable Patroni auto-restart for the restore.

        VM overrides the systemd Restart= condition to "no" through PatroniManager;
        K8s overrides the postgresql pebble service on-failure action to "ignore"
        and refreshes the pebble layer through the injected bridge.
        """
        if self.state.substrate == Substrates.VM:
            return self._override_patroni_systemd_restart_condition("no", repeat_cause)
        return self._override_patroni_on_failure_condition("ignore", repeat_cause)

    def _override_patroni_systemd_restart_condition(
        self, new_condition: str, repeat_cause: str | None
    ) -> bool:
        """Temporary override Patroni systemd service restart condition (VM)."""
        current_condition = self.patroni_manager.get_patroni_restart_condition()
        if "overridden-patroni-restart-condition" in self.state.peer.data:
            original_condition = self.state.peer.data["overridden-patroni-restart-condition"]
            if repeat_cause is None:
                logger.error(
                    f"failure trying to override patroni restart condition to {new_condition}"
                    f"as it already overridden from {original_condition} to {current_condition}"
                )
                return False
            previous_repeat_cause = self.state.peer.data.get(
                "overridden-patroni-restart-condition-repeat-cause", None
            )
            if previous_repeat_cause != repeat_cause:
                logger.error(
                    f"failure trying to override patroni restart condition to {new_condition}"
                    f"as it already overridden from {original_condition} to {current_condition}"
                    f"and repeat cause is not equal: {previous_repeat_cause} != {repeat_cause}"
                )
                return False
            # There repeat cause is equal
            self.patroni_manager.update_patroni_restart_condition(new_condition)
            logger.debug(
                f"Patroni restart condition re-overridden to {new_condition} within repeat"
                f" cause {repeat_cause}"
                f"(original restart condition reference is untouched and is {original_condition})"
            )
            return True
        self.patroni_manager.update_patroni_restart_condition(new_condition)
        self.state.peer.data["overridden-patroni-restart-condition"] = current_condition
        if repeat_cause is not None:
            self.state.peer.data["overridden-patroni-restart-condition-repeat-cause"] = (
                repeat_cause
            )
        logger.debug(
            f"Patroni restart condition overridden from {current_condition} to {new_condition}"
            f"{' with repeat cause ' + repeat_cause if repeat_cause is not None else ''}"
        )
        return True

    def _override_patroni_on_failure_condition(
        self, new_condition: str, repeat_cause: str | None
    ) -> bool:
        """Temporary override Patroni pebble service on-failure condition (K8s)."""
        if "patroni-on-failure-condition-override" in self.state.peer.data:
            current_condition = self.state.peer.data["patroni-on-failure-condition-override"]
            if repeat_cause is None:
                logger.error(
                    f"failure trying to override patroni on-failure condition to {new_condition}"
                    f"as it already overridden from {ORIGINAL_PATRONI_ON_FAILURE_CONDITION}"
                    f" to {current_condition}"
                )
                return False
            previous_repeat_cause = self.state.peer.data.get(
                "overridden-patroni-on-failure-condition-repeat-cause", None
            )
            if previous_repeat_cause != repeat_cause:
                logger.error(
                    f"failure trying to override patroni on-failure condition to {new_condition}"
                    f"as it already overridden from {ORIGINAL_PATRONI_ON_FAILURE_CONDITION}"
                    f" to {current_condition}"
                    f"and repeat cause is not equal: {previous_repeat_cause} != {repeat_cause}"
                )
                return False
            self.state.peer.data["patroni-on-failure-condition-override"] = new_condition
            self._update_pebble_layers()
            logger.debug(
                f"Patroni on-failure condition re-overridden to {new_condition} within repeat"
                f" cause {repeat_cause}"
                f"(original on-failure condition reference is untouched and is"
                f" {ORIGINAL_PATRONI_ON_FAILURE_CONDITION})"
            )
            return True

        self.state.peer.data["patroni-on-failure-condition-override"] = new_condition
        if repeat_cause:
            self.state.peer.data["overridden-patroni-on-failure-condition-repeat-cause"] = (
                repeat_cause
            )
        self._update_pebble_layers()
        logger.debug(
            f"Patroni on-failure condition overridden from"
            f" {ORIGINAL_PATRONI_ON_FAILURE_CONDITION} to {new_condition}"
            f"{' with repeat cause ' + repeat_cause if repeat_cause is not None else ''}"
        )
        return True

    def _update_pebble_layers(self) -> bool:
        """Refresh the pebble layers through the injected K8s bridge."""
        if self._update_pebble_layers_bridge is None:
            logger.error("the pebble-layer refresh bridge is not injected")
            return False
        self._update_pebble_layers_bridge()
        return True

    def restore_patroni_restart_condition(self) -> None:
        """Restore the Patroni service restart/on-failure condition that was before overriding.

        Will do nothing if not overridden. Executes only on current unit.
        """
        if self.state.substrate == Substrates.VM:
            if "overridden-patroni-restart-condition" in self.state.peer.data:
                original_condition = self.state.peer.data["overridden-patroni-restart-condition"]
                self.patroni_manager.update_patroni_restart_condition(original_condition)
                self.state.peer.data.update({
                    "overridden-patroni-restart-condition": "",
                    "overridden-patroni-restart-condition-repeat-cause": "",
                })
                logger.debug(f"restored Patroni restart condition to {original_condition}")
            else:
                logger.warning("not restoring patroni restart condition as it's not overridden")
            return
        if "patroni-on-failure-condition-override" in self.state.peer.data:
            self.state.peer.data.update({
                "patroni-on-failure-condition-override": "",
                "overridden-patroni-on-failure-condition-repeat-cause": "",
            })
            self._update_pebble_layers()
            logger.debug(
                f"restored Patroni on-failure condition to {ORIGINAL_PATRONI_ON_FAILURE_CONDITION}"
            )
        else:
            logger.warning("not restoring patroni on-failure condition as it's not overridden")
