#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for backup-related charm events.

The charm is the composition root: the handler receives the managers and owns
only the observers and the mapping of manager results/exceptions onto action
results, unit statuses and defers. No business logic lives here.
"""

import logging
import time
from typing import TYPE_CHECKING

from ops import Object
from ops.charm import ActionEvent
from ops.pebble import ExecError
from tenacity import RetryError

from single_kernel_postgresql.config.exceptions import ListBackupsError
from single_kernel_postgresql.managers.backup import BackupManager
from single_kernel_postgresql.utils.backup import STANDBY_CLUSTER_LIST_BACKUPS_ERROR_MESSAGE

if TYPE_CHECKING:
    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm
    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.managers.restore import RestoreManager

logger = logging.getLogger(__name__)


class BackupEventsHandler(Object):
    """Class implementing backup-related charm events handling."""

    backup_manager: BackupManager

    def __init__(
        self,
        charm: "AbstractPostgreSQLCharm",
        state: "CharmState",
        backup_manager: BackupManager,
        restore_manager: "RestoreManager",
    ) -> None:
        super().__init__(charm, key="backup_events")
        self.charm = charm
        self.state = state
        self.backup_manager = backup_manager
        self.restore_manager = restore_manager

        # s3 relation handles the config options for s3 backups
        self.framework.observe(
            self.state.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        # When the leader unit is being removed, s3_client.on.credentials_gone is performed
        # on it (and only on it). After a new leader is elected, the S3 connection must be
        # reinitialized.
        self.framework.observe(self.charm.on.leader_elected, self._on_s3_credential_changed)
        self.framework.observe(
            self.state.s3_requirer.on.credentials_gone, self._on_s3_credential_gone
        )
        self.framework.observe(self.charm.on.create_backup_action, self._on_create_backup_action)
        self.framework.observe(self.charm.on.list_backups_action, self._on_list_backups_action)
        self.framework.observe(self.charm.on.restore_action, self._on_restore_action)

    def _on_s3_credential_changed(self, event) -> None:
        """Call the stanza initialization when the credentials or the connection info change."""
        proceed, defer = self.backup_manager._credential_changed_checks()
        if not proceed:
            # The charms defer on every branch that deferred there: credentials
            # arriving too early, mid-PITR, mid-restore, or on a unit that
            # cannot initialise yet must be retried later, not dropped.
            if defer:
                event.defer()
            return

        # Start the pgBackRest service for the check_stanza to be successful. It's
        # required to run on all the units if the tls is enabled.
        try:
            self.backup_manager.start_stop_pgbackrest_service()
        except RetryError:
            logger.debug("Cannot initialise service yet.")
            event.defer()
            return

        if self.state.peer.is_app_leader:
            application = self.state.application
            application.s3_initialization_block_message = ""
            application.s3_initialization_start = time.asctime(time.gmtime())
            application.stanza = ""
            application.s3_initialization_done = ""

        if self.backup_manager.is_primary and self.state.peer.s3_initialization_done is None:
            self.backup_manager.initialise_s3_repository()

    def _on_s3_credential_gone(self, _) -> None:
        """Clear the stanza and the S3 initialization markers when credentials are gone."""
        self.backup_manager.clear_s3_state()

    def _on_create_backup_action(self, event: ActionEvent) -> None:
        """Request that pgBackRest creates a backup."""
        backup_type = event.params.get("type", "full")
        # The manager owns the creating-backup status bracket: the charms set
        # Maintenance only after validation and the metadata upload, and reset
        # Active only once the run started — never on a gate failure.
        ok, message = self.backup_manager.create_backup(backup_type)
        if not ok:
            logger.error(f"Backup failed: {message}")
            event.fail(message)
        else:
            event.set_results({"backup-status": "backup created"})

    def _on_list_backups_action(self, event: ActionEvent) -> None:
        """List the previously created backups."""
        if self.backup_manager._is_standby_cluster:
            logger.warning(STANDBY_CLUSTER_LIST_BACKUPS_ERROR_MESSAGE)
            event.fail(STANDBY_CLUSTER_LIST_BACKUPS_ERROR_MESSAGE)
            return

        are_backup_settings_ok, validation_message = self.backup_manager._are_backup_settings_ok()
        if not are_backup_settings_ok:
            logger.warning(validation_message)
            event.fail(validation_message)
            return

        try:
            formatted_list = self.backup_manager._generate_backup_list_output()
            event.set_results({"backups": formatted_list})
        except (ListBackupsError, ExecError) as e:
            # The K8s charm catches ExecError here (pgbackrest failures raise
            # instead of returning a code); the message matches both charms.
            logger.exception(e)
            event.fail(f"Failed to list PostgreSQL backups with error: {e!s}")

    def _on_restore_action(self, event: ActionEvent) -> None:
        """Request that pgBackRest restores a backup."""
        backup_id = event.params.get("backup-id")
        restore_to_time = event.params.get("restore-to-time")
        logger.info(
            f"A restore with backup-id {backup_id}"
            f"{f' to time point {restore_to_time}' if restore_to_time else ''}"
            f" has been requested on the unit"
        )

        can_restore, error_message = self.restore_manager._pre_restore_checks(
            backup_id, restore_to_time
        )
        if not can_restore:
            event.fail(error_message)
            return

        # Validate the provided backup id and restore to time.
        logger.info("Validating provided backup-id and restore-to-time")
        try:
            restore_stanza_timeline, is_backup_id_real, error_message = (
                self.restore_manager._resolve_restore_target(backup_id, restore_to_time)
            )
        except ListBackupsError as e:
            logger.exception(e)
            error_message = "Failed to retrieve backups list"
            logger.error(f"Restore failed: {error_message}")
            event.fail(error_message)
            return
        if restore_stanza_timeline is None:
            logger.error(f"Restore failed: {error_message}")
            event.fail(error_message)
            return

        self.charm.set_unit_status(MaintenanceStatus("restoring backup"))
        ok, message = self.restore_manager.restore(
            restore_stanza_timeline, backup_id, restore_to_time, is_backup_id_real
        )
        if not ok:
            logger.error(f"Restore failed: {message}")
            event.fail(message)
            return

        event.set_results({"restore-status": "restore started"})
