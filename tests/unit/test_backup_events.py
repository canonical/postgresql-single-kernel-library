#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the BackupEventsHandler (s3 lifecycle and backup actions)."""

from unittest.mock import MagicMock

import pytest
from ops.pebble import ExecError
from single_kernel_postgresql.events.backup import BackupEventsHandler
from tenacity import RetryError

ACTION_EVENT_KWARGS = {}


@pytest.fixture
def backup_manager():
    return MagicMock()


@pytest.fixture
def restore_manager():
    return MagicMock()


@pytest.fixture
def handler(harness, backup_manager, restore_manager):
    return BackupEventsHandler(harness.charm, harness.charm.state, backup_manager, restore_manager)


def make_action_event(params=None):
    event = MagicMock()
    event.params = params or {}
    return event


# -- s3 credentials lifecycle --------------------------------------------------------


@pytest.mark.parametrize(
    ("proceed", "defer"),
    [(False, False), (False, True)],
)
def test_credential_changed_stops_when_checks_fail(
    handler, backup_manager, proceed, defer
):
    backup_manager._credential_changed_checks.return_value = (proceed, defer)
    event = make_action_event()
    handler._on_s3_credential_changed(event)
    backup_manager.start_stop_pgbackrest_service.assert_not_called()
    assert event.defer.called is defer


def test_credential_changed_defers_on_service_start_retry(handler, backup_manager):
    backup_manager._credential_changed_checks.return_value = (True, False)
    backup_manager.start_stop_pgbackrest_service.side_effect = RetryError(MagicMock())
    event = make_action_event()
    handler._on_s3_credential_changed(event)
    event.defer.assert_called_once()
    backup_manager.initialise_s3_repository.assert_not_called()


def test_credential_changed_initialises_on_primary(harness, handler, backup_manager):
    with harness.hooks_disabled():
        harness.set_leader()
    backup_manager._credential_changed_checks.return_value = (True, False)
    backup_manager.is_primary = True
    handler._on_s3_credential_changed(make_action_event())
    application = handler.state.application
    # writing "" to the databag removes the key
    assert not application.stanza
    assert application.s3_initialization_start
    backup_manager.initialise_s3_repository.assert_called_once()


def test_credential_changed_skips_initialization_without_done_marker(
    harness, handler, backup_manager
):
    backup_manager._credential_changed_checks.return_value = (True, False)
    backup_manager.is_primary = False
    handler._on_s3_credential_changed(make_action_event())


def test_credential_gone_clears_s3_state(handler, backup_manager):
    handler._on_s3_credential_gone(MagicMock())
    backup_manager.clear_s3_state.assert_called_once()


# -- create-backup action --------------------------------------------------------------


def test_create_backup_action_success(handler, backup_manager):
    backup_manager.create_backup.return_value = (True, "backup created")
    event = make_action_event({"type": "full"})
    handler._on_create_backup_action(event)
    backup_manager.create_backup.assert_called_once_with("full")
    event.set_results.assert_called_once_with({"backup-status": "backup created"})


def test_create_backup_action_failure(handler, backup_manager):
    backup_manager.create_backup.return_value = (False, "backup failed")
    event = make_action_event({})
    handler._on_create_backup_action(event)
    event.fail.assert_called_once_with("backup failed")
    event.set_results.assert_not_called()


# -- list-backups action ---------------------------------------------------------------


def test_list_backups_action_fails_on_exec_error(handler, backup_manager):
    """The K8s charm fails the action on a pgbackrest failure instead of erroring the hook."""
    backup_manager._is_standby_cluster = False
    backup_manager._are_backup_settings_ok.return_value = (True, "")
    backup_manager._generate_backup_list_output.side_effect = ExecError(
        service="pgbackrest", change=None, err=b"boom", stderr=b"boom"
    )
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once_with("Failed to list PostgreSQL backups with error: boom")


def test_list_backups_action_success(handler, backup_manager):
    backup_manager._is_standby_cluster = False
    backup_manager._are_backup_settings_ok.return_value = (True, "")
    backup_manager._generate_backup_list_output.return_value = "table"
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.set_results.assert_called_once_with({"backups": "table"})


def test_list_backups_action_rejects_standby_cluster(handler, backup_manager, substrate):
    if substrate != "vm":
        pytest.skip("standby cluster is a VM-only concept")
    backup_manager._is_standby_cluster = True
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once()


def test_list_backups_action_rejects_bad_settings(handler, backup_manager):
    backup_manager._is_standby_cluster = False
    backup_manager._are_backup_settings_ok.return_value = (False, "no relation")
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once_with("no relation")


def test_list_backups_action_maps_listing_error(handler, backup_manager):
    from single_kernel_postgresql.config.exceptions import ListBackupsError

    backup_manager._is_standby_cluster = False
    backup_manager._are_backup_settings_ok.return_value = (True, "")
    backup_manager._generate_backup_list_output.side_effect = ListBackupsError("boom")
    event = make_action_event()
    handler._on_list_backups_action(event)
    assert "Failed to list PostgreSQL backups" in event.fail.call_args[0][0]


# -- restore action --------------------------------------------------------------------


def test_restore_action_maps_pre_check_failure(handler, restore_manager):
    restore_manager._pre_restore_checks.return_value = (False, "cannot restore")
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("cannot restore")
    restore_manager.restore.assert_not_called()


def test_restore_action_maps_resolution_error(handler, restore_manager):
    from single_kernel_postgresql.config.exceptions import ListBackupsError

    restore_manager._pre_restore_checks.return_value = (True, "")
    restore_manager._resolve_restore_target.side_effect = ListBackupsError("boom")
    event = make_action_event({"restore-to-time": "2024-01-02 10:10:10"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Failed to retrieve backups list")


def test_restore_action_maps_invalid_target(handler, restore_manager):
    restore_manager._pre_restore_checks.return_value = (True, "")
    restore_manager._resolve_restore_target.return_value = (None, False, "Invalid backup-id: x")
    event = make_action_event({"backup-id": "x"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Invalid backup-id: x")


def test_restore_action_sets_maintenance_and_results(harness, handler, restore_manager):
    restore_manager._pre_restore_checks.return_value = (True, "")
    restore_manager._resolve_restore_target.return_value = (
        ("model.cluster", "2"),
        False,
        "",
    )
    restore_manager.restore.return_value = (True, "restore started")
    event = make_action_event({"restore-to-time": "2024-01-02 10:10:10"})
    handler._on_restore_action(event)
    restore_manager.restore.assert_called_once_with(
        ("model.cluster", "2"), None, "2024-01-02 10:10:10", False
    )
    event.set_results.assert_called_once_with({"restore-status": "restore started"})
    assert harness.charm.unit.status.name == "maintenance"


def test_restore_action_maps_restore_failure(harness, handler, restore_manager):
    restore_manager._pre_restore_checks.return_value = (True, "")
    restore_manager._resolve_restore_target.return_value = (("model.cluster", "1"), True, "")
    restore_manager.restore.return_value = (False, "Failed to stop database service")
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Failed to stop database service")
    event.set_results.assert_not_called()
