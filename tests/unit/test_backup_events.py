#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the BackupEventsHandler (s3 lifecycle and backup actions)."""

from unittest.mock import MagicMock

import pytest
from ops.pebble import ExecError
from tenacity import RetryError


@pytest.fixture
def backup_manager():
    return MagicMock()


@pytest.fixture
def restore_manager():
    return MagicMock()


@pytest.fixture
def handler(harness, backup_manager, restore_manager):
    handler = harness.charm.backup_events_handler
    handler.backup_manager = backup_manager
    handler.restore_manager = restore_manager
    return handler


def make_action_event(params=None):
    event = MagicMock()
    event.params = params or {}
    return event


# -- s3 credentials lifecycle --------------------------------------------------------


@pytest.mark.parametrize(
    ("proceed", "defer"),
    [(False, False), (False, True)],
)
def test_credential_changed_stops_when_checks_fail(handler, backup_manager, proceed, defer):
    backup_manager.credential_changed_checks.return_value = (proceed, defer)
    event = make_action_event()
    handler._on_s3_credential_changed(event)
    backup_manager.start_stop_pgbackrest_service.assert_not_called()
    assert event.defer.called is defer


def test_credential_changed_defers_on_service_start_retry(handler, backup_manager):
    backup_manager.credential_changed_checks.return_value = (True, False)
    backup_manager.start_stop_pgbackrest_service.side_effect = RetryError(MagicMock())
    event = make_action_event()
    handler._on_s3_credential_changed(event)
    event.defer.assert_called_once()
    backup_manager.initialise_s3_repository.assert_not_called()


def test_purge_runs_before_render(handler, backup_manager):
    """credential_changed_checks reconciles departed endpoints before the render."""
    from single_kernel_postgresql.managers.backup import BackupManager

    # Drive the REAL credential_changed_checks against the mock manager so the
    # internal purge/render ordering is observable.
    backup_manager.state.application.is_cluster_restoring_backup = False
    backup_manager.state.application.is_cluster_restoring_to_time = False
    backup_manager.credential_changed_checks = BackupManager.credential_changed_checks.__get__(
        backup_manager
    )
    handler._on_s3_credential_changed(make_action_event())
    names = [name for name, _args, _kwargs in backup_manager.mock_calls]
    assert "remove_departed_endpoints" in names
    assert "_render_pgbackrest_conf_file" in names
    assert names.index("remove_departed_endpoints") < names.index("_render_pgbackrest_conf_file")


def test_credential_changed_initialises_on_primary(harness, handler, backup_manager):
    with harness.hooks_disabled():
        harness.set_leader()
    backup_manager.credential_changed_checks.return_value = (True, False)
    backup_manager.is_primary = True
    handler._on_s3_credential_changed(make_action_event())
    backup_manager.reset_s3_initialization_markers.assert_called_once()
    backup_manager.initialise_s3_repository.assert_called_once()


def test_credential_changed_skips_initialization_without_done_marker(
    harness, handler, backup_manager
):
    backup_manager.credential_changed_checks.return_value = (True, False)
    backup_manager.is_primary = False
    handler._on_s3_credential_changed(make_action_event())
    backup_manager.initialise_s3_repository.assert_not_called()


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
    backup_manager.is_standby_cluster = False
    backup_manager.are_backup_settings_ok.return_value = (True, "")
    backup_manager.generate_backup_list_output.side_effect = ExecError(
        command=["pgbackrest", "info"], exit_code=1, stdout="", stderr="boom"
    )
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once()
    assert "Failed to list PostgreSQL backups with error:" in event.fail.call_args.args[0]


def test_list_backups_action_success(handler, backup_manager):
    backup_manager.is_standby_cluster = False
    backup_manager.are_backup_settings_ok.return_value = (True, "")
    backup_manager.generate_backup_list_output.return_value = "table"
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.set_results.assert_called_once_with({"backups": "table"})


def test_list_backups_action_rejects_standby_cluster(handler, backup_manager, substrate):
    if substrate != "vm":
        pytest.skip("standby cluster is a VM-only concept")
    backup_manager.is_standby_cluster = True
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once()


def test_list_backups_action_rejects_bad_settings(handler, backup_manager):
    backup_manager.is_standby_cluster = False
    backup_manager.are_backup_settings_ok.return_value = (False, "no relation")
    event = make_action_event()
    handler._on_list_backups_action(event)
    event.fail.assert_called_once_with("no relation")


def test_list_backups_action_maps_listing_error(handler, backup_manager):
    from single_kernel_postgresql.config.exceptions import ListBackupsError

    backup_manager.is_standby_cluster = False
    backup_manager.are_backup_settings_ok.return_value = (True, "")
    backup_manager.generate_backup_list_output.side_effect = ListBackupsError("boom")
    event = make_action_event()
    handler._on_list_backups_action(event)
    assert "Failed to list PostgreSQL backups" in event.fail.call_args[0][0]


# -- restore action --------------------------------------------------------------------


def test_restore_action_maps_pre_check_failure(handler, restore_manager):
    restore_manager.pre_restore_checks.return_value = (False, "cannot restore")
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("cannot restore")
    restore_manager.restore.assert_not_called()


def test_restore_action_maps_resolution_error(handler, restore_manager):
    from single_kernel_postgresql.config.exceptions import ListBackupsError

    restore_manager.pre_restore_checks.return_value = (True, "")
    restore_manager.resolve_restore_target.side_effect = ListBackupsError("boom")
    event = make_action_event({"restore-to-time": "2024-01-02 10:10:10"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Failed to retrieve backups list")


def test_restore_action_maps_invalid_target(handler, restore_manager):
    restore_manager.pre_restore_checks.return_value = (True, "")
    restore_manager.resolve_restore_target.return_value = (None, False, "Invalid backup-id: x")
    event = make_action_event({"backup-id": "x"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Invalid backup-id: x")


def test_restore_action_sets_maintenance_and_results(harness, handler, restore_manager, substrate):
    restore_manager.pre_restore_checks.return_value = (True, "")
    restore_manager.resolve_restore_target.return_value = (
        ("model.cluster", "2"),
        False,
        "",
    )
    restore_manager.restore.return_value = (True, "restore started")
    # Nothing was preserved (e.g. safety=none or an already-restoring unit):
    # the results must then carry only the restore status.
    restore_manager.last_preserved = None
    event = make_action_event({"restore-to-time": "2024-01-02 10:10:10"})
    handler._on_restore_action(event)
    safety = "none" if substrate == "k8s" else "local"
    restore_manager.restore.assert_called_once_with(
        ("model.cluster", "2"), None, "2024-01-02 10:10:10", False, safety=safety
    )
    event.set_results.assert_called_once_with({"restore-status": "restore started"})
    assert harness.charm.unit.status.name == "maintenance"


def test_restore_action_maps_restore_failure(harness, handler, restore_manager):
    restore_manager.pre_restore_checks.return_value = (True, "")
    restore_manager.resolve_restore_target.return_value = (("model.cluster", "1"), True, "")
    restore_manager.restore.return_value = (False, "Failed to stop database service")
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("Failed to stop database service")
    event.set_results.assert_not_called()


# -- safety parameter ---------------------------------------------------------------


def test_restore_action_rejects_bogus_safety(handler, restore_manager):
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z", "safety": "bogus"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with(
        "Invalid safety parameter: bogus. Valid choices: local, none"
    )
    restore_manager.restore.assert_not_called()


def test_restore_action_fails_fast_on_s3(handler, restore_manager):
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z", "safety": "s3"})
    handler._on_restore_action(event)
    event.fail.assert_called_once_with("safety=s3 is not supported yet")
    restore_manager.restore.assert_not_called()


def test_restore_action_reports_the_safety_copy(handler, restore_manager):
    from single_kernel_postgresql.workload.base import SafetyCopyInfo

    restore_manager.pre_restore_checks.return_value = (True, "")
    restore_manager.resolve_restore_target.return_value = (("model.cluster", "2"), True, "")
    restore_manager.restore.return_value = (True, "restore started")
    restore_manager.last_preserved = SafetyCopyInfo(
        copy_id="restore-20261022T1015Z",
        operation="restore",
        created="2026-10-22T10:15:00Z",
        path="/tmp/x",
        manifest={},
    )
    event = make_action_event({"backup-id": "2024-01-01T10:10:10Z", "safety": "local"})
    handler._on_restore_action(event)
    assert event.set_results.call_args[0][0]["safety-copy"] == "restore-20261022T1015Z"
