#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the RestoreManager (restore checks, PITR helpers and orchestration)."""

import logging
from functools import partial
from unittest.mock import MagicMock

import pytest
from lightkube.core.exceptions import ApiError
from lightkube.models.meta_v1 import Status
from ops import BlockedStatus
from single_kernel_postgresql.config.literals import K8S_POSTGRESQL_SERVICE_NAME
from single_kernel_postgresql.core.peer_relation import PostgreSQLApplication
from single_kernel_postgresql.managers.restore import RestoreManager
from single_kernel_postgresql.utils.backup import (
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    CANNOT_RESTORE_PITR,
    STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE,
)
from single_kernel_postgresql.workload.base import CommandResult
from single_kernel_postgresql.workload.vm import VMWorkload

BACKUP_ID = "2024-01-01T10:10:10Z"
BACKUP_LABEL = "20240101-101010F"
TIMELINE_ID = "2024-01-02T10:10:10Z"


@pytest.fixture
def workload(substrate):
    """A mock workload for the restore seams."""
    workload = MagicMock()
    workload.empty_data_files.return_value = True
    workload.remove_cluster_info.return_value = CommandResult(return_code=0)
    if substrate == "k8s":
        workload.workload_present = True
    return workload


@pytest.fixture
def backup_manager():
    """A mocked BackupManager: the RestoreManager's read-only repository dependency."""
    manager = MagicMock()

    def list_backups(show_failed, parse=True):
        key = BACKUP_LABEL if not parse else BACKUP_ID
        return {key: ("model.cluster", "1")}

    manager._list_backups.side_effect = list_backups
    manager._list_timelines.return_value = {TIMELINE_ID: ("model.cluster", "2")}
    manager._are_backup_settings_ok.return_value = (True, "")
    return manager


@pytest.fixture
def restore_manager(harness, substrate, workload, backup_manager, monkeypatch):
    """A RestoreManager wired to the harness state and mocked collaborators."""
    monkeypatch.setattr("single_kernel_postgresql.managers.restore.time.sleep", lambda _: None)
    manager = RestoreManager(
        state=harness.charm.state,
        workload=workload,
        patroni_manager=MagicMock(),
        update_config=MagicMock(return_value=True),
        backup_manager=backup_manager,
        is_standby_cluster=None if substrate == "k8s" else MagicMock(return_value=False),
        update_pebble_layers=MagicMock() if substrate == "k8s" else None,
    )
    manager.patroni_manager.get_patroni_restart_condition.return_value = "always"
    # The conftest harness counts the remote peer unit, so the app reads as having
    # two planned units; the restore pre-checks require a single-unit cluster.
    monkeypatch.setattr(PostgreSQLApplication, "planned_units", property(lambda self: 1))
    with harness.hooks_disabled():
        harness.set_leader()
    return manager


# -- _pre_restore_checks -----------------------------------------------------------


def test_pre_restore_checks_rejects_standby_cluster(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("standby cluster is a VM-only concept")
    restore_manager._is_standby_cluster_bridge.return_value = True
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE


def test_pre_restore_checks_rejects_missing_target(restore_manager):
    ok, message = restore_manager._pre_restore_checks(None, None)
    assert not ok
    assert "Either backup-id or restore-to-time" in message


def test_pre_restore_checks_rejects_bad_timestamp(restore_manager):
    ok, message = restore_manager._pre_restore_checks(None, "yesterday")
    assert not ok
    assert message == "Bad restore-to-time format"


def test_pre_restore_checks_rejects_container_not_ready(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("container readiness is a K8s-only check")
    restore_manager.workload.workload_present = False
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == "Workload container not ready yet!"


def test_pre_restore_checks_rejects_blocking_state(harness, restore_manager):
    harness.charm.unit.status = BlockedStatus("some blocking state")
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == "Cluster or unit is in a blocking state"


def test_pre_restore_checks_allows_pitr_blocked_status(harness, restore_manager):
    """A PITR failure blocked state must not block a new restore request."""
    harness.charm.unit.status = BlockedStatus(CANNOT_RESTORE_PITR)
    ok, _ = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert ok


def test_pre_restore_checks_allows_foreign_repository_blocked_status(harness, restore_manager):
    harness.charm.unit.status = BlockedStatus(ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    ok, _ = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert ok


def test_pre_restore_checks_rejects_multiple_units(harness, restore_manager, monkeypatch):
    monkeypatch.setattr(PostgreSQLApplication, "planned_units", property(lambda self: 2))
    peer_rel_id = harness.model.get_relation("database-peers").id
    harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/1")
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == "Unit cannot restore backup as there are more than one unit in the cluster"


def test_pre_restore_checks_rejects_active_async_relation(harness, restore_manager):
    harness.add_relation("replication", "other-postgresql")
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == "Unit cannot restore backup with an active async replication relation"


def test_pre_restore_checks_rejects_non_leader(harness, restore_manager):
    with harness.hooks_disabled():
        harness.set_leader(False)
    ok, message = restore_manager._pre_restore_checks(BACKUP_ID, None)
    assert not ok
    assert message == "Unit cannot restore backup as it was not elected the leader unit yet"


# -- _resolve_restore_target --------------------------------------------------------


def test_resolve_restore_target_real_backup_id(restore_manager):
    target, is_real, message = restore_manager._resolve_restore_target(BACKUP_ID, None)
    assert target == ("model.cluster", "1")
    assert is_real is True
    assert message == ""


def test_resolve_restore_target_rejects_invalid_backup_id(restore_manager):
    target, _is_real, message = restore_manager._resolve_restore_target("nope", None)
    assert target is None
    assert message == "Invalid backup-id: nope"


def test_resolve_restore_target_rejects_timeline_without_time(restore_manager):
    target, _is_real, message = restore_manager._resolve_restore_target(TIMELINE_ID, None)
    assert target is None
    assert message == "Cannot restore to the timeline without restore-to-time parameter"


def test_resolve_restore_target_latest_requires_base_backup(restore_manager):
    """No backup was created from the latest timeline, so "latest" must be rejected."""
    target, _is_real, message = restore_manager._resolve_restore_target(None, "latest")
    assert target is None
    assert message == "There is no base backup created from the latest timeline"


def test_resolve_restore_target_resolves_nearest_timeline(restore_manager):
    restore_manager.backup_manager._list_backups.side_effect = None
    restore_manager.backup_manager._list_backups.return_value = {}
    target, _is_real, message = restore_manager._resolve_restore_target(
        None, "2024-01-03 00:00:00"
    )
    assert target == ("model.cluster", "2")
    assert _is_real is False
    assert message == ""


def test_resolve_restore_target_rejects_missing_timeline(restore_manager):
    restore_manager.backup_manager._list_backups.side_effect = None
    restore_manager.backup_manager._list_backups.return_value = {}
    restore_manager.backup_manager._list_timelines.return_value = {}
    target, _is_real, message = restore_manager._resolve_restore_target(
        None, "2024-01-03 00:00:00"
    )
    assert target is None
    assert message.startswith("Can't find the nearest timeline before timestamp")


# -- restore orchestration -----------------------------------------------------------


def test_restore_vm_stops_patroni_then_reconfigures(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only orchestration")
    restore_manager.patroni_manager.get_patroni_restart_condition.return_value = "always"
    ok, message = restore_manager.restore(
        ("model.cluster", "2"), None, "2024-01-03 00:00:00", False
    )
    assert (ok, message) == (True, "restore started")
    restore_manager.patroni_manager.stop_patroni.assert_called_once()
    restore_manager.patroni_manager.update_patroni_restart_condition.assert_called_once_with("no")
    restore_manager.workload.empty_data_files.assert_called_once()
    restore_manager.update_config.assert_called_once()
    restore_manager.patroni_manager.start_patroni.assert_called_once()
    app_data = restore_manager.state.application.data
    assert app_data["restore-stanza"] == "model.cluster"
    assert app_data["restore-timeline"] == "2"
    assert app_data["restore-to-time"] == "2024-01-03 00:00:00"
    # ops deletes a databag key when it is set to the empty string
    assert app_data.get("restoring-backup", "") == ""
    assert app_data.get("s3-initialization-block-message", "") == ""


def test_restore_vm_removes_cluster_info_after_start(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only orchestration")
    restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    calls = [call[0] for call in restore_manager.workload.mock_calls]
    assert calls.index("empty_data_files") < calls.index("remove_cluster_info")
    restore_manager.workload.remove_cluster_info.assert_called_once_with(
        restore_manager.state.cluster_name
    )


def test_vm_remove_cluster_info_sends_confirmation_stdin(substrate):
    """The VM patronictl remove carries the cluster name + confirmation on stdin."""
    if substrate != "vm":
        pytest.skip("VM-only cluster-info removal")
    workload = MagicMock()
    workload.paths.patroni_config = "/etc/patroni"
    remove = partial(VMWorkload.remove_cluster_info, workload)
    remove("model.cluster")
    args, kwargs = workload.run_cmd.call_args
    assert "remove" in args[0]
    assert "model.cluster" in args[0]
    assert kwargs["stdin"] == "model.cluster\nYes I am aware"
    assert kwargs["timeout"] == 10


def test_restore_k8s_stops_and_controls_pebble_services(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s-only orchestration")
    ok, message = restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    assert (ok, message) == (True, "restore started")
    restore_manager.workload.stop_service.assert_called_once_with(K8S_POSTGRESQL_SERVICE_NAME)
    restore_manager.workload.start_service.assert_called_once_with(K8S_POSTGRESQL_SERVICE_NAME)
    restore_manager.workload.init_storage.assert_called_once()
    restore_manager.workload.remove_cluster_info.assert_called_once_with(
        restore_manager.state.cluster_name, restore_manager.state.model_name
    )


def test_restore_k8s_overrides_on_failure_condition(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s-only orchestration")
    restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    unit_data = restore_manager.state.peer.data
    assert unit_data["patroni-on-failure-condition-override"] == "ignore"
    assert unit_data["overridden-patroni-on-failure-condition-repeat-cause"] == "restore-backup"
    restore_manager._update_pebble_layers_bridge.assert_called()


def test_restore_k8s_removes_cluster_info_before_wipe(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s-only orchestration")
    restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    calls = [call[0] for call in restore_manager.workload.mock_calls]
    assert calls.index("remove_cluster_info") < calls.index("empty_data_files")


def test_restore_failure_to_stop_database_returns_error(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only stop path")
    restore_manager.patroni_manager.stop_patroni.return_value = False
    ok, message = restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    assert (ok, message) == (False, "Failed to stop database service")
    restore_manager.workload.empty_data_files.assert_not_called()


def test_restore_override_failure_recovers_database(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only restart-condition override")
    unit_data = restore_manager.state.peer.data
    unit_data["overridden-patroni-restart-condition"] = "always"
    unit_data["overridden-patroni-restart-condition-repeat-cause"] = "other-cause"
    ok, message = restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    assert (ok, message) == (False, "Failed to override Patroni restart condition")
    restore_manager.update_config.assert_called_once()  # from _restart_database
    restore_manager.patroni_manager.start_patroni.assert_called_once()


def test_restore_empty_failure_recovers_database(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only empty failure path")
    restore_manager.workload.empty_data_files.return_value = False
    ok, message = restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    assert (ok, message) == (False, "Failed to remove contents of the data directory")
    restore_manager.patroni_manager.start_patroni.assert_called_once()


def test_restore_k8s_endpoint_failure_recovers_database(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s-only endpoint removal")
    restore_manager.workload.remove_cluster_info.side_effect = ApiError(
        status=Status(message="boom")
    )
    ok, message = restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    assert ok is False
    assert message.startswith("Failed to remove previous cluster information")
    restore_manager.workload.start_service.assert_called_once_with(K8S_POSTGRESQL_SERVICE_NAME)


def test_restore_patroni_restart_condition_restores_override(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only restart-condition override")
    restore_manager.patroni_manager.get_patroni_restart_condition.return_value = "always"
    restore_manager.restore(("model.cluster", "1"), BACKUP_ID, None, True)
    restore_manager.restore_patroni_restart_condition()
    unit_data = restore_manager.state.peer.data
    # setting the override fields to "" removes them from the databag
    assert "overridden-patroni-restart-condition" not in unit_data
    restore_manager.patroni_manager.update_patroni_restart_condition.assert_any_call("always")


# -- PITR helpers ---------------------------------------------------------------------


def test_is_pitr_failed_tracks_new_failures(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only patroni logs")
    restore_manager.patroni_manager.patroni_logs.return_value = (
        "2024-01-01T00:00:00.000Z localhost patroni.exceptions.PatroniFatalException:"
        " Failed to bootstrap cluster"
    )
    assert restore_manager.is_pitr_failed() == (True, True)
    assert restore_manager.is_pitr_failed() == (True, False)
    restore_manager.patroni_manager.patroni_logs.return_value = (
        "2024-01-02T00:00:00.000Z localhost patroni.exceptions.PatroniFatalException:"
        " Failed to bootstrap cluster"
    )
    assert restore_manager.is_pitr_failed() == (True, True)


def test_is_pitr_failed_reports_no_failure(restore_manager, substrate):
    if substrate != "vm":
        pytest.skip("VM-only patroni logs")
    restore_manager.patroni_manager.patroni_logs.return_value = ""
    assert restore_manager.is_pitr_failed() == (False, False)


def test_is_pitr_failed_k8s_scans_pebble_logs(restore_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s-only pebble logs")
    restore_manager.workload.pitr_bootstrap_failure_logs.return_value = (
        (
            "2024-01-01T00:00:00.000Z [postgresql] patroni.exceptions.PatroniFatalException:"
            " Failed to bootstrap cluster"
        ),
        False,
    )
    assert restore_manager.is_pitr_failed() == (True, True)


def test_log_pitr_last_transaction_time(restore_manager, caplog):
    caplog.set_level(logging.INFO)
    restore_manager.patroni_manager.last_postgresql_logs.return_value = (
        "2024-01-01 00:00:00 UTC [123]: LOG:  last completed transaction was at log time"
        " 2024-01-01 00:00:00 UTC"
    )
    restore_manager.log_pitr_last_transaction_time()
    assert any("Last completed transaction was at" in record.message for record in caplog.records)


def test_log_pitr_last_transaction_time_reports_missing_time(restore_manager, caplog):
    restore_manager.patroni_manager.last_postgresql_logs.return_value = ""
    restore_manager.log_pitr_last_transaction_time()
    assert any(
        "Can't tell last completed transaction time" in record.message for record in caplog.records
    )
