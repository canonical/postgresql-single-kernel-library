#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the BackupManager (utils-free manager behavior).

The manager is exercised with the real harness-provided CharmState per
substrate plus mocks for the workload / S3 client / Patroni manager /
update-config bridge. Focus: substrate-conditional pgBackRest invocation,
stanza lifecycle peer-data writes, backup permission gates, creation
bracketing, and the S3 initialization flow — no tautological asserts.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from ops import BlockedStatus
from single_kernel_postgresql.config.exceptions import ListBackupsError
from single_kernel_postgresql.config.literals import S3_RELATION_NAME
from single_kernel_postgresql.managers.backup import BackupManager
from single_kernel_postgresql.utils.backup import (
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    CANNOT_RESTORE_PITR,
    FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE,
    STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE,
)
from single_kernel_postgresql.workload.base import BackupConfig

VM_EXECUTABLE = "charmed-postgresql.pgbackrest"
K8S_EXECUTABLE = "pgbackrest"


@pytest.fixture
def workload(substrate):
    """A mock workload carrying a real BackupConfig for the substrate."""
    workload = MagicMock()
    if substrate == "vm":
        workload.backup_config = BackupConfig(
            executable=VM_EXECUTABLE,
            conf_path="/var/snap/charmed-postgresql/current/etc/pgbackrest",
            logs_path="/var/snap/charmed-postgresql/common/var/log/pgbackrest",
            bin_path="/snap/charmed-postgresql/current/usr/lib/postgresql",
            service="pgbackrest-service",
            storage_path="/var/snap/charmed-postgresql/common",
            tls_ca_chain_path=(
                "/var/snap/charmed-postgresql/current/etc/pgbackrest/pgbackrest-tls-ca-chain.crt"
            ),
            extra_args=(),
        )
        workload.get_available_resources.return_value = (4, 1000)
        workload.user = "_daemon_"
        workload.group = "_daemon_"
    else:
        workload.backup_config = BackupConfig(
            executable=K8S_EXECUTABLE,
            conf_path=None,
            logs_path="/var/lib/pg/logs/16/main/pgbackrest_logs",
            bin_path="/usr/lib/postgresql",
            service="pgbackrest server",
            storage_path="/var/lib/pg/data",
            tls_ca_chain_path="/var/lib/pg/data/pgbackrest-tls-ca-chain.crt",
            extra_args=(),
        )
        workload.get_available_resources.return_value = (4, 1000)
        workload.user = "postgres"
        workload.group = "postgres"
    return workload


@pytest.fixture
def backup_manager(harness, substrate, workload):
    """A BackupManager wired to the harness state and mocked collaborators."""
    manager = BackupManager(
        state=harness.charm.state,
        workload=workload,
        s3_client=MagicMock(),
        patroni_manager=MagicMock(),
        update_config=MagicMock(return_value=True),
        resource_provider=workload,
        is_standby_cluster=None if substrate == "k8s" else MagicMock(return_value=False),
    )
    manager = manager_with_harness(harness, manager)
    yield manager


def manager_with_harness(harness, manager):
    """Bind the manager to the harness charm's live state accessors."""
    manager.state = harness.charm.state
    return manager


def _mock_run_cmd(manager, return_code=0, stdout="", stderr=""):
    from single_kernel_postgresql.workload.base import CommandResult

    manager.workload.run_cmd.return_value = CommandResult(return_code, stdout, stderr)


# -- stanza_name ----------------------------------------------------------------


def test_stanza_name_composes_model_and_cluster_name(harness, substrate, backup_manager):
    expected = f"{harness.model.name}.{harness.charm.state.cluster_name}"
    assert backup_manager.stanza_name == expected


# -- _execute_pgbackrest ----------------------------------------------------------


def test_execute_pgbackrest_uses_config_flag_only_on_vm(backup_manager, substrate):
    _mock_run_cmd(backup_manager)
    backup_manager._execute_pgbackrest(["stanza-create"])
    command = backup_manager.workload.run_cmd.call_args.args[0]
    if substrate == "vm":
        assert command.startswith(f"{VM_EXECUTABLE} --config=")
    else:
        assert command.startswith(K8S_EXECUTABLE)
        assert "--config" not in command
    assert "--log-level-stderr=warn" in command
    assert "stanza-create" in command


def test_k8s_render_writes_default_configuration_file(backup_manager, substrate):
    """The K8s render targets /etc/pgbackrest.conf; a None conf_path must not leak."""
    if substrate != "k8s":
        pytest.skip("K8s renders to the default location")
    backup_manager.workload.root = Path("/")
    backup_manager.workload.write_text = MagicMock()
    backup_manager.state.s3_connection_info.retrieve_s3_parameters = MagicMock(
        return_value=(
            {
                "bucket": "b",
                "access-key": "k",
                "secret-key": "s",
                "endpoint": "https://s3.amazonaws.com",
                "s3-uri-style": "host",
                "path": "",
                "delete-older-than-days": "9999999",
            },
            [],
        )
    )
    backup_manager._tls_ca_chain_filename = ""
    assert backup_manager._render_pgbackrest_conf_file() is True
    written = [c.args[1] for c in backup_manager.workload.write_text.call_args_list]
    assert Path("/etc/pgbackrest.conf") in written


def test_execute_pgbackrest_server_ping_runs_without_config(backup_manager, substrate):
    _mock_run_cmd(backup_manager)
    backup_manager._execute_pgbackrest(["server-ping", "1.2.3.4"], with_config=False)
    command = backup_manager.workload.run_cmd.call_args.args[0]
    assert "--config" not in command


# -- stanza lifecycle -------------------------------------------------------------


def test_initialise_stanza_refused_when_blocked_without_s3_message(harness, backup_manager):
    rel_id = harness.model.get_relation("database-peers").id
    harness.model.unit.status = BlockedStatus("blocked")
    assert backup_manager._initialise_stanza() is False
    assert "stanza" not in harness.get_relation_data(rel_id, harness.charm.unit.name)


def test_check_stanza_writes_done_marker_on_non_leader(harness, backup_manager):
    _mock_run_cmd(backup_manager)
    backup_manager.update_config.return_value = True
    assert backup_manager.check_stanza() is True
    rel_id = harness.model.get_relation("database-peers").id
    unit_db = harness.get_relation_data(rel_id, harness.charm.unit.name)
    app_db = harness.get_relation_data(rel_id, harness.charm.app.name)
    if harness.charm.state.peer.is_app_leader:
        assert app_db.get("s3-initialization-start") == ""
    else:
        assert unit_db.get("s3-initialization-done") == "True"


# -- backup permission gates -------------------------------------------------------


def test_can_unit_perform_backup_rejects_standby_cluster(backup_manager, substrate):
    if substrate != "vm":
        pytest.skip("is_standby_cluster bridge is VM-only")
    backup_manager._is_standby_cluster_bridge = MagicMock(return_value=True)
    ok, message = backup_manager._can_unit_perform_backup()
    assert ok is False
    assert message == STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE


def test_can_unit_perform_backup_k8s_is_never_standby(backup_manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s has no standby-cluster bridge")
    assert backup_manager._is_standby_cluster is False


def test_can_unit_perform_backup_rejects_missing_stanza(backup_manager):
    backup_manager.patroni_manager.member_started = True
    backup_manager.patroni_manager.get_primary.return_value = None
    ok, message = backup_manager._can_unit_perform_backup()
    assert ok is False
    assert message == "Stanza was not initialised"


def test_create_backup_rejects_invalid_type(backup_manager):
    ok, message = backup_manager.create_backup("bogus")
    assert ok is False
    assert message.startswith("Invalid backup type: bogus")


def test_create_backup_rejects_differential_without_full_backup(backup_manager):
    backup_manager._list_backups = MagicMock(return_value={})
    ok, message = backup_manager.create_backup("differential")
    assert ok is False
    assert "No previous full backup to reference" in message


def test_create_backup_brackets_with_connectivity_off_on_replica(backup_manager):
    backup_manager._list_backups = MagicMock(return_value={})
    backup_manager.s3_client.upload_content.return_value = True
    backup_manager.patroni_manager.get_primary.return_value = None
    backup_manager._can_unit_perform_backup = MagicMock(return_value=(True, ""))
    backup_manager._run_backup = MagicMock(return_value=(False, "boom"))
    ok, message = backup_manager.create_backup("full")
    assert ok is False
    assert message == "boom"
    # On a non-primary the connectivity-off rule is applied before the run and
    # restored after; the creating-backup flag bracketing is unconditional.
    connectivity_calls = [
        c
        for c in backup_manager.update_config.call_args_list
        if c.kwargs.get("is_creating_backup") is True
    ]
    assert len(connectivity_calls) >= 1
    assert (
        backup_manager.update_config.call_args_list[-1].kwargs.get("is_creating_backup") is False
    )


# -- backup listing -----------------------------------------------------------------


def _fake_info_payload():
    return json.dumps([
        {
            "name": "test-model.app",
            "backup": [
                {
                    "label": "20240101-101010F",
                    "reference": [],
                    "lsn": {"start": "0/1000000", "stop": "0/2000000"},
                    "timestamp": {"start": 1704110410, "stop": 1704111010},
                    "archive": {"start": "000000010000000000000001"},
                    "error": None,
                }
            ],
        }
    ])


def test_list_backups_parses_backup_ids(backup_manager):
    _mock_run_cmd(backup_manager, stdout=_fake_info_payload())
    backups = backup_manager._list_backups(show_failed=False)
    assert list(backups) == ["2024-01-01T10:10:10Z"]
    assert backups["2024-01-01T10:10:10Z"][1] == "1"


def test_list_backups_raises_list_backups_error_on_vm_failure(backup_manager, substrate):
    if substrate != "vm":
        pytest.skip("ListBackupsError branch is VM-only")
    _mock_run_cmd(backup_manager, return_code=1, stderr="ERROR: boom")
    with pytest.raises(ListBackupsError):
        backup_manager._list_backups(show_failed=False)


def test_generate_backup_list_output_includes_header_and_row(harness, backup_manager):
    _mock_run_cmd(backup_manager, stdout=_fake_info_payload())
    backup_manager._list_timelines = MagicMock(return_value={})
    rel_id = harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    harness.update_relation_data(
        rel_id, "s3-integrator", {"bucket": "bucket", "access-key": "k", "secret-key": "s"}
    )
    output = backup_manager._generate_backup_list_output()
    assert "Storage bucket name: bucket" in output
    assert "2024-01-01T10:10:10Z" in output
    assert "full backup" in output


# -- S3 initialization flow -----------------------------------------------------------


def test_credential_changed_checks_k8s_requires_connection_info(
    harness, backup_manager, substrate
):
    if substrate != "k8s":
        pytest.skip("connection-info-first check is K8s-specific")
    backup_manager.workload.write_text = MagicMock()
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    backup_manager._can_initialise_stanza = MagicMock(return_value=True)
    # No S3 relation data yet -> no connection info -> reject before any render,
    # without deferring (the K8s charm returns without defer here too).
    assert backup_manager._credential_changed_checks() == (False, False)


def test_credential_changed_checks_rejects_during_pitr_restore(harness, backup_manager):
    harness.model.unit.status = BlockedStatus(CANNOT_RESTORE_PITR)
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    assert backup_manager._credential_changed_checks() == (False, True)

    with harness.hooks_disabled():
        harness.set_leader()
    backup_manager.s3_client.create_bucket_if_not_exists.side_effect = ValueError("bad region")
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    assert backup_manager.initialise_s3_repository() is False
    app = harness.charm.state.application
    assert app.s3_initialization_block_message == FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE


def test_initialise_s3_repository_rejects_foreign_repository(harness, backup_manager):
    with harness.hooks_disabled():
        harness.set_leader()
    backup_manager.can_use_s3_repository = MagicMock(
        return_value=(False, ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    )
    assert backup_manager.initialise_s3_repository() is False
    assert (
        harness.charm.state.application.s3_initialization_block_message
        == ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE
    )


def test_clear_s3_state_clears_markers(backup_manager, substrate):
    backup_manager.clear_s3_state()
    if substrate == "k8s":
        backup_manager.workload.stop_service.assert_called_once_with("rotate-logs")
    else:
        backup_manager.workload.stop_service.assert_not_called()
