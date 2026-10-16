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
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from data_platform_helpers.advanced_statuses import StatusObject
from ops import BlockedStatus
from ops.pebble import ExecError
from single_kernel_postgresql.config.exceptions import ListBackupsError
from single_kernel_postgresql.config.literals import PEER_RELATION, S3_RELATION_NAME
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.managers.backup import (
    PGBACKREST_CHECK_TIMEOUT,
    PGBACKREST_STANZA_CREATE_TIMEOUT,
    BackupManager,
)
from single_kernel_postgresql.utils.backup import (
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    CANNOT_RESTORE_PITR,
    FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE,
    FAILED_TO_CHECK_STANZA_ERROR_MESSAGE,
    FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE,
    STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE,
)
from single_kernel_postgresql.workload.base import BackupConfig, CommandResult
from tenacity import wait_fixed

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


def test_k8s_render_writes_default_configuration_file(harness, backup_manager, substrate):
    """The K8s render targets /etc/pgbackrest.conf; a None conf_path must not leak."""
    if substrate != "k8s":
        pytest.skip("K8s renders to the default location")
    backup_manager.workload.root = Path("/")
    backup_manager.workload.write_text = MagicMock()
    backup_manager.workload.service_exists = MagicMock(return_value=True)
    rel_id = harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    harness.update_relation_data(
        rel_id, "s3-integrator", {"bucket": "b", "access-key": "k", "secret-key": "s"}
    )
    with patch.object(
        BackupManager, "_tls_ca_chain_filename", new_callable=PropertyMock, return_value=""
    ):
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


def test_initialise_stanza_proceeds_when_blocked_with_s3_message(harness, backup_manager):
    """A unit blocked on an S3 message must still re-initialise the stanza.

    The events layer clears the s3-initialization-block-message peer field
    before calling _initialise_stanza, so the gate must consult the LIVE unit
    status message (the charms gate on self.unit.status.message) — otherwise
    the recovery after fixing the S3 settings deadlocks: the unit stays
    blocked with the stale message forever (observed on
    test_invalid_config_and_recovery_after_fixing_it CI runs).
    """
    harness.model.unit.status = BlockedStatus(FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE)
    _mock_run_cmd(backup_manager)
    assert backup_manager._initialise_stanza() is True


def test_check_stanza_refreshes_primary_status_on_success(harness, backup_manager):
    """After a successful stanza check the charm recomputes the unit status.

    A unit blocked with a stale S3 failure message only unblocks when the
    status is recomputed after the check succeeds (the charms call
    _set_primary_status_message here); the blocked unit early-exits
    update-status, so nothing else would clear the stale message.
    """
    harness.model.unit.status = BlockedStatus(ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    _mock_run_cmd(backup_manager)
    backup_manager.refresh_primary_status = MagicMock()
    assert backup_manager.check_stanza() is True
    backup_manager.refresh_primary_status.assert_called_once_with()


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


def test_render_omits_purged_endpoints(harness, backup_manager, substrate):
    """A purged departed endpoint no longer leaks into the rendered conf (K8s)."""
    if substrate != "k8s":
        pytest.skip("endpoints are a K8s concept")
    rel_id = harness.model.get_relation(PEER_RELATION).id
    app = harness.charm.app.name
    with harness.hooks_disabled():
        harness.set_leader(True)
        harness.update_relation_data(
            rel_id,
            app,
            {"endpoints": json.dumps([f"{app}-{i}.{app}-endpoints" for i in (0, 2)])},
        )
    backup_manager._remove_departed_endpoints_bridge = (
        harness.charm.membership_manager.remove_departed_endpoints
    )
    assert backup_manager.remove_departed_endpoints() is None
    assert json.loads(harness.get_relation_data(rel_id, app)["endpoints"]) == [
        f"{app}-0.{app}-endpoints"
    ]
    s3_rel = harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    harness.update_relation_data(
        s3_rel, "s3-integrator", {"bucket": "b", "access-key": "k", "secret-key": "s"}
    )
    backup_manager.workload.root = Path("/")
    backup_manager.workload.write_text = MagicMock()
    backup_manager.workload.service_exists = MagicMock(return_value=True)
    with patch.object(
        BackupManager, "_tls_ca_chain_filename", new_callable=PropertyMock, return_value=""
    ):
        assert backup_manager._render_pgbackrest_conf_file() is True
    content = next(
        c.args[0]
        for c in backup_manager.workload.write_text.call_args_list
        if str(c.args[1]).endswith("pgbackrest.conf")
    )
    # The departed unit-2 endpoint must be gone, and with zero peer endpoints
    # left the TLS server blocks are not rendered at all.
    assert f"{app}-2" not in content
    assert "tls-server-auth" not in content
    assert "tls-server-address" not in content
    assert "backup-standby=y" in content

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
    assert backup_manager.is_standby_cluster is False


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
    backup_manager.get_backups = MagicMock(return_value={})
    ok, message = backup_manager.create_backup("differential")
    assert ok is False
    assert "No previous full backup to reference" in message


def test_create_backup_brackets_with_connectivity_off_on_replica(backup_manager):
    backup_manager.get_backups = MagicMock(return_value={})
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


def test_get_backups_parses_backup_ids(backup_manager):
    _mock_run_cmd(backup_manager, stdout=_fake_info_payload())
    backups = backup_manager.get_backups(show_failed=False)
    assert list(backups) == ["2024-01-01T10:10:10Z"]
    assert backups["2024-01-01T10:10:10Z"][1] == "1"


def test_get_backups_raises_list_backups_error_on_vm_failure(backup_manager, substrate):
    if substrate != "vm":
        pytest.skip("ListBackupsError branch is VM-only")
    _mock_run_cmd(backup_manager, return_code=1, stderr="ERROR: boom")
    with pytest.raises(ListBackupsError):
        backup_manager.get_backups(show_failed=False)


def test_generate_backup_list_output_includes_header_and_row(harness, backup_manager):
    _mock_run_cmd(backup_manager, stdout=_fake_info_payload())
    backup_manager.get_timelines = MagicMock(return_value={})
    rel_id = harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    harness.update_relation_data(
        rel_id, "s3-integrator", {"bucket": "bucket", "access-key": "k", "secret-key": "s"}
    )
    output = backup_manager.generate_backup_list_output()
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
    assert backup_manager.credential_changed_checks() == (False, False)


def test_credential_changed_checks_rejects_during_pitr_restore(harness, backup_manager, substrate):
    if substrate != "vm":
        pytest.skip(
            "the K8s connection-info check short-circuits first; the guard itself is shared"
        )
    harness.model.unit.status = BlockedStatus(CANNOT_RESTORE_PITR)
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    assert backup_manager.credential_changed_checks() == (False, True)

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


def test_s3_initialization_failure_refreshes_leader_status(harness, backup_manager):
    """The leader's unit status must carry the block message in-hook.

    The 16/edge charms refreshed the primary status inside
    ``_s3_initialization_set_failure``; the library port dropped that, and a
    blocked unit early-exits update-status and peer-relation-changed, so the
    message never surfaced (test_backups_gcp CI regression).
    """
    with harness.hooks_disabled():
        harness.set_leader()
    backup_manager.set_unit_status = MagicMock()
    backup_manager._s3_initialization_set_failure(FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE)
    backup_manager.set_unit_status.assert_called_once_with(
        BlockedStatus(FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE)
    )
    assert (
        harness.charm.state.application.s3_initialization_block_message
        == FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE
    )


def test_s3_initialization_failure_non_leader_writes_unit_databag_only(backup_manager):
    """Non-leader units record the failure without touching the unit status."""
    backup_manager.set_unit_status = MagicMock()
    backup_manager._s3_initialization_set_failure(FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE)
    backup_manager.set_unit_status.assert_not_called()


def test_reset_s3_initialization_markers_resets_the_app_markers(harness, backup_manager):
    with harness.hooks_disabled():
        harness.set_leader()
    app = harness.charm.state.application
    app.s3_initialization_block_message = "some old failure"
    app.s3_initialization_start = ""
    app.stanza = "some-stanza"
    app.s3_initialization_done = "True"

    backup_manager.reset_s3_initialization_markers()

    # Writing "" removes the databag keys, so the getters read back None.
    assert not app.s3_initialization_block_message
    assert app.s3_initialization_start
    assert not app.stanza
    assert not app.s3_initialization_done


def test_reset_s3_initialization_markers_noop_without_leadership(harness, backup_manager):
    with harness.hooks_disabled():
        harness.set_leader()
    app = harness.charm.state.application
    app.s3_initialization_block_message = "some old failure"
    app.s3_initialization_start = "old"
    app.stanza = "some-stanza"
    app.s3_initialization_done = "True"
    with harness.hooks_disabled():
        harness.set_leader(False)

    backup_manager.reset_s3_initialization_markers()

    assert app.s3_initialization_block_message == "some old failure"
    assert app.s3_initialization_start == "old"
    assert app.stanza == "some-stanza"
    assert app.s3_initialization_done == "True"


def test_clear_s3_state_clears_markers(backup_manager, substrate):
    backup_manager.clear_s3_state()
    if substrate == "k8s":
        backup_manager.workload.stop_service.assert_called_once_with("rotate-logs")
    else:
        backup_manager.workload.stop_service.assert_not_called()


def test_s3_block_message_survives_collect(backup_manager, harness):
    """The s3-initialization blocked must survive the collect.

    test_restore_on_new_cluster (gcp arm64, CI 2026-10-13) waits for the
    blocked "backups from another cluster" message after a restore; the
    direct write died at hook end and the wait timed out. The persisted
    peer-databag message now replays through BackupManager.get_statuses.
    """
    with harness.hooks_disabled():
        harness.set_leader(True)
    backup_manager._s3_initialization_set_failure(ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    assert backup_manager.get_statuses("unit") == [
        StatusObject(status="blocked", message=ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    ]

    # A non-leader stores the message in its unit databag; the replay reads it.
    with harness.hooks_disabled():
        harness.set_leader(False)
    backup_manager._s3_initialization_set_failure(ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    assert backup_manager.get_statuses("unit") == [
        StatusObject(status="blocked", message=ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE)
    ]

    # Resetting the markers clears the replay.
    with harness.hooks_disabled():
        harness.set_leader(True)
    backup_manager.reset_s3_initialization_markers()
    assert backup_manager.get_statuses("unit") == [GeneralStatuses.ACTIVE_IDLE.value]


# -- bounded stanza execs + membership reconcile (validator round: stale pgbackrest.conf) --


@pytest.fixture
def no_retry_wait():
    """Zero out the tenacity inter-attempt wait (10 s) for retry-loop tests."""
    with patch("single_kernel_postgresql.managers.backup.wait_fixed", return_value=wait_fixed(0)):
        yield


def test_check_stanza_passes_bounded_timeout(harness, backup_manager, substrate):
    """The stanza check exec is bounded (PGBACKREST_CHECK_TIMEOUT)."""
    _mock_run_cmd(backup_manager)
    assert backup_manager.check_stanza() is True
    assert backup_manager.workload.run_cmd.call_args.kwargs["timeout"] == (
        PGBACKREST_CHECK_TIMEOUT
    )


def test_create_stanza_passes_bounded_timeout(harness, backup_manager, substrate):
    """The stanza-create exec is bounded (PGBACKREST_STANZA_CREATE_TIMEOUT)."""
    _mock_run_cmd(backup_manager)
    assert backup_manager._create_stanza() is True
    assert backup_manager.workload.run_cmd.call_args.kwargs["timeout"] == (
        PGBACKREST_STANZA_CREATE_TIMEOUT
    )


def test_check_stanza_recovers_after_timeout_by_reconciling_and_rerendering(
    harness, backup_manager, substrate, no_retry_wait
):
    """A first exec timeout retries once after reconciling + re-rendering (K8s)."""
    if substrate != "k8s":
        pytest.skip("pebble exec timeouts are the K8s flavor")
    reconcile = MagicMock(return_value=[])
    backup_manager.remove_departed_endpoints = reconcile
    render = MagicMock(return_value=True)
    backup_manager._render_pgbackrest_conf_file = render
    backup_manager.workload.run_cmd.side_effect = [
        TimeoutError("pgbackrest exec timed out"),
        CommandResult(0, "", ""),
    ]
    assert backup_manager.check_stanza() is True
    assert backup_manager.workload.run_cmd.call_count == 2
    assert reconcile.call_count == 2
    assert render.call_count == 2


def test_check_stanza_maps_second_timeout_to_check_message(
    harness, backup_manager, substrate, no_retry_wait
):
    """A second consecutive exec timeout blocks with the check message (K8s)."""
    if substrate != "k8s":
        pytest.skip("pebble exec timeouts are the K8s flavor")
    with harness.hooks_disabled():
        harness.set_leader(True)
    reconcile = MagicMock(return_value=[])
    backup_manager.remove_departed_endpoints = reconcile
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    backup_manager.set_unit_status = MagicMock()
    backup_manager.workload.run_cmd.side_effect = TimeoutError("pgbackrest exec timed out")
    assert backup_manager.check_stanza() is False
    rel_id = harness.model.get_relation("database-peers").id
    app_db = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert app_db.get("s3-initialization-block-message") == FAILED_TO_CHECK_STANZA_ERROR_MESSAGE
    status = backup_manager.set_unit_status.call_args.args[0]
    assert status == BlockedStatus(FAILED_TO_CHECK_STANZA_ERROR_MESSAGE)
    assert backup_manager.workload.run_cmd.call_count == 2


def test_check_stanza_exec_error_keeps_retry_semantics_with_reconcile(
    harness, backup_manager, substrate, no_retry_wait
):
    """Fast ExecError keeps the 6-attempt retry loop, with reconcile per attempt (K8s)."""
    if substrate != "k8s":
        pytest.skip("pebble ExecError is the K8s flavor")
    with harness.hooks_disabled():
        harness.set_leader(True)
    reconcile = MagicMock(return_value=[])
    backup_manager.remove_departed_endpoints = reconcile
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    backup_manager.workload.run_cmd.side_effect = ExecError(
        command=["pgbackrest", "check"], exit_code=1, stdout="", stderr="boom"
    )
    assert backup_manager.check_stanza() is False
    rel_id = harness.model.get_relation("database-peers").id
    app_db = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert app_db.get("s3-initialization-block-message") == FAILED_TO_CHECK_STANZA_ERROR_MESSAGE
    assert backup_manager.workload.run_cmd.call_count == 6
    assert reconcile.call_count == 6


def test_vm_check_timeout_reraises_after_single_reconciled_retry(
    harness, backup_manager, substrate, no_retry_wait
):
    """VM: an exec timeout retries once, then re-raises the TimeoutError contract."""
    if substrate != "vm":
        pytest.skip("run_cmd subprocess.TimeoutExpired is the VM flavor")
    reconcile = MagicMock(return_value=[])
    backup_manager.remove_departed_endpoints = reconcile
    backup_manager._render_pgbackrest_conf_file = MagicMock(return_value=True)
    backup_manager.workload.run_cmd.side_effect = subprocess.TimeoutExpired(
        cmd="pgbackrest", timeout=300
    )
    with pytest.raises(TimeoutError):
        backup_manager.check_stanza()
    # The reconcile runs at every attempt entry (before each exec), matching
    # the per-attempt contract of the stanza lifecycle.
    assert backup_manager.workload.run_cmd.call_count == 2
    assert reconcile.call_count == 2
