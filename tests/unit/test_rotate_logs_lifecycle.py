# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the BackupManager rotate-logs lifecycle (VM spawn, K8s no-op)."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from single_kernel_postgresql.config.literals import (
    PGBACKREST_LOGROTATE_FILE,
    VM_ROTATE_LOGS_LOG_FILE,
)
from single_kernel_postgresql.managers.backup import BackupManager
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
        workload.root = Path("/")
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
    manager.state = harness.charm.state
    return manager


@pytest.fixture
def manager(harness, backup_manager):
    """The backup manager with a rendered logrotate file on disk."""
    backup_manager.workload.exists.return_value = True
    return backup_manager


def _set_pid(harness, pid):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, {"rotate-logs-pid": str(pid)})


def _read_pid(harness):
    rel_id = harness.model.get_relation("database-peers").id
    return harness.get_relation_data(rel_id, harness.charm.unit.name).get("rotate-logs-pid")


def test_start_log_rotation_spawns_loop_and_stores_pid(
    harness, manager, substrate, monkeypatch, tmp_path
):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    harness.model.unit.status = ActiveStatus()
    monkeypatch.setattr(
        "single_kernel_postgresql.managers.backup.VM_ROTATE_LOGS_LOG_FILE",
        str(tmp_path / "rotate_logs.log"),
    )
    popen = MagicMock(return_value=MagicMock(pid=4242))
    monkeypatch.setattr("single_kernel_postgresql.managers.backup.subprocess.Popen", popen)
    manager.start_log_rotation()
    script_path = popen.call_args.args[0][1]
    assert script_path.endswith("rotate_logs.py")
    assert _read_pid(harness) == "4242"
    assert VM_ROTATE_LOGS_LOG_FILE


def test_start_log_rotation_reuses_running_loop(harness, manager, substrate, monkeypatch):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    harness.model.unit.status = ActiveStatus()
    _set_pid(harness, 4242)
    kill = MagicMock()
    monkeypatch.setattr("single_kernel_postgresql.managers.backup.os.kill", kill)
    popen = MagicMock()
    monkeypatch.setattr("single_kernel_postgresql.managers.backup.subprocess.Popen", popen)
    manager.start_log_rotation()
    kill.assert_called_once_with(4242, 0)
    popen.assert_not_called()


def test_start_log_rotation_respawns_dead_loop(harness, manager, substrate, monkeypatch, tmp_path):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    harness.model.unit.status = ActiveStatus()
    monkeypatch.setattr(
        "single_kernel_postgresql.managers.backup.VM_ROTATE_LOGS_LOG_FILE",
        str(tmp_path / "rotate_logs.log"),
    )
    _set_pid(harness, 4242)
    monkeypatch.setattr(
        "single_kernel_postgresql.managers.backup.os.kill",
        MagicMock(side_effect=OSError),
    )
    popen = MagicMock(return_value=MagicMock(pid=5150))
    monkeypatch.setattr("single_kernel_postgresql.managers.backup.subprocess.Popen", popen)
    manager.start_log_rotation()
    popen.assert_called_once()
    assert _read_pid(harness) == "5150"


@pytest.mark.parametrize("status", [BlockedStatus(), MaintenanceStatus()])
def test_start_log_rotation_gates_on_unit_status(harness, manager, substrate, status):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    harness.model.unit.status = status
    manager.start_log_rotation()
    manager.workload.exists.assert_not_called()


def test_start_log_rotation_gates_on_missing_logrotate_file(harness, manager, substrate):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    harness.model.unit.status = ActiveStatus()
    manager.workload.exists.return_value = False
    manager.start_log_rotation()
    manager.workload.exists.assert_called_once_with(Path(PGBACKREST_LOGROTATE_FILE))


def test_stop_log_rotation_kills_and_clears_pid(harness, manager, substrate, monkeypatch):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    _set_pid(harness, 4242)
    kill = MagicMock()
    monkeypatch.setattr("single_kernel_postgresql.managers.backup.os.kill", kill)
    manager.stop_log_rotation()
    kill.assert_called_once_with(4242, 2)  # signal.SIGINT
    assert _read_pid(harness) is None


def test_stop_log_rotation_tolerates_dead_pid(harness, manager, substrate, monkeypatch):
    if substrate != "vm":
        pytest.skip("rotate-logs spawn is VM-only")
    _set_pid(harness, 4242)
    monkeypatch.setattr(
        "single_kernel_postgresql.managers.backup.os.kill",
        MagicMock(side_effect=OSError),
    )
    manager.stop_log_rotation()
    assert _read_pid(harness) == "4242"


def test_stop_log_rotation_noop_on_k8s(manager, substrate):
    if substrate != "k8s":
        pytest.skip("K8s rotation is the Pebble service's job")
    manager.stop_log_rotation()
    manager.start_log_rotation()
    manager.workload.exists.assert_not_called()
