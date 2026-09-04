# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for run_cmd, by-name service control, and pgbackrest logs paths."""

import signal
from subprocess import TimeoutExpired
from unittest.mock import Mock, patch

import pytest
from charmlibs import pathops, snap
from ops.pebble import ExecError, ServiceStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.workload.base import CommandResult
from single_kernel_postgresql.workload.k8s import K8sWorkload
from single_kernel_postgresql.workload.paths.k8s import K8sPaths
from single_kernel_postgresql.workload.paths.vm import VMPaths
from single_kernel_postgresql.workload.vm import VMWorkload

PGBACKREST_SERVICE = "pgbackrest-service"


@pytest.fixture
def workload(substrate):
    """A workload for the substrate: real VMWorkload, K8sWorkload with a mocked container."""
    if substrate == Substrates.VM:
        return VMWorkload(".")
    return K8sWorkload(".", Mock(name="container"))


# --- run_cmd


@patch("single_kernel_postgresql.workload.vm.subprocess.run")
def test_vm_run_cmd_returns_code_stdout_stderr(mock_run, substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    mock_run.return_value = Mock(returncode=3, stdout=b"out", stderr=b"err")
    result = workload.run_cmd("pgbackrest", args="--stanza=db info")
    assert result == CommandResult(return_code=3, stdout="out", stderr="err")
    assert not result.ok
    mock_run.assert_called_once_with(
        ["pgbackrest", "--stanza=db", "info"],
        input=None,
        capture_output=True,
        timeout=None,
    )


@patch("single_kernel_postgresql.workload.vm.subprocess.run")
def test_vm_run_cmd_passes_stdin_and_timeout(mock_run, substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    mock_run.return_value = Mock(returncode=0, stdout=b"", stderr=b"")
    workload.run_cmd("cat", stdin="hello", timeout=30)
    assert mock_run.call_args.kwargs["input"] == b"hello"
    assert mock_run.call_args.kwargs["timeout"] == 30


@patch("single_kernel_postgresql.workload.vm.subprocess.run")
def test_vm_run_cmd_timeout_raises_without_replace(mock_run, substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    mock_run.side_effect = TimeoutExpired(cmd="pgbackrest", timeout=5)
    with pytest.raises(TimeoutExpired):
        workload.run_cmd("pgbackrest", timeout=5)


@patch("single_kernel_postgresql.workload.vm.subprocess.run")
def test_vm_run_cmd_timeout_replaced_with_result(mock_run, substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    mock_run.side_effect = TimeoutExpired(
        cmd="pgbackrest", timeout=5, output=b"partial", stderr=b"too slow"
    )
    result = workload.run_cmd("pgbackrest", timeout=5, use_errors_replace=True)
    assert result == CommandResult(return_code=124, stdout="partial", stderr="too slow")


def test_k8s_run_cmd_success(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    process = Mock()
    process.wait_output.return_value = ("info output", "warning")
    workload.container.exec.return_value = process
    result = workload.run_cmd("pgbackrest", args="info --output=json", timeout=30)
    assert result == CommandResult(return_code=0, stdout="info output", stderr="warning")
    assert result.ok
    workload.container.exec.assert_called_once_with(
        ["pgbackrest", "info", "--output=json"],
        user=workload.user,
        group=workload.group,
        timeout=30,
        stdin=None,
    )


def test_k8s_run_cmd_runs_as_workload_user_and_group(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    process = Mock()
    process.wait_output.return_value = ("", "")
    workload.container.exec.return_value = process
    workload.run_cmd("pgbackrest")
    assert workload.container.exec.call_args.kwargs["user"] == "postgres"
    assert workload.container.exec.call_args.kwargs["group"] == "postgres"


def test_k8s_run_cmd_exec_error_raises(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    process = Mock()
    process.wait_output.side_effect = ExecError(
        command=["pgbackrest", "backup"], exit_code=1, stdout="", stderr="boom"
    )
    workload.container.exec.return_value = process
    with pytest.raises(ExecError):
        workload.run_cmd("pgbackrest", "backup")


def test_k8s_run_cmd_exec_error_replaced_with_result(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    process = Mock()
    process.wait_output.side_effect = ExecError(
        command=["pgbackrest", "backup"], exit_code=1, stdout="progress", stderr="boom"
    )
    workload.container.exec.return_value = process
    result = workload.run_cmd("pgbackrest", "backup", use_errors_replace=True)
    assert result == CommandResult(return_code=1, stdout="progress", stderr="boom")
    assert not result.ok


def test_k8s_run_cmd_passes_stdin(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    process = Mock()
    process.wait_output.return_value = ("", "")
    workload.container.exec.return_value = process
    workload.run_cmd("cat", stdin="hello")
    assert workload.container.exec.call_args.kwargs["stdin"] == "hello"


# --- service control: VM


def _mock_vm_snap(**services):
    """Patch SnapCache in the VM workload module with a snap exposing the given services."""
    selected_snap = Mock()
    selected_snap.services = services
    cache = Mock()
    cache.__getitem__ = Mock(return_value=selected_snap)
    return patch("single_kernel_postgresql.workload.vm.snap.SnapCache", return_value=cache)


def test_vm_service_control_uses_snap_by_name(substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    with _mock_vm_snap() as mock_cache:
        workload.start_service(PGBACKREST_SERVICE)
        workload.stop_service(PGBACKREST_SERVICE)
        workload.restart_service(PGBACKREST_SERVICE)
        workload.reload_service(PGBACKREST_SERVICE)
    selected_snap = mock_cache.return_value.__getitem__.return_value
    selected_snap.start.assert_called_once_with(services=[PGBACKREST_SERVICE])
    selected_snap.stop.assert_called_once_with(services=[PGBACKREST_SERVICE])
    # Snap has no signal channel: reload is a restart.
    assert selected_snap.restart.call_count == 2


@pytest.mark.parametrize(
    "services,expected",
    [
        pytest.param({PGBACKREST_SERVICE: {"active": True}}, True, id="running"),
        pytest.param({PGBACKREST_SERVICE: {"active": False}}, False, id="inactive"),
        pytest.param({}, False, id="missing-from-snap-revision"),
    ],
)
def test_vm_service_is_running(substrate, workload, services, expected):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    with _mock_vm_snap(**services):
        assert workload.service_is_running(PGBACKREST_SERVICE) is expected


def test_vm_service_is_running_snap_error_reads_as_not_running(substrate, workload):
    if substrate == Substrates.K8S:
        pytest.skip("VM only")
        return
    with patch(
        "single_kernel_postgresql.workload.vm.snap.SnapCache",
        side_effect=snap.SnapNotFoundError("no snap"),
    ):
        assert workload.service_is_running(PGBACKREST_SERVICE) is False


# --- service control: K8s


def test_k8s_service_control_uses_container_by_name(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    workload.start_service(PGBACKREST_SERVICE)
    workload.stop_service(PGBACKREST_SERVICE)
    workload.restart_service(PGBACKREST_SERVICE)
    workload.container.start.assert_called_once_with(PGBACKREST_SERVICE)
    workload.container.stop.assert_called_once_with(PGBACKREST_SERVICE)
    workload.container.restart.assert_called_once_with(PGBACKREST_SERVICE)


def test_k8s_reload_running_service_sends_sighup(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    workload.container.can_connect.return_value = True
    service = Mock(current=ServiceStatus.ACTIVE)
    workload.container.pebble.get_services.return_value = [service]
    workload.reload_service(PGBACKREST_SERVICE)
    workload.container.send_signal.assert_called_once_with(signal.SIGHUP, PGBACKREST_SERVICE)
    workload.container.restart.assert_not_called()


def test_k8s_reload_stopped_service_restarts(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    workload.container.can_connect.return_value = True
    service = Mock(current=ServiceStatus.INACTIVE)
    workload.container.pebble.get_services.return_value = [service]
    workload.reload_service(PGBACKREST_SERVICE)
    workload.container.restart.assert_called_once_with(PGBACKREST_SERVICE)
    workload.container.send_signal.assert_not_called()


@pytest.mark.parametrize(
    "get_services,expected",
    [
        pytest.param([Mock(current=ServiceStatus.ACTIVE)], True, id="active"),
        pytest.param([Mock(current=ServiceStatus.ERROR)], False, id="error"),
        pytest.param([], False, id="missing-from-plan"),
    ],
)
def test_k8s_service_is_running(substrate, workload, get_services, expected):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    workload.container.can_connect.return_value = True
    workload.container.pebble.get_services.return_value = get_services
    assert workload.service_is_running(PGBACKREST_SERVICE) is expected


def test_k8s_service_is_running_before_connect_reads_as_not_running(substrate, workload):
    if substrate == Substrates.VM:
        pytest.skip("K8s only")
        return
    workload.container.can_connect.return_value = False
    assert workload.service_is_running(PGBACKREST_SERVICE) is False
    workload.container.pebble.get_services.assert_not_called()


# --- pgbackrest_logs paths


def test_vm_pgbackrest_logs_path():
    paths = VMPaths(pathops.LocalPath("/"), "16")
    assert str(paths.pgbackrest_logs) == ("/var/snap/charmed-postgresql/common/var/log/pgbackrest")


def test_k8s_pgbackrest_logs_path():
    paths = K8sPaths(pathops.LocalPath("/"), "16")
    assert str(paths.pgbackrest_logs) == str(paths.logs / "16/main/pgbackrest_logs")
