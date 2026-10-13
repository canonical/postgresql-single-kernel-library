# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the workload classes: resource discovery and snap installation."""

from unittest.mock import Mock, call, mock_open, patch

import pytest
from charmlibs import snap
from lightkube.core.exceptions import ApiError
from single_kernel_postgresql.config.exceptions import DeployedWithoutTrustError
from single_kernel_postgresql.managers.k8s import K8sManager
from single_kernel_postgresql.workload.vm import VMWorkload


def test_vm_get_available_resources_uses_cpu_count_and_available_memory():
    workload = VMWorkload(".")
    with (
        patch("os.cpu_count", return_value=4),
        patch.object(VMWorkload, "get_available_memory", return_value=8_000_000_000),
    ):
        assert workload.get_available_resources() == (4, 8_000_000_000)


def test_vm_get_available_resources_falls_back_to_one_cpu_when_undetectable():
    workload = VMWorkload(".")
    with (
        patch("os.cpu_count", return_value=None),
        patch.object(VMWorkload, "get_available_memory", return_value=1_000),
    ):
        assert workload.get_available_resources() == (1, 1_000)


def test_vm_get_available_memory_parses_memtotal_kb_to_bytes():
    """MemTotal is read in kB from /proc/meminfo and converted to bytes (* 1024)."""
    workload = VMWorkload(".")
    meminfo = (
        "SwapTotal:             0 kB\nMemTotal:       16089488 kB\nMemFree:          799284 kB\n"
    )
    with patch("builtins.open", mock_open(read_data=meminfo)):
        assert workload.get_available_memory() == 16089488 * 1024


def test_vm_get_available_memory_returns_zero_when_memtotal_absent():
    workload = VMWorkload(".")
    with patch("builtins.open", mock_open(read_data="")):
        assert workload.get_available_memory() == 0


def _node(cpu: str, memory: str) -> Mock:
    node = Mock()
    node.status.allocatable = {"cpu": cpu, "memory": memory}
    return node


def _pod(node_name: str = "node-1", container_limits: dict | None = None) -> Mock:
    pod = Mock()
    pod.spec.nodeName = node_name
    container = Mock()
    container.name = "postgresql"
    container.resources.limits = container_limits or {}
    pod.spec.containers = [container]
    return pod


@pytest.fixture
def k8s_manager() -> K8sManager:
    state = Mock()
    state.peer.unit_name = "postgresql-k8s/0"
    state.model_name = "test-model"
    return K8sManager(state, Mock(name="workload"))


def test_k8s_get_available_resources_reads_node_allocatable(k8s_manager):
    """get_node_cpu_cores and get_node_allocable_memory each look up the pod, then its node."""
    client = Mock()
    client.get.side_effect = [
        _pod(),
        _node(cpu="4", memory="8Gi"),
        _pod(),
        _node("4", "8Gi"),
        _pod(),
    ]
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        assert k8s_manager.get_available_resources() == (4, 8 * 1024**3)


def test_k8s_get_available_resources_constrains_to_container_limits(k8s_manager):
    client = Mock()
    limits = {"cpu": "2", "memory": "1Gi"}
    client.get.side_effect = [
        _pod(),
        _node(cpu="4", memory="8Gi"),
        _pod(),
        _node(cpu="4", memory="8Gi"),
        _pod(container_limits=limits),
    ]
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        assert k8s_manager.get_available_resources() == (2, 1024**3)


def test_k8s_get_available_resources_ignores_container_limits_above_node_allocatable(
    k8s_manager,
):
    """A container limit looser than the node's own allocatable resources is not a constraint."""
    client = Mock()
    limits = {"cpu": "16", "memory": "64Gi"}
    client.get.side_effect = [
        _pod(),
        _node(cpu="4", memory="8Gi"),
        _pod(),
        _node(cpu="4", memory="8Gi"),
        _pod(container_limits=limits),
    ]
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        assert k8s_manager.get_available_resources() == (4, 8 * 1024**3)


def test_k8s_get_available_resources_raises_trust_error_on_403(k8s_manager):
    response = Mock(json=Mock(return_value={"code": 403, "message": "Forbidden"}))
    error = ApiError(response=response)
    client = Mock()
    client.get.side_effect = error
    with (
        patch("single_kernel_postgresql.managers.k8s.Client", return_value=client),
        pytest.raises(DeployedWithoutTrustError),
    ):
        k8s_manager.get_available_resources()


def test_k8s_get_available_resources_reraises_non_403_api_errors(k8s_manager):
    response = Mock(json=Mock(return_value={"code": 500, "message": "Internal error"}))
    error = ApiError(response=response)
    client = Mock()
    client.get.side_effect = error
    with (
        patch("single_kernel_postgresql.managers.k8s.Client", return_value=client),
        pytest.raises(ApiError),
    ):
        k8s_manager.get_available_resources()


@pytest.fixture
def refresh_versions_dir(tmp_path, monkeypatch):
    """A working directory with a minimal refresh_versions.toml for revision=None installs."""
    (tmp_path / "refresh_versions.toml").write_text(
        'workload = "16.15"\ncharm = "16/1.19.0"\n\n'
        '[snap]\nname = "charmed-postgresql"\n\n[snap.revisions]\nx86_64 = "416"\n'
    )
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_install_snap_package(refresh_versions_dir):
    workload = VMWorkload(".")
    with (
        patch("single_kernel_postgresql.workload.vm.snap.SnapCache") as _snap_cache,
        patch("single_kernel_postgresql.workload.vm.ensure_snap_oom_protection"),
    ):
        _snap_package = _snap_cache.return_value.__getitem__.return_value
        _snap_package.present = False
        _revision = "416"

        # Test for problem with snap update.
        _snap_package.ensure.side_effect = snap.SnapError("update failed")
        with pytest.raises(snap.SnapError):
            workload.install_snap_package(revision=None)
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_cache.assert_called_once_with()
        _snap_package.ensure.assert_called_once_with(snap.SnapState.Present, revision=_revision)

        # Test for problem with snap missing.
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        _snap_package.ensure.side_effect = snap.SnapNotFoundError
        with pytest.raises(snap.SnapNotFoundError):
            workload.install_snap_package(revision=None)
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_cache.assert_called_once_with()
        _snap_package.ensure.assert_called_once_with(snap.SnapState.Present, revision=_revision)

        # Test for correct install.
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        _snap_package.ensure.side_effect = None
        workload.install_snap_package(revision=None)
        _snap_cache.assert_called_once_with()
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_package.ensure.assert_called_once_with(snap.SnapState.Present, revision=_revision)
        _snap_package.hold.assert_called_once_with()

        # Test with revision
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        workload.install_snap_package(revision="42")
        _snap_cache.assert_called_once_with()
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_package.ensure.assert_called_once_with(snap.SnapState.Present, revision="42")
        _snap_package.hold.assert_called_once_with()

        # Test with refresh
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        _snap_package.present = True
        _refresh = Mock()
        workload.install_snap_package(revision="42", refresh=_refresh)
        _snap_cache.assert_called_once_with()
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_package.ensure.assert_called_once_with(snap.SnapState.Present, revision="42")
        _refresh.update_snap_revision.assert_called_once_with()
        _snap_package.hold.assert_called_once_with()

        # Test without refresh
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        workload.install_snap_package(revision="42")
        _snap_cache.assert_called_once_with()
        _snap_cache.return_value.__getitem__.assert_called_once_with("charmed-postgresql")
        _snap_package.ensure.assert_not_called()
        _snap_package.hold.assert_not_called()
        _refresh.update_snap_revision.assert_called_once_with()

        # Test with invalid machine architecture
        _snap_cache.reset_mock()
        _snap_package.reset_mock()
        with patch("platform.machine") as _machine:
            _machine.return_value = "missingarch"
            with pytest.raises(KeyError):
                workload.install_snap_package(revision=None)
        assert not _snap_package.ensure.called
        assert not _snap_package.hold.called


@pytest.mark.parametrize("present,refreshing", [(False, False), (True, False), (True, True)])
def test_install_snap_package_configures_oom_before_install(
    refresh_versions_dir, present, refreshing
):
    with (
        patch("single_kernel_postgresql.workload.vm.snap.SnapCache") as cache,
        patch(
            "single_kernel_postgresql.workload.vm.ensure_snap_oom_protection",
            return_value=-898,
        ) as protect,
    ):
        calls = Mock()
        calls.attach_mock(protect, "protect")
        calls.attach_mock(cache, "cache")
        package = cache.return_value.__getitem__.return_value
        package.present = present
        refresh = Mock() if refreshing else None

        VMWorkload(".").install_snap_package(revision="416", refresh=refresh)

        assert calls.mock_calls[:2] == [call.protect("charmed-postgresql"), call.cache()]
        if not present or refreshing:
            package.ensure.assert_called_once_with(snap.SnapState.Present, revision="416")
        else:
            package.ensure.assert_not_called()
        package.start.assert_not_called()
        package.restart.assert_not_called()
        package.stop.assert_not_called()


def test_install_snap_package_stops_on_oom_failure(refresh_versions_dir):
    with (
        patch("single_kernel_postgresql.workload.vm.snap.SnapCache") as cache,
        patch(
            "single_kernel_postgresql.workload.vm.ensure_snap_oom_protection",
            side_effect=snap.SnapError("cannot protect"),
        ),
    ):
        with pytest.raises(snap.SnapError, match="cannot protect"):
            VMWorkload(".").install_snap_package(revision="416")

        cache.assert_not_called()
