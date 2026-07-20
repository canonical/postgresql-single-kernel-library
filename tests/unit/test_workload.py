# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for get_available_resources: substrate-specific resource discovery."""

from unittest.mock import Mock, mock_open, patch

import pytest
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
