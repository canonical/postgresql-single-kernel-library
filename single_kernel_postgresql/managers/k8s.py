#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes Manager.

This manager is responsible for handling operations related to Kubernetes,
such as interacting with the Kubernetes API for pod resources and trust.
"""

import logging

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Node, Pod

from single_kernel_postgresql.config.exceptions import DeployedWithoutTrustError
from single_kernel_postgresql.config.literals import K8S_POSTGRESQL_SERVICE_NAME
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import (
    any_cpu_to_cores,
    any_memory_to_bytes,
    unit_name_to_pod_name,
)
from single_kernel_postgresql.workload.k8s import K8sWorkload

logger = logging.getLogger(__name__)


class K8sManager(BaseManager):
    """PostgreSQL Kubernetes Manager.

    This manager is responsible for handling operations related to Kubernetes.
    """

    def __init__(self, state: CharmState, workload: K8sWorkload):
        super().__init__(state, workload, "pebble_manager")
        self.workload: K8sWorkload = workload  # type: ignore[assignment]

    def _get_node_name_for_pod(self) -> str:
        """Return the node name for this unit's own pod."""
        client = Client()
        pod = client.get(
            Pod,
            name=unit_name_to_pod_name(self.state.peer.unit_name),
            namespace=self.state.model_name,
        )
        if pod.spec and pod.spec.nodeName:
            return pod.spec.nodeName
        raise RuntimeError("Pod doesn't exist")

    def get_resources_limits(self, container_name: str) -> dict:
        """Return resources limits for a given container.

        Args:
            container_name: name of the container to get resources limits for.
        """
        client = Client()
        pod = client.get(
            Pod,
            name=unit_name_to_pod_name(self.state.peer.unit_name),
            namespace=self.state.model_name,
        )
        if pod.spec:
            for container in pod.spec.containers:
                if container.name == container_name and container.resources:
                    return container.resources.limits or {}
        return {}

    def get_node_allocable_memory(self) -> int:
        """Return the allocable memory in bytes for the current K8s node."""
        client = Client()
        node = client.get(Node, name=self._get_node_name_for_pod())
        return any_memory_to_bytes(node.status.allocatable["memory"])  # type: ignore

    def get_node_cpu_cores(self) -> int:
        """Return the number of CPU cores for the current K8s node."""
        client = Client()
        node = client.get(Node, name=self._get_node_name_for_pod())
        return any_cpu_to_cores(node.status.allocatable["cpu"])  # type: ignore

    def get_available_resources(self) -> tuple[int, int]:
        """Return the available (cpu_cores, memory_bytes) for the workload.

        Raises:
            DeployedWithoutTrustError: if the K8s API denies access (403), meaning the
                app wasn't deployed with ``--trust``.
        """
        try:
            cpu_cores = self.get_node_cpu_cores()
            allocable_memory = self.get_node_allocable_memory()
            container_limits = self.get_resources_limits(
                container_name=K8S_POSTGRESQL_SERVICE_NAME
            )
        except ApiError as e:
            if e.status.code == 403:
                raise DeployedWithoutTrustError from e
            raise

        if "cpu" in container_limits:
            cpu_str = container_limits["cpu"]
            constrained_cpu = any_cpu_to_cores(cpu_str)
            if constrained_cpu < cpu_cores:
                logger.debug(f"CPU constrained to {cpu_str} cores from resource limit")
                cpu_cores = constrained_cpu
        if "memory" in container_limits:
            memory_str = container_limits["memory"]
            constrained_memory = any_memory_to_bytes(memory_str)
            if constrained_memory < allocable_memory:
                logger.debug(f"Memory constrained to {memory_str} from resource limit")
                allocable_memory = constrained_memory

        return cpu_cores, allocable_memory

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        return [GeneralStatuses.ACTIVE_IDLE.value]
