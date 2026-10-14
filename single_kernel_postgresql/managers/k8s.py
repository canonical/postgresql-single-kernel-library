#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes Manager.

This manager is responsible for handling operations related to Kubernetes,
such as interacting with the Kubernetes API for pod resources and trust.
"""

import itertools
import logging
from collections.abc import Callable

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Endpoints, Node, Pod, Service

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

    def __init__(
        self,
        state: CharmState,
        workload: K8sWorkload,
        on_deployed_without_trust: Callable[[], None] | None = None,
    ):
        super().__init__(state, workload, "pebble_manager")
        self.workload: K8sWorkload = workload  # type: ignore[assignment]
        # The trust-loss callback (set_unit_status lives on the charm); no-op when
        # the composition root does not wire it (the lib suites).
        self.on_deployed_without_trust = on_deployed_without_trust or (lambda: None)

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

    def delete_patroni_cluster_resources(self) -> None:
        """Delete the Patroni Endpoints and Services left by a previous cluster."""
        client = Client()
        name = f"patroni-{self.state.model.app.name}"
        for values in itertools.product(
            [Endpoints, Service],
            [name, f"{name}-config", f"{name}-sync"],
        ):
            try:
                client.delete(
                    values[0],
                    name=values[1],
                    namespace=self.state.model_name,
                )
                logger.debug(f"Deleted {values[0]} {values[1]}")
            except ApiError as e:
                # Ignore the error only when the resource doesn't exist.
                if e.status.code != 404:
                    raise e
                logger.debug(f"{values[0]} {values[1]} not found")

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

    # -- K8s resource management (port of the K8s charm's section A) ------------------

    def _client(self) -> Client:
        return Client()

    def create_services(self) -> None:
        """Create the kubernetes services for the primary and replicas endpoints (port)."""
        client = self._client()
        app_name = self.state.model.app.name
        pod0 = client.get(res=Pod, name=f"{app_name}-0", namespace=self.state.model.name)
        if not pod0 or not pod0.metadata:
            raise RuntimeError("Unable to get pod0")

        services = {
            "primary": "primary",
            "replicas": "replica",
        }
        for service_name_suffix, role_selector in services.items():
            name = f"{app_name}-{service_name_suffix}"
            service = Service(
                metadata=ObjectMeta(
                    name=name,
                    namespace=self.state.model.name,
                    ownerReferences=pod0.metadata.ownerReferences,
                    labels={"app.kubernetes.io/name": app_name},
                ),
                spec=ServiceSpec(
                    ports=[
                        ServicePort(name="api", port=8008, targetPort=8008),
                        ServicePort(name="database", port=5432, targetPort=5432),
                    ],
                    selector={
                        "app.kubernetes.io/name": app_name,
                        "cluster-name": f"patroni-{app_name}",
                        "role": role_selector,
                    },
                ),
            )
            client.apply(
                obj=service,
                name=name,
                namespace=self.state.model.name,
                force=True,
                field_manager=self.state.model.app.name,
            )

    def patch_pod_labels(self, member: str) -> None:
        """Add the labels required for replication to the given unit's pod (port)."""
        client = self._client()
        patch = {
            "metadata": {
                "labels": {
                    "application": "patroni",
                    "cluster-name": f"patroni-{self.state.model.app.name}",
                }
            }
        }
        client.patch(
            Pod,
            name=unit_name_to_pod_name(member),
            namespace=self.state.model.name,
            obj=patch,
        )

    def check_headless_service(self) -> None:
        """Raise if the headless endpoint service is missing (port).

        Puts the unit into error state so the operator can recreate the service
        and run ``juju resolve``.
        """
        client = self._client()
        app_name = self.state.model.app.name
        svc_name = f"{app_name}-endpoints"
        try:
            client.get(Service, name=svc_name, namespace=self.state.model.name)
        except ApiError as e:
            if e.status.code == 404:
                logger.error(
                    "error: headless service %r is missing - recreate it and run "
                    "'juju resolve' on each unit. See "
                    "https://github.com/canonical/postgresql-k8s-operator/issues/392",
                    svc_name,
                )
                raise RuntimeError from None
            raise

    def reparent_services_on_stop(self) -> None:
        """Reparent charm/Patroni-created resources onto the StatefulSet (port).

        Runs at stop: the resources are reparented onto pod0's owner so they are
        garbage-collected when the StatefulSet is deleted. Only ownerReferences
        are applied: the whole listed object would carry a stale resourceVersion
        (Patroni churns it) and 409, and would revert Patroni's in-flight changes.
        """
        app_name = self.state.model.app.name
        client = self._client()
        try:
            pod0 = client.get(res=Pod, name=f"{app_name}-0", namespace=self.state.model.name)
        except ApiError:
            logger.exception("failed to get first pod info")
            return
        if not pod0 or not pod0.metadata:
            logger.error("Failed to get pod0 details")
            return

        try:
            resources_to_patch = []
            for kind in [Endpoints, Service]:
                # Resources with Juju's created-by label.
                resources_to_patch.extend(
                    client.list(
                        kind,
                        namespace=self.state.model.name,
                        labels={"app.juju.is/created-by": app_name},
                    )
                )
                # Since Juju 3.6.13 the mutating webhook no longer processes
                # Endpoints: Patroni creates its own with these labels.
                resources_to_patch.extend(
                    client.list(
                        kind,
                        namespace=self.state.model.name,
                        labels={
                            "application": "patroni",
                            "cluster-name": f"patroni-{app_name}",
                        },
                    )
                )
        except ApiError:
            logger.exception("failed to get the k8s resources created by the charm and Patroni")
            return

        charm_managed = {
            app_name,
            f"{app_name}-endpoints",
            f"{app_name}-primary",
            f"{app_name}-replicas",
        }
        for resource in resources_to_patch:
            if (
                not resource.metadata
                or not resource.metadata.name
                or not resource.metadata.namespace
                or (type(resource) is Service and resource.metadata.name in charm_managed)
                or resource.metadata.ownerReferences == pod0.metadata.ownerReferences
            ):
                continue
            try:
                patch = type(resource)(
                    metadata=ObjectMeta(
                        name=resource.metadata.name,
                        namespace=resource.metadata.namespace,
                        ownerReferences=pod0.metadata.ownerReferences,
                    )
                )
                client.apply(
                    obj=patch,
                    name=resource.metadata.name,
                    namespace=resource.metadata.namespace,
                    force=True,
                )
            except ApiError:
                logger.exception(
                    f"failed to patch k8s {type(resource).__name__} {resource.metadata.name}"
                )

    def cleanup_old_cluster_resources(self) -> None:
        """Delete kubernetes services and endpoints from the previous deployment (port)."""
        if self.state.application.is_cluster_initialised:
            logger.debug("Early exit _cleanup_old_cluster_resources: cluster already initialised")
            return

        client = self._client()
        for kind, suffix in itertools.product([Service, Endpoints], ["", "-config", "-sync"]):
            try:
                client.delete(
                    res=kind,
                    name=f"{self.state.cluster_name}{suffix}",
                    namespace=self.state.model.name,
                )
                logger.info(f"deleted {kind.__name__}/{self.state.cluster_name}{suffix}")
            except ApiError as e:
                if e.status.code == 403:
                    self.on_deployed_without_trust()
                    return
                # Ignore the error only when the resource doesn't exist.
                if e.status.code != 404:
                    raise e
                logger.debug(f"{kind.__name__}/{self.state.cluster_name}{suffix} already absent")
