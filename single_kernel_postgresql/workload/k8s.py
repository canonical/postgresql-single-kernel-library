# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes Workload."""

import logging
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from charmlibs import pathops
from charmlibs.pathops import PathProtocol
from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Node, Pod
from ops import Container, ModelError
from ops.pebble import Plan, ServiceStatus

from single_kernel_postgresql.config.exceptions import (
    DeployedWithoutTrustError,
    PostgreSQLFileOperationError,
)
from single_kernel_postgresql.config.literals import (
    DIR_PERMISSIONS_READONLY,
    K8S_POSTGRESQL_SERVICE_NAME,
)
from single_kernel_postgresql.utils import (
    any_cpu_to_cores,
    any_memory_to_bytes,
    unit_name_to_pod_name,
)
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.workload.paths.base import Paths as BasePaths
from single_kernel_postgresql.workload.paths.k8s import K8sPaths

logger = logging.getLogger(__name__)


class K8sWorkload(BaseWorkload):
    """Kubernetes PostgreSQL Workload."""

    def __init__(self, charm_dir: Path, container: Container, *, unit_name: str, namespace: str):
        """Initialize workload.

        Args:
            charm_dir: the path to charm code.
            container: the Container instance.
            unit_name: the Juju unit name (e.g. "postgresql-k8s/0"), used to resolve
                this unit's own pod for K8s resource-limit lookups.
            namespace: the Juju model name, i.e. the K8s namespace the pod lives in.
        """
        super().__init__(charm_dir=charm_dir)
        self.container = container
        self.unit_name = unit_name
        self.namespace = namespace
        self._paths: BasePaths | None = None

    def install(self) -> None:
        """Install the workload."""
        pass

    def get_current_layer(self) -> Plan:
        """Get the current Pebble layer."""
        return self.container.get_plan()

    def reconcile_pebble_layer(self, new_layer, replan: bool = False) -> None:
        """Reconcile the Pebble layer."""
        current_layer = self.get_current_layer()
        # Check if there are any changes to layer services.
        if current_layer.services != new_layer.services:
            # Changes were made, add the new layer.
            self.container.add_layer(K8S_POSTGRESQL_SERVICE_NAME, new_layer, combine=True)
            logging.info("Added updated layer 'postgresql' to Pebble plan")
            if replan:
                self.container.replan()
                logging.info("Restarted postgresql service")
        if current_layer.checks != new_layer.checks:
            # Changes were made, add the new layer.
            self.container.add_layer(K8S_POSTGRESQL_SERVICE_NAME, new_layer, combine=True)
            logging.info("Updated health checks")

    def is_service_started(self, paused: bool | None = False) -> bool:
        """Check if the snap service is running.

        Set paused=True if the process was intentionally paused.
        """
        raise NotImplementedError

    def start_service_only(self):
        """Start the actual service only (snap / pebble)."""
        raise NotImplementedError

    def is_patroni_running(self) -> bool:
        """Check if the Patroni service is running."""
        if not self.container.can_connect():
            return False

        services = self.container.pebble.get_services(names=[K8S_POSTGRESQL_SERVICE_NAME])
        if len(services) == 0:
            return False

        return services[0].current == ServiceStatus.ACTIVE

    def run_cmd(
        self,
        command: str,
        args: str | None = None,
        use_errors_replace: bool = False,
        stdin: str | None = None,
    ) -> SimpleNamespace:
        """Run Command in CLI."""
        raise NotImplementedError

    def is_failed(self) -> bool:
        """Check if snap service failed."""
        raise NotImplementedError

    def stop(self) -> None:
        """Stop the PostgreSQL service."""
        ...

    def start_service(self):
        """Start the PostgreSQL service."""
        ...

    def get_workload_version(self) -> str:
        """Get the workload version."""
        raise NotImplementedError

    @contextmanager
    def temp_file(
        self,
        mode: str = "w+b",
        data: str | None = None,
        encoding: str | None = None,
        directory: PathProtocol | None = None,
        delete: bool = True,
        chown: str | None = None,
        *,
        errors: str | None = None,
        suffix: str | None = None,
    ) -> Generator[PathProtocol, None, None]:
        """Create a temporary file in the container and return the file path.

        Args:
            mode: file mode
            data: Optional string data to write to the file.
            encoding: encoding for data writing (defaults to utf-8).
            directory: Optional directory path.
            delete: If True, delete the file when context exits.
            errors: Error handling mode
            suffix: Optional suffix to append to filename.
            chown: Optional user to chown the file to after creation.

        Yields:
            PathProtocol: Path object representing the temporary file.

        Raises:
            PebbleError: if file operations fail.
        """
        # PathProtocol exposes text operations.
        temp_dir_path = directory or self.paths.temp
        self.mkdir(
            temp_dir_path,
            mode=DIR_PERMISSIONS_READONLY,
            parents=True,
            exist_ok=True,
        )

        temp_filename = "temp_{}{}".format(uuid.uuid4().hex, suffix or "")
        file_path = temp_dir_path / temp_filename

        try:
            if data is not None:
                file_path.write_text(data)
            yield file_path
        finally:
            if delete:
                try:
                    self.unlink(file_path, missing_ok=True)
                except PostgreSQLFileOperationError as e:
                    logger.warning(f"Failed to delete temporary file {file_path}: {e}")

    @property
    def user(self) -> str:
        """The OS user that owns workload files in the K8s container."""
        return "postgres"

    @property
    def group(self) -> str:
        """The OS group that owns workload files in the K8s container."""
        return "postgres"

    @property
    def tls_file_mode(self) -> int:
        """K8s pushes TLS material owner-read-only, matching the original charm."""
        return 0o400

    @property
    def root(self) -> PathProtocol:
        """Return the root path for container filesystem.

        For K8s containers, use PathOps ContainerPath for container API.
        ContainerPath handles pull/push operations internally via its read_text/write_text methods.

        Returns:
            PathProtocol: ContainerPath instance bound to the container.
        """
        return pathops.ContainerPath("/", container=self.container)

    @property
    def paths(self) -> BasePaths:
        """Return Workload's paths.

        This is cached to avoid recreating K8sPaths on every access, since self.root
        is a ContainerPath bound to self.container.
        """
        if self._paths is None:
            # access self.root which depends on self.container
            # this may raise RuntimeError if container isn't set, which is expected
            # during initialization before container is available
            root_path = self.root
            self._paths = K8sPaths(root_path, self.get_postgresql_version().split(".")[0])
        return self._paths

    @property
    def workload_present(self) -> bool:
        """Check if the container is ready and connected.

        Returns:
            bool: True if container is ready and can connect, False otherwise.
        """
        try:
            container = self.container
            return container.can_connect()
        except (RuntimeError, ModelError):
            return False

    def get_available_memory(self) -> int:
        """Returns the system available memory in bytes."""
        raise NotImplementedError

    def _get_node_name_for_pod(self) -> str:
        """Return the node name for this unit's own pod."""
        client = Client()
        pod = client.get(Pod, name=unit_name_to_pod_name(self.unit_name), namespace=self.namespace)
        if pod.spec and pod.spec.nodeName:
            return pod.spec.nodeName
        raise RuntimeError("Pod doesn't exist")

    def get_resources_limits(self, container_name: str) -> dict:
        """Return resources limits for a given container.

        Args:
            container_name: name of the container to get resources limits for.
        """
        client = Client()
        pod = client.get(Pod, name=unit_name_to_pod_name(self.unit_name), namespace=self.namespace)
        if pod.spec:
            for container in pod.spec.containers:
                if container.name == container_name and container.resources:
                    return container.resources.limits or {}
        return {}

    def get_node_allocable_memory(self) -> int:
        """Return the allocable memory in bytes for the current K8s node."""
        client = Client()
        node = client.get(Node, name=self._get_node_name_for_pod(), namespace=self.namespace)  # type: ignore
        return any_memory_to_bytes(node.status.allocatable["memory"])

    def get_node_cpu_cores(self) -> int:
        """Return the number of CPU cores for the current K8s node."""
        client = Client()
        node = client.get(Node, name=self._get_node_name_for_pod(), namespace=self.namespace)  # type: ignore
        return any_cpu_to_cores(node.status.allocatable["cpu"])

    def get_available_resources(self) -> tuple[int, int]:
        """Returns the available (cpu_cores, memory_bytes) for the workload.

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
