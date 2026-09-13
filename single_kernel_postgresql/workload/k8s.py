# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes Workload."""

import logging
import shlex
import signal
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from charmlibs import pathops
from charmlibs.pathops import PathProtocol
from ops import Container, ModelError
from ops.pebble import ExecError, Plan, ServiceStatus

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError
from single_kernel_postgresql.config.literals import (
    DIR_PERMISSIONS_READONLY,
    K8S_PGBACK_REST_SERVER_SERVICE_NAME,
    K8S_POSTGRESQL_SERVICE_NAME,
)
from single_kernel_postgresql.workload.base import BackupConfig, BaseWorkload, CommandResult
from single_kernel_postgresql.workload.paths.base import Paths as BasePaths
from single_kernel_postgresql.workload.paths.k8s import K8sPaths

logger = logging.getLogger(__name__)


class K8sWorkload(BaseWorkload):
    """Kubernetes PostgreSQL Workload."""

    def __init__(self, charm_dir: Path, container: Container):
        """Initialize workload.

        Args:
            charm_dir: the path to charm code.
            container: the Container instance.
        """
        super().__init__(charm_dir=charm_dir)
        self.container = container
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
        timeout: float | None = None,
    ) -> CommandResult:
        """Run a command in the workload container as the workload user and group."""
        command_list = shlex.split(command)
        if args:
            command_list += shlex.split(args)
        logger.debug("Running command %s", " ".join(command_list))
        process = self.container.exec(
            command_list,
            user=self.user,
            group=self.group,
            timeout=timeout,
            stdin=stdin,
        )
        try:
            stdout, stderr = process.wait_output()
        except ExecError as e:
            if not use_errors_replace:
                raise
            return CommandResult(
                return_code=e.exit_code,
                stdout=e.stdout or "",
                stderr=e.stderr or "",
            )
        return CommandResult(return_code=0, stdout=stdout, stderr=stderr)

    @property
    def backup_config(self) -> BackupConfig:
        """Return the K8s pgBackRest invocation settings.

        pgBackRest reads /etc/pgbackrest.conf by default, so no --config flag
        is passed (conf_path None) and the config directory holds only the
        default-location file the charm pushes.
        """
        return BackupConfig(
            executable="pgbackrest",
            conf_path=None,
            logs_path=str(self.paths.pgbackrest_logs),
            bin_path="/usr/lib/postgresql",
            service=K8S_PGBACK_REST_SERVER_SERVICE_NAME,
            storage_path=str(self.paths.tls),
            tls_ca_chain_path=f"{self.paths.tls}/pgbackrest-tls-ca-chain.crt",
            extra_args=(),
        )

    def is_failed(self) -> bool:
        """Check if snap service failed."""
        raise NotImplementedError

    def stop(self) -> None:
        """Stop the PostgreSQL service."""
        ...

    def start_service(self, service: str) -> None:
        """Start a named Pebble service."""
        self.container.start(service)

    def stop_service(self, service: str) -> None:
        """Stop a named Pebble service."""
        self.container.stop(service)

    def restart_service(self, service: str) -> None:
        """Restart a named Pebble service."""
        self.container.restart(service)

    def reload_service(self, service: str) -> None:
        """Reload a named Pebble service.

        Sends SIGHUP to the service when it is running; restarts it otherwise.
        """
        if self.service_is_running(service):
            logger.debug("Sending SIGHUP to %s to reload configuration", service)
            self.container.send_signal(signal.SIGHUP, service)
        else:
            self.container.restart(service)

    def service_is_running(self, service: str) -> bool:
        """Check whether a named Pebble service is running.

        A layer revision predating the service omits it from the plan; and
        Pebble cannot be asked before the container connects. Both read as
        not running.
        """
        if not self.container.can_connect():
            return False
        services = self.container.pebble.get_services(names=[service])
        if len(services) == 0:
            return False
        return services[0].current == ServiceStatus.ACTIVE

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
