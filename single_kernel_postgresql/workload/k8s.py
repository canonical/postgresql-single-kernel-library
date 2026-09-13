# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes Workload."""

import logging
import pathlib
import shlex
import signal
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from charmlibs import pathops
from charmlibs.pathops import PathProtocol
from lightkube import Client
from lightkube.resources.core_v1 import Endpoints
from ops import Container, ModelError
from ops.pebble import ExecError, FileInfo, FileType, Plan, ServiceStatus

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError
from single_kernel_postgresql.config.literals import (
    DIR_PERMISSIONS_READONLY,
    K8S_ARCHIVE_PATH,
    K8S_DATA_PATH,
    K8S_DEBIAN_DATA_SYMLINK,
    K8S_LOGS_STORAGE_PATH,
    K8S_PATRONI_LOGS_PATH,
    K8S_PATRONI_LOGS_SYMLINK_PATH,
    K8S_PG_LOGS_PATH,
    K8S_PGBACK_REST_SERVER_SERVICE_NAME,
    K8S_PGBACKREST_LOGS_PATH,
    K8S_PGBACKREST_LOGS_SYMLINK_PATH,
    K8S_POSTGRESQL_LOGS_SYMLINK_PATH,
    K8S_POSTGRESQL_SERVICE_NAME,
    K8S_TEMP_STORAGE_PATH,
    K8S_TEMP_TABLESPACE_DIR,
    K8S_WAL_DIR,
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

    def service_exists(self, service: str) -> bool:
        """Whether the Pebble plan declares the named service."""
        if not self.container.can_connect():
            return False
        return len(self.container.pebble.get_services(names=[service])) > 0

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

    def empty_data_files(self) -> bool:
        """Empty the PostgreSQL data directory in preparation of backup restore."""
        # Clear all storage directories, not just data. The logs directory must be cleared
        # so that when new replicas join after restore, pg_basebackup can use the --waldir
        # option (which requires an empty directory).
        for path in [
            self.root / K8S_ARCHIVE_PATH,
            self.paths.data,
            self.root / K8S_LOGS_STORAGE_PATH,
            self.root / K8S_TEMP_STORAGE_PATH,
        ]:
            try:
                self.container.exec(["find", str(path), "-mindepth", "1", "-delete"]).wait_output()
            except ExecError as e:
                # If previous PITR restore was unsuccessful, there may be no such directory.
                if "No such file or directory" not in str(e.stderr):
                    logger.exception(
                        f"Failed to empty {path} in prep for backup restore", exc_info=e
                    )
                    raise
        return True

    def remove_cluster_info(
        self, cluster_name: str, namespace: str | None = None
    ) -> CommandResult:
        """Delete the K8s endpoints that track the cluster information, including its id.

        This is the same as "patronictl remove patroni-<name>", but the latter doesn't
        work after the database service is stopped on Pebble.

        Args:
            cluster_name: the Patroni cluster name ("patroni-<app>" on K8s).
            namespace: the K8s namespace of the patroni Endpoints.
        """
        if namespace is None:
            raise ValueError("the K8s cluster-info removal requires a namespace")
        client = Client()
        client.delete(Endpoints, name=cluster_name, namespace=namespace)
        client.delete(Endpoints, name=f"{cluster_name}-config", namespace=namespace)
        return CommandResult(return_code=0)

    def init_storage(self) -> None:
        """Create the PostgreSQL data directories, clearing stale data if needed.

        Port of the K8s charm's _create_pgdata/_ensure_pgdata_dirs_and_symlinks
        helpers. The stale-data clearing branch of the pebble-ready helper (replica
        joins on an initialized cluster) does not apply here: the restore action only
        runs on the leader unit.
        """
        logs_storage = self.root / K8S_LOGS_STORAGE_PATH
        waldir_path = logs_storage / self.paths.versioned_path / K8S_WAL_DIR
        temp_tablespace_path = (
            self.root / K8S_TEMP_STORAGE_PATH / self.paths.versioned_path / K8S_TEMP_TABLESPACE_DIR
        )
        archive_path = self.root / K8S_ARCHIVE_PATH / self.paths.versioned_path
        pgdata_path = self.paths.data

        # Create the pgdata directory on the storage mount (e.g., /var/lib/pg/data/16/main)
        if not self.container.exists(str(pgdata_path)):
            self.container.make_dir(
                str(pgdata_path),
                permissions=0o700,
                user=self.user,
                group=self.group,
                make_parents=True,
            )
        # Create the WAL directory (e.g., /var/lib/pg/logs/16/main/pg_wal)
        if not self.container.exists(str(waldir_path)):
            self.container.make_dir(
                str(waldir_path),
                permissions=0o700,
                user=self.user,
                group=self.group,
                make_parents=True,
            )
        for path in [
            logs_storage / K8S_PG_LOGS_PATH,
            logs_storage / K8S_PATRONI_LOGS_PATH,
            logs_storage / K8S_PGBACKREST_LOGS_PATH,
        ]:
            if not self.container.exists(str(path)):
                self.container.make_dir(
                    str(path),
                    permissions=0o755,
                    user=self.user,
                    group=self.group,
                    make_parents=True,
                )
            self.container.exec(["chmod", "755", str(path)]).wait()
            self.container.exec(["chown", f"{self.user}:{self.group}", str(path)]).wait()
        # Create the temp tablespace directory (e.g., /var/lib/pg/temp/16/main/pgsql_tmp)
        if not self.container.exists(str(temp_tablespace_path)):
            self.container.make_dir(
                str(temp_tablespace_path),
                permissions=0o700,
                user=self.user,
                group=self.group,
                make_parents=True,
            )
        # Create the archive directory (e.g., /var/lib/pg/archive/16/main)
        if not self.container.exists(str(archive_path)):
            self.container.make_dir(
                str(archive_path),
                permissions=0o700,
                user=self.user,
                group=self.group,
                make_parents=True,
            )
        # Create a debian-style symlink at the version level:
        # /var/lib/postgresql/16 -> /var/lib/pg/data/16
        # This keeps /var/lib/postgresql/16/main as a valid alias for the real pgdata
        # directory for any tools that rely on the traditional Debian path layout.
        # Note: This symlink is on ephemeral storage and may not persist across container
        # restarts. It gets recreated on each pebble-ready event.
        # /var/lib/postgresql is created by the postgresql Debian package and exists in
        # the container image, so no make_dir is needed before creating the symlink.
        self.container.exec([
            "ln",
            "-sfn",
            str(self.root / K8S_DATA_PATH / "16"),
            str(self.root / K8S_DEBIAN_DATA_SYMLINK),
        ]).wait()
        self.container.exec([
            "chown",
            "-h",
            f"{self.user}:{self.group}",
            str(self.root / K8S_DEBIAN_DATA_SYMLINK),
        ]).wait()
        self._ensure_log_symlink(K8S_PG_LOGS_PATH, self.root / K8S_POSTGRESQL_LOGS_SYMLINK_PATH)
        self._ensure_log_symlink(K8S_PATRONI_LOGS_PATH, self.root / K8S_PATRONI_LOGS_SYMLINK_PATH)
        self._ensure_log_symlink(
            K8S_PGBACKREST_LOGS_PATH, self.root / K8S_PGBACKREST_LOGS_SYMLINK_PATH
        )
        # Also, fix the permissions from the parent directory.
        for path in [
            self.root / K8S_ARCHIVE_PATH,
            self.root / K8S_DATA_PATH,
            logs_storage,
            self.root / K8S_TEMP_STORAGE_PATH,
        ]:
            self.container.exec(["chown", f"{self.user}:{self.group}", str(path)]).wait()

    def _ensure_log_symlink(self, target_name: str, symlink_path: PathProtocol) -> None:
        """Ensure symlink_path points to target in the logs storage."""
        path_info = self._get_container_path_info(str(symlink_path))
        if path_info is not None:
            if path_info.type == FileType.DIRECTORY:
                self._remove_empty_log_directory(str(symlink_path))
            elif path_info.type != FileType.SYMLINK:
                logger.error(
                    "error: %s exists but is neither a symlink nor a directory and cannot"
                    " be replaced with a symlink to the logs storage - remove it manually"
                    " and run 'juju resolve' on each unit to recover.",
                    symlink_path,
                )
                raise RuntimeError from None

        logs_storage = self.root / K8S_LOGS_STORAGE_PATH
        self.container.exec([
            "ln",
            "-sfn",
            str(logs_storage / self.paths.versioned_path / target_name),
            str(symlink_path),
        ]).wait()
        self.container.exec([
            "chown",
            "-h",
            f"{self.user}:{self.group}",
            str(symlink_path),
        ]).wait()

    def _get_container_path_info(self, path: str) -> FileInfo | None:
        """Return file info for a specific container path without dereferencing symlinks."""
        path_obj = pathlib.PurePosixPath(path)
        path_infos = self.container.list_files(str(path_obj.parent), pattern=path_obj.name)
        if not path_infos:
            return None
        return path_infos[0]

    def _remove_empty_log_directory(self, symlink_path: str) -> None:
        """Remove a log directory at symlink_path only when it is empty."""
        if self.container.list_files(symlink_path):
            logger.error(
                "error: %s is a non-empty directory and cannot be replaced with a"
                " symlink to the logs storage - move or remove its contents manually"
                " and run 'juju resolve' on each unit to recover.",
                symlink_path,
            )
            raise RuntimeError from None

        self.container.remove_path(symlink_path)

    def pitr_bootstrap_failure_logs(self) -> tuple[str, bool]:
        """Fetch the postgresql pebble service logs for PITR bootstrap-failure scanning.

        Falls back to concatenating the patroni log files when the pebble logs client
        is unavailable (Juju 2).
        """
        try:
            log_exec = self.container.pebble.exec(
                ["pebble", "logs", K8S_POSTGRESQL_SERVICE_NAME, "-n", "all"],
                combine_stderr=True,
            )
            return log_exec.wait_output()[0], False
        except ExecError:  # For Juju 2.
            patroni_logs_dir = self.root / K8S_LOGS_STORAGE_PATH / K8S_PATRONI_LOGS_PATH
            current = self.container.exec([
                "cat",
                str(patroni_logs_dir / "patroni.log"),
            ]).wait_output()[0]
            older = self.container.exec([
                "find",
                str(patroni_logs_dir) + "/",
                "-name",
                "patroni.log.*",
                "-exec",
                "cat",
                "{}",
                "+",
            ]).wait_output()[0]
            return f"{current}\n{older}", True

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
