# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Machine Workload."""

import logging
import os
import pathlib
import platform
import pwd
import shutil
import subprocess
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from subprocess import run
from types import SimpleNamespace

import charm_refresh
import tomli
from charmlibs import pathops, snap
from charmlibs.pathops import PathProtocol

from single_kernel_postgresql.config.literals import SNAP, SNAP_USER
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.workload.paths.base import Paths as BasePaths
from single_kernel_postgresql.workload.paths.vm import VMPaths

logger = logging.getLogger(__name__)


class VMWorkload(BaseWorkload):
    """Machine PostgreSQL Workload."""

    def __init__(self, charm_dir: Path):
        super().__init__(charm_dir)

    def is_storage_attached(self) -> bool:
        """Returns if storage is attached.

        This is VM specific.
        """
        try:
            # Storage path is constant
            subprocess.check_call(["/usr/bin/mountpoint", "-q", self.paths.data])  # noqa: S603 #type: ignore
            return True
        except subprocess.CalledProcessError:
            return False

    def install(self) -> None:
        """Install the workload."""

    def create_snap_alias(self, alias_name: str) -> None:
        """Create alias for the snap binary."""
        cache = snap.SnapCache()
        postgres_snap = cache[charm_refresh.snap_name()]
        try:
            postgres_snap.alias(alias_name)
        except snap.SnapError:
            logger.warning("Unable to create %s alias", alias_name)

    def install_snap_package(
        self, *, revision: str | None, refresh: charm_refresh.Machines | None = None
    ) -> None:
        """Installs PostgreSQL snap.

        Args:
            revision: snap revision to install.
            refresh: refresh class; will refresh installed snap if not `None`
        """
        if revision is None:
            if refresh is not None:
                raise ValueError
            # TODO: consider using `self.refresh.pinned_snap_revision` instead (requires waiting
            # for refresh peer relation to be ready before installing snap)
            with pathlib.Path("refresh_versions.toml").open("rb") as file:
                revisions = tomli.load(file)["snap"]["revisions"]
            try:
                revision = revisions[platform.machine()]
            except KeyError:
                logger.error("Unavailable snap architecture %s", platform.machine())
                raise
        try:
            snap_cache = snap.SnapCache()
            snap_package = snap_cache[charm_refresh.snap_name()]
            if not snap_package.present or refresh is not None:
                snap_package.ensure(snap.SnapState.Present, revision=revision)
                if refresh is not None:
                    refresh.update_snap_revision()
                snap_package.hold()
        except (snap.SnapError, snap.SnapNotFoundError) as e:
            logger.error(
                "An exception occurred when installing %s. Reason: %s",
                charm_refresh.snap_name(),
                str(e),
            )
            raise

    def start_patroni(self) -> bool:
        """Start Patroni service using snap.

        Returns:
            Whether the service started successfully.
        """
        try:
            logger.debug("Starting Patroni...")
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            selected_snap.start(services=["patroni"])
            return selected_snap.services["patroni"]["active"]
        except snap.SnapError as e:
            error_message = "Failed to start patroni snap service"
            logger.exception(error_message, exc_info=e)
            return False

    def is_patroni_running(self) -> bool:
        """Check if the Patroni service is running."""
        try:
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            return selected_snap.services["patroni"]["active"]
        except snap.SnapError as e:
            logger.debug(f"Failed to check Patroni service: {e}")
            return False

    def is_service_started(self, paused: bool | None = False) -> bool:
        """Check if the snap service is running.

        Set paused=True if the process was intentionally paused.
        """
        raise NotImplementedError

    def start_service_only(self):
        """Start the actual service only (snap / pebble)."""
        raise NotImplementedError

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

    def get_system_identifier(self) -> tuple[str | None, str | None]:
        """Returns the PostgreSQL system identifier from this instance."""

        def demote():
            pw_record = pwd.getpwnam(SNAP_USER)

            def result():
                os.setgid(pw_record.pw_gid)
                os.setuid(pw_record.pw_uid)

            return result

        major_version = self.get_postgresql_version().split(".")[0]
        # Input is hardcoded
        process = run(  # noqa: S603
            [
                f"{SNAP}/usr/lib/postgresql/{major_version}/bin/pg_controldata",
                str(self.paths.data),
            ],
            capture_output=True,
            preexec_fn=demote(),
        )
        if process.returncode != 0:
            return None, process.stderr.decode()
        system_identifier = next(
            line
            for line in process.stdout.decode().splitlines()
            if "Database system identifier" in line
        ).split(" ")[-1]
        return system_identifier, None

    def create_data_backup_tarball(self) -> str:
        """Store the current data folder in a tar.gz file and return its name."""
        filename = (
            f"{self.paths.data.parent}-"
            f"{str(datetime.now()).replace(' ', '-').replace(':', '-')}.tar.gz"
        )
        # Input is hardcoded
        subprocess.check_call(f"tar -zcf {filename} {self.paths.data}".split())  # noqa: S603
        return filename

    def clear_data_directories(self) -> None:
        """Remove the contents of the data directories to initialise a new cluster."""
        paths = [
            self.paths.archive,
            self.paths.data,
            self.paths.wal,
            self.paths.temp,
        ]
        path = None
        try:
            for path in paths:
                path_object = Path(str(path))
                if path_object.exists() and path_object.is_dir():
                    for item in os.listdir(path_object):
                        item_path = os.path.join(path_object, item)
                        if os.path.isfile(item_path) or os.path.islink(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
        except OSError as e:
            raise Exception(f"Failed to remove contents from {path} with error: {e!s}") from e

    def remove_raft_state(self) -> None:
        """Remove previous cluster information to make it possible to initialise a new cluster."""
        try:
            path = Path(f"{self.paths.patroni_conf}/raft")
            if path.exists() and path.is_dir():
                shutil.rmtree(path)
        except OSError as e:
            raise Exception(
                f"Failed to remove previous cluster information with error: {e!s}"
            ) from e

    def get_workload_version(self) -> str:
        """Get the workload version."""
        raise NotImplementedError

    @contextmanager
    def temp_file(
        self,
        mode="w+b",
        data: str | None = None,
        encoding: str | None = None,
        directory: PathProtocol | None = None,
        delete: bool = True,
        chown: str | None = None,
        *,
        errors: str | None = None,
        suffix: str | None = None,
    ) -> Generator[PathProtocol, None, None]:
        """Create a temporary file and return the file, clean it once context is closed."""
        f = tempfile.NamedTemporaryFile(  # noqa: SIM115
            mode=mode,
            encoding=encoding,
            dir=directory.as_posix() if directory else None,
            delete=False,
            errors=errors,
            suffix=suffix,
        )
        if chown is not None:
            command = f"sudo chown {chown} {f.name}"
            self.run_cmd(command)
        file_path: PathProtocol = self.root / f.name
        try:
            if data:
                self.write_text(data, file_path)
            yield file_path
        finally:
            if not f.closed:
                f.close()
            if delete:
                try:
                    file_path.unlink()
                except OSError as e:
                    raise e

    @property
    def user(self) -> str:
        """The OS user that owns workload files on VM."""
        return "_daemon_"

    @property
    def group(self) -> str:
        """The OS group that owns workload files on VM."""
        return "_daemon_"

    @property
    def paths(self) -> BasePaths:
        """Return Workload's paths."""
        return VMPaths(self.root, self.get_postgresql_version().split(".")[0])

    @property
    def root(self) -> PathProtocol:
        """Return the root path."""
        return pathops.LocalPath("/")

    @property
    def workload_present(self) -> bool:
        """Flag to check if workload is present and ready."""
        # check if snap is installed as an indicator of workload presence
        cache = snap.SnapCache()
        try:
            return cache[charm_refresh.snap_name()].present
        except snap.SnapError:
            return False

    def get_available_memory(self) -> int:
        """Returns the system available memory in bytes."""
        with open("/proc/meminfo") as meminfo:
            for line in meminfo:
                if "MemTotal" in line:
                    return int(line.split()[1]) * 1024

        return 0

    def get_available_resources(self) -> tuple[int, int]:
        """Returns the available (cpu_cores, memory_bytes) for the workload."""
        return os.cpu_count() or 1, self.get_available_memory()

    def get_snap_revision(self) -> str:
        """Returns the installed workload snap revision."""
        cache = snap.SnapCache()
        return cache[charm_refresh.snap_name()].revision
