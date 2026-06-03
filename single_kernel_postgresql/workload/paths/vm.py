# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""PostgreSQL Machine Paths."""

from charmlibs.pathops import PathProtocol
from single_kernel_postgresql.workload.paths.base import Paths
from single_kernel_postgresql.config.literals import BASE_SNAP_DIR, SNAP_DATA, SNAP_COMMON, SNAP, DATA_PATH

from single_kernel_postgresql.workload.paths.base import Paths


class VMPaths(Paths):
    """This class represents the set of Paths that need to be exposed for the Machine substrate.

    Args:
            conf: Path to the config folder of PostgreSQL
            data: Path to the data folder of PostgreSQL
            logs: Path to the logs folder of PostgreSQL
            tmp: Temporary directory
            bin: Path to the bin/ folder
    """


    @property
    def base_snap_dir(self) -> PathProtocol:
        """Get path to the Base snap directory."""
        return self.root / BASE_SNAP_DIR

    @property
    def snap_data(self) -> PathProtocol:
        """Get path to the snap data directory."""
        return self.base_snap_dir / SNAP_DATA

    @property
    def snap_common(self) -> PathProtocol:
        """Get path to the snap common directory."""
        return self.base_snap_dir / SNAP_COMMON

    @property
    def snap(self) -> PathProtocol:
        """Get path to the snap directory."""
        return self.root / SNAP

    @property
    def conf(self) -> PathProtocol:
        """Path to the config folder of PostgreSQL."""
        # TODO: Update path
        return self.root / "config"

    @property
    def data(self) -> PathProtocol:
        """Path to the data folder of PostgreSQL."""
        return self.snap_common / DATA_PATH

    @property
    def logs(self) -> PathProtocol:
        """Path to the logs folder of PostgreSQL."""
        # TODO: Update path
        return self.root / "logs"

    @property
    def tmp(self) -> PathProtocol:
        """Path to the temporary directory."""
        return self.root / "tmp"
