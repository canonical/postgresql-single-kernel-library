# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Pre-wipe data preservation for destructive operations.

``DataSafetyManager`` orchestrates the workload-level preservation primitives:
it garbage-collects incomplete copies, checks the conservative space budget
(``free >= uncompressed dir size``), evicts the oldest complete copies when
short, creates the verified copy, and re-checks the post-copy free space before
allowing any destructive step. Every failure raises before any data is removed.
"""

from __future__ import annotations

import importlib.metadata
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from charmlibs.pathops import PathProtocol

from single_kernel_postgresql.config.exceptions import (
    DataSafetyError,
    DataSafetyPostflightError,
    DataSafetyPreflightError,
)
from single_kernel_postgresql.config.literals import BACKUP_SAFETY_SCHEMA_VERSION
from single_kernel_postgresql.workload.base import BaseWorkload, SafetyCopyInfo

if TYPE_CHECKING:
    from single_kernel_postgresql.core.state import CharmState

logger = logging.getLogger(__name__)


def _lib_version() -> str:
    """The library's own version for the manifest, empty when unavailable."""
    try:
        return importlib.metadata.version("postgresql-charms-single-kernel")
    except importlib.metadata.PackageNotFoundError:
        return ""


class DataSafetyManager:
    """Pre-wipe preservation orchestration for destructive operations."""

    def __init__(self, state: "CharmState", workload: BaseWorkload):
        self.state = state
        self.workload = workload

    def preflight(self, operation: str, copies: list[tuple[str, PathProtocol]]) -> None:
        """Garbage-collect junk copies and ensure the conservative space budget holds.

        Raises:
            DataSafetyPreflightError: when the space budget cannot be met.
        """
        self.workload.evict_data_safety_copies(0)  # GC manifest-less residue (spec §7)
        needed = sum(self.workload.dir_size(path) for _role, path in copies)
        free = self.workload.free_space(self.workload.data_safety_root())
        if free < needed:
            self.workload.evict_data_safety_copies(needed - free)
        free = self.workload.free_space(self.workload.data_safety_root())
        if free < needed:
            raise DataSafetyPreflightError(
                f"Not enough free space to preserve the data before removal "
                f"(need {needed} bytes, have {free}). Free space, remove old copies "
                f"under {self.workload.data_safety_root()}, or re-run with safety=none."
            )

    def _build_manifest(self, operation: str) -> dict:
        system_identifier, _ = self.workload.get_system_identifier()
        return {
            "schema_version": BACKUP_SAFETY_SCHEMA_VERSION,
            "operation": operation,
            "created": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "cluster_name": self.state.cluster_name,
            "system_identifier": system_identifier or "",
            "model": self.state.model.name,
            "unit": self.state.model.unit.name,
            "versions": {
                "charm": "",
                "lib": _lib_version(),
                "postgresql": self.workload.get_workload_version(),
            },
        }

    def preserve(self, operation: str, copies: list[tuple[str, PathProtocol]]) -> SafetyCopyInfo:
        """Preflight, create the verified copy, and gate on post-copy free space.

        Raises:
            DataSafetyError: on substrate-unsupported seams or any preservation failure.
            DataSafetyPostflightError: when the copy exists but the post-copy space
                gate fails (the copy stays on disk; the operation is aborted).
        """
        try:
            self.preflight(operation, copies)
            needed = sum(self.workload.dir_size(path) for _role, path in copies)
            info = self.workload.preserve_data_directories(
                operation, copies, self._build_manifest(operation)
            )
            free = self.workload.free_space(self.workload.data_safety_root())
            if free < needed:
                raise DataSafetyPostflightError(
                    f"Not enough free space to re-materialize the data after preservation "
                    f"({info.path}); the preserved copy is safe on disk. Free space or "
                    f"re-run with safety=none."
                )
            logger.info(
                "Safety copy %s created under %s (%d bytes)",
                info.copy_id,
                info.path,
                info.total_size_bytes,
            )
            return info
        except NotImplementedError as e:
            raise DataSafetyError(
                f"safety=local is not supported on this substrate yet: {e}"
            ) from e
