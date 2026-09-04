#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Refresh Manager.

This module ports the refresh (rolling upgrade) logic from the PostgreSQL VM and K8s
charms' 16/edge branches. It hosts the charm-specific callbacks handed to
``charm_refresh`` and the manager that owns the refresh-aware unit status handling.
"""

import dataclasses
import logging
from typing import TYPE_CHECKING

import charm_refresh
from charm_refresh import CharmVersion, PrecheckFailed

from single_kernel_postgresql.config.exceptions import SwitchoverFailedError

if TYPE_CHECKING:
    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm

logger = logging.getLogger(__name__)


@dataclasses.dataclass(eq=False)
class PostgreSQLRefreshBase(charm_refresh.CharmSpecificCommon):
    """Base class for PostgreSQL refresh operations, shared by both substrates."""

    _charm: "AbstractPostgreSQLCharm"

    @classmethod
    def is_compatible(
        cls,
        *,
        old_charm_version: CharmVersion,
        new_charm_version: CharmVersion,
        old_workload_version: str,
        new_workload_version: str,
    ) -> bool:
        """Checks charm and workload version compatibility."""
        if not super().is_compatible(
            old_charm_version=old_charm_version,
            new_charm_version=new_charm_version,
            old_workload_version=old_workload_version,
            new_workload_version=new_workload_version,
        ):
            return False

        # Check workload version compatibility
        old_major, old_minor = (int(component) for component in old_workload_version.split("."))
        new_major, new_minor = (int(component) for component in new_workload_version.split("."))
        if old_major != new_major:
            return False
        return new_minor >= old_minor


@dataclasses.dataclass(eq=False)
class PostgreSQLRefreshK8s(PostgreSQLRefreshBase, charm_refresh.CharmSpecificKubernetes):
    """Charm-specific refresh callbacks for the Kubernetes substrate."""

    def run_pre_refresh_checks_after_1_unit_refreshed(self) -> None:
        """Implement pre-refresh checks after 1 unit refreshed."""
        logger.debug("Running pre-refresh checks")
        if self._charm.patroni_manager.is_creating_backup:
            raise PrecheckFailed("Backup in progress")

        # Check if all units except the highest unit (first to be refreshed) are online.
        running_members = self._charm.patroni_manager.get_running_cluster_members()

        # The highest unit number is planned_units - 1 (e.g., if 3 units, highest is unit 2).
        # Members are named like "postgresql-k8s-0", "postgresql-k8s-1", etc.
        highest_unit_number = self._charm.app.planned_units() - 1

        # Check if all units except the highest unit are online.
        for unit_number in range(self._charm.app.planned_units()):
            member_name = f"{self._charm.app.name}-{unit_number}"
            if unit_number != highest_unit_number and member_name not in running_members:
                raise PrecheckFailed(f"PostgreSQL is not running on unit {unit_number}")

        # Switch primary to last unit to refresh (lowest unit number).
        last_unit_to_refresh = f"{self._charm.app.name}/0"
        if self._charm.patroni_manager.get_primary(unit_name_pattern=True) == last_unit_to_refresh:
            logger.info(
                f"Unit {last_unit_to_refresh} was already primary during pre-refresh check"
            )
        else:
            try:
                self._charm.patroni_manager.switchover(
                    candidate=last_unit_to_refresh,
                    async_cluster=bool(self._charm.get_async_primary_cluster_endpoint()),
                )
            except SwitchoverFailedError as e:
                logger.warning(f"switchover failed with reason: {e}")
                raise PrecheckFailed("Unable to switch primary") from None
            else:
                logger.info(
                    f"Switched primary to unit {last_unit_to_refresh} during pre-refresh check"
                )

    def run_pre_refresh_checks_before_any_units_refreshed(self) -> None:
        """Implement pre-refresh checks before any unit refreshed."""
        if not self._charm.patroni_manager.are_all_members_ready():
            raise PrecheckFailed("PostgreSQL is not running on 1+ units")

        self.run_pre_refresh_checks_after_1_unit_refreshed()


__all__ = [
    "PostgreSQLRefreshBase",
    "PostgreSQLRefreshK8s",
]
