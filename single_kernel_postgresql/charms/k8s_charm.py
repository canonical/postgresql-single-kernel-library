#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL Kubernetes Charm."""

import logging
from typing import TYPE_CHECKING

from ops import ActiveStatus, StatusBase

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import CONTAINER_NAME, SYSTEM_USERS, USER
from single_kernel_postgresql.managers.k8s import K8sManager
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.workload.k8s import K8sWorkload

if TYPE_CHECKING:
    import charm_refresh

logger = logging.getLogger(__name__)


class PostgreSQLK8sCharm(AbstractPostgreSQLCharm):
    """PostgreSQL K8s Charm."""

    def __init__(self, *args):
        """Initialize the PostgreSQL Kubernetes Charm."""
        super().__init__(*args)
        assert isinstance(self.workload, K8sWorkload), (  # noqa: S101
            "Workload must be an instance of K8sWorkload"
        )
        self.k8s_manager = K8sManager(self.state, self.workload)

    @property
    def postgresql(self) -> PostgreSQL:
        """Return a PostgreSQL client."""
        return PostgreSQL(
            substrate=Substrates.K8S,
            primary_host="localhost",
            current_host="localhost",
            user=USER,
            # The password is hardcoded because this is an abstract charm and
            # it meant to be used only in unit tests.
            password="test-password",  # noqa S106
            database="test-database",
            system_users=SYSTEM_USERS,
        )

    @property
    def workload(self) -> BaseWorkload:
        """Access current workload instance.

        Returns the workload object.

        Returns:
            BaseWorkload: The K8sWorkload instance for this charm
        """
        return K8sWorkload(
            charm_dir=self.charm_dir,
            container=self.unit.get_container(CONTAINER_NAME),
        )

    @property
    def substrate(self) -> Substrates:
        """Access current substrate type.

        Returns:
            Substrates: always Substrates.K8S for this charm
        """
        return Substrates.K8S

    # The concrete production charm owns these bridges (update_scrape_job_spec +
    # acquire_lock, pebble metrics/ldap restarts, the async app status and the config
    # re-render), so they are minimal here.
    def get_resource_provider(self) -> K8sManager:
        """Return the substrate's (cpu_cores, memory_bytes) introspector."""
        return self.k8s_manager

    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""

    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""

    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> None:
        """Set the unit status without overriding a higher-priority refresh status."""
        self.refresh_manager.set_unit_status(status, refresh=refresh)

    def set_default_unit_status(self) -> None:
        """Set the unit status that applies when no refresh status is active."""
        self.unit.status = ActiveStatus()

    def set_app_status(self) -> None:
        """Set the application status from the async-replication state."""

    def update_config(
        self, *, refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None
    ) -> bool:
        """Re-render the Patroni configuration and apply it."""
        return self.config_manager.update_config(self.postgresql)

    @property
    def primary_endpoint(self) -> str | None:
        """Address of the cluster primary's Service."""
        return self.state.primary_endpoint

    def get_async_primary_cluster_endpoint(self) -> str | None:
        """Endpoint of the primary cluster of the async replication partner, if any."""
        return None

    def update_relation_endpoints(self) -> None:
        """Refresh the client and async relation endpoints after a switchover."""

    def update_pebble_layers(self) -> None:
        """Reconcile the workload's Pebble layers."""
        self.k8s_manager.update_pebble_layers(replan=True)

    def ensure_pgdata_dirs_and_symlinks(self) -> None:
        """Create the storage directories and symlinks for the PostgreSQL data paths."""
