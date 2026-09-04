# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Skeleton for the abstract charm."""

from abc import ABC, abstractmethod

from data_platform_helpers.advanced_statuses import StatusHandler
from ops import StatusBase
from ops.charm import CharmBase

from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.events.database import DatabaseEventsHandler
from single_kernel_postgresql.events.postgresql import PostgreSQLEventsHandler
from single_kernel_postgresql.events.tls import TLS
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
)
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.managers.database import DatabaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.workload.base import BaseWorkload, ResourceProvider

from ..config.enums import Substrates
from ..config.literals import DATABASE
from ..utils.postgresql import PostgreSQL


class AbstractPostgreSQLCharm(CharmBase, ABC):
    """An abstract PostgreSQL charm."""

    def __init__(self, *args):
        super().__init__(*args)

        # State
        self.state = CharmState(charm=self, substrate=self.substrate)

        # TLS events handler owns the two certificate requirers; build it before the
        # TLS manager so the manager can constructor-inject them for its live-fetch getters.
        self.tls = TLS(self, self.state)

        # Managers
        self.tls_manager = TLSManager(
            state=self.state,
            workload=self.workload,
            client_certificate=self.tls.client_certificate,
            peer_certificate=self.tls.peer_certificate,
        )
        self.patroni_manager = PatroniManager(state=self.state, workload=self.workload)
        self.cluster_manager = ClusterManager(state=self.state, workload=self.workload)

        # Client-relation subsystem: the charm is the composition root, as with the
        # other managers; the handler owns only the observers and guard/defer decisions.
        self.database_manager = DatabaseManager(
            state=self.state,
            workload=self.workload,
            database_provides=DatabaseProvides(self, relation_name=DATABASE),
            set_unit_status=self.set_unit_status,
        )
        self.database = DatabaseEventsHandler(
            self, self.state, self.database_manager, self.patroni_manager, self.tls_manager
        )

        self.config_manager = ConfigManager(
            state=self.state,
            workload=self.workload,
            tls_manager=self.tls_manager,
            patroni_manager=self.patroni_manager,
            database_manager=self.database_manager,
            resource_provider=self.get_resource_provider,
            request_restart=self.request_restart,
            restart_services=self.restart_services,
        )

        # Events Handler
        self.postgresql_events_handler = PostgreSQLEventsHandler(
            self,
            self.workload,
            self.state,
            self.cluster_manager,
            self.tls_manager,
            self.config_manager,
            self.patroni_manager,
        )

        # Status Handler
        self.status_handler = StatusHandler(
            self,
            self.cluster_manager,
            self.tls_manager,
            self.config_manager,
            self.patroni_manager,
        )

    # Postgresql Client
    @property
    @abstractmethod
    def postgresql(self) -> PostgreSQL:
        """Return a PostgreSQL client."""
        pass

    # Postgresql Workload
    @property
    @abstractmethod
    def workload(self) -> BaseWorkload:
        """Access current workload."""
        pass

    # Postgresql Substrate
    @property
    @abstractmethod
    def substrate(self) -> Substrates:
        """Access current substrate."""
        pass

    # Charm-side bridges the lib calls back into. request_restart/restart_services are
    # substrate-tangled and stay until their own migration phases; update_config still
    # supplies the ldap/async/watcher values those phases own; primary_endpoint is the
    # VM's Patroni-derived primary lookup. set_unit_status routes status writes through
    # the charm_refresh priority gate and stays until the refresh logic itself migrates
    # into the library, at which point the managers own their status writes.
    @abstractmethod
    def get_resource_provider(self) -> ResourceProvider:
        """Return the substrate's (cpu_cores, memory_bytes) introspector."""
        pass

    @abstractmethod
    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""
        pass

    @abstractmethod
    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""
        pass

    @abstractmethod
    def set_unit_status(self, status: StatusBase) -> None:
        """Set the unit status without overriding a higher-priority refresh status."""
        pass

    @abstractmethod
    def update_config(self) -> bool:
        """Re-render the Patroni configuration and apply it."""
        pass

    @property
    @abstractmethod
    def primary_endpoint(self) -> str | None:
        """Address of the cluster primary, or None when there is not one."""
        pass

    @abstractmethod
    def get_async_primary_cluster_endpoint(self) -> str | None:
        """Endpoint of the primary cluster of the async replication partner, if any.

        Owned by the async-replication module until that phase migrates; the refresh
        pre-refresh checks need it to decide whether a switchover crosses clusters.
        """
        pass
