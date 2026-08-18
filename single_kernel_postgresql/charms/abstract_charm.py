# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Skeleton for the abstract charm."""

from abc import ABC, abstractmethod

from data_platform_helpers.advanced_statuses import StatusHandler
from ops.charm import CharmBase

from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.events.postgresql import PostgreSQLEventsHandler
from single_kernel_postgresql.events.tls import TLS
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.workload.base import BaseWorkload

from ..config.enums import Substrates
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
        self.config_manager = ConfigManager(
            state=self.state,
            workload=self.workload,
            tls_manager=self.tls_manager,
            patroni_manager=self.patroni_manager,
            request_restart=self.request_restart,
            refresh_endpoints=self.refresh_endpoints,
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

    # Config-update bridges: charm-side callables the ConfigManager invokes for the
    # substrate-tangled restart trigger, endpoint refresh and monitoring/ldap service
    # restarts. They stay in the charm until their own migration phases.
    @abstractmethod
    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""
        pass

    @abstractmethod
    def refresh_endpoints(self) -> None:
        """Refresh the client relation endpoints."""
        pass

    @abstractmethod
    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""
        pass
