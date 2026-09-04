# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Skeleton for the abstract charm."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from data_platform_helpers.advanced_statuses import StatusHandler
from ops import StatusBase
from ops.charm import CharmBase

if TYPE_CHECKING:
    import charm_refresh


from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.events.async_replication import PostgreSQLAsyncReplication
from single_kernel_postgresql.events.database import DatabaseEventsHandler
from single_kernel_postgresql.events.ldap import LDAP
from single_kernel_postgresql.events.postgresql import PostgreSQLEventsHandler
from single_kernel_postgresql.events.tls import TLS
from single_kernel_postgresql.events.watcher import WatcherEventsHandler
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
)
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.s3 import S3Requirer
from single_kernel_postgresql.managers.async_replication import (
    AsyncReplicationManager,
)
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.managers.database import DatabaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.refresh import RefreshManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.workload.base import BaseWorkload, ResourceProvider

from ..config.enums import Substrates
from ..config.literals import DATABASE, S3_RELATION_NAME
from ..utils.postgresql import PostgreSQL

if TYPE_CHECKING:
    from single_kernel_postgresql.managers.k8s import K8sManager


class AbstractPostgreSQLCharm(CharmBase, ABC):
    """An abstract PostgreSQL charm."""

    def __init__(self, *args):
        super().__init__(*args)

        # State
        self.state = CharmState(
            charm=self,
            substrate=self.substrate,
            s3_requirer=S3Requirer(self, relation_name=S3_RELATION_NAME),
        )

        # TLS events handler owns the two certificate requirers; build it before the
        # TLS manager so the manager can constructor-inject them for its live-fetch getters.
        self.tls = TLS(self, self.state)

        # LDAP events handler owns the ldap requirer relation and the auth parameters
        # the config manager renders into the Patroni hba section.
        self.ldap = LDAP(self, self.state)

        # Managers
        self.tls_manager = TLSManager(
            state=self.state,
            workload=self.workload,
            client_certificate=self.tls.client_certificate,
            peer_certificate=self.tls.peer_certificate,
        )
        self.patroni_manager = PatroniManager(state=self.state, workload=self.workload)
        self.cluster_manager = ClusterManager(state=self.state, workload=self.workload)

        # Substrate-only K8s API seam: the K8s charm builds it by overriding
        # build_k8s_manager(); VM charms need no K8s API access. Built here so the
        # async-replication subsystem (below) can consume it.
        k8s_manager = self.build_k8s_manager()

        # Async-replication subsystem: the manager owns the data plane and the
        # promotion/standby flows; the handler owns the observers and the public facade.
        watcher = self._async_watcher()
        self.async_replication_manager = AsyncReplicationManager(
            state=self.state,
            workload=self.workload,
            patroni_manager=self.patroni_manager,
            update_config=self.update_config,
            set_unit_status=self.set_unit_status,
            set_primary_status_message=self.set_primary_status_message,
            set_app_status=self._recompute_async_app_status,
            create_pgdata=self.create_pgdata,
            fix_leader_annotation=self.fix_leader_annotation,
            re_emit_relation_changed=self._re_emit_async_relation_changed,
            k8s_manager=k8s_manager,
            watcher=watcher,
        )
        self.async_replication = PostgreSQLAsyncReplication(
            self,
            self.state,
            self.async_replication_manager,
            self.patroni_manager,
            self.workload,
            k8s_manager=k8s_manager,
            watcher=watcher,
        )

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
            ldap_handler=self.ldap,
            resource_provider=self.get_resource_provider,
            request_restart=self.request_restart,
            restart_services=self.restart_services,
        )

        # The refresh manager owns the charm_refresh integration and the priority gate
        # every unit status write routes through. Constructed before the events handler
        # so the K8s pebble-ready handler can consult the refresh state.
        self.refresh_manager = RefreshManager(
            state=self.state,
            workload=self.workload,
            charm=self,
            set_default_status=self.set_default_unit_status,
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
        self.watcher_handler = WatcherEventsHandler(self, self.workload, self.state)

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
    # supplies the async/watcher values those phases own; primary_endpoint is the
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
    def set_app_status(self, status: StatusBase) -> None:
        """Set the application status through the charm's own status gates."""
        pass

    @abstractmethod
    def set_primary_status_message(self) -> None:
        """Recompute the unit's primary/standby status message."""
        pass

    @abstractmethod
    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> None:
        """Set the unit status without overriding a higher-priority refresh status."""
        pass

    @abstractmethod
    def set_default_unit_status(self) -> None:
        """Set the unit status that applies when no refresh status is active."""
        pass

    @abstractmethod
    def update_config(self) -> bool:
        """Re-render the Patroni configuration and apply it."""
        pass

    # Async-replication wiring helpers. The manager is constructed before the handler,
    # so the framework-facing plumbing is re-exposed through these late-binding bridges;
    # create_pgdata/fix_leader_annotation are K8s-only and default to no-ops the K8s
    # charm overrides.
    def _recompute_async_app_status(self) -> None:
        """Recompute the async-replication app status through the handler."""
        self.async_replication.set_app_status()

    def _re_emit_async_relation_changed(self) -> None:
        """Re-emitting the async relation-changed event goes through the handler."""
        self.async_replication._re_emit_async_relation_changed_event()

    def _async_watcher(self) -> "WatcherEventsHandler | None":
        """Overridable hook supplying the VM watcher bridge (K8s has none)."""
        return None

    def build_k8s_manager(self) -> "K8sManager | None":
        """Overridable hook supplying the K8s API seam (K8s charm only)."""
        return None

    def create_pgdata(self) -> None:
        """Create the PostgreSQL data directories (K8s only; overridden there)."""
        return None

    def fix_leader_annotation(self) -> bool:
        """Fix the leader annotation (K8s only; overridden there)."""
        return False

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
