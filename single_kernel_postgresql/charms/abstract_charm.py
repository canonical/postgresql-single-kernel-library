# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Skeleton for the abstract charm."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, cast

from data_platform_helpers.advanced_statuses import StatusHandler
from ops import StatusBase
from ops.charm import CharmBase, UpdateStatusEvent

if TYPE_CHECKING:
    import charm_refresh

    from single_kernel_postgresql.managers.k8s import K8sManager


from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.events.async_replication import PostgreSQLAsyncReplication
from single_kernel_postgresql.events.database import DatabaseEventsHandler
from single_kernel_postgresql.events.ldap import LDAP
from single_kernel_postgresql.events.logical_replication import PostgreSQLLogicalReplication
from single_kernel_postgresql.events.observer import ClusterTopologyChangeCharmEvents
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
from single_kernel_postgresql.managers.backup import BackupManager
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.managers.cluster_membership import ClusterMembershipManager
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.managers.database import DatabaseManager
from single_kernel_postgresql.managers.logical_replication import LogicalReplicationManager
from single_kernel_postgresql.managers.observer import ObserverManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.raft import RaftManager
from single_kernel_postgresql.managers.refresh import RefreshManager
from single_kernel_postgresql.managers.restore import RestoreManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils.s3 import S3Client
from single_kernel_postgresql.workload.base import BaseWorkload, ResourceProvider

from ..config.enums import Substrates
from ..config.literals import (
    DATABASE,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
    S3_RELATION_NAME,
)
from ..utils.postgresql import PostgreSQL


class AbstractPostgreSQLCharm(CharmBase, ABC):
    """An abstract PostgreSQL charm."""

    # Custom charm events dispatched by the observer scripts (cluster topology
    # changes and raft reconnection).
    on = ClusterTopologyChangeCharmEvents()

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

        # Retry pending logical-replication validations on the update-status heartbeat;
        # observed BEFORE StatusHandler so the charm-side checks run before the status
        # recompute.
        self.framework.observe(self.on.update_status, self._on_logical_replication_update_status)

        # The manager owns the two logical-replication relations' data plane; the events
        # handler owns the observers and event-flow guards. The config manager reads the
        # manager's published slots for the Patroni render and API sync.
        self.logical_replication_manager = LogicalReplicationManager(
            state=self.state,
            workload=self.workload,
            # Per-call bridges: the client and the primary lookup are freshly
            # constructed per access (Patroni primary lookup + app secret).
            postgresql=lambda: self.postgresql,
            primary_endpoint=lambda: self.primary_endpoint,
            update_config=self.update_config,
            set_unit_status=self.set_unit_status,
        )
        self.logical_replication = PostgreSQLLogicalReplication(
            self, self.state, self.logical_replication_manager
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
            set_unit_status=self.set_unit_status,
            logical_replication_slots=self.logical_replication.replication_slots,
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

        # The watcher handler feeds the membership subsystem (endpoints, raft addresses).
        self.watcher_handler = WatcherEventsHandler(self, self.workload, self.state)

        # The RAFT manager owns the low-level raft operations; the membership
        # manager owns the VM peer-relation orchestration and the raft
        # recovery state machine (late-bound bridges as callables).
        self.raft_manager = RaftManager(
            state=self.state,
            workload=self.workload,
            patroni_manager=self.patroni_manager,
            watcher_handler=self.watcher_handler,
            update_config=self.update_config,
            set_unit_status=self.set_unit_status,
            peer_relation_changed=lambda event: (
                self.postgresql_events_handler._on_peer_relation_changed(event)  # ty: ignore[unresolved-attribute]
            ),
            remove_from_members_ips=lambda ip: self.membership_manager.remove_member_ip(ip),
        )
        self.membership_manager = ClusterMembershipManager(
            state=self.state,
            workload=self.workload,
            patroni_manager=self.patroni_manager,
            update_config=self.update_config,
            set_unit_status=self.set_unit_status,
            watcher_handler=self.watcher_handler,
            async_replication_manager=self.async_replication_manager,
            update_relation_endpoints=self.update_relation_endpoints,
            raft_manager=self.raft_manager,
            set_primary_status_message=self.set_primary_status_message,
        )

        # The backup subsystem (port of the VM charm's composition wiring): the
        # charm-side duplicates are retired when the thin charm lands.
        self.s3_client = S3Client(self.workload)
        self.backup_manager = BackupManager(
            state=self.state,
            workload=self.workload,
            s3_client=self.s3_client,
            patroni_manager=self.patroni_manager,
            update_config=self.update_config,
            resource_provider=cast("ResourceProvider", self.workload),
            is_standby_cluster=self._is_standby_cluster,
            set_unit_status=self.set_unit_status,
            refresh_primary_status=self.set_primary_status_message,
        )
        self.restore_manager = RestoreManager(
            state=self.state,
            workload=self.workload,
            patroni_manager=self.patroni_manager,
            update_config=self.update_config,
            backup_manager=self.backup_manager,
            is_standby_cluster=self._is_standby_cluster,
        )
        self.observer_manager = ObserverManager(self.state, self.workload)

        # Events Handler
        self.postgresql_events_handler = PostgreSQLEventsHandler(
            self,
            self.workload,
            self.state,
            self.cluster_manager,
            self.tls_manager,
            self.config_manager,
            self.patroni_manager,
            self.refresh_manager,
            membership_manager=self.membership_manager,
            backup_manager=self.backup_manager,
            restore_manager=self.restore_manager,
            observer_manager=self.observer_manager,
            async_replication_manager=self.async_replication_manager,
            database_manager=self.database_manager,
            postgresql=lambda: self.postgresql,
        )

        # Resume or prepare the refresh (the charms' post-construction resume block).
        self.refresh_manager.on_init()

        # Status Handler
        self.status_handler = StatusHandler(
            self,
            self.refresh_manager,
            self.cluster_manager,
            self.tls_manager,
            self.config_manager,
            self.patroni_manager,
            self.logical_replication_manager,
        )

    def _is_standby_cluster(self) -> bool:
        """Whether this unit belongs to a standby (read-only) cluster (port)."""
        if (
            self.state.model.get_relation(REPLICATION_CONSUMER_RELATION) is None
            and self.state.model.get_relation(REPLICATION_OFFER_RELATION) is None
        ):
            return False
        return not self.async_replication_manager.is_primary_cluster()

    # Postgresql Client
    @property
    @abstractmethod
    def postgresql(self) -> PostgreSQL:
        """Return a PostgreSQL client."""
        pass

    def _on_logical_replication_update_status(self, event: UpdateStatusEvent) -> None:
        """Retry pending logical-replication validations on the update-status heartbeat.

        Runs BEFORE the StatusHandler's own update-status listener (constructed
        later observes later), so the retry's validation results are part of the
        status recompute that follows.
        """
        self.logical_replication.retry_validations()

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
    def update_config(
        self,
        is_creating_backup: bool = False,
        no_peers: bool = False,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> bool:
        """Re-render the Patroni configuration and apply it."""
        pass

    @abstractmethod
    def post_refresh_side_effects(self) -> None:
        """Run the post-snap-refresh side effects owned by not-yet-migrated modules.

        The VM charm sets up the exporter and pgBackRest exporter, starts/stops the
        pgBackRest service and updates the watcher unit address here.
        """
        pass

    @abstractmethod
    def has_async_replication_relation(self) -> bool:
        """Whether this unit is related to an async replication partner.

        Owned by the async-replication module until that phase migrates; the temp
        tablespace migration skips units inside an async cluster.
        """
        pass

    @abstractmethod
    def update_relation_endpoints(self) -> None:
        """Refresh the client and async relation endpoints after a switchover.

        Owned by the client-relation and async-replication modules until those
        phases migrate; the VM pre-refresh checks call it after switching primary.
        """
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

    @abstractmethod
    def update_pebble_layers(self) -> None:
        """Reconcile the workload's Pebble layers (K8s)."""
        pass

    @abstractmethod
    def ensure_pgdata_dirs_and_symlinks(self) -> None:
        """Create the storage directories and symlinks for the PostgreSQL data paths (K8s)."""
        pass
