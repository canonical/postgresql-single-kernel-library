#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL Kubernetes Charm."""

import logging
from typing import TYPE_CHECKING, cast

from lightkube.core.exceptions import ApiError
from ops import ActiveStatus, BlockedStatus, StatusBase
from ops.pebble import ChangeError

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    CERTS_PATH,
    CONTAINER_NAME,
    PEER_RELATION,
    SYSTEM_USERS,
    TLS_CA_BUNDLE_FILE,
    UNIT_SCOPE,
    USER,
)
from single_kernel_postgresql.events.tls_transfer import TLSTransfer
from single_kernel_postgresql.managers.k8s import K8sManager
from single_kernel_postgresql.workload.base import BaseWorkload, PebbleLayerSpec
from single_kernel_postgresql.workload.k8s import K8sWorkload

if TYPE_CHECKING:
    import charm_refresh

logger = logging.getLogger(__name__)


class PostgreSQLK8sCharm(AbstractPostgreSQLCharm):
    """PostgreSQL K8s Charm."""

    # Built by build_k8s_manager() while the abstract __init__ wires the
    # async-replication subsystem; never None on the K8s substrate.
    k8s_manager: K8sManager

    def __init__(self, *args):
        """Initialize the PostgreSQL Kubernetes Charm."""
        super().__init__(*args)
        assert isinstance(self.workload, K8sWorkload), (  # noqa: S101
            "Workload must be an instance of K8sWorkload"
        )
        # TLS-transfer requirer: feeds get_ca_secret_names() during TLS bootstrap.
        self.tls_transfer = TLSTransfer(self, PEER_RELATION)

    def build_k8s_manager(self) -> K8sManager:
        """Build the K8s API seam the async-replication subsystem consumes."""
        self.k8s_manager = K8sManager(
            self.state,
            cast("K8sWorkload", self.workload),
        )
        return self.k8s_manager

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

    def post_refresh_side_effects(self) -> None:
        """Run the post-snap-refresh side effects owned by not-yet-migrated modules."""

    def has_async_replication_relation(self) -> bool:
        """Whether this unit is related to an async replication partner."""
        return False

    def update_config(
        self,
        is_creating_backup: bool = False,
        no_peers: bool = False,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> bool:
        """Re-render the Patroni configuration and apply it."""
        return self.config_manager.update_config(self.postgresql)

    def set_app_status(self, status: StatusBase) -> None:
        """Set the application status; the production charm gates this on its own state."""
        self.app.status = status

    def set_primary_status_message(self) -> None:
        """Recompute the unit's primary/standby status message."""

    def fix_leader_annotation(self) -> bool:
        """Fix the leader annotation; the production charm owns the real implementation."""
        return False

    def create_pgdata(self) -> None:
        """Create the PostgreSQL data directories (ported from the K8s charm)."""
        self.workload.init_storage()

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
        """Reconcile the workload's Pebble layers, guarded against replan failure."""
        try:
            self.workload.update_pebble_layers(PebbleLayerSpec.from_state(self.state))
        except ChangeError as e:
            # A stale pgBackRest exporter can hold its port and fail the replan
            # mid-hook; the next hook retries instead of erroring the unit.
            logger.warning("Failed to replan pebble services: %s", e)

    def ensure_pgdata_dirs_and_symlinks(self) -> None:
        """Create the storage directories and symlinks for the PostgreSQL data paths."""
        self.workload.init_storage()

    def push_ca_file_into_workload(self, secret_name: str) -> bool:
        """Upload one transferred CA certificate into the workload container (port)."""
        certificates = self.state.get_secret(UNIT_SCOPE, secret_name)
        if certificates is not None:
            workload = cast("K8sWorkload", self.workload)
            workload.push_file(f"{CERTS_PATH}/{secret_name}.crt", certificates)
            workload.container.exec(["update-ca-certificates"]).wait()
        return self.update_config()

    def fix_pod(self) -> None:
        """Recreate the k8s resources and replication labels after a pod loss (port)."""
        if self.state.get_secret(APP_SCOPE, "internal-ca"):
            self.tls_manager.push_tls_files()
            self.sync_tls_trust_store_and_bundle()
        self.update_config()

        if (
            self.refresh_manager.refresh is not None
            and not self.refresh_manager.refresh.in_progress
        ):
            try:
                self.k8s_manager.create_services()
            except ApiError:
                logger.exception("failed to create k8s services")
                self.set_unit_status(BlockedStatus("failed to create k8s services"))
                return

        try:
            self.k8s_manager.patch_pod_labels(self.unit.name)
        except ApiError as e:
            logger.error("failed to patch pod")
            self.set_unit_status(BlockedStatus(f"failed to patch pod with error {e}"))
            return

        # Update the sync-standby endpoint in the async replication data.
        self.async_replication_manager.update_async_replication_data()

    def sync_tls_trust_store_and_bundle(self) -> None:
        """Sync the K8s CA trust store and the charm-local CA bundle (port).

        The Patroni REST client runs on the unit, so its verify= bundle lives
        charm-local; the in-container pem files are written by TLSManager.push_tls_files.
        """
        _, ca, _ = self.tls_manager.get_client_tls_files()
        if ca is not None:
            cast("K8sWorkload", self.workload).sync_tls_trust_store(ca, CERTS_PATH)

        # CA bundle is not secret; it backs the unit-local Patroni REST client.
        with open(f"/tmp/{TLS_CA_BUNDLE_FILE}", "w") as fp:  # noqa: S108
            fp.write(self.tls_manager.get_peer_ca_bundle())
