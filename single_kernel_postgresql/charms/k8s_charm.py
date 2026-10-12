#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL Kubernetes Charm."""

import json
import logging
import os
import sys
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, cast

import psycopg2
from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Endpoints
from ops import ActiveStatus, BlockedStatus, ModelError, StatusBase
from ops.log import JujuLogHandler
from ops.pebble import ChangeError, Layer, LayerDict, ServiceDict, ServiceStatus

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import DeployedWithoutTrustError
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    CERTS_PATH,
    CONTAINER_NAME,
    DATABASE,
    DATABASE_DEFAULT_NAME,
    K8S_LDAP_SYNC_SERVICE_NAME,
    K8S_METRICS_SERVER_SERVICE_NAME,
    K8S_POSTGRESQL_SERVICE_NAME,
    METRICS_PORT,
    MONITORING_PASSWORD_KEY,
    PGBACKREST_METRICS_PORT,
    SYSTEM_USERS,
    TLS_CA_BUNDLE_FILE,
    UNIT_SCOPE,
    USER,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.managers.k8s import K8sManager
from single_kernel_postgresql.utils import unit_name_to_pod_name
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQL as PostgreSQLClient,
)
from single_kernel_postgresql.utils.postgresql import PostgreSQLUpdateUserPasswordError
from single_kernel_postgresql.workload.base import BaseWorkload, PebbleLayerSpec
from single_kernel_postgresql.workload.k8s import K8sWorkload

if TYPE_CHECKING:
    from collections.abc import Callable

    import charm_refresh

    from single_kernel_postgresql.events.tls_transfer import TLSTransfer

logger = logging.getLogger(__name__)


class PostgreSQLK8sCharm(AbstractPostgreSQLCharm):
    """PostgreSQL K8s Charm."""

    # Built by build_k8s_manager() while the abstract __init__ wires the
    # async-replication subsystem; never None on the K8s substrate.
    k8s_manager: K8sManager

    def __init__(
        self,
        *args,
        restart_lock_acquire: "Callable[[], None] | None" = None,
        tls_transfer: "TLSTransfer | None" = None,
        metrics_endpoint=None,
        grafana_dashboards=None,
        loki_push=None,
    ):
        """Initialize the PostgreSQL Kubernetes Charm.

        The thin consuming charm constructs the COS requirers and the TLS-transfer
        handler (their charm libs stay charm-side) and passes them in, alongside the
        rolling-restart lock acquisition.
        """
        super().__init__(*args)
        self.restart_lock_acquire = restart_lock_acquire
        if tls_transfer is not None:
            self.tls_transfer = tls_transfer
        self.metrics_endpoint = metrics_endpoint
        self.grafana_dashboards = grafana_dashboards
        self.loki_push = loki_push
        assert isinstance(self.workload, K8sWorkload), (  # noqa: S101
            "Workload must be an instance of K8sWorkload"
        )

        # Show the logger name (module name) in logs (port).
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, JujuLogHandler):
                handler.setFormatter(logging.Formatter("{name}:{message}", style="{"))

        # Open the API and database ports when Juju supports it on K8s (port).
        if self.model.juju_version.supports_open_port_on_k8s:
            try:
                self.unit.set_ports(5432, 8008)
            except ModelError:
                logger.exception("failed to open port")

        # The lib TLS handler emits tls_files_pushed after a successful push; the
        # charm then refreshes the K8s trust store + charm-local bundle and reloads.
        self.framework.observe(self.tls.tls_files_pushed, self._reload_tls_after_push)

        # Start the authorisation-rules observer process at construction (K8s parity
        # with the VM charm's topology-observer start).
        self.observer_manager.start_observer()

        # Support for disabling the operator (port).
        disable_file = Path(f"{os.environ.get('CHARM_DIR')}/disable")
        if disable_file.exists():
            logger.warning(
                f"\n\tDisable file `{disable_file.resolve()}` found, the charm will skip all events."
                "\n\tTo resume normal operations, please remove the file."
            )
            self.set_unit_status(BlockedStatus("Disabled"))
            sys.exit(0)

    def build_k8s_manager(self) -> K8sManager:
        """Build the K8s API seam the async-replication subsystem consumes."""
        self.k8s_manager = K8sManager(
            self.state,
            cast("K8sWorkload", self.workload),
        )
        return self.k8s_manager

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

    # -- Composition properties (port) ------------------------------------------------

    @cached_property
    def postgresql(self) -> PostgreSQL:
        """Return a PostgreSQL client (port of the K8s charm's cached property)."""
        return PostgreSQL(
            substrate=Substrates.K8S,
            primary_host=self.primary_endpoint,
            current_host=self.endpoint,
            user=USER,
            password=str(self.state.get_secret(APP_SCOPE, f"{USER}-password")),
            database=DATABASE_DEFAULT_NAME,
            system_users=SYSTEM_USERS,
        )

    @property
    def endpoint(self) -> str:
        """Returns the endpoint of this instance's pod (port)."""
        unit_name = self.state.peer.unit_name.replace("/", "-")
        return f"{unit_name}.{self._build_service_name('endpoints')}"

    @property
    def replicas_endpoint(self) -> str:
        """Returns the endpoint of the replicas instances' service (port)."""
        return self._build_service_name("replicas")

    def _build_service_name(self, service: str) -> str:
        """Build a full k8s service name based on the service name (port)."""
        return f"{self.model.app.name}-{service}.{self.model.name}.svc.cluster.local"

    @property
    def is_tls_enabled(self) -> bool:
        """Return whether TLS is enabled (port)."""
        return all(self.tls_manager.get_client_tls_files())

    @property
    def is_primary(self) -> bool:
        """Return whether this unit is the primary instance (port)."""
        return self.unit.name == self.patroni_manager.get_primary(unit_name_pattern=True)

    @property
    def is_ldap_charm_related(self) -> bool:
        """Return whether this unit has an LDAP charm related (port)."""
        return self.state.application.data.get("ldap_enabled", "False") == "True"

    @property
    def is_ldap_enabled(self) -> bool:
        """Return whether this unit has LDAP enabled (port)."""
        return self.is_ldap_charm_related and self.state.application.is_cluster_initialised

    @property
    def _is_workload_running(self) -> bool:
        """Returns whether the workload is running, in an active state (port)."""
        container = cast("K8sWorkload", self.workload).container
        if not container.can_connect():
            return False
        services = container.pebble.get_services(names=[K8S_POSTGRESQL_SERVICE_NAME])
        if len(services) == 0:
            return False
        return services[0].current == ServiceStatus.ACTIVE

    # -- Status bridges (port) --------------------------------------------------------

    def get_resource_provider(self) -> K8sManager:
        """Return the substrate's (cpu_cores, memory_bytes) introspector."""
        return self.k8s_manager

    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> None:
        """Set unit status without overriding a higher priority refresh status (port)."""
        if refresh is None:
            refresh = self.refresh_manager.refresh
        if refresh is not None and refresh.unit_status_higher_priority:
            return
        if (
            isinstance(status, ActiveStatus)
            and refresh is not None
            and (refresh_status := refresh.unit_status_lower_priority())
        ):
            self.unit.status = refresh_status
            Path(".last_refresh_unit_status.json").write_text(json.dumps(refresh_status.message))
            return
        self.unit.status = status

    def set_app_status(self, status: StatusBase) -> None:
        """Set the application status through the refresh gates (port)."""
        if not self.refresh_manager.can_set_app_status:
            return
        if self.refresh_manager.refresh is not None and (
            higher := self.refresh_manager.refresh.app_status_higher_priority
        ):
            self.app.status = higher
            return
        self.app.status = status

    def set_default_unit_status(self) -> None:
        """Set the unit status that applies when no refresh status is active."""
        self.set_unit_status(ActiveStatus())

    def set_primary_status_message(self) -> None:
        """Recompute the unit's primary/standby status message (port)."""
        self.postgresql_events_handler._set_active_status()

    # -- Restart seams (port) ---------------------------------------------------------

    def request_restart(self) -> None:
        """Refresh the metrics scrape jobs and acquire the restart lock (port)."""
        if self.metrics_endpoint is not None:
            self.metrics_endpoint.update_scrape_job_spec(
                self._generate_metrics_jobs(self.is_tls_enabled)
            )
        if self.restart_lock_acquire:
            self.restart_lock_acquire()

    def restart_services(self) -> None:
        """Restart the metrics and LDAP-sync sidecar services (port)."""
        self._restart_metrics_service()
        self._restart_ldap_sync_service()

    def _restart_metrics_service(self) -> None:
        """Restart the monitoring service if the password was rotated (port)."""
        workload = cast("K8sWorkload", self.workload)
        metrics_service = workload.container.get_plan().services[K8S_METRICS_SERVER_SERVICE_NAME]
        data_source_pass = metrics_service.environment.get("DATA_SOURCE_PASS", "")
        if metrics_service and (
            data_source_pass != self.state.get_secret(APP_SCOPE, MONITORING_PASSWORD_KEY)
        ):
            workload.container.add_layer(
                K8S_METRICS_SERVER_SERVICE_NAME,
                Layer(
                    LayerDict({
                        "services": {
                            K8S_METRICS_SERVER_SERVICE_NAME: cast(
                                "ServiceDict", self._generate_metrics_service()
                            )
                        }
                    })
                ),
                combine=True,
            )
            workload.container.restart(K8S_METRICS_SERVER_SERVICE_NAME)

    def _restart_ldap_sync_service(self) -> None:
        """Restart the LDAP sync service in case any configuration changed (port)."""
        workload = cast("K8sWorkload", self.workload)
        if not self.patroni_manager.member_started:
            logger.debug("Restart LDAP sync early exit: Patroni has not started yet")
            return

        sync_service = workload.container.pebble.get_services(names=[K8S_LDAP_SYNC_SERVICE_NAME])

        if not self.is_primary and sync_service[0].is_running():
            logger.debug("Stopping LDAP sync service. It must only run in the primary")
            workload.container.stop(K8S_LDAP_SYNC_SERVICE_NAME)

        if self.is_primary and not self.is_ldap_enabled:
            logger.debug("Stopping LDAP sync service")
            workload.container.stop(K8S_LDAP_SYNC_SERVICE_NAME)
            return

        if self.is_primary and self.is_ldap_enabled:
            workload.container.add_layer(
                K8S_LDAP_SYNC_SERVICE_NAME,
                Layer(
                    LayerDict({
                        "services": {
                            K8S_LDAP_SYNC_SERVICE_NAME: cast(
                                "ServiceDict", self._generate_ldap_service()
                            )
                        }
                    })
                ),
                combine=True,
            )
            logger.debug("Starting LDAP sync service")
            workload.container.restart(K8S_LDAP_SYNC_SERVICE_NAME)

    def _generate_metrics_jobs(self, enable_tls: bool) -> list[dict]:
        """Generate spec for Prometheus scraping (port)."""
        return [
            {"static_configs": [{"targets": [f"*:{METRICS_PORT}"]}]},
            {"static_configs": [{"targets": [f"*:{PGBACKREST_METRICS_PORT}"]}]},
            {
                "static_configs": [{"targets": ["*:8008"]}],
                "scheme": "https",
                "tls_config": {"insecure_skip_verify": True},
            },
        ]

    def _generate_metrics_service(self) -> dict:
        """Generate the metrics exporter service definition (port)."""
        workload = cast("K8sWorkload", self.workload)
        monitoring_password = self.state.get_secret(APP_SCOPE, MONITORING_PASSWORD_KEY)
        return workload.generate_metrics_service(monitoring_password, K8S_POSTGRESQL_SERVICE_NAME)

    def _generate_ldap_service(self) -> dict:
        """Generate the LDAP synchronizer service definition (port)."""
        workload = cast("K8sWorkload", self.workload)
        ldap_params = self.ldap.get_ldap_parameters()
        group_mappings = dict(
            PostgreSQLClient.build_postgresql_group_map(self.state.config.ldap_map)
        )
        if not (password := self.state.get_secret(APP_SCOPE, USER_PASSWORD_KEY)):
            raise Exception("No password generated yet")
        return workload.generate_ldap_service(
            ldap_params, group_mappings, password, K8S_POSTGRESQL_SERVICE_NAME
        )

    # -- Config re-render (port) ------------------------------------------------------

    def update_config(
        self,
        is_creating_backup: bool = False,
        no_peers: bool = False,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> bool:
        """Updates Patroni config file based on the existence of the TLS files (port)."""
        primary_cluster_endpoint = self.async_replication_manager.get_primary_cluster_endpoint()
        try:
            result = self.config_manager.update_config(
                self.postgresql,
                is_creating_backup=is_creating_backup,
                relations_user_databases_map=self.config_manager.build_relations_user_databases_map(
                    self.postgresql, self.model.relations.get(DATABASE, [])
                ),
                async_primary_cluster_endpoint=primary_cluster_endpoint,
                async_standby_endpoints=self.async_replication_manager.get_standby_endpoints(),
            )
        except DeployedWithoutTrustError:
            self.on_deployed_without_trust()
            return False
        # The lib's apply_api_config only SETS the DCS standby_cluster (when another
        # cluster is primary) and never CLEARS it. A force-promote bumps the promoted-
        # cluster-counter but - while the dead-DC relation still lingers - does not call
        # promote_standby_cluster(), so without this the cluster stays a read-only
        # standby leader (DPE-10203).
        if result and self.patroni_manager.member_started and primary_cluster_endpoint is None:
            self.patroni_manager.bulk_update_parameters_controller_by_patroni(
                {}, {"standby_cluster": None}
            )
        return result

    def _on_databases_change(self, _) -> None:
        """Handle databases change event (port)."""
        self.update_config()
        logger.debug("databases changed")
        timestamp = datetime.now()
        self.state.peer.data.update({"pg_hba_needs_update_timestamp": str(timestamp)})
        logger.debug(f"authorisation rules changed at {timestamp}")

    def _on_config_changed(self, event) -> None:
        """Handle configuration changes, like enabling plugins (port)."""
        if not self.state.application.is_cluster_initialised:
            logger.debug("Defer on_config_changed: cluster not initialised yet")
            event.defer()
            return

        if self.refresh_manager.refresh is None:
            logger.warning("Warning _on_config_changed: Refresh could be in progress")
        elif self.refresh_manager.refresh.in_progress:
            logger.debug("Defer on_config_changed: Refresh in progress")
            event.defer()
            return

        try:
            self.config_manager.validate_config_options(self.postgresql)
            # update config on every run
            self.update_config()
        except psycopg2.OperationalError:
            logger.debug("Defer on_config_changed: Cannot connect to database")
            event.defer()
            return
        except ValueError as e:
            self.set_unit_status(BlockedStatus("Configuration Error. Please check the logs"))
            logger.error("Invalid configuration: %s", str(e))
            return

        if self.config_manager.is_blocked and "Configuration Error" in self.unit.status.message:
            self.postgresql_events_handler._set_active_status()

        # Update the sync-standby endpoint in the async replication data.
        self.async_replication_manager.update_async_replication_data()

        if not self.unit.is_leader():
            return

        # Enable and/or disable the extensions.
        self.config_manager.reconcile_extensions(self.postgresql)

        if admin_secret_id := self.state.config.system_users:
            try:
                self.cluster_manager.rotate_system_user_passwords(self.postgresql, admin_secret_id)
            except PostgreSQLUpdateUserPasswordError:
                event.defer()

    # -- K8s API fixes and TLS reload (port) ------------------------------------------

    def fix_leader_annotation(self) -> bool:
        """Fix the leader annotation if it's missing (port)."""
        client = Client()
        try:
            endpoint = client.get(
                Endpoints, name=self.state.application.cluster_name, namespace=self.model.name
            )
            if (
                endpoint.metadata
                and endpoint.metadata.annotations is not None
                and "leader" not in endpoint.metadata.annotations
            ):
                patch = {
                    "metadata": {"annotations": {"leader": unit_name_to_pod_name(self.unit.name)}}
                }
                client.patch(
                    Endpoints,
                    name=self.state.application.cluster_name,
                    namespace=self.model.name,
                    obj=patch,
                )
                self.state.application.data.pop("cluster_initialised", None)
                logger.info("Fixed missing leader annotation")
        except ApiError as e:
            if e.status.code == 403:
                self.on_deployed_without_trust()
                return False
            # Ignore the error only when the resource doesn't exist.
            if e.status.code != 404:
                raise e
        return True

    def on_deployed_without_trust(self) -> None:
        """Blocks the application for deployments made without --trust (port)."""
        self.set_unit_status(
            BlockedStatus(
                f"Insufficient permissions, try: `juju trust {self.model.app.name} --scope=cluster`"
            )
        )
        logger.error(
            f"""
            Access to k8s cluster resources is not authorized. This happens when RBAC is enabled and the deployed application was not trusted by the juju admin.
            To fix this issue, run `juju trust {self.model.app.name} --scope=cluster` (or remove & re-deploy {self.model.app.name} with `--trust`)
            """
        )

    def update_pebble_layers(self) -> None:
        """Reconcile the workload's Pebble layers (port, with the replan guard)."""
        try:
            self.workload.update_pebble_layers(PebbleLayerSpec.from_state(self.state))
        except ChangeError as e:
            # A stale pgBackRest exporter from before a restore can still hold its
            # port ("bind: address already in use"), failing the replan mid-hook. Let
            # the hook finish; the next hook retries the replan instead of crashing
            # the unit into error state.
            logger.warning(
                "Failed to replan pebble services: %s - deferring the restart to the next hook",
                e,
            )

    def _reload_tls_after_push(self, event) -> None:
        """Reload PostgreSQL after the lib TLS handler pushed the cert files (port).

        The charm owns the container system CA trust store and the charm-local CA
        bundle; refresh both, then reload. A deferred push never emits, so this never
        runs against files that were never written.
        """
        self.sync_tls_trust_store_and_bundle()
        try:
            if not self.update_config():
                event.defer()
        except Exception:
            # Mirror the charm's broad push-failure guard: a transient Patroni/render
            # failure defers and retries rather than failing the hook.
            logger.exception("TLS reload (update_config) failed; deferring")
            event.defer()

    def create_pgdata(self) -> None:
        """Create the PostgreSQL data directories (ported from the K8s charm)."""
        # The stale-clear applies when a replica joins an initialized cluster; the
        # workload adds the pgdata-populated check.
        clear_stale_replica = (
            not self.state.model.unit.is_leader() and self.state.application.is_cluster_initialised
        )
        self.workload.init_storage(clear_stale_replica=clear_stale_replica)

    @property
    def primary_endpoint(self) -> str | None:
        """Address of the cluster primary's Service."""
        return self.state.primary_endpoint

    def get_async_primary_cluster_endpoint(self) -> str | None:
        """Endpoint of the primary cluster of the async replication partner, if any."""
        return None

    def update_relation_endpoints(self) -> None:
        """Refresh the client and async relation endpoints after a switchover."""

    def post_refresh_side_effects(self) -> None:
        """Run the post-refresh side effects; K8s has none (snap exporters are VM-only)."""

    def has_async_replication_relation(self) -> bool:
        """Whether this unit is related to an async replication partner."""
        return False

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

    def clean_ca_file_from_workload(self, secret_name: str) -> bool:
        """Clean up the CA certificate from the workload container (port)."""
        workload = cast("K8sWorkload", self.workload)
        workload.container.remove_path(f"{CERTS_PATH}/{secret_name}.crt")
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
