#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL VM Charm."""

import json
import logging
import os
import pathlib
import subprocess
import sys
from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import charm_refresh
from charmlibs import snap
from ops import ActiveStatus, BlockedStatus, StatusBase
from ops.log import JujuLogHandler

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    DATABASE,
    DATABASE_DEFAULT_NAME,
    MONITORING_PASSWORD_KEY,
    PEER_RELATION,
    REPLICATION_OFFER_RELATION,
    REPLICATION_PASSWORD_KEY,
    SYSTEM_USERS,
    UNIT_SCOPE,
    USER,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_IDENTITY,
)
from single_kernel_postgresql.utils.postgresql import (
    PostgreSQL as PostgreSQLClient,
)
from single_kernel_postgresql.workload.vm import VMWorkload

if TYPE_CHECKING:
    from ops.model import Relation

    from single_kernel_postgresql.workload.base import ResourceProvider

logger = logging.getLogger(__name__)

CERTS_PATH = "/usr/local/share/ca-certificates"


class PostgreSQLVMCharm(AbstractPostgreSQLCharm):
    """PostgreSQL VM Charm."""

    def __init__(
        self,
        *args,
        restart_lock_acquire: Callable[[], None] | None = None,
    ):
        """Initialize the PostgreSQL VM Charm.

        The thin charm constructs the COS requirer objects (charm libs stay
        charm-side) and passes any late-bound bridges in; everything else
        lives in the library.
        """
        super().__init__(*args)
        self.restart_lock_acquire = restart_lock_acquire
        self._charm_dir = pathlib.Path(os.environ.get("CHARM_DIR", "."))

        # Show the logger name (module name) in logs.
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, JujuLogHandler):
                handler.setFormatter(logging.Formatter("{name}:{message}", style="{"))

        # The lib TLS handler emits tls_files_pushed after a successful push;
        # the VM charm reloads (update_config) on it (port of _reload_tls_after_push).
        self.framework.observe(self.tls.tls_files_pushed, self._reload_tls_after_push)

        # Start the topology observer and the rotate-logs loop at construction
        # (port of the VM charm's init tail).
        self.observer_manager.start_observer()
        self.backup_manager.start_log_rotation()

        # Support for disabling the operator (port of the disable-file guard).
        disable_file = pathlib.Path(f"{os.environ.get('CHARM_DIR')}/disable")
        if disable_file.exists():
            logger.warning(
                f"\n\tDisable file `{disable_file.resolve()}` found, the charm will skip all events."
                "\n\tTo resume normal operations, please remove the file."
            )
            self.unit.status = BlockedStatus("Disabled")
            sys.exit(0)

    @property
    def workload(self) -> VMWorkload:
        """Access the current workload instance (port of the VM charm's property)."""
        return VMWorkload(charm_dir=self.charm_dir)

    @property
    def substrate(self) -> Substrates:
        """The VM substrate (port)."""
        return Substrates.VM

    @property
    def _peers(self) -> "Relation | None":
        """Fetch the peer relation (port)."""
        return self.model.get_relation(PEER_RELATION)

    @cached_property
    def postgresql(self) -> PostgreSQL:
        """Returns a client to interact with the database (port of the VM charm's property)."""
        return PostgreSQLClient(
            substrate=Substrates.VM,
            primary_host=self.primary_endpoint,
            # Connecting to the local PostgreSQL socket.
            current_host="/tmp/snap-private-tmp/snap.charmed-postgresql/tmp/",  # noqa: S108
            user=USER,
            password=str(self.state.get_secret(APP_SCOPE, f"{USER}-password")),
            database=DATABASE_DEFAULT_NAME,
            system_users=SYSTEM_USERS,
        )

    @cached_property
    def primary_endpoint(self) -> str | None:
        """The endpoint of the primary instance or None when no primary is available (port)."""
        if not self._peers:
            logger.debug("primary endpoint early exit: Peer relation not joined yet.")
            return None
        primary = self.patroni_manager.get_primary() or self.patroni_manager.get_standby_leader()
        primary_endpoint = self.patroni_manager.get_member_ip(primary) if primary else None
        # Force a retry if there is no primary or the member that was returned is
        # not in the list of the current cluster members (like when the cluster
        # was not updated yet after a failed switchover).
        if not primary_endpoint:
            logger.warning(f"Missing primary IP for {primary}")
        return primary_endpoint

    @property
    def is_primary(self) -> bool:
        """Whether this unit is the primary instance (port)."""
        return self.unit.name == self.patroni_manager.get_primary(unit_name_pattern=True)

    @property
    def is_standby_leader(self) -> bool:
        """Whether this unit is the standby leader instance (port)."""
        return self.unit.name == self.patroni_manager.get_standby_leader(unit_name_pattern=True)

    @property
    def is_ldap_charm_related(self) -> bool:
        """Whether an LDAP charm is related (port)."""
        return self.state.application.data.get("ldap_enabled", "False") == "True"

    @property
    def is_ldap_enabled(self) -> bool:
        """Whether LDAP is enabled (port)."""
        return self.is_ldap_charm_related and self.state.application.is_cluster_initialised

    @property
    def _get_password(self) -> str | None:
        """The operator user password from the peer relation (port)."""
        return self.state.get_secret(APP_SCOPE, USER_PASSWORD_KEY)

    @property
    def _replication_password(self) -> str | None:
        """The replication user password from the peer relation (port)."""
        return self.state.get_secret(APP_SCOPE, REPLICATION_PASSWORD_KEY)

    def get_resource_provider(self) -> "ResourceProvider":
        """The (cpu_cores, memory_bytes) introspector; bridge for ConfigManager (port)."""
        return self.workload

    def request_restart(self) -> None:
        """Clear the restarted flag and acquire the rolling-restart lock (port).

        The rolling-ops manager is a charm lib and stays thin-side; it is
        injected through ``restart_lock_acquire``.
        """
        self.state.peer.data.pop("postgresql_restarted", None)
        if self.restart_lock_acquire:
            self.restart_lock_acquire()

    def restart_services(self) -> None:
        """Restart the monitoring and LDAP sync snap services if needed (port)."""
        cache = snap.SnapCache()
        postgres_snap = cache[charm_refresh.snap_name()]
        self._restart_metrics_service(postgres_snap)
        self._restart_ldap_sync_service(postgres_snap)

    def _restart_metrics_service(self, postgres_snap: snap.Snap) -> None:
        """Restart the monitoring service if the password was rotated (port)."""
        try:
            snap_password = postgres_snap.get("exporter.password")
        except snap.SnapError:
            logger.warning("Early exit: Trying to reset metrics service with no configuration set")
            return None

        if snap_password != self.state.get_secret(APP_SCOPE, MONITORING_PASSWORD_KEY):
            self._setup_exporter(postgres_snap)

    def _restart_ldap_sync_service(self, postgres_snap: snap.Snap) -> None:
        """Restart the LDAP sync service in case any configuration changed (port)."""
        if not self.patroni_manager.member_started:
            logger.debug("Restart LDAP sync early exit: Patroni has not started yet")
            return

        sync_service = postgres_snap.services["ldap-sync"]

        if not self.is_primary and sync_service["active"]:
            logger.debug("Stopping LDAP sync service. It must only run in the primary")
            postgres_snap.stop(services=["ldap-sync"])

        if self.is_primary and not self.is_ldap_enabled:
            logger.debug("Stopping LDAP sync service")
            postgres_snap.stop(services=["ldap-sync"])
            return

        if self.is_primary and self.is_ldap_enabled:
            self._setup_ldap_sync(postgres_snap)

    def _setup_exporter(self, postgres_snap: snap.Snap | None = None) -> None:
        """Set up postgresql_exporter options (port)."""
        if postgres_snap is None:
            cache = snap.SnapCache()
            postgres_snap = cache[charm_refresh.snap_name()]

        postgres_snap.set({
            "exporter.user": "monitoring",
            "exporter.password": self.state.get_secret(APP_SCOPE, MONITORING_PASSWORD_KEY),
        })

        if postgres_snap.services["prometheus-postgres-exporter"]["active"] is False:
            postgres_snap.start(services=["prometheus-postgres-exporter"], enable=True)
        else:
            postgres_snap.restart(services=["prometheus-postgres-exporter"])

        self.state.peer.data.update({"exporter-started": "True"})

    def _setup_pgbackrest_exporter(self, postgres_snap: snap.Snap | None = None) -> None:
        """Set up pgbackrest_exporter (port)."""
        if postgres_snap is None:
            cache = snap.SnapCache()
            postgres_snap = cache[charm_refresh.snap_name()]

        if postgres_snap.services["pgbackrest-exporter"]["active"] is False:
            postgres_snap.start(services=["pgbackrest-exporter"], enable=True)
        else:
            postgres_snap.restart(services=["pgbackrest-exporter"])

        self.state.peer.data.update({"pgbackrest-exporter-started": "True"})

    def _setup_ldap_sync(self, postgres_snap: snap.Snap | None = None) -> None:
        """Set up postgresql_ldap_sync options (port)."""
        if postgres_snap is None:
            cache = snap.SnapCache()
            postgres_snap = cache[charm_refresh.snap_name()]

        ldap_params = self.ldap.get_ldap_parameters()
        ldap_url = urlparse(ldap_params["ldapurl"])
        ldap_group_mappings = self.postgresql.build_postgresql_group_map(
            self.state.config.ldap_map
        )

        postgres_snap.set({
            "ldap-sync.ldap_host": ldap_url.hostname,
            "ldap-sync.ldap_port": ldap_url.port,
            "ldap-sync.ldap_base_dn": ldap_params["ldapbasedn"],
            "ldap-sync.ldap_bind_username": ldap_params["ldapbinddn"],
            "ldap-sync.ldap_bind_password": ldap_params["ldapbindpasswd"],
            "ldap-sync.ldap_group_identity": json.dumps(ACCESS_GROUP_IDENTITY),
            "ldap-sync.ldap_group_mappings": json.dumps(ldap_group_mappings),
            "ldap-sync.postgres_host": "127.0.0.1",
            "ldap-sync.postgres_port": 5432,
            "ldap-sync.postgres_database": DATABASE_DEFAULT_NAME,
            "ldap-sync.postgres_username": USER,
            "ldap-sync.postgres_password": self._get_password,
        })

        logger.debug("Starting LDAP sync service")
        postgres_snap.restart(services=["ldap-sync"])

    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> None:
        """Set unit status without overriding higher priority refresh status (port)."""
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
            pathlib.Path(".last_refresh_unit_status.json").write_text(
                json.dumps(refresh_status.message)
            )
            return
        self.unit.status = status

    def set_app_status(self, status: StatusBase) -> None:
        """Set the application status without overriding a higher-priority refresh status (port)."""
        if self.refresh_manager.refresh is not None and (
            higher := self.refresh_manager.refresh.app_status_higher_priority
        ):
            self.app.status = higher
            return
        self.app.status = status

    def set_default_unit_status(self) -> None:
        """Set the unit status that applies when no refresh status is active (port)."""
        self.set_unit_status(ActiveStatus())

    def set_primary_status_message(self) -> None:
        """Display 'Primary' in the unit status message if this unit is the primary (port)."""
        try:
            if (
                self.unit.is_leader()
                and "s3-initialization-block-message" in self.state.application.data
            ):
                self.set_unit_status(
                    BlockedStatus(self.state.application.data["s3-initialization-block-message"])
                )
                return
            if (
                self.patroni_manager.get_primary(unit_name_pattern=True) == self.unit.name
                or self.is_standby_leader
            ):
                danger_state = ""
                if not self.raft_manager.has_raft_quorum():
                    danger_state = " (read-only)"
                elif (
                    len(self.patroni_manager.get_running_cluster_members())
                    < self.state.application.planned_units
                ):
                    danger_state = " (degraded)"
                unit_status = "Standby" if self.is_standby_leader else "Primary"
                self.set_unit_status(ActiveStatus(f"{unit_status}{danger_state}"))
            elif self.patroni_manager.member_started:
                self.set_unit_status(ActiveStatus())
        except Exception as e:
            logger.error(f"failed to get primary with error {e}")

    def update_config(
        self,
        is_creating_backup: bool = False,
        no_peers: bool = False,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> bool:
        """Update the Patroni config file based on the TLS files (port).

        Widen port of the charm's wrapper: it feeds the manager the per-relation
        inputs and clears the stale DCS standby_cluster (DPE-10203).
        """
        if refresh is None:
            refresh = self.refresh_manager.refresh
        primary_cluster_endpoint = self.async_replication.get_primary_cluster_endpoint()
        result = self.config_manager.update_config(
            self.postgresql,
            is_creating_backup=is_creating_backup,
            relations_user_databases_map=self.config_manager.build_relations_user_databases_map(
                self.postgresql, self.state.model.relations.get(DATABASE, [])
            ),
            async_primary_cluster_endpoint=primary_cluster_endpoint,
            async_partner_addresses=self.async_replication.get_partner_addresses(),
            async_standby_endpoints=self.async_replication.get_standby_endpoints(),
            watcher_raft_address=(
                self.watcher_handler.watcher_raft_address
                if self.watcher_handler.is_active
                else None
            ),
            no_peers=no_peers,
            refresh=refresh,
        )
        # The lib's apply_api_config only SETS the DCS standby_cluster (when another
        # cluster is primary) and never CLEARS it. A force-promote bumps the
        # promoted-cluster-counter but — while the dead-DC relation still lingers — does
        # not call promote_standby_cluster(), so without this the reconciler never clears
        # the stale standby and the cluster stays a read-only standby leader (DPE-10203).
        if (
            result
            and not no_peers
            and self.patroni_manager.member_started
            and primary_cluster_endpoint is None
        ):
            self.patroni_manager.bulk_update_parameters_controller_by_patroni(
                {}, {"standby_cluster": None}
            )
        return result

    def post_refresh_side_effects(self) -> None:
        """Post-refresh workload side effects (port): exporters, pgbackrest, watcher."""
        self._setup_exporter()
        self._setup_pgbackrest_exporter()
        self.backup_manager.start_stop_pgbackrest_service()
        self.watcher_handler.update_unit_address()

    def has_async_replication_relation(self) -> bool:
        """Whether the async-replication (replication-offer) relation exists (port)."""
        return self.model.get_relation(REPLICATION_OFFER_RELATION) is not None

    def get_async_primary_cluster_endpoint(self) -> str | None:
        """The async primary cluster endpoint via the replication facade (port)."""
        return self.async_replication.get_primary_cluster_endpoint()

    def update_relation_endpoints(self) -> None:
        """Update endpoints and the read-only endpoint in all relations (port)."""
        self.database_manager.update_endpoints(**self._client_endpoint_inputs())

    def _client_endpoint_inputs(self) -> dict:
        """Gather the update_endpoints inputs the manager takes caller-side (port).

        The Patroni cluster-status query is a live REST call: gather it only on
        the leader (update_endpoints returns early otherwise), so non-leader
        units fire no extra REST calls.
        """
        return {
            "online_members": (
                self.patroni_manager.online_cluster_members() if self.unit.is_leader() else None
            ),
            "client_tls_files": self.tls_manager.get_client_tls_files(),
        }

    def update_pebble_layers(self) -> None:
        """No-op on VM: Pebble layers are a K8s concern (the snap services are managed)."""
        logger.debug("update_pebble_layers: not applicable on the VM substrate")

    def ensure_pgdata_dirs_and_symlinks(self) -> None:
        """Ensure the VM storage layout (port: the charm's _ensure_storage_layout)."""
        self.workload.ensure_storage_layout()

    def push_ca_file_into_workload(self, secret_name: str) -> bool:
        """Move the CA certificates file into the PostgreSQL storage path (port)."""
        certs = self.state.get_secret(UNIT_SCOPE, secret_name)
        if certs is not None:
            certs_file = pathlib.Path(CERTS_PATH, f"{secret_name}.crt")
            certs_file.write_text(certs)
            subprocess.check_call(["/usr/sbin/update-ca-certificates"])

        try:
            return self.update_config()
        except Exception:
            logger.exception("CA file failed to push. Error in config update")
            return False

    def clean_ca_file_from_workload(self, secret_name: str) -> bool:
        """Clean up the CA certificates from the PostgreSQL storage path (port)."""
        certs_file = pathlib.Path(CERTS_PATH, f"{secret_name}.crt")
        certs_file.unlink()

        subprocess.check_call(["/usr/sbin/update-ca-certificates"])

        try:
            return self.update_config()
        except Exception:
            logger.exception("CA file failed to clean. Error in config update")
            return False

    def _reload_tls_after_push(self, event) -> None:
        """Reload PostgreSQL after the lib TLS handler has pushed the cert files (port)."""
        try:
            if not self.update_config():
                event.defer()
        except Exception:
            logger.exception("TLS reload (update_config) failed; deferring")
            event.defer()

    def _regenerate_internal_cert(self, *, reload: bool = True) -> None:
        """Generate the internal peer cert, push it, and optionally reload (port).

        reload=False is used at cluster bootstrap: the leader renders patroni.yml
        on leader-elected and each replica renders it in the peer-relation-changed
        flow just before starting Patroni, so a reload here would be redundant.
        """
        self.tls_manager.generate_internal_peer_cert()
        self.tls_manager.push_tls_files()
        if reload:
            self.update_config()

    def _update_certificate(self) -> None:
        """Update the TLS certificate if the unit IP changes (port)."""
        # Request the certificate only if there is already one. If there isn't,
        # the certificate will be generated in the relation-joined event when
        # relating to the TLS Certificates Operator.
        if all(self.tls_manager.get_client_tls_files()) or all(
            self.tls_manager.get_peer_tls_files()
        ):
            self.tls.refresh_tls_certificates_event.emit()
        if self.state.get_secret(UNIT_SCOPE, "internal-cert"):
            self._regenerate_internal_cert()

    def patroni_scrape_config(self) -> list[dict]:
        """The scrape config for the Patroni metrics endpoint (port)."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self.state.unit_ip}:8008"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "https",
            }
        ]
