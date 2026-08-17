#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config Manager.

Responsible for managing the configuration of the PostgreSQL instance.
"""

import importlib.resources
import logging
from collections.abc import Callable
from functools import cached_property
from hashlib import shake_128
from typing import TYPE_CHECKING, Any, cast

import charm_refresh
import psycopg2
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from jinja2 import Template
from tenacity import RetryError, Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import PostgreSQLCannotConnectError
from single_kernel_postgresql.config.literals import (
    PGBACKREST_CONF_FILE,
    POSTGRESQL_STORAGE_PERMISSIONS,
    REWIND_USER,
    USER,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils import _change_owner, render_file
from single_kernel_postgresql.utils.postgresql import PostgreSQL as PostgreSQLClient
from single_kernel_postgresql.workload.base import BaseWorkload, ResourceProvider

if TYPE_CHECKING:
    # Import-time only: the VM workload pulls the snap charm lib, which K8s does not ship.
    from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


class ConfigManager(BaseManager):
    """PostgreSQL Config Manager.

    This manager is responsible for handling configuration operations.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        tls_manager: TLSManager,
        patroni_manager: PatroniManager,
        resource_provider: Callable[[], ResourceProvider],
        request_restart: Callable[[], None],
        refresh_endpoints: Callable[[], None],
        restart_services: Callable[[], None],
    ):
        super().__init__(state, workload, "config_manager")
        self.tls_manager = tls_manager
        self.patroni_manager = patroni_manager
        # Resolved on use, not at construction: the K8s manager that provides it is built
        # after this manager in the charm's __init__.
        self.resource_provider = resource_provider
        # Charm-side bridges: the substrate-tangled restart trigger, endpoint refresh and
        # monitoring/ldap service restarts stay in the charm until their own migration phases.
        self.request_restart = request_restart
        self.refresh_endpoints = refresh_endpoints
        self.restart_services = restart_services

    @staticmethod
    def _dict_to_hba_string(_dict: dict[str, Any]) -> str:
        """Transform a dictionary into a Host Based Authentication valid string."""
        for key, value in _dict.items():
            if isinstance(value, bool):
                _dict[key] = int(value)
            if isinstance(value, str):
                _dict[key] = f'"{value}"'

        return " ".join(f"{key}={value}" for key, value in _dict.items())

    def configure_patroni_on_unit(self):
        """Configure Patroni (configuration files and service) on the unit."""
        _change_owner(self.state.substrate, str(self.workload.paths.data))

        # Create empty base config
        self.workload.write_text("", self.workload.paths.postgresql_conf)

        # Expected permission
        # Replicas refuse to start with the default permissions
        self.workload.mkdir(
            self.workload.paths.data, mode=POSTGRESQL_STORAGE_PERMISSIONS, exist_ok=True
        )

    def _calculate_max_worker_processes(self, cpu_cores: int) -> str | None:
        """Calculate cpu_max_worker_processes configuration value."""
        if self.state.config.cpu_max_worker_processes == "auto":
            # auto = minimum(8, 2 * vCores)
            return str(min(8, 2 * cpu_cores))
        elif self.state.config.cpu_max_worker_processes is not None:
            value = self.state.config.cpu_max_worker_processes
            cap = 10 * cpu_cores
            if value > cap:
                raise ValueError(
                    f"cpu-max-worker-processes value {value} exceeds maximum allowed "
                    f"of {cap} (10 * vCores). Please set a value <= {cap}."
                )
            return str(value)
        return None

    def _validate_worker_config_value(self, param_name: str, value: int, cpu_cores: int) -> str:
        """Shared validation logic for worker process parameters.

        Args:
            param_name: the configuration parameter name (for error messages).
            value: the integer value to validate.
            cpu_cores: the number of available CPU cores.

        Returns:
            String representation of the validated value.

        Raises:
            ValueError: if value exceeds 10 * vCores.
        """
        cap = 10 * cpu_cores
        if value > cap:
            raise ValueError(
                f"{param_name} value {value} exceeds maximum allowed "
                f"of {cap} (10 * vCores). Please set a value <= {cap}."
            )
        return str(value)

    def _calculate_max_parallel_workers(self, base_max_workers: int, cpu_cores: int) -> str | None:
        """Calculate cpu_max_parallel_workers configuration value."""
        if self.state.config.cpu_max_parallel_workers == "auto":
            return str(base_max_workers)
        elif self.state.config.cpu_max_parallel_workers is not None:
            validated_value_str = self._validate_worker_config_value(
                "cpu-max-parallel-workers", self.state.config.cpu_max_parallel_workers, cpu_cores
            )
            # Apply the min constraint with base_max_workers
            return str(min(int(validated_value_str), base_max_workers))
        return None

    def _calculate_max_parallel_maintenance_workers(
        self, base_max_workers: int, cpu_cores: int
    ) -> str | None:
        """Calculate cpu_max_parallel_maintenance_workers configuration value."""
        if self.state.config.cpu_max_parallel_maintenance_workers == "auto":
            return str(base_max_workers)
        elif self.state.config.cpu_max_parallel_maintenance_workers is not None:
            return self._validate_worker_config_value(
                "cpu-max-parallel-maintenance-workers",
                self.state.config.cpu_max_parallel_maintenance_workers,
                cpu_cores,
            )
        return None

    def _calculate_max_logical_replication_workers(
        self, base_max_workers: int, cpu_cores: int
    ) -> str | None:
        """Calculate cpu_max_logical_replication_workers configuration value."""
        if self.state.config.cpu_max_logical_replication_workers == "auto":
            return str(base_max_workers)
        elif self.state.config.cpu_max_logical_replication_workers is not None:
            return self._validate_worker_config_value(
                "cpu-max-logical-replication-workers",
                self.state.config.cpu_max_logical_replication_workers,
                cpu_cores,
            )
        return None

    def _calculate_max_sync_workers_per_subscription(
        self, base_max_workers: int, cpu_cores: int
    ) -> str | None:
        """Calculate cpu_max_sync_workers_per_subscription configuration value."""
        if self.state.config.cpu_max_sync_workers_per_subscription == "auto":
            return str(base_max_workers)
        elif self.state.config.cpu_max_sync_workers_per_subscription is not None:
            return self._validate_worker_config_value(
                "cpu-max-sync-workers-per-subscription",
                self.state.config.cpu_max_sync_workers_per_subscription,
                cpu_cores,
            )
        return None

    def _calculate_max_parallel_apply_workers_per_subscription(
        self, base_max_workers: int, cpu_cores: int
    ) -> str | None:
        """Calculate cpu_max_parallel_apply_workers_per_subscription configuration value."""
        if self.state.config.cpu_max_parallel_apply_workers_per_subscription == "auto":
            return str(base_max_workers)
        elif self.state.config.cpu_max_parallel_apply_workers_per_subscription is not None:
            return self._validate_worker_config_value(
                "cpu-max-parallel-apply-workers-per-subscription",
                self.state.config.cpu_max_parallel_apply_workers_per_subscription,
                cpu_cores,
            )
        return None

    def _calculate_worker_process_config(self, cpu_cores: int) -> dict[str, str]:
        """Calculate worker process configuration values.

        Handles 'auto' values and capping logic for worker process parameters.
        Returns a dictionary with the calculated values ready for PostgreSQL.
        """
        result: dict[str, str] = {}

        # Calculate cpu_max_worker_processes (baseline for other worker configs)
        cpu_max_worker_processes_value = self._calculate_max_worker_processes(cpu_cores)
        if cpu_max_worker_processes_value is not None:
            result["max_worker_processes"] = cpu_max_worker_processes_value

        # Get the effective cpu_max_worker_processes for dependent configs
        # Use the calculated value, or fall back to PostgreSQL default (8)
        base_max_workers = int(result.get("max_worker_processes", "8"))

        # Calculate other worker parameters
        cpu_max_parallel_workers_value = self._calculate_max_parallel_workers(
            base_max_workers, cpu_cores
        )
        if cpu_max_parallel_workers_value is not None:
            result["max_parallel_workers"] = cpu_max_parallel_workers_value

        cpu_max_parallel_maintenance_workers_value = (
            self._calculate_max_parallel_maintenance_workers(base_max_workers, cpu_cores)
        )
        if cpu_max_parallel_maintenance_workers_value is not None:
            result["max_parallel_maintenance_workers"] = cpu_max_parallel_maintenance_workers_value

        cpu_max_logical_replication_workers_value = (
            self._calculate_max_logical_replication_workers(base_max_workers, cpu_cores)
        )
        if cpu_max_logical_replication_workers_value is not None:
            result["max_logical_replication_workers"] = cpu_max_logical_replication_workers_value

        cpu_max_sync_workers_per_subscription_value = (
            self._calculate_max_sync_workers_per_subscription(base_max_workers, cpu_cores)
        )
        if cpu_max_sync_workers_per_subscription_value is not None:
            result["max_sync_workers_per_subscription"] = (
                cpu_max_sync_workers_per_subscription_value
            )

        cpu_max_parallel_apply_workers_per_subscription_value = (
            self._calculate_max_parallel_apply_workers_per_subscription(
                base_max_workers, cpu_cores
            )
        )
        if cpu_max_parallel_apply_workers_per_subscription_value is not None:
            result["max_parallel_apply_workers_per_subscription"] = (
                cpu_max_parallel_apply_workers_per_subscription_value
            )

        return result

    def _build_postgresql_parameters(
        self, postgresql_client: PostgreSQLClient, cpu_cores: int, available_memory: int
    ) -> dict[str, str] | None:
        """Build PostgreSQL configuration parameters.

        Returns:
            Dictionary of PostgreSQL parameters or None if base parameters couldn't be built.
        """
        limit_memory = None
        if self.state.config.profile_limit_memory:
            limit_memory = self.state.config.profile_limit_memory * 10**6

        # Build PostgreSQL parameters.
        pg_parameters = postgresql_client.build_postgresql_parameters(
            self.state.model_config, available_memory, limit_memory
        )

        # Calculate and merge worker process configurations
        worker_configs = self._calculate_worker_process_config(cpu_cores)

        # Add cpu_wal_compression configuration (separate from worker processes)
        if self.state.config.cpu_wal_compression is not None:
            cpu_wal_compression = "on" if self.state.config.cpu_wal_compression else "off"
        else:
            # Use config.yaml default when unset (default: true)
            cpu_wal_compression = "on"

        if pg_parameters is not None:
            pg_parameters.update(worker_configs)
            pg_parameters["wal_compression"] = cpu_wal_compression
        else:
            pg_parameters = dict(worker_configs)
            pg_parameters["wal_compression"] = cpu_wal_compression

        return pg_parameters

    @property
    def is_tls_enabled(self) -> bool:
        """Return whether client TLS is enabled and the files are serveable.

        Issued certs reach the relation databag before the push writes them to the
        workload, so the on-disk check is what stops a render turning ssl on against
        files that are not there yet.
        """
        return all(self.tls_manager.get_client_tls_files()) and (
            self.tls_manager.client_tls_files_on_disk()
        )

    @cached_property
    def generate_config_hash(self) -> str:
        """Generate current configuration hash."""
        return shake_128(str(self.state.config.model_dump()).encode()).hexdigest(16)

    def _can_connect_to_postgresql(self, postgresql_client: PostgreSQLClient) -> bool:
        if self.state.substrate == Substrates.VM and (
            not postgresql_client.password or not postgresql_client.current_host
        ):
            return False
        try:
            for attempt in Retrying(stop=stop_after_delay(10), wait=wait_fixed(3)):
                with attempt:
                    if not postgresql_client.get_postgresql_timezones():
                        logger.debug("Cannot connect to database (CannotConnectError)")
                        raise PostgreSQLCannotConnectError
        except RetryError:
            logger.debug("Cannot connect to database (RetryError)")
            return False
        return True

    def is_restart_pending(self, postgresql_client: PostgreSQLClient) -> bool:
        """Query pg_settings for pending restart."""
        connection = None
        try:
            with (
                postgresql_client._connect_to_database(
                    database_host=postgresql_client.current_host
                ) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("SELECT COUNT(*) FROM pg_settings WHERE pending_restart=True;")
                result = cursor.fetchone()
                if result is not None:
                    return result[0] > 0
                else:
                    return False
        except psycopg2.OperationalError:
            logger.warning("Failed to connect to PostgreSQL.")
            return False
        except psycopg2.Error as e:
            logger.error(f"Failed to check if restart is pending: {e}")
            return False
        finally:
            if connection:
                connection.close()

    def apply_api_config(
        self,
        cpu_cores: int,
        async_primary_cluster_endpoint: str | None = None,
    ) -> bool:
        """Update the parameters controlled by Patroni via its API."""
        # Use config value if set, calculate otherwise
        max_connections = (
            self.state.config.experimental_max_connections
            if self.state.config.experimental_max_connections
            else max(4 * cpu_cores, 100)
        )
        cfg_patch: dict[str, int | str | None] = {
            "max_connections": max_connections,
            "max_prepared_transactions": self.state.config.memory_max_prepared_transactions,
            "max_replication_slots": 25,
            "max_wal_senders": 25,
            "shared_buffers": self.state.config.memory_shared_buffers,
            "wal_keep_size": self.state.config.durability_wal_keep_size,
        }

        # Add restart-required worker process parameters via Patroni API
        worker_configs = self._calculate_worker_process_config(cpu_cores)
        if "max_worker_processes" in worker_configs:
            cfg_patch["max_worker_processes"] = worker_configs["max_worker_processes"]
        if "max_logical_replication_workers" in worker_configs:
            cfg_patch["max_logical_replication_workers"] = worker_configs[
                "max_logical_replication_workers"
            ]

        base_patch = {
            **self.state.synchronous_configuration,
            "maximum_lag_on_failover": self.state.config.durability_maximum_lag_on_failover,
        }
        if async_primary_cluster_endpoint:
            base_patch["standby_cluster"] = {"host": async_primary_cluster_endpoint}
        try:
            self.patroni_manager.bulk_update_parameters_controller_by_patroni(
                cfg_patch, base_patch
            )
        except RetryError:
            return False
        return True

    def handle_restart_need(
        self, postgresql_client: PostgreSQLClient, config_changed: bool
    ) -> None:
        """Handle PostgreSQL restart need based on the TLS configuration and configuration changes."""
        if self._can_connect_to_postgresql(postgresql_client):
            # check_current_host is a VM-only precision in the live TLS probe.
            check_current_host = (
                {"check_current_host": True} if (self.state.substrate == Substrates.VM) else {}
            )
            restart_postgresql = self.is_tls_enabled != postgresql_client.is_tls_enabled(
                **check_current_host
            )
        else:
            restart_postgresql = False

        try:
            self.patroni_manager.reload_patroni_configuration()
        except Exception as e:
            logger.error(f"Reload patroni call failed! error: {e!s}")

        if config_changed and not restart_postgresql:
            # Wait for some more time than the Patroni's loop_wait default value (10 seconds),
            # which tells how much time Patroni will wait before checking the configuration
            # file again to reload it.
            try:
                for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3)):
                    with attempt:
                        restart_postgresql = restart_postgresql or self.is_restart_pending(
                            postgresql_client
                        )
                        if not restart_postgresql:
                            raise Exception
            except RetryError:
                # Ignore the error, as it happens only to indicate that the configuration has not changed.
                pass

        self.state.peer.tls = self.is_tls_enabled
        self.refresh_endpoints()

        # Restart PostgreSQL if TLS configuration has changed
        # (so the both old and new connections use the configuration).
        if restart_postgresql:
            logger.info("PostgreSQL restart required")
            self.request_restart()

    def update_config(
        self,
        postgresql_client: PostgreSQLClient,
        user_hash: str,
        is_creating_backup: bool = False,
        # TODO add rel handler
        relations_user_databases_map: dict[str, Any] | None = None,
        # TODO add rel handler
        ldap_parameters: dict[str, Any] | None = None,
        # TODO add rel handler
        async_primary_cluster_endpoint: str | None = None,
        async_partner_addresses: list[str] | None = None,
        async_standby_endpoints: list[str] | None = None,
        # TODO add rel handler
        watcher_raft_address: str | None = None,
        no_peers: bool = False,
        *,
        refresh: charm_refresh.Machines | None = None,
    ) -> bool:
        """Updates Patroni config file based on the existence of the TLS files.

        Raises:
            DeployedWithoutTrustError: on K8s when the app lacks cluster trust; the caller
                is expected to catch this.
        """
        # Snapshot resources once so parameter-building and the API patch agree. On K8s
        # these are lightkube reads; re-fetching per callee doubled the API calls and
        # could disagree mid-scaling.
        cpu_cores, available_memory = self.resource_provider().get_available_resources()

        # Build PostgreSQL parameters
        pg_parameters = self._build_postgresql_parameters(
            postgresql_client, cpu_cores, available_memory
        )

        # replication_slots = self.logical_replication.replication_slots()
        replication_slots = {}

        # TODO add rel handler
        relations_user_databases_map = relations_user_databases_map or {}

        # Update and reload configuration based on TLS files availability.
        logger.info("Updating Patroni config file")
        logger.debug(f"Calling render_patroni_yml_file with parameters = {pg_parameters}")
        self.render_patroni_yml_file(
            connectivity=self.state.peer.is_connectivity_enabled,
            is_creating_backup=is_creating_backup,
            enable_ldap=self.state.application.is_ldap_enabled,
            enable_tls=self.is_tls_enabled,
            backup_id=self.state.application.data.get("restoring-backup"),
            pitr_target=self.state.application.data.get("restore-to-time"),
            restore_timeline=self.state.application.data.get("restore-timeline"),
            restore_to_latest=self.state.application.data.get("restore-to-time", None) == "latest",
            stanza=self.state.application.data.get("stanza", self.state.peer.data.get("stanza")),
            restore_stanza=self.state.application.data.get("restore-stanza"),
            parameters=pg_parameters,
            user_databases_map=relations_user_databases_map,
            slots=replication_slots,
            ldap_parameters=ldap_parameters,
            async_primary_cluster_endpoint=async_primary_cluster_endpoint,
            async_partner_addresses=async_partner_addresses,
            async_standby_endpoints=async_standby_endpoints,
            watcher_raft_address=watcher_raft_address,
            no_peers=no_peers,
        )
        if no_peers:
            return True

        if not self.workload.is_patroni_running():
            # If Patroni/PostgreSQL has not started yet and TLS relations was initialised,
            # then mark TLS as enabled. This commonly happens when the charm is deployed
            # in a bundle together with the TLS certificates operator. This flag is used to
            # know when to call the Patroni API using HTTP or HTTPS.
            self.state.peer.tls = self.is_tls_enabled
            self.refresh_endpoints()
            logger.debug("Early exit update_config: Workload not started yet")
            return True

        if not self.patroni_manager.member_started:
            if self.is_tls_enabled:
                logger.debug(
                    "Early exit update_config: patroni not responding but TLS is enabled."
                )
                self.handle_restart_need(postgresql_client, True)
                return True
            logger.debug("Early exit update_config: Patroni not started yet")
            return False

        # Try to connect. Patroni's REST API patch (below) doesn't need the PG-client
        # connection, so this standalone gate is VM-only; K8s proceeds straight to it.
        if self.state.substrate == Substrates.VM and not self._can_connect_to_postgresql(
            postgresql_client
        ):
            logger.warning("Early exit update_config: Cannot connect to Postgresql")
            return False

        if not self.apply_api_config(cpu_cores, async_primary_cluster_endpoint):
            logger.warning("Early exit update_config: Unable to patch Patroni API")
            return False

        if self.state.substrate == Substrates.K8S and not (
            self.patroni_manager.ensure_slots_controller_by_patroni(replication_slots)
        ):
            logger.warning(
                "Failed to sync replication slots with Patroni — will retry on next config update"
            )

        self.handle_restart_need(
            postgresql_client, self.state.peer.config_hash != self.generate_config_hash
        )

        # TODO handle case of scale up while refresh in progress & `refresh` is None
        if (
            self.state.substrate == Substrates.VM
            and refresh is not None
            and cast("VMWorkload", self.workload).get_snap_revision()
            != refresh.pinned_snap_revision
        ):
            logger.debug("Early exit: snap was not refreshed to the right version yet")
            return True

        self.restart_services()

        self.state.peer.user_hash = user_hash
        self.state.peer.config_hash = self.generate_config_hash
        if self.state.peer.is_app_leader:
            self.state.application.user_hash = user_hash
        return True

    def render_patroni_yml_file(
        self,
        connectivity: bool = False,
        is_creating_backup: bool = False,
        enable_ldap: bool = False,
        enable_tls: bool = False,
        is_no_sync_member: bool = False,
        stanza: str | None = None,
        restore_stanza: str | None = None,
        disable_pgbackrest_archiving: bool = False,
        backup_id: str | None = None,
        pitr_target: str | None = None,
        restore_timeline: str | None = None,
        restore_to_latest: bool = False,
        parameters: dict[str, str] | None = None,
        no_peers: bool = False,
        user_databases_map: dict[str, str] | None = None,
        slots: dict[str, str] | None = None,
        # LDAP rel
        ldap_parameters: dict[str, Any] | None = None,
        # Async rel
        async_primary_cluster_endpoint: str | None = None,
        async_partner_addresses: list[str] | None = None,
        async_standby_endpoints: list[str] | None = None,
        # VM watcher rel
        watcher_raft_address: str | None = None,
    ) -> None:
        """Render the Patroni configuration file.

        Args:
            connectivity: whether to allow external connections to the database.
            is_creating_backup: whether this unit is creating a backup.
            enable_ldap: whether to enable LDAP authentication.
            enable_tls: whether to enable client TLS.
            is_no_sync_member: whether this member shouldn't be a synchronous standby
                (when it's a replica). K8s only.
            stanza: name of the stanza created by pgBackRest.
            restore_stanza: name of the stanza used when restoring a backup.
            disable_pgbackrest_archiving: whether to force disable pgBackRest WAL archiving.
            backup_id: id of the backup that is being restored.
            pitr_target: point-in-time-recovery target for the restore.
            restore_timeline: timeline to restore from.
            restore_to_latest: restore all the WAL transaction logs from the stanza.
            parameters: PostgreSQL parameters to be added to the postgresql.conf file.
            no_peers: Don't include peers.
            user_databases_map: map of databases to be accessible by each user.
            slots: replication slots (keys) with assigned database name (values).
            ldap_parameters: LDAP configuration.
            async_primary_cluster_endpoint: Primary async cluster endpoint.
            async_standby_endpoints: Primary async cluster endpoint.
            async_partner_addresses: Primary async cluster endpoint.
            watcher_raft_address: IP address of a related Raft watcher.
        """
        slots = slots or {}
        ldap_parameters = ldap_parameters or {}
        async_partner_addresses = async_partner_addresses or []
        async_standby_endpoints = async_standby_endpoints or []
        if not self._are_passwords_set:
            logger.warning("Passwords are not yet generated by the leader")
            return

        # Load the template shipped as package data, not relative to the CWD.
        template_source = (
            importlib.resources
            .files("single_kernel_postgresql.templates")
            .joinpath("patroni.yml.j2")
            .read_text()
        )
        template = Template(template_source)

        confs = {
            "substrate": self.state.substrate,
            "connectivity": connectivity,
            "enable_ldap": enable_ldap,
            "enable_tls": enable_tls,
            "member_name": self.state.peer.member_name,
            "superuser": USER,
            "superuser_password": self.state.application.user_password,
            "rewind_user": REWIND_USER,
            "rewind_password": self.state.application.rewind_password,
            "replication_password": self.state.application.replication_password,
            "enable_pgbackrest_archiving": stanza is not None
            and disable_pgbackrest_archiving is False,
            "stanza": stanza,
            "restore_stanza": restore_stanza,
            "restoring_backup": backup_id is not None or pitr_target is not None,
            "backup_id": backup_id,
            "pitr_target": pitr_target if not restore_to_latest else None,
            "restore_timeline": restore_timeline,
            "restore_to_latest": restore_to_latest,
            "is_creating_backup": is_creating_backup,
            "version": self.workload.get_postgresql_version().split(".")[0],
            "synchronous_node_count": self.state.synchronous_node_count,
            "maximum_lag_on_failover": self.state.config.durability_maximum_lag_on_failover,
            "pg_parameters": parameters,
            "primary_cluster_endpoint": async_primary_cluster_endpoint,
            "ldap_parameters": self._dict_to_hba_string(ldap_parameters),
            "patroni_password": self.state.application.patroni_password,
            "user_databases_map": user_databases_map,
            "slots": slots,
            "instance_password_encryption": self.state.config.instance_password_encryption,
            "extra_replication_endpoints": async_standby_endpoints,
        }
        if self.state.substrate == Substrates.VM:
            confs.update({
                "conf_path": str(self.workload.paths.patroni_conf),
                "log_path": str(self.workload.paths.patroni_logs),
                "postgresql_log_path": str(self.workload.paths.logs),
                "data_path": str(self.workload.paths.data),
                "wal_dir": str(self.workload.paths.wal),
                "partner_addrs": async_partner_addresses if not no_peers else [],
                "peers_ips": sorted(self.state.endpoints) if not no_peers else [],
                "pgbackrest_configuration_file": f"--config={self.workload.paths.pgbackrest_conf / PGBACKREST_CONF_FILE}",
                "scope": self.state.application.cluster_name,
                "self_ip": self.state.unit_ip,
                "listen_ips": self.state.listen_ips,
                "raft_password": self.state.application.raft_password,
                "watcher": watcher_raft_address,
            })
            perms = 0o600
        else:
            confs.update({
                "endpoint": self.state.endpoint,
                "endpoints": list(self.state.endpoints),
                "is_no_sync_member": is_no_sync_member,
                "namespace": self.state.model_name,
                "storage_path": str(self.workload.paths.patroni_conf),
                "logs_storage_path": str(self.workload.paths.logs),
                "pgdata_path": str(self.workload.paths.data),
                "restoring_backup": backup_id is not None or pitr_target is not None,
            })
            perms = 0o644
        rendered = template.render(**confs)
        render_file(
            self.state.substrate,
            str(self.workload.paths.patroni_config),
            rendered,
            perms,
        )

    @property
    def _are_passwords_set(self) -> bool:
        passes = [
            self.state.application.user_password,
            self.state.application.replication_password,
            self.state.application.rewind_password,
            self.state.application.patroni_password,
        ]
        if self.state.substrate == Substrates.VM:
            passes.append(self.state.application.raft_password)
        return all(passes)

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        return [GeneralStatuses.ACTIVE_IDLE.value]
