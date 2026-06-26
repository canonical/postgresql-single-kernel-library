#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config Manager.

Responsible for managing the configuration of the PostgreSQL instance.
"""

import logging
from typing import Any

import charm_refresh
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from jinja2 import Template

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    PGBACKREST_CONF_FILE,
    POSTGRESQL_STORAGE_PERMISSIONS,
    REWIND_USER,
    USER,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import _change_owner, render_file
from single_kernel_postgresql.utils.postgresql import PostgreSQL as PostgreSQLClient
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class ConfigManager(BaseManager):
    """PostgreSQL Config Manager.

    This manager is responsible for handling configuration operations.
    """

    def __init__(self, state: CharmState, workload: BaseWorkload, client: PostgreSQLClient):
        super().__init__(state, workload, "config_manager")
        self.postgresql_client = client

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

    def _build_postgresql_parameters(self) -> dict[str, str] | None:
        """Build PostgreSQL configuration parameters.

        Returns:
            Dictionary of PostgreSQL parameters or None if base parameters couldn't be built.
        """
        limit_memory = None
        if self.state.config.profile_limit_memory:
            limit_memory = self.state.config.profile_limit_memory * 10**6

        # Build PostgreSQL parameters.
        pg_parameters = self.postgresql_client.build_postgresql_parameters(
            self.state.model_config, self.workload.get_available_memory(), limit_memory
        )

        # Calculate and merge worker process configurations
        # TODO: Add additional parameters

        return pg_parameters

    def update_config(
        self,
        is_creating_backup: bool = False,
        # TODO add rel handler
        is_tls_enabled: bool = False,
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
        """Updates Patroni config file based on the existence of the TLS files."""
        # Build PostgreSQL parameters
        pg_parameters = self._build_postgresql_parameters()

        # replication_slots = self.logical_replication.replication_slots()
        replication_slots = {}

        # TODO add rel handler
        relations_user_databases_map = relations_user_databases_map or {}

        # Update and reload configuration based on TLS files availability.
        logger.debug(f"Calling render_patroni_yml_file with parameters = {pg_parameters}")
        self.render_patroni_yml_file(
            connectivity=self.state.peer.is_connectivity_enabled,
            is_creating_backup=is_creating_backup,
            enable_ldap=self.state.application.is_ldap_enabled,
            enable_tls=is_tls_enabled,
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

        # Open the template patroni.yml file.
        with open("templates/patroni.yml.j2") as file:
            template = Template(file.read())

        confs = {
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
                "storage_path": str(self.workload.paths.data),
                "logs_storage_path": str(self.workload.paths.logs),
                "pgdata_path": str(self.workload.paths.data),
                "restoring_backup": backup_id is not None or pitr_target is not None,
            })
            perms = 0o644
        rendered = template.render(**confs)
        render_file(
            self.state.substrate,
            str(self.workload.paths.patroni_conf / "patroni.yaml"),
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
