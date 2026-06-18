#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Config Manager.

Responsible for managing the configuration of the PostgreSQL instance.
"""

import logging

import charm_refresh
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from jinja2 import Template

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    POSTGRESQL_STORAGE_PERMISSIONS,
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
        super().__init__(state, workload, "config_manager", client)

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
        no_peers: bool = False,
        *,
        refresh: charm_refresh.Machines | None = None,
    ) -> bool:
        """Updates Patroni config file based on the existence of the TLS files."""
        # if refresh is None:
        # refresh = self.refresh

        # Build PostgreSQL parameters
        pg_parameters = self._build_postgresql_parameters()

        # replication_slots = self.logical_replication.replication_slots()

        # Update and reload configuration based on TLS files availability.
        logger.debug(f"Calling render_patroni_yml_file with parameters = {pg_parameters}")
        self.render_patroni_yml_file(
            parameters=pg_parameters,
            slots={},
        )
        return True

    def render_patroni_yml_file(
        self,
        connectivity: bool = False,
        is_creating_backup: bool = False,
        enable_ldap: bool = False,
        enable_tls: bool = False,
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
    ) -> None:
        """Render the Patroni configuration file.

        Args:
            connectivity: whether to allow external connections to the database.
            is_creating_backup: whether this unit is creating a backup.
            enable_ldap: whether to enable LDAP authentication.
            enable_tls: whether to enable client TLS.
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
        """
        slots = slots or {}
        if not self._are_passwords_set:
            logger.warning("Passwords are not yet generated by the leader")
            return

        # Open the template patroni.yml file.
        with open("templates/patroni.yml.j2") as file:
            template = Template(file.read())

        # TODO: configure LDAP
        # ldap_params = self.charm.get_ldap_parameters()

        # Render the template file with the correct values.
        rendered = template.render(
            conf_path=self.workload.paths.patroni_conf,
            connectivity=connectivity,
            is_creating_backup=is_creating_backup,
            log_path=self.workload.paths.patroni_logs,
            postgresql_log_path=self.workload.paths.logs,
            data_path=self.workload.paths.data,
            enable_ldap=enable_ldap,
            enable_tls=enable_tls,
            member_name=self.state.peer.member_name,
            partner_addrs=[],
            peers_ips=set(),
            scope=self.state.application.cluster_name,
            self_ip=self.state.unit_ip,
            listen_ips=self.state.listen_ips,
            superuser=USER,
            superuser_password=self.state.application.user_password,
            replication_password=self.state.application.replication_password,
            rewind_password=self.state.application.rewind_password,
            raft_password=self.state.application.raft_password,
            version=self.workload.get_postgresql_version().split(".")[0],
            pg_parameters=parameters,
            patroni_password=self.state.application.patroni_password,
            slots=slots,
            instance_password_encryption=self.state.config.instance_password_encryption,
        )
        render_file(
            self.state.substrate,
            f"{self.workload.paths.patroni_conf}/patroni.yaml",
            rendered,
            0o600,
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
