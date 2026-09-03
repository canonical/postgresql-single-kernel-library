#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager of PostgreSQL backups via pgBackRest.

Ported from the 16/edge charm backup modules (``src/backups.py`` on the VM and
K8s charms). Event orchestration (defer/fail/status writes) stays in the events
layer; this manager raises or returns values only.
"""

import importlib
import json
import logging
import re
import shlex
import subprocess
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

import jinja2
from ops import JujuVersion
from ops.pebble import ExecError
from tenacity import RetryError, Retrying, stop_after_attempt, wait_fixed

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import ListBackupsError
from single_kernel_postgresql.config.literals import (
    BACKUP_TYPE_OVERRIDES,
    BACKUP_USER,
    PGBACKREST_ARCHIVE_TIMEOUT_ERROR_CODE,
    PGBACKREST_LOG_LEVEL_STDERR,
    PGBACKREST_LOGROTATE_FILE,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.utils.backup import (
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    BACKUP_LABEL_STDOUT_PATTERN,
    FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE,
    S3_BLOCK_MESSAGES,
    STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE,
    extract_error_message,
    generate_fake_backup_id,
)
from single_kernel_postgresql.workload.base import (
    BaseWorkload,
    CommandResult,
    ResourceProvider,
)

if TYPE_CHECKING:
    from single_kernel_postgresql.managers.s3_client import S3Client


if TYPE_CHECKING:
    from single_kernel_postgresql.managers.s3_client import S3Client

logger = logging.getLogger(__name__)

# The charm-side hooks that re-render Patroni configuration and refresh unit
# statuses; the composition root injects them (the manager never calls
# ConfigManager.update_config directly, whose signature takes still-injected
# inputs it must not guess).
type UpdateConfigFunction = Callable[..., bool]
type IsStandbyClusterFunction = Callable[[], bool]

# Bridge: VM-only async-replication concept. When the callable is omitted (K8s),
# the cluster is never a standby cluster.
STANZA_CREATE_CONNECTION_TIMEOUT_ERROR_CODE = 49


class BackupManager(BaseManager):
    """In this class, we manage PostgreSQL backups."""

    s3_client: "S3Client"
    patroni_manager: PatroniManager

    def __init__(
        self,
        state: CharmState,
        workload: "BaseWorkload",
        s3_client: "S3Client",
        patroni_manager: PatroniManager,
        update_config: UpdateConfigFunction,
        resource_provider: ResourceProvider,
        is_standby_cluster: IsStandbyClusterFunction | None = None,
        set_unit_status: Callable[..., None] | None = None,
    ):
        """Manager of PostgreSQL backups."""
        super().__init__(state, workload, "backup")
        self.s3_client = s3_client
        self.patroni_manager = patroni_manager
        self.update_config = update_config
        self.resource_provider = resource_provider
        self.set_unit_status = set_unit_status
        self._is_standby_cluster_bridge = is_standby_cluster

    @property
    def stanza_name(self) -> str:
        """Stanza name, composed by model and cluster name."""
        return f"{self.state.model_name}.{self.state.cluster_name}"

    # -- pgBackRest execution -------------------------------------------------

    def _execute_pgbackrest(
        self,
        args: list[str],
        timeout: float | None = None,
        with_config: bool = True,
    ) -> CommandResult:
        """Execute a pgBackRest command on the workload.

        The command is built as [executable, --config=<conf>/pgbackrest.conf?,
        --log-level-stderr=warn, *substrate extra args, *args]. K8s runs
        without --config because its pgBackRest configuration lives at the
        default /etc/pgbackrest.conf location.

        On VM, a non-zero return code is carried on the CommandResult; on K8s
        the underlying pebble exec raises ExecError instead, mirroring the two
        charms.

        Args:
            args: pgBackRest command and arguments.
            timeout: optional command timeout in seconds.
            with_config: pass --config (False for server-ping, which runs
                outside any stanza context on both charms).
        """
        config = self.workload.backup_config
        command = [config.executable]
        if with_config and config.conf_path is not None:
            command.append(f"--config={config.configuration_file}")
        command.append(PGBACKREST_LOG_LEVEL_STDERR)
        command.extend(config.extra_args)
        command.extend(args)
        return self.workload.run_cmd(shlex.join(command), timeout=timeout)

    # -- Substrate-bridged predicates -------------------------------------------

    @property
    def is_primary(self) -> bool:
        """Return whether this unit is the primary instance."""
        return self.state.peer.unit_name == self.patroni_manager.get_primary(
            unit_name_pattern=True
        )

    @property
    def _is_standby_cluster(self) -> bool:
        """Whether this cluster is a standby (read-only) cluster.

        The charm-side async-replication check is injected as a callable; when
        omitted (K8s) the cluster is never a standby cluster.
        """
        return bool(self._is_standby_cluster_bridge and self._is_standby_cluster_bridge())

    @property
    def _peer_members(self) -> set[str]:
        """Addresses/endpoints of the other cluster members (VM: IPs, K8s: hostnames)."""
        peers = set(self.state.endpoints)
        peers.discard(self.state.endpoint)
        return peers

    @property
    def _primary_endpoint(self) -> str | None:
        """Address of the primary unit for pgBackRest TLS server-ping.

        VM resolves the primary member IP; K8s derives the pod hostname from
        the primary member name, matching each charm.
        """
        try:
            if self.state.substrate == Substrates.VM:
                primary = self.patroni_manager.get_primary() or (
                    self.patroni_manager.get_standby_leader()
                )
                member_ip = self.patroni_manager.get_member_ip(primary) if primary else None
                if member_ip is not None and member_ip not in self.state.peer_members_ips:
                    logger.debug("Early exit primary_endpoint: Primary IP not in cached peer list")
                    return None
                return member_ip
            primary = self.patroni_manager.get_primary()
        except (RetryError, ConnectionError) as e:
            logger.error(f"failed to get primary with error {e!s}")
            return None
        if primary is None:
            logger.debug("the primary was not elected yet")
            return None
        return self.state._get_hostname_from_unit(primary)

    @property
    def _has_s3_block_message(self) -> bool:
        """Whether the unit is blocked because of an S3 initialization failure.

        State-derived replacement of the charms' unit-status-message reads:
        the blocked status message comes from the s3-initialization-block-message
        peer field (see the charms' _set_primary_status_message).
        """
        return self.state.application.s3_initialization_block_message in S3_BLOCK_MESSAGES or (
            self.state.peer.s3_initialization_block_message in S3_BLOCK_MESSAGES
        )

    def _s3_initialization_set_failure(self, block_message: str) -> None:
        """Record a failed s3 initialization with the corresponding block message.

        Written to the app databag on the leader (leader == primary, so no
        cross-unit sync is needed) or to the unit databag otherwise. The events
        layer refreshes the unit status.
        """
        if self.state.peer.is_app_leader:
            self.state.application.s3_initialization_block_message = block_message
            self.state.application.s3_initialization_start = ""
            self.state.application.stanza = ""
        else:
            self.state.peer.s3_initialization_block_message = block_message
            self.state.peer.s3_initialization_done = "True"
            self.state.peer.stanza = ""

    # -- Stanza configuration rendering ----------------------------------------

    @property
    def _tls_ca_chain_filename(self) -> str:
        """Returns the path to the TLS CA chain file."""
        s3_parameters, _ = self.state.s3_connection_info.retrieve_s3_parameters()
        if s3_parameters.get("tls-ca-chain") is not None:
            return self.workload.backup_config.tls_ca_chain_path
        return ""

    def _render_pgbackrest_conf_file(self) -> bool:
        """Render the pgBackRest configuration and logrotate files.

        Returns:
            a boolean indicating whether rendering was successful.
        """
        s3_parameters, missing_parameters = self.state.s3_connection_info.retrieve_s3_parameters()
        if missing_parameters:
            logger.warning(
                f"Cannot set pgBackRest configurations due to missing S3 parameters: {missing_parameters}"
            )
            return False

        config = self.workload.backup_config

        if self._tls_ca_chain_filename != "":
            self.workload.write_text(
                "\n".join(s3_parameters["tls-ca-chain"]),
                self.workload.root / self._tls_ca_chain_filename.lstrip("/"),
                mode=0o644,
                user=self.workload.user,
                group=self.workload.group,
            )

        template = jinja2.Template(
            importlib.resources
            .files("single_kernel_postgresql.templates")
            .joinpath(self.state.substrate.name.lower(), "pgbackrest.conf.j2")
            .read_text()
        )
        cpu_count, _ = self.workload.get_available_resources()
        rendered = template.render(
            enable_tls=len(self._peer_members) > 0,
            peer_endpoints=self._peer_members,
            path=s3_parameters["path"],
            data_path=str(self.workload.paths.data),
            pgdata_path=str(self.workload.paths.data),
            log_path=str(config.logs_path),
            pgbackrest_logs_path=str(config.logs_path),
            region=s3_parameters.get("region"),
            endpoint=s3_parameters["endpoint"],
            bucket=s3_parameters["bucket"],
            s3_uri_style=s3_parameters["s3-uri-style"],
            tls_ca_chain=self._tls_ca_chain_filename,
            access_key=s3_parameters["access-key"],
            secret_key=s3_parameters["secret-key"],
            stanza=self.stanza_name,
            storage_path=config.storage_path,
            user=BACKUP_USER,
            retention_full=s3_parameters["delete-older-than-days"],
            process_max=max(cpu_count - 2, 1),
        )
        self.workload.write_text(
            rendered,
            self.workload.root / config.configuration_file.lstrip("/"),
            mode=0o640 if self.state.substrate == Substrates.VM else None,
            user=self.workload.user,
            group=self.workload.group,
        )

        logrotate_template = jinja2.Template(
            importlib.resources
            .files("single_kernel_postgresql.templates")
            .joinpath(self.state.substrate.name.lower(), "pgbackrest.logrotate.j2")
            .read_text()
        )
        self.workload.write_text(
            logrotate_template.render(pgbackrest_logs_path=str(config.logs_path)),
            self.workload.root / PGBACKREST_LOGROTATE_FILE.lstrip("/"),
            mode=0o644,
            user=None,
            group=None,
        )
        return True

    # -- Stanza lifecycle -------------------------------------------------------

    def _initialise_stanza(self) -> bool:
        """Initialize the stanza.

        A stanza is the configuration for a PostgreSQL database cluster that defines where it is located, how it will
        be backed up, archiving options, etc. (more info in
        https://pgbackrest.org/user-guide.html#quickstart/configure-stanza).

        Returns:
            whether stanza initialization was successful.
        """
        # Enable stanza initialisation if the backup settings were fixed after being invalid
        # or pointing to a repository where there are backups from another cluster.
        if self.state.peer.is_blocked and not self._has_s3_block_message:
            logger.warning("couldn't initialize stanza due to a blocked status")
            return False

        # Create the stanza.
        try:
            # If the tls is enabled, it requires all the units in the cluster to run the pgBackRest service to
            # successfully complete validation, and upon receiving the same parent event other units should start it.
            # Therefore, the first retry may fail due to the delay of these other units to start this service. 60s given
            # for that or else the s3 initialization sequence will fail.
            for attempt in Retrying(stop=stop_after_attempt(6), wait=wait_fixed(10), reraise=True):
                with attempt:
                    result = self._execute_pgbackrest([
                        f"--stanza={self.stanza_name}",
                        "stanza-create",
                    ])
                    if self.state.substrate == Substrates.VM:
                        if result.return_code == STANZA_CREATE_CONNECTION_TIMEOUT_ERROR_CODE:
                            # Raise an error if the connection timeouts, so the user has the possibility to
                            # fix network issues and call juju resolve to re-trigger the hook that calls
                            # this method.
                            logger.error(
                                f"error: {result.stderr} - please fix the error and call juju resolve on this unit"
                            )
                            raise TimeoutError
                        if result.return_code != 0:
                            raise Exception(result.stderr)
        except TimeoutError as e:
            raise e
        except ExecError:
            # On K8s a failed stanza-create surfaces as pebble ExecError.
            logger.exception("Failed to initialise stanza:")
            self._s3_initialization_set_failure(FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE)
            return False
        except Exception:
            # If the stanza-create command doesn't succeed, remove the stanza name
            # and rollback the configuration.
            logger.exception("Failed to initialise stanza:")
            self._s3_initialization_set_failure(FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE)
            return False

        self.start_stop_pgbackrest_service()

        # Rest of the successful s3 initialization sequence such as s3-initialization-start and s3-initialization-done
        # are left to the check_stanza func.
        if self.state.peer.is_app_leader:
            self.state.application.stanza = self.stanza_name
        else:
            self.state.peer.stanza = self.stanza_name

        return True

    def check_stanza(self) -> bool:
        """Runs the pgbackrest stanza validation.

        Returns:
            whether stanza validation was successful.
        """
        # Update the configuration to use pgBackRest as the archiving mechanism.
        self.update_config()

        try:
            # If the tls is enabled, it requires all the units in the cluster to run the pgBackRest service to
            # successfully complete validation, and upon receiving the same parent event other units should start it.
            # Therefore, the first retry may fail due to the delay of these other units to start this service. 60s given
            # for that or else the s3 initialization sequence will fail.
            for attempt in Retrying(stop=stop_after_attempt(6), wait=wait_fixed(10), reraise=True):
                with attempt:
                    result = self._execute_pgbackrest([f"--stanza={self.stanza_name}", "check"])
                    if self.state.substrate == Substrates.VM:
                        if result.return_code == PGBACKREST_ARCHIVE_TIMEOUT_ERROR_CODE:
                            # Raise an error if the archive command timeouts, so the user has the possibility
                            # to fix network issues and call juju resolve to re-trigger the hook that calls
                            # this method.
                            extracted_error = extract_error_message(
                                result.stderr, str(self.workload.backup_config.logs_path)
                            )
                            logger.error(
                                f"error: {extracted_error} - please fix the error and call juju resolve on this unit"
                            )
                            raise TimeoutError
                        if result.return_code != 0:
                            raise Exception(result.stderr)
        except TimeoutError as e:
            if self.state.substrate == Substrates.K8S:
                # The K8s charm folds every failure (including timeouts) into the
                # initialization-failure path instead of re-raising.
                logger.exception("Failed to check stanza:")
                self._s3_initialization_set_failure(FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE)
                return False
            # Re-raise to put charm in error state (not blocked), allowing juju resolve
            raise e
        except Exception:
            # If the check command doesn't succeed, remove the stanza name
            # and rollback the configuration. Only the VM charm rolls the
            # configuration back here; the K8s charm logs and blocks.
            logger.exception("Failed to check stanza:")
            self._s3_initialization_set_failure(FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE)
            if self.state.substrate == Substrates.VM:
                self.update_config()
            return False

        if self.state.peer.is_app_leader:
            self.state.application.s3_initialization_start = ""
        else:
            self.state.peer.s3_initialization_done = "True"

        return True

    def coordinate_stanza_fields(self) -> None:
        """Coordinate the stanza name between the primary and the leader units."""
        if not self.state.peer.is_app_leader or not self.state.application.s3_initialization_start:
            return

        for unit_data in [self.state.application, *self.state.application_peers]:
            if not unit_data.s3_initialization_done:
                continue

            self.state.application.stanza = unit_data.stanza or ""
            self.state.application.s3_initialization_block_message = (
                unit_data.s3_initialization_block_message or ""
            )
            self.state.application.s3_initialization_start = ""
            self.state.application.s3_initialization_done = "True"

            self.update_config()
            break

    # -- pgBackRest TLS server service -------------------------------------------

    @property
    def _is_primary_pgbackrest_service_running(self) -> bool:
        """Returns whether the pgBackRest TLS server is running in the primary unit."""
        primary_endpoint = self._primary_endpoint
        if not primary_endpoint:
            logger.warning("Failed to contact pgBackRest TLS server: no primary endpoint")
            return False
        try:
            result = self._execute_pgbackrest(
                ["server-ping", "--io-timeout=10", primary_endpoint],
                with_config=False,
            )
        except ExecError as e:
            logger.warning(
                f"Failed to contact pgBackRest TLS server on {primary_endpoint} with error {e!s}"
            )
            return False
        extracted_error = extract_error_message(
            result.stderr, str(self.workload.backup_config.logs_path)
        )
        if not result.ok:
            logger.warning(
                f"Failed to contact pgBackRest TLS server on {primary_endpoint} with error {extracted_error}"
            )
        return result.ok

    def start_stop_pgbackrest_service(self) -> bool:
        """Start or stop the pgBackRest TLS server service.

        Returns:
            a boolean indicating whether the operation succeeded.
        """
        # Ignore this operation if backups settings aren't ok.
        are_backup_settings_ok, _ = self._are_backup_settings_ok()
        if not are_backup_settings_ok:
            return True

        # Update pgBackRest configuration (to update the TLS settings).
        if not self._render_pgbackrest_conf_file():
            return False

        service = self.workload.backup_config.service
        if self.state.substrate == Substrates.VM and not self.workload.workload_present:
            logger.error("Cannot start/stop service, snap is not yet installed.")
            return False

        # Stop the service if TLS is not enabled or there are no replicas.
        if len(self._peer_members) == 0 or self.patroni_manager.get_standby_leader():
            self.workload.stop_service(service)
            return True

        # Don't start the service if the service hasn't started yet in the primary.
        if not self.is_primary and not self._is_primary_pgbackrest_service_running:
            return False

        # Start the service.
        if self.state.substrate == Substrates.K8S:
            if not self.workload.service_exists(service):
                # A layer revision predating the service declaration: the charm
                # returned False here instead of erroring the hook.
                return False
            if self.workload.service_is_running(service):
                logger.debug("Sending SIGHUP to pgBackRest TLS server to reload configuration")
                self.workload.reload_service(service)
            else:
                logger.debug("Starting pgBackRest TLS server service")
                self.workload.restart_service(service)
        else:
            self.workload.restart_service(service)
        return True

    # -- Backup settings and permissions -----------------------------------------

    def _are_backup_settings_ok(self) -> tuple[bool, str]:
        """Validates whether backup settings are OK."""
        if self.state.s3_relation is None:
            return (
                False,
                "Relation with s3-integrator charm missing, cannot create/restore backup.",
            )

        _, missing_parameters = self.state.s3_connection_info.retrieve_s3_parameters()
        if missing_parameters:
            return False, f"Missing S3 parameters: {missing_parameters}"

        return True, ""

    def _can_unit_perform_backup(self) -> tuple[bool, str | None]:
        """Validates whether this unit can perform a backup."""
        if self._is_standby_cluster:
            return False, STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE

        if self.state.peer.is_blocked:
            return False, "Unit is in a blocking state"

        # Check if this unit is the primary (if it was not possible to retrieve that information,
        # then show that the unit cannot perform a backup, because possibly the database is offline).
        try:
            is_primary = self.is_primary
        except RetryError:
            return False, "Unit cannot perform backups as the database seems to be offline"

        # Only enable backups on primary if there are replicas but TLS is not enabled.
        if is_primary and self.state.application.planned_units > 1:
            return False, "Unit cannot perform backups as it is the cluster primary"

        if not self.patroni_manager.member_started:
            return False, "Unit cannot perform backups as it's not in running state"

        if not self.state.cluster_stanza:
            return False, "Stanza was not initialised"

        return self._are_backup_settings_ok()

    def _can_initialise_stanza(self) -> bool:
        """Validates whether this unit can initialise a stanza."""
        # Don't allow stanza initialisation if this unit hasn't started the database
        # yet and either hasn't joined the peer relation yet or hasn't configured TLS
        # yet while other unit already has TLS enabled.
        return not (
            not self.patroni_manager.member_started and (len(self.state.application_peers) == 1)
        )

    def can_use_s3_repository(self) -> tuple[bool, str]:
        """Returns whether the charm was configured to use another cluster repository."""
        # Check model uuid
        s3_parameters, _ = self.state.s3_connection_info.retrieve_s3_parameters()
        s3_model_uuid = self.s3_client.read_content(
            "model-uuid.txt",
            s3_parameters,
        )
        if s3_model_uuid and s3_model_uuid.strip() != self.state.model.uuid:
            logger.debug(
                f"can_use_s3_repository: incompatible model-uuid s3={s3_model_uuid.strip()}, local={self.state.model.uuid}"
            )
            return False, ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE

        try:
            result = self._execute_pgbackrest(["info", "--output=json"], timeout=30)
        except ExecError as e:
            logger.error(f"Failed to execute pgbackrest info: {e!s}")
            return False, FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE
        except subprocess.TimeoutExpired as e:
            # Raise an error if the connection timeouts, so the user has the possibility to
            # fix network issues and call juju resolve to re-trigger the hook that calls
            # this method.
            logger.error(f"error: {e!s} - please fix the error and call juju resolve on this unit")
            raise TimeoutError from e
        except TimeoutError as e:
            # K8s pebble exec timeout: the K8s charm treats any execution failure
            # as a stanza initialization failure instead of re-raising.
            if self.state.substrate == Substrates.K8S:
                logger.error(f"Failed to execute pgbackrest info: {e!s}")
                return False, FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE
            raise

        if self.state.substrate == Substrates.VM and result.return_code != 0:
            extracted_error = extract_error_message(
                result.stderr, str(self.workload.backup_config.logs_path)
            )
            logger.error(f"Failed to run pgbackrest: {extracted_error}")
            return False, FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE

        for stanza in json.loads(result.stdout):
            is_valid, validation_message = self._validate_stanza(stanza)
            if not is_valid:
                return False, validation_message

        return True, ""

    def _validate_stanza(self, stanza: dict) -> tuple[bool, str]:
        """Validate one stanza entry from pgBackRest info against this cluster.

        Returns:
            (is_valid, message); message is empty when the stanza is compatible.
        """
        if (stanza_name := stanza.get("name")) and stanza_name == "[invalid]":
            logger.error("Invalid stanza name from s3")
            return False, FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE
        if stanza_name != self.stanza_name:
            logger.debug(
                f"can_use_s3_repository: incompatible stanza name s3={stanza_name or ''}, local={self.stanza_name}"
            )
            return False, ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE

        # Guard the bare next(): an empty pg_controldata output would raise
        # StopIteration inside a generator context and mask the real failure.
        control_data = self._read_pg_controldata()
        system_identifier_from_instance = next(
            (line for line in control_data.splitlines() if "Database system identifier" in line),
            None,
        )
        if system_identifier_from_instance is None:
            raise Exception("Database system identifier not found in pg_controldata output")
        system_identifier_from_instance = system_identifier_from_instance.split(" ")[-1]
        stanza_dbs = stanza.get("db")
        system_identifier_from_stanza = (
            str(stanza_dbs[0]["system-id"]) if len(stanza_dbs) else None
        )
        if system_identifier_from_instance != system_identifier_from_stanza:
            logger.debug(
                f"can_use_s3_repository: incompatible system identifier s3={system_identifier_from_stanza}, local={system_identifier_from_instance}"
            )
            return False, ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE
        return True, ""

    def _read_pg_controldata(self) -> str:
        """Read the pg_controldata output, per substrate.

        VM inspects the return code; on K8s a failing exec raises ExecError
        (the K8s charm lets it propagate uncaught).
        """
        config = self.workload.backup_config
        command = "{controldata} {data}".format(
            controldata=config.pg_controldata(
                self.workload.get_postgresql_version().split(".")[0]
            ),
            data=self.workload.paths.data,
        )
        if self.state.substrate == Substrates.VM:
            result = self.workload.run_cmd(command)
            if not result.ok:
                raise Exception(result.stderr)
            return result.stdout
        return self.workload.run_cmd(command).stdout

    # -- Backup creation ----------------------------------------------------------

    def _change_connectivity_to_database(self, connectivity: bool) -> None:
        """Enable or disable the connectivity to the database."""
        self.state.peer.is_connectivity_enabled = connectivity
        # Reconciled to the K8s form: the K8s charm brackets backup creation with
        # update_config(is_creating_backup=True) while the VM charm passed no
        # flag here. The flag marks the cluster as creating a backup in the
        # Patroni configuration.
        self.update_config(is_creating_backup=True)

    def create_backup(self, backup_type: str) -> tuple[bool, str]:
        """Request that pgBackRest creates a backup.

        Args:
            backup_type: one of "full", "differential", or "incremental".

        Returns:
            (success, message) tuple; message is "backup created" on success or
            the validation/error message on failure. The events layer maps
            these onto action results/failures and unit statuses.
        """
        if backup_type not in BACKUP_TYPE_OVERRIDES:
            error_message = f"Invalid backup type: {backup_type}. Possible values: {', '.join(BACKUP_TYPE_OVERRIDES.keys())}."
            logger.error(f"Backup failed: {error_message}")
            return False, error_message

        if (
            backup_type in ["differential", "incremental"]
            and len(self._list_backups(show_failed=False)) == 0
        ):
            error_message = (
                f"Invalid backup type: {backup_type}. No previous full backup to reference."
            )
            logger.error(f"Backup failed: {error_message}")
            return False, error_message

        logger.info(f"A {backup_type} backup has been requested on unit")
        can_unit_perform_backup, validation_message = self._can_unit_perform_backup()
        if not can_unit_perform_backup:
            logger.error(f"Backup failed: {validation_message}")
            return False, validation_message or "Backup failed"

        # Retrieve the S3 Parameters to use when uploading the backup logs to S3.
        s3_parameters, _ = self.state.s3_connection_info.retrieve_s3_parameters()

        # Test uploading metadata to S3 to test credentials before backup.
        datetime_backup_requested = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        metadata = f"""Date Backup Requested: {datetime_backup_requested}
Model Name: {self.state.model_name}
Application Name: {self.state.model.app.name}
Unit Name: {self.state.model.unit.name}
Juju Version: {JujuVersion.from_environ()!s}
"""
        if not self.s3_client.upload_content(
            metadata,
            f"backup/{self.stanza_name}/latest",
            s3_parameters,
        ):
            error_message = "Failed to upload metadata to provided S3"
            logger.error(f"Backup failed: {error_message}")
            return False, error_message

        if not self.is_primary:
            # Create a rule to mark the cluster as in a creating backup state and update
            # the Patroni configuration.
            self._change_connectivity_to_database(connectivity=False)

        try:
            return self._run_backup(s3_parameters, datetime_backup_requested, backup_type)
        finally:
            if not self.is_primary:
                # Remove the rule that marks the cluster as in a creating backup state
                # and update the Patroni configuration.
                self._change_connectivity_to_database(connectivity=True)
            # Set flag due to missing in progress backups on JSON output
            # (reference: https://github.com/pgbackrest/pgbackrest/issues/2007)
            self.update_config(is_creating_backup=False)

    def _run_backup(
        self,
        s3_parameters: dict,
        datetime_backup_requested: str,
        backup_type: str,
    ) -> tuple[bool, str]:
        """Runs the pgBackRest backup command.

        VM: non-streaming execution with a return-code branch, error message
        extraction, and a backup id recovered from stdout (fake id fallback).
        K8s: stream-mode execution reconciled to the wave-1 run_cmd (wait_output)
        with an ExecError branch. Both upload the backup logs to S3.

        Returns:
            (success, message) tuple; message is "backup created" on success.
        """
        command = [
            f"--stanza={self.stanza_name}",
            "--log-level-console=debug",
            f"--type={BACKUP_TYPE_OVERRIDES[backup_type]}",
            "backup",
        ]
        if self.state.substrate == Substrates.K8S:
            command.insert(2, "--log-subprocess")
        if self.is_primary:
            # Force the backup to run in the primary if it's not possible to run it
            # on the replicas (that happens when TLS is not enabled).
            command.append("--no-backup-standby")

        backup_id = None
        if self.state.substrate == Substrates.K8S:
            try:
                # The backup id lookup is inside the try, mirroring the K8s charm
                # where an ExecError from the info command takes the failure
                # branch too.
                result = self._execute_pgbackrest(command)
                backup_id = list(self._list_backups(show_failed=True).keys())[-1]
                stdout, stderr, return_code = result.stdout, result.stderr, 0
            except ExecError as e:
                return self._handle_failed_backup(
                    e.stdout or "", e.stderr or "", s3_parameters, backup_type, str(e)
                )
        else:
            result = self._execute_pgbackrest(command)
            stdout, stderr, return_code = result.stdout, result.stderr, result.return_code

        if return_code != 0:
            return self._handle_failed_backup(stdout, stderr, s3_parameters, backup_type)

        if backup_id is None:
            try:
                backup_id = list(self._list_backups(show_failed=True).keys())[-1]
            except ListBackupsError:
                error_message = "Failed to retrieve backup id"
                logger.exception(error_message)
                logger.error(f"Backup failed: {error_message}")
                return False, error_message

        # Upload the logs to S3 and fail the action if it doesn't succeed.
        logs = f"""Stdout:
{stdout}

Stderr:
{stderr}
"""
        if not self.s3_client.upload_content(
            logs,
            f"backup/{self.stanza_name}/{backup_id}/backup.log",
            s3_parameters,
        ):
            error_message = "Error uploading logs to S3"
            logger.error(f"Backup failed: {error_message}")
            return False, error_message
        logger.info(f"Backup succeeded: with backup-id {datetime_backup_requested}")
        return True, "backup created"

    def _handle_failed_backup(
        self,
        stdout: str,
        stderr: str,
        s3_parameters: dict,
        backup_type: str,
        error: str,
    ) -> tuple[bool, str]:
        """Uploads the failed backup logs and reports the failure message."""
        logger.error(stderr)

        # Recover the backup id from the logs.
        backup_label_stdout_line = re.findall(BACKUP_LABEL_STDOUT_PATTERN, stdout, re.MULTILINE)
        if len(backup_label_stdout_line) > 0:
            backup_id = backup_label_stdout_line[0][1]
        else:
            # Generate a backup id from the current date and time if the backup failed before
            # generating the backup label (our backup id).
            backup_id = generate_fake_backup_id(
                backup_type, self._list_backups(show_failed=False, parse=False).keys()
            )

        # Upload the logs to S3.
        logs = f"""Stdout:
{stdout}

Stderr:
{stderr}
"""
        self.s3_client.upload_content(
            logs,
            f"backup/{self.stanza_name}/{backup_id}/backup.log",
            s3_parameters,
        )
        if self.state.substrate == Substrates.VM:
            extracted_error = extract_error_message(
                stderr, str(self.workload.backup_config.logs_path)
            )
            error_message = f"Failed to backup PostgreSQL with error: {extracted_error}"
        else:
            error_message = f"Failed to backup PostgreSQL with error: {error}"
        logger.error(f"Backup failed: {error_message}")
        return False, error_message
