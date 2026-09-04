#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager of PostgreSQL backups via pgBackRest.

Ported from the 16/edge charm backup modules (``src/backups.py`` on the VM and
K8s charms). Event orchestration (defer/fail/status writes) stays in the events
layer; this manager raises or returns values only.
"""

import logging
import shlex
from collections.abc import Callable
from typing import TYPE_CHECKING

from tenacity import RetryError

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import PGBACKREST_LOG_LEVEL_STDERR
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.utils.backup import S3_BLOCK_MESSAGES
from single_kernel_postgresql.workload.base import (
    BaseWorkload,
    CommandResult,
    ResourceProvider,
)

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
