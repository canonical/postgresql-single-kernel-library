#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for General PostgreSQL charm events."""
from typing import TYPE_CHECKING
from ops import Object, InstallEvent, StartEvent
from datetime import datetime
import logging
from tenacity import Retrying, stop_after_attempt, wait_fixed

from single_kernel_postgresql.workload.vm import VMWorkload
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.config.exceptions import StorageUnavailableError

if TYPE_CHECKING:
    from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm

logger = logging.getLogger(__name__)


class PostgreSQLEventsHandler(Object):
    """Class implementing PostgreSQL Charm events handling."""

    def __init__(self, charm: "AbstractPostgreSQLCharm", workload: BaseWorkload, state: CharmState, cluster_manager: ClusterManager) -> None:
        super().__init__(charm, key="postgresql_events")
        self.charm = charm
        self.workload = workload
        self.state = state
        self.cluster_manager = cluster_manager

        # Charm events
        self.framework.observe(self.charm.on.install, self._on_install)
        self.framework.observe(self.charm.on.start, self._on_start)

    
    def _on_install(self, event: InstallEvent) -> None:
        """Install prerequisites for the application."""
        logger.debug("Install start time: %s", datetime.now())
        if self.charm.substrate == Substrates.VM and (type(self.workload) is  VMWorkload):
            self._check_detached_storage(self.workload)

        self.state.add_status_if_not_present(
            GeneralStatuses.MAINTAINENANCE_INSTALLING,
            scope="unit",
            component=self.cluster_manager.name
        )
        # Install the charmed PostgreSQL snap.
        if self.charm.substrate == Substrates.VM and (type(self.workload) is  VMWorkload):
            self.workload.install_snap_package(revision=None)
            self.workload.create_snap_alias("patronictl")
            self.workload.create_snap_alias("psql")

        self.state.remove_status_if_present(
            GeneralStatuses.MAINTAINENANCE_INSTALLING,
            scope="unit",
            component=self.cluster_manager.name
        )
        self.state.add_status_if_not_present(
            GeneralStatuses.WAITING_POSTGRESQL_START,
            scope="unit",
            component=self.cluster_manager.name
        )

    def _on_start(self, event: StartEvent) -> None:  
        """Event handler for start event."""
        if not self._can_start(event):
            return

    def _check_detached_storage(self, workload: VMWorkload) -> None:
        """Wait for storage to become available.

        Workaround for lxd containers not getting storage attached on startups.
        """
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(1), reraise=True):
            with attempt:
                if not workload.is_storage_attached():
                    logger.error("Data directory not attached.")
                    self.state.add_status_if_not_present(
                        GeneralStatuses.WAITING_DIRECTORY_NOT_ATTACHED,
                        scope="unit",
                        component=self.cluster_manager.name
                    )
                    raise StorageUnavailableError()

        self.state.remove_status_if_present(
            GeneralStatuses.WAITING_DIRECTORY_NOT_ATTACHED,
            scope="unit",
            component=self.cluster_manager.name
        )

    def _can_start(self, event: StartEvent) -> bool:
        """Returns whether the workload can be started on this unit."""
        if self.charm.substrate == Substrates.VM and (type(self.workload) is  VMWorkload):
            self._check_detached_storage(self.workload)

        # Safeguard against starting while refreshing.
        # TODO: Add refresh checks once refresh is refactored

        # Doesn't try to bootstrap the cluster if it's in a blocked state
        # caused, for example, because a failed installation of packages.
        if self.state.peer.is_blocked:
            logger.debug("Early exit on_start: Unit blocked")
            return False

        return True