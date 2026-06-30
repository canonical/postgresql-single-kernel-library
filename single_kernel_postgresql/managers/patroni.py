#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Patroni Manager.

This manager is responsible for handling operations related to Patroni,
such as starting the service and checking its status.
"""

import logging
from functools import cached_property
from typing import TypedDict

import requests
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from requests.auth import HTTPBasicAuth
from tenacity import (
    RetryError,
    Retrying,
    stop_after_delay,
    wait_fixed,
)

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    API_REQUEST_TIMEOUT,
    RUNNING_STATES,
    TLS_CA_BUNDLE_FILE,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class ClusterMember(TypedDict):
    """Type for cluster member."""

    name: str
    role: str
    state: str
    api_url: str
    host: str
    port: int
    timeline: int
    lag: int


class PatroniManager(BaseManager):
    """PostgreSQL Patroni Manager.

    This manager is responsible for handling operations related to Patroni.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
    ):
        super().__init__(state, workload, "patroni_manager")
        # Variable mapping to requests library verify parameter.
        # The CA bundle file is used to validate the server certificate when
        # TLS is enabled, otherwise True is set because it's the default value.
        if self.state.substrate == Substrates.VM:
            self.verify = f"{self.workload.paths.patroni_conf}/{TLS_CA_BUNDLE_FILE}"
        else:
            # CA bundle is not secret
            self.verify = f"/tmp/{TLS_CA_BUNDLE_FILE}"  # noqa: S108

    def start_patroni(self) -> bool:
        """Start Patroni."""
        if self.state.substrate == Substrates.VM:
            return self.workload.start_patroni()  # type: ignore
        else:
            # TODO: Implement for other substrates
            return False

    @property
    def member_started(self) -> bool:
        """Has the member started Patroni and PostgreSQL.

        Returns:
            True if services is ready False otherwise. Retries over a period of 60 seconds times to
            allow server time to start up.
        """
        if not self.workload.is_patroni_running():
            return False
        try:
            response = self.cached_patroni_health
        except RetryError:
            return False

        return response["state"] in RUNNING_STATES

    @cached_property
    def cached_patroni_health(self) -> dict[str, str]:
        """Cached local unit health."""
        return self.get_patroni_health()

    def get_patroni_health(self) -> dict[str, str]:
        """Gets, retires and parses the Patroni health endpoint."""
        # TODO: Revert stop after delay to 60 and wait fixed to 7 after testing
        for attempt in Retrying(stop=stop_after_delay(1), wait=wait_fixed(1)):
            with attempt:
                r = requests.get(
                    f"{self.state.patroni_url}/health",
                    verify=self.verify,
                    timeout=API_REQUEST_TIMEOUT,
                    auth=self._patroni_auth,
                )
                logger.debug("API get_patroni_health: %s (%s)", r, r.elapsed.total_seconds())

        return r.json()

    @cached_property
    def _patroni_auth(self) -> HTTPBasicAuth | None:
        if self.state.application.patroni_password:
            return HTTPBasicAuth("patroni", self.state.application.patroni_password)

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        # if self.workload.workload_present and self.state.substrate == Substrates.VM and not self.member_started:
        #    return [PatroniStatuses.WAITING_MEMBER_START.value]
        return [GeneralStatuses.ACTIVE_IDLE.value]
