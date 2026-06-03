#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TLS Manager.

Responsible for managing the TLS configuration of the PostgreSQL instance.
"""
import logging
from ops import ModelError, SecretNotFoundError

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from charmlibs.interfaces.tls_certificates import (
    generate_ca,
    generate_private_key,
)
from datetime import timedelta

from single_kernel_postgresql.compat.postgresql import PostgreSQLBase as PostgreSQLClient
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
)

logger = logging.getLogger(__name__)

class TLSManager(BaseManager):
    """PostgreSQL TLS Manager.

    This manager is responsible for handling TLS configuration operations.
    """

    def __init__(self, state: CharmState, workload: BaseWorkload, client: PostgreSQLClient):
        super().__init__(state, workload, "tls_manager", client)


    def configure_internal_peer_ca(self) -> None:
        """Configure TLS internal peer CA.
        """
        if not self.state.get_secret(APP_SCOPE, "internal-ca"):
            self.generate_internal_peer_ca() 

    def configure_internal_peer_cert(self) -> None:
        """Configure TLS internal peer certificate.
        """
        if not self.state.peer.internal_cert:
            self.generate_internal_peer_cert()

    def generate_internal_peer_cert(self) -> None:
        """Generate internal peer certificate using the tls lib."""
        raise NotImplementedError()

    def generate_internal_peer_ca(self) -> None:
        """Generate internal peer CA using the tls lib."""
        private_key = generate_private_key()
        ca = generate_ca(
            private_key,
            common_name=self.state.internal_peer_ca_common_name,
            validity=timedelta(days=7300),
        )
        logger.warning("Internal peer CA generated. Please use a proper TLS operator if possible.")
        self.state.set_secret(APP_SCOPE, "internal-ca-key", str(private_key))
        self.state.set_secret(APP_SCOPE, "internal-ca", str(ca))


    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        return [GeneralStatuses.ACTIVE_IDLE.value]

