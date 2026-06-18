#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""TLS Manager.

Responsible for managing the TLS configuration of the PostgreSQL instance.
"""

import logging
from datetime import timedelta

from charmlibs.interfaces.tls_certificates import (
    Certificate,
    PrivateKey,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope

from single_kernel_postgresql.config.exceptions import TlsError
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    TLS_CA_BUNDLE_FILE,
    TLS_CA_FILE,
    TLS_CERT_FILE,
    TLS_KEY_FILE,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils.postgresql import PostgreSQL as PostgreSQLClient
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class TLSManager(BaseManager):
    """PostgreSQL TLS Manager.

    This manager is responsible for handling TLS configuration operations.
    """

    def __init__(self, state: CharmState, workload: BaseWorkload, client: PostgreSQLClient):
        super().__init__(state, workload, "tls_manager", client)

    def configure_internal_peer_ca(self) -> None:
        """Configure TLS internal peer CA."""
        if not self.state.get_secret(APP_SCOPE, "internal-ca"):
            self.generate_internal_peer_ca()

    def configure_internal_peer_cert(self) -> None:
        """Configure TLS internal peer certificate."""
        if not self.state.peer.internal_cert:
            self.generate_internal_peer_cert()

    def generate_internal_peer_cert(self) -> None:
        """Generate internal peer certificate using the tls lib."""
        if not (ca_key_secret := self.state.application.internal_ca_key):
            raise TlsError("No CA key content.")
        ca_key = PrivateKey.from_string(ca_key_secret)
        if not (ca_secret := self.state.application.internal_ca):
            raise TlsError("No CA cert content.")
        ca = Certificate.from_string(ca_secret)
        private_key = generate_private_key()
        csr = generate_csr(
            private_key,
            common_name=self.state.peer_common_name,
            sans_ip=frozenset(self.state.peer.peer_addresses),
            sans_dns=frozenset({
                *self.state.common_hosts,
                # IP address need to be part of the DNS SANs list due to
                # https://github.com/pgbackrest/pgbackrest/issues/1977.
                *self.state.peer.peer_addresses,
            }),
        )
        cert = generate_certificate(csr, ca, ca_key, validity=timedelta(days=7300))
        self.state.peer.internal_cert = str(cert)
        self.state.peer.internal_key = str(private_key)

        # NOTE: pushing the internal-peer cert/CA to disk is owned by the config
        # subsystem (not yet migrated); operator certs are pushed via
        # TLSManager.push_tls_files from the events.tls handler.
        logger.info(
            "Internal peer certificate generated. Please use a proper TLS operator if possible."
        )

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

    def store_client_tls(self, key: str, cert: str, ca: str) -> None:
        """Persist the operator-provided client key/cert/ca into peer state."""
        self.state.peer.operator_client_key = key
        self.state.peer.operator_client_cert = cert
        self.state.peer.operator_client_ca = ca

    def clear_client_tls(self) -> None:
        """Remove the operator client material so getters fall back to internal."""
        self.state.peer.remove_secret("operator-client-key")
        self.state.peer.remove_secret("operator-client-cert")
        self.state.peer.remove_secret("operator-client-ca")

    def store_peer_tls(self, key: str, cert: str, ca: str) -> None:
        """Persist the operator-provided peer key/cert and rotate the peer CA."""
        self.state.peer.operator_peer_key = key
        self.state.peer.operator_peer_cert = cert
        if ca != self.state.peer.current_ca:
            if self.state.peer.current_ca:
                self.state.peer.old_ca = self.state.peer.current_ca
            self.state.peer.current_ca = ca

    def clear_peer_tls(self) -> None:
        """Remove the operator peer material and rotate the peer CA on removal."""
        current = self.state.peer.current_ca
        if current:
            self.state.peer.old_ca = current
        self.state.peer.remove_secret("current-ca")
        self.state.peer.remove_secret("operator-peer-key")
        self.state.peer.remove_secret("operator-peer-cert")

    def get_client_tls_files(self) -> tuple[str | None, str | None, str | None]:
        """Return (key, ca, cert) for the operator client certificate from state."""
        cert = self.state.peer.operator_client_cert
        if cert is None:
            return None, None, None
        return (
            self.state.peer.operator_client_key,
            self.state.peer.operator_client_ca,
            cert,
        )

    def get_peer_ca_bundle(self) -> str:
        """Compose the peer CA bundle: current CA, old CA, internal CA."""
        cas = [
            self.state.peer.current_ca,
            self.state.peer.old_ca,
            self.state.get_secret(APP_SCOPE, "internal-ca"),
        ]
        return "\n".join(ca for ca in cas if ca).strip()

    def get_peer_tls_files(self) -> tuple[str | None, str | None, str | None]:
        """Return (key, ca, cert) for the peer certificate.

        Prefers the operator-provided peer material (with the composed CA
        bundle); falls back to the internally generated peer material.
        """
        if self.state.peer.operator_peer_cert is not None:
            return (
                self.state.peer.operator_peer_key,
                self.get_peer_ca_bundle(),
                self.state.peer.operator_peer_cert,
            )
        return (
            self.state.peer.internal_key,
            self.state.get_secret(APP_SCOPE, "internal-ca"),
            self.state.peer.internal_cert,
        )

    def push_tls_files(self) -> None:
        """Write the client, peer, and CA-bundle TLS files to the workload."""
        conf = self.workload.paths.conf

        key, ca, cert = self.get_client_tls_files()
        if key is not None:
            self.workload.write_text(key, conf / TLS_KEY_FILE, 0o600)
        if ca is not None:
            self.workload.write_text(ca, conf / TLS_CA_FILE, 0o600)
        if cert is not None:
            self.workload.write_text(cert, conf / TLS_CERT_FILE, 0o600)

        key, ca, cert = self.get_peer_tls_files()
        if key is not None:
            self.workload.write_text(key, conf / f"peer_{TLS_KEY_FILE}", 0o600)
        if ca is not None:
            self.workload.write_text(ca, conf / f"peer_{TLS_CA_FILE}", 0o600)
        if cert is not None:
            self.workload.write_text(cert, conf / f"peer_{TLS_CERT_FILE}", 0o600)

        self.workload.write_text(self.get_peer_ca_bundle(), conf / TLS_CA_BUNDLE_FILE, 0o600)

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        return [GeneralStatuses.ACTIVE_IDLE.value]
