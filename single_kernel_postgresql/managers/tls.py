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
    TLSCertificatesRequiresV4,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError, TlsError
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

    def __init__(
        self, state: CharmState, workload: BaseWorkload, client: PostgreSQLClient | None = None
    ) -> None:
        super().__init__(state, workload, "tls_manager", client)
        # Operator-certificate requirers, wired by events.tls.TLS after it builds them.
        # The getters below fetch cert/key LIVE from the durable relation databag — no
        # operator cert/key is persisted to state (matches the pre-port charm). Only the
        # peer CA is tracked in state (current-ca / old-ca), for rotation.
        self.client_certificate: TLSCertificatesRequiresV4 | None = None
        self.peer_certificate: TLSCertificatesRequiresV4 | None = None

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
            # substrate-aware: K8s excludes the ip SAN (matches the original charm).
            sans_ip=frozenset(self.state.peer_addresses),
            sans_dns=frozenset({
                *self.state.common_hosts,
                # IP address need to be part of the DNS SANs list due to
                # https://github.com/pgbackrest/pgbackrest/issues/1977.
                *self.state.peer_addresses,
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

    def rotate_peer_ca(self, ca: str) -> None:
        """Track the operator peer CA for rotation (current-ca -> old-ca).

        Only the CA is tracked in state; the operator cert/key are fetched live
        from the requirer. Mirrors the pre-port _on_peer_certificate_available.
        """
        if ca != self.state.peer.current_ca:
            if self.state.peer.current_ca:
                self.state.peer.old_ca = self.state.peer.current_ca
            else:
                # Re-enabling after a disable cleared current-ca: nothing is being
                # rotated out, so drop any stale old CA left from before the disable.
                self.state.peer.remove_secret("old-ca")
            self.state.peer.current_ca = ca

    def clear_peer_ca(self) -> None:
        """Retire the operator peer CA into old-ca on relation removal."""
        current = self.state.peer.current_ca
        if current:
            self.state.peer.old_ca = current
        self.state.peer.remove_secret("current-ca")

    def get_client_tls_files(self) -> tuple[str | None, str | None, str | None]:
        """Return (key, ca, cert) for the operator client cert, fetched live.

        Reads from the requirer's assigned certificate (the durable relation
        databag) rather than persisted state — matches the pre-port charm
        (postgresql-operator/src/relations/tls.py get_client_tls_files).
        """
        key = ca = cert = None
        if self.client_certificate is not None:
            certs, private_key = self.client_certificate.get_assigned_certificates()
            if private_key:
                key = str(private_key)
            if certs:
                cert = str(certs[0].certificate)
                ca = str(certs[0].ca)
        return key, ca, cert

    def client_tls_files_on_disk(self) -> bool:
        """Whether the client TLS files this unit serves are present on disk.

        The reload bridge checks this before enabling TLS in the config so it never
        renders ssl:on against files the push has not yet written: on K8s the Pebble
        push can defer while the local config render would still succeed. A workload
        that cannot be read (container down) counts as not-on-disk, so the caller defers.
        """
        tls = self.workload.paths.tls
        try:
            return all(
                self.workload.exists(tls / f) for f in (TLS_KEY_FILE, TLS_CERT_FILE, TLS_CA_FILE)
            )
        except PostgreSQLFileOperationError:
            return False

    def get_peer_ca_bundle(self) -> str:
        """Compose the peer CA bundle: live operator CA, old CA, internal CA.

        The current operator CA is read live from the requirer (pre-port style);
        the old CA and internal CA come from state. Mirrors the pre-port
        postgresql-operator/src/relations/tls.py get_peer_ca_bundle.
        """
        operator_ca = ""
        if self.peer_certificate is not None:
            certs, _ = self.peer_certificate.get_assigned_certificates()
            operator_ca = str(certs[0].ca) if certs else ""
        cas = [
            operator_ca,
            self.state.peer.old_ca,
            self.state.get_secret(APP_SCOPE, "internal-ca"),
        ]
        return "\n".join(ca for ca in cas if ca).strip()

    def get_peer_tls_files(self) -> tuple[str | None, str | None, str | None]:
        """Return (key, ca, cert) for the peer certificate, operator cert fetched live.

        Prefers the operator-provided peer material (read live from the requirer,
        with the composed CA bundle); falls back to the internally generated peer
        material. Mirrors the pre-port get_peer_tls_files.
        """
        key = cert = None
        if self.peer_certificate is not None:
            certs, private_key = self.peer_certificate.get_assigned_certificates()
            if private_key:
                key = str(private_key)
            if certs:
                cert = str(certs[0].certificate)
        if not all((key, cert)):
            return (
                self.state.peer.internal_key,
                self.state.get_secret(APP_SCOPE, "internal-ca"),
                self.state.peer.internal_cert,
            )
        return key, self.get_peer_ca_bundle(), cert

    def _write_tls_file(self, content: str, path) -> None:
        """Write a TLS file with substrate-specific permissions and ownership.

        The mode comes from the workload (VM 0o600, K8s 0o400, matching the
        pre-migration charms). Ownership is forwarded through pathops so it works
        on both VM (LocalPath uses os.chown) and K8s (ContainerPath uses Pebble push).
        """
        self.workload.write_text(
            content,
            path,
            self.workload.tls_file_mode,
            user=self.workload.user,
            group=self.workload.group,
        )

    def push_tls_files(self) -> None:
        """Write the client, peer, and CA-bundle TLS files to the workload."""
        tls = self.workload.paths.tls

        key, ca, cert = self.get_client_tls_files()
        if key is not None:
            self._write_tls_file(key, tls / TLS_KEY_FILE)
        if ca is not None:
            self._write_tls_file(ca, tls / TLS_CA_FILE)
        if cert is not None:
            self._write_tls_file(cert, tls / TLS_CERT_FILE)

        key, ca, cert = self.get_peer_tls_files()
        if key is not None:
            self._write_tls_file(key, tls / f"peer_{TLS_KEY_FILE}")
        if ca is not None:
            self._write_tls_file(ca, tls / f"peer_{TLS_CA_FILE}")
        if cert is not None:
            self._write_tls_file(cert, tls / f"peer_{TLS_CERT_FILE}")

        self._write_tls_file(self.get_peer_ca_bundle(), tls / TLS_CA_BUNDLE_FILE)

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        return [GeneralStatuses.ACTIVE_IDLE.value]
