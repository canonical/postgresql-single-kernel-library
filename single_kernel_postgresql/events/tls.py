# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""TLS events handler — owns the operator-certificate requirers, delegates to TLSManager."""

import logging

from charmlibs.interfaces.tls_certificates import (
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from ops.framework import Object

from single_kernel_postgresql.config.literals import TLS_CLIENT_RELATION, TLS_PEER_RELATION

logger = logging.getLogger(__name__)


class TLS(Object):
    """Owns the client/peer certificate requirers and pushes assigned certs into state.

    First-cut operator-certificate handler: observes certificate_available and stores/pushes
    via TLSManager. Deferred follow-ups (added when the real charm consumes this): observing
    relation_broken, CA-rotation on certificate removal, and refresh_events re-requests.
    """

    def __init__(self, charm, state, workload, tls_manager):
        super().__init__(charm, key="tls")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.tls_manager = tls_manager

        peer_addresses = self.state.peer.peer_addresses

        # TODO: client and peer requesters currently share SANs; distinguish client vs peer
        # address sets when CharmState exposes them separately.
        self.client_certificate = TLSCertificatesRequiresV4(
            self.charm,
            TLS_CLIENT_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.state.peer_common_name,
                    sans_ip=frozenset(peer_addresses),
                    sans_dns=frozenset({*self.state.common_hosts, *peer_addresses}),
                ),
            ],
        )
        self.peer_certificate = TLSCertificatesRequiresV4(
            self.charm,
            TLS_PEER_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.state.peer_common_name,
                    sans_ip=frozenset(peer_addresses),
                    sans_dns=frozenset({*self.state.common_hosts, *peer_addresses}),
                ),
            ],
        )

        self.framework.observe(
            self.client_certificate.on.certificate_available, self._on_certificate_available
        )
        self.framework.observe(
            self.peer_certificate.on.certificate_available, self._on_peer_certificate_available
        )

    def _on_certificate_available(self, event) -> None:
        """Store the operator client cert and push TLS files."""
        certs, private_key = self.client_certificate.get_assigned_certificates()
        if not certs or private_key is None:
            return
        provider_cert = certs[0]
        self.tls_manager.store_client_tls(
            key=str(private_key),
            cert=str(provider_cert.certificate),
            ca=str(provider_cert.ca),
        )
        self.tls_manager.push_tls_files()

    def _on_peer_certificate_available(self, event) -> None:
        """Store the operator peer cert (rotating the CA) and push TLS files."""
        certs, private_key = self.peer_certificate.get_assigned_certificates()
        if not certs or private_key is None:
            return
        provider_cert = certs[0]
        self.tls_manager.store_peer_tls(
            key=str(private_key),
            cert=str(provider_cert.certificate),
            ca=str(provider_cert.ca),
        )
        self.tls_manager.push_tls_files()
