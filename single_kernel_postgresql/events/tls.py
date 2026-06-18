# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""TLS events handler — owns the operator-certificate requirers, delegates to TLSManager."""

import logging

from charmlibs.interfaces.tls_certificates import (
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from ops import EventSource
from ops.framework import EventBase, Object

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError
from single_kernel_postgresql.config.literals import (
    PEER_RELATION,
    TLS_CLIENT_RELATION,
    TLS_PEER_RELATION,
)

logger = logging.getLogger(__name__)


class RefreshTLSCertificatesEvent(EventBase):
    """Event emitted to trigger a re-request of TLS certificates with updated SANs."""


class TLS(Object):
    """Owns the client/peer certificate requirers and pushes assigned certs into state.

    Operator-certificate handler: observes certificate_available and stores/pushes
    via TLSManager. Also owns the refresh_tls_certificates_event that re-requests
    certificates whenever SANs change (emitted on peer relation_changed).

    Design notes
    ------------
    1. **State-backed model.** This handler is deliberately state-backed: it reads
       certificates from TLSManager (which reads from CharmState/peer databag) rather
       than calling get_assigned_certificates() on every access.  This diverges from
       the charm's live-read style; the logic lives in TLSManager and the handler stays
       thin.  Do not "reconcile" the two copies.

    2. **CA bundle terminology.** The ``current_ca`` term used in state mirrors the
       charm's live operator-CA term (postgresql-operator/src/relations/tls.py).  It
       refers to the most recent peer CA from the TLS operator, not the internal
       self-signed CA.

    3. **SANs and the relation_changed trigger.** The ``relation_changed``-driven
       ``refresh_tls_certificates_event`` is a stand-in for the charm's IP-change
       trigger.  The client/peer SANs in the certificate requests remain inert until
       the cluster (address-writer) code migrates and starts writing
       ``database-address`` / ``database-peers-address`` keys into the peer databag.
    """

    refresh_tls_certificates_event = EventSource(RefreshTLSCertificatesEvent)

    def __init__(self, charm, state, workload, tls_manager):
        super().__init__(charm, key="tls")
        self.charm = charm
        self.state = state
        self.workload = workload
        self.tls_manager = tls_manager

        client_addresses = self.state.client_addresses
        peer_addresses = self.state.peer.peer_addresses

        self.client_certificate = TLSCertificatesRequiresV4(
            self.charm,
            TLS_CLIENT_RELATION,
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.state.client_common_name,
                    sans_ip=frozenset(client_addresses),
                    sans_dns=frozenset({*self.state.common_hosts, *client_addresses}),
                ),
            ],
            refresh_events=[self.refresh_tls_certificates_event],
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
            refresh_events=[self.refresh_tls_certificates_event],
        )

        self.framework.observe(
            self.client_certificate.on.certificate_available, self._on_certificate_available
        )
        self.framework.observe(
            self.peer_certificate.on.certificate_available, self._on_peer_certificate_available
        )
        self.framework.observe(
            self.charm.on[TLS_CLIENT_RELATION].relation_broken, self._on_certificate_available
        )
        self.framework.observe(
            self.charm.on[TLS_PEER_RELATION].relation_broken, self._on_peer_certificate_available
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed, self._on_peer_relation_changed
        )

    def _on_peer_relation_changed(self, event) -> None:
        """Re-request certificates when peer addresses change."""
        # TODO: narrow this to fire only on address changes (database-address /
        # database-peers-address key diffs) once the cluster code lands and starts
        # writing those keys.  Currently fires on any peer-databag change.
        self.refresh_tls_certificates_event.emit()

    def _push_tls_files(self, event) -> None:
        """Guard-then-push helper: defer if the workload is not yet ready.

        Two conditions must hold before files can be written:
        1. The internal CA secret must exist — it is written by the leader on
           leader-elected, so non-leaders and early hooks may see it absent.
        2. The workload must accept file writes — on K8s the Pebble container
           may not be ready yet, causing PostgreSQLFileOperationError.

        Mirrors postgresql-operator/src/relations/tls.py lines 157-170.
        """
        if not self.state.application.internal_ca:
            logger.debug("Internal CA not yet present; deferring TLS file push.")
            event.defer()
            return
        try:
            self.tls_manager.push_tls_files()
        except PostgreSQLFileOperationError:
            logger.debug("Workload not ready for TLS file write; deferring.")
            event.defer()

    def _on_certificate_available(self, event) -> None:
        """Store or clear the operator client cert and push TLS files."""
        certs, private_key = self.client_certificate.get_assigned_certificates()
        if certs and private_key is not None:
            provider_cert = certs[0]
            self.tls_manager.store_client_tls(
                key=str(private_key),
                cert=str(provider_cert.certificate),
                ca=str(provider_cert.ca),
            )
        else:
            self.tls_manager.clear_client_tls()
        self._push_tls_files(event)

    def _on_peer_certificate_available(self, event) -> None:
        """Store or clear the operator peer cert (rotating the CA) and push TLS files."""
        certs, private_key = self.peer_certificate.get_assigned_certificates()
        if certs and private_key is not None:
            provider_cert = certs[0]
            self.tls_manager.store_peer_tls(
                key=str(private_key),
                cert=str(provider_cert.certificate),
                ca=str(provider_cert.ca),
            )
        else:
            self.tls_manager.clear_peer_tls()
        self._push_tls_files(event)
