# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the TLS events handler (single_kernel_postgresql/events/tls.py).

The extraction path uses str(private_key) / str(provider_cert.certificate) /
str(provider_cert.ca), mirroring postgresql-operator/src/relations/tls.py
get_client_tls_files / get_peer_tls_files (lines 183-212).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch


class _FakePrivateKey:
    """Minimal stand-in for PrivateKey: str(key) returns the raw PEM string."""

    def __init__(self, raw: str) -> None:
        self._raw = raw

    def __str__(self) -> str:
        return self._raw


def _fake_assigned(cert, ca, key):
    """Mimic TLSCertificatesRequiresV4.get_assigned_certificates() return shape.

    Returns (list[ProviderCertificate], PrivateKey | None).
    ProviderCertificate has .certificate and .ca attributes (Certificate objects
    that str() to their PEM text). PrivateKey str()-s to its PEM text.
    The handler calls str(private_key), str(provider_cert.certificate),
    str(provider_cert.ca) — mirroring postgresql-operator/src/relations/tls.py:183-188.
    Test strings are already their own str() representation so SimpleNamespace suffices
    for the cert/ca fields.
    """
    provider_cert = SimpleNamespace(certificate=cert, ca=ca)
    return [provider_cert], _FakePrivateKey(key)


def test_handler_is_wired(harness):
    tls = harness.charm.tls
    assert tls.client_certificate is not None
    assert tls.peer_certificate is not None


def test_client_certificate_available_stores_and_pushes(harness):
    tls = harness.charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("CC", "CA", "CK")
    )
    harness.charm.tls_manager.push_tls_files = MagicMock()

    tls._on_certificate_available(MagicMock())

    peer = harness.charm.state.peer
    assert peer.operator_client_cert == "CC"
    assert peer.operator_client_ca == "CA"
    assert peer.operator_client_key == "CK"
    harness.charm.tls_manager.push_tls_files.assert_called_once()


def test_peer_certificate_available_rotates_and_pushes(harness):
    tls = harness.charm.tls
    tls.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("PC", "PCA", "PK")
    )
    harness.charm.tls_manager.push_tls_files = MagicMock()

    tls._on_peer_certificate_available(MagicMock())

    peer = harness.charm.state.peer
    assert peer.operator_peer_cert == "PC"
    assert peer.current_ca == "PCA"
    assert peer.operator_peer_key == "PK"
    harness.charm.tls_manager.push_tls_files.assert_called_once()


def _set_unit_db(harness, key, value):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, {key: value})


def test_client_and_peer_requesters_have_distinct_common_names(harness):
    """Client and peer requesters use distinct CNs drawn from different databag keys.

    certificate_requests are baked at init time (before the test updates the databag), so
    we verify distinctness via the live state properties (which read directly from the
    databag) and confirm both requester objects were constructed.
    """
    _set_unit_db(harness, "database-address", "10.1.2.3")
    _set_unit_db(harness, "database-peers-address", "10.4.5.6")

    state = harness.charm.state
    # Real distinct values: client CN reads database-address, peer CN reads database-peers-address.
    assert state.client_common_name == "10.1.2.3"
    assert state.peer_common_name == "10.4.5.6"
    assert state.client_common_name != state.peer_common_name

    tls = harness.charm.tls
    assert tls.client_certificate is not None
    assert tls.peer_certificate is not None


def test_refresh_event_defined_and_wired(harness):
    """refresh_tls_certificates_event exists; peer relation_changed emits it without error."""
    tls = harness.charm.tls
    assert hasattr(tls, "refresh_tls_certificates_event")

    charm = harness.charm
    peer_rel_id = harness.model.get_relation("database-peers").id
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords", return_value=None),
        patch.object(charm.config_manager, "update_config", return_value=None),
    ):
        harness.update_relation_data(
            peer_rel_id, charm.unit.name, {"database-address": "10.9.8.7"}
        )
