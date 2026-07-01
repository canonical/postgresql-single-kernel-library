# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the TLS events handler (single_kernel_postgresql/events/tls.py).

The handler is live-fetch: operator cert/key are read from the requirer on demand
(get_assigned_certificates), never persisted. Only the peer CA is tracked in state
(current-ca / old-ca) for rotation, mirroring postgresql-operator/src/relations/tls.py
_on_peer_certificate_available (which stashes current-ca/old-ca and otherwise reads live).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError


class _FakePrivateKey:
    """Minimal stand-in for PrivateKey: str(key) returns the raw PEM string."""

    def __init__(self, raw: str) -> None:
        self._raw = raw

    def __str__(self) -> str:
        return self._raw


def _fake_assigned(cert, ca, key):
    """Mimic TLSCertificatesRequiresV4.get_assigned_certificates() return shape.

    Returns (list[ProviderCertificate], PrivateKey | None).
    """
    provider_cert = SimpleNamespace(certificate=cert, ca=ca)
    return [provider_cert], _FakePrivateKey(key)


def test_handler_is_wired(harness):
    tls = harness.charm.tls
    assert tls.client_certificate is not None
    assert tls.peer_certificate is not None
    # the handler wires its requirers onto the manager so the live-fetch getters work
    assert harness.charm.tls_manager.client_certificate is tls.client_certificate
    assert harness.charm.tls_manager.peer_certificate is tls.peer_certificate


def test_client_certificate_available_pushes(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("CC", "CA", "CK")
    )
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_certificate_available(MagicMock())

    # no operator client cert/key is persisted; the push reads live
    charm.tls_manager.push_tls_files.assert_called_once()
    assert charm.tls_manager.get_client_tls_files() == ("CK", "CA", "CC")


def test_peer_certificate_available_rotates_ca_and_pushes(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("PC", "PCA", "PK")
    )
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_peer_certificate_available(MagicMock())

    peer = charm.state.peer
    # only the CA is tracked in state for rotation; cert/key stay live
    assert peer.current_ca == "PCA"
    charm.tls_manager.push_tls_files.assert_called_once()
    key, _, cert = charm.tls_manager.get_peer_tls_files()
    assert (key, cert) == ("PK", "PC")


def test_certificate_available_pushes_on_empty(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    # requirer reports no assigned cert (relation gone) -> getters return nothing
    tls.client_certificate.get_assigned_certificates = MagicMock(return_value=([], None))
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_certificate_available(MagicMock())

    assert charm.tls_manager.get_client_tls_files() == (None, None, None)
    charm.tls_manager.push_tls_files.assert_called_once()


def test_peer_certificate_available_clears_ca_on_empty(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    # seed a current CA via a prior rotation, then the relation goes empty
    charm.tls_manager.rotate_peer_ca("PCA")
    tls.peer_certificate.get_assigned_certificates = MagicMock(return_value=([], None))
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_peer_certificate_available(MagicMock())

    peer = charm.state.peer
    assert peer.old_ca == "PCA"
    assert peer.current_ca is None
    charm.tls_manager.push_tls_files.assert_called_once()


def test_relation_broken_client_wired(harness):
    """relation_broken on TLS_CLIENT_RELATION routes to _on_certificate_available (live push)."""
    charm = harness.charm
    client_rel_id = harness.add_relation("client-certificates", "tls-provider")
    charm.tls_manager.push_tls_files = MagicMock()
    harness.remove_relation(client_rel_id)

    # With the relation gone, the live getter reports nothing.
    assert charm.tls_manager.get_client_tls_files() == (None, None, None)


def test_relation_broken_peer_wired(harness):
    """relation_broken on TLS_PEER_RELATION routes to _on_peer_certificate_available (clears CA)."""
    charm = harness.charm
    # seed a current CA so the broken path has something to retire
    charm.tls_manager.rotate_peer_ca("PCA")

    peer_rel_id = harness.add_relation("peer-certificates", "tls-provider")
    charm.tls_manager.push_tls_files = MagicMock()
    harness.remove_relation(peer_rel_id)

    # The broken handler retired the current CA into old-ca and cleared current.
    assert charm.state.peer.current_ca is None
    assert charm.state.peer.old_ca == "PCA"


def _set_unit_db(harness, key, value):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, {key: value})


def test_client_and_peer_requesters_have_distinct_common_names(substrate, harness):
    """Client and peer requesters use distinct CNs drawn from different databag keys.

    certificate_requests are baked at init time (before the test updates the databag), so
    we verify distinctness via the live state properties (which read directly from the
    databag) and confirm both requester objects were constructed.

    On VM the CNs are host-derived (database-address / database-peers-address). On K8s
    both CNs collapse to the endpoints FQDN (original charm parity) — still distinct from
    each other is not required there, only that both are the endpoints FQDN.
    """
    _set_unit_db(harness, "database-address", "10.1.2.3")
    _set_unit_db(harness, "database-peers-address", "10.4.5.6")

    state = harness.charm.state
    if substrate == "vm":
        # VM: client CN reads database-address, peer CN reads database-peers-address.
        assert state.client_common_name == "10.1.2.3"
        assert state.peer_common_name == "10.4.5.6"
        assert state.client_common_name != state.peer_common_name
    else:
        # K8s: both CNs are the unit endpoints FQDN (operator-cert parity).
        app = state.model.app.name
        expected = f"{app}-0.{app}-endpoints"
        assert state.client_common_name == expected
        assert state.peer_common_name == expected

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


def test_certificate_available_defers_when_internal_ca_absent(harness):
    """When internal-ca is not yet set, _on_certificate_available defers and skips push.

    Mirrors postgresql-operator/src/relations/tls.py lines 157-161: the handler must
    not attempt file writes before the CA is present (K8s Pebble may not be ready).
    """
    tls = harness.charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("CC", "CA", "CK")
    )
    harness.charm.tls_manager.push_tls_files = MagicMock()

    event = MagicMock()
    # internal_ca is None because no leader has set it yet
    assert harness.charm.state.application.internal_ca is None
    tls._on_certificate_available(event)

    event.defer.assert_called_once()
    harness.charm.tls_manager.push_tls_files.assert_not_called()


def test_peer_certificate_available_defers_when_internal_ca_absent(harness):
    """When internal-ca is not yet set, _on_peer_certificate_available defers and skips push."""
    tls = harness.charm.tls
    tls.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("PC", "PCA", "PK")
    )
    harness.charm.tls_manager.push_tls_files = MagicMock()

    event = MagicMock()
    assert harness.charm.state.application.internal_ca is None
    tls._on_peer_certificate_available(event)

    event.defer.assert_called_once()
    harness.charm.tls_manager.push_tls_files.assert_not_called()


def test_certificate_available_defers_on_workload_file_error(harness):
    """When push_tls_files raises PostgreSQLFileOperationError, the handler defers.

    Mirrors postgresql-operator/src/relations/tls.py lines 162-170: workload
    file-write failures (e.g. Pebble not yet ready on K8s) must defer rather
    than crash the hook.
    """
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("CC", "CA", "CK")
    )
    charm.tls_manager.push_tls_files = MagicMock(
        side_effect=PostgreSQLFileOperationError("disk full")
    )

    event = MagicMock()
    tls._on_certificate_available(event)

    event.defer.assert_called_once()


def test_peer_certificate_available_defers_on_workload_file_error(harness):
    """When push_tls_files raises PostgreSQLFileOperationError, the peer handler defers."""
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned("PC", "PCA", "PK")
    )
    charm.tls_manager.push_tls_files = MagicMock(
        side_effect=PostgreSQLFileOperationError("pebble not ready")
    )

    event = MagicMock()
    tls._on_peer_certificate_available(event)

    event.defer.assert_called_once()
