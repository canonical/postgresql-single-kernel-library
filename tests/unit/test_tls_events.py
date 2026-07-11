# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the TLS events handler (single_kernel_postgresql/events/tls.py).

The handler is live-fetch: operator cert/key are read from the requirer on demand
(get_assigned_certificates), never persisted. Only the peer CA is tracked in state
(current-ca / old-ca) for rotation, matching the pre-port charm's peer-cert handler
(which stashes current-ca/old-ca and otherwise reads live).
"""

from unittest.mock import MagicMock, patch

from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError
from tls_helpers import fake_assigned


def test_handler_is_wired(harness):
    tls = harness.charm.tls
    assert tls.client_certificate is not None
    assert tls.peer_certificate is not None
    # the handler wires its requirers onto the manager so the live-fetch getters work
    assert harness.charm.tls_manager.client_certificate is tls.client_certificate
    assert harness.charm.tls_manager.peer_certificate is tls.peer_certificate


def test_client_certificate_available_pushes(harness, patch_crypto):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=fake_assigned("CC", "CA", "CK")
    )
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_certificate_available(MagicMock())

    # no operator client cert/key is persisted; the push reads live
    charm.tls_manager.push_tls_files.assert_called_once()
    assert charm.tls_manager.get_client_tls_files() == ("CK", "CA", "CC")


def test_peer_certificate_available_rotates_ca_and_pushes(harness, patch_crypto):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=fake_assigned("PC", "PCA", "PK")
    )
    charm.tls_manager.push_tls_files = MagicMock()

    tls._on_peer_certificate_available(MagicMock())

    peer = charm.state.peer
    # only the CA is tracked in state for rotation; cert/key stay live
    assert peer.current_ca == "PCA"
    charm.tls_manager.push_tls_files.assert_called_once()
    key, _, cert = charm.tls_manager.get_peer_tls_files()
    assert (key, cert) == ("PK", "PC")


def test_certificate_available_pushes_on_empty(harness, patch_crypto):
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


def test_peer_certificate_available_clears_ca_on_empty(harness, patch_crypto):
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


def test_relation_broken_client_routes_to_live_push(harness, patch_crypto):
    """relation_broken on TLS_CLIENT_RELATION routes to the live-push path.

    Distinct from the peer variant (which retires the CA): the client route only pushes,
    so routing is verified by the push being reached (internal-ca present via leadership)
    while peer CA rotation is left untouched.
    """
    charm = harness.charm
    # seed a peer CA so we can prove the client route does not touch peer rotation
    charm.tls_manager.rotate_peer_ca("PCA")
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)  # internal-ca present so the route reaches push
    charm.tls_manager.push_tls_files = MagicMock()

    client_rel_id = harness.add_relation("client-certificates", "tls-provider")
    harness.remove_relation(client_rel_id)

    # the client broken route reached the live push; peer CA rotation was not touched
    charm.tls_manager.push_tls_files.assert_called_once()
    assert charm.state.peer.current_ca == "PCA"
    assert charm.state.peer.old_ca is None
    # with the relation gone, the live getter reports nothing
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


def test_peer_relation_changed_emits_refresh(harness):
    """Peer relation_changed emits refresh_tls_certificates_event to re-request certs with updated SANs."""
    from single_kernel_postgresql.events.tls import TLS

    charm = harness.charm
    with patch.object(TLS, "refresh_tls_certificates_event") as _refresh:
        charm.tls._on_peer_relation_changed(MagicMock())

    _refresh.emit.assert_called_once()


def test_certificate_available_defers_when_internal_ca_absent(harness):
    """When internal-ca is not yet set, _on_certificate_available defers and skips push.

    The handler must not attempt file writes before the CA is present (K8s Pebble
    may not be ready), matching the pre-port charm's defer-before-CA-present guard.
    The defer guard lives in the shared _push_tls_files, so this also covers the peer
    handler's path (both route through it).
    """
    tls = harness.charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=fake_assigned("CC", "CA", "CK")
    )
    harness.charm.tls_manager.push_tls_files = MagicMock()

    event = MagicMock()
    # internal_ca is None because no leader has set it yet
    assert harness.charm.state.application.internal_ca is None
    tls._on_certificate_available(event)

    event.defer.assert_called_once()
    harness.charm.tls_manager.push_tls_files.assert_not_called()


def test_certificate_available_defers_on_workload_file_error(harness, patch_crypto):
    """When push_tls_files raises PostgreSQLFileOperationError, the handler defers.

    Workload file-write failures (e.g. Pebble not yet ready on K8s) must defer
    rather than crash the hook, matching the pre-port charm's defer-on-write-failure guard.
    The guard lives in the shared _push_tls_files, so this also covers the peer handler's path.
    """
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    tls = charm.tls
    tls.client_certificate.get_assigned_certificates = MagicMock(
        return_value=fake_assigned("CC", "CA", "CK")
    )
    charm.tls_manager.push_tls_files = MagicMock(
        side_effect=PostgreSQLFileOperationError("disk full")
    )

    event = MagicMock()
    tls._on_certificate_available(event)

    event.defer.assert_called_once()
