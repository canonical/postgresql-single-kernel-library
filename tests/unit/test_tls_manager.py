# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.


def test_store_client_tls_writes_state(harness):
    mgr = harness.charm.tls_manager
    mgr.store_client_tls(key="K", cert="C", ca="A")

    peer = harness.charm.state.peer
    assert peer.operator_client_key == "K"
    assert peer.operator_client_cert == "C"
    assert peer.operator_client_ca == "A"


def test_store_peer_tls_sets_current_ca(harness):
    mgr = harness.charm.tls_manager
    mgr.store_peer_tls(key="K1", cert="C1", ca="CA1")

    peer = harness.charm.state.peer
    assert peer.operator_peer_key == "K1"
    assert peer.operator_peer_cert == "C1"
    assert peer.current_ca == "CA1"
    assert peer.old_ca is None


def test_store_peer_tls_rotates_ca_on_change(harness):
    mgr = harness.charm.tls_manager
    mgr.store_peer_tls(key="K1", cert="C1", ca="CA1")
    mgr.store_peer_tls(key="K2", cert="C2", ca="CA2")

    peer = harness.charm.state.peer
    assert peer.current_ca == "CA2"
    assert peer.old_ca == "CA1"


def test_store_peer_tls_no_rotation_when_ca_unchanged(harness):
    mgr = harness.charm.tls_manager
    mgr.store_peer_tls(key="K1", cert="C1", ca="CA1")
    mgr.store_peer_tls(key="K2", cert="C2", ca="CA1")

    peer = harness.charm.state.peer
    assert peer.current_ca == "CA1"
    assert peer.old_ca is None
