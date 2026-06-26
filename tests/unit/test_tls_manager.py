# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import MagicMock, patch


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


def test_get_client_tls_files_none_when_absent(harness):
    assert harness.charm.tls_manager.get_client_tls_files() == (None, None, None)


def test_get_client_tls_files_returns_stored(harness):
    mgr = harness.charm.tls_manager
    mgr.store_client_tls(key="K", cert="C", ca="A")
    assert mgr.get_client_tls_files() == ("K", "A", "C")


def test_get_peer_ca_bundle_composes_current_old_internal(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    peer = charm.state.peer
    peer.current_ca = "CUR"
    peer.old_ca = "OLD"
    charm.state.set_secret("app", "internal-ca", "INT")

    assert charm.tls_manager.get_peer_ca_bundle() == "CUR\nOLD\nINT"


def test_get_peer_tls_files_prefers_operator(harness):
    mgr = harness.charm.tls_manager
    mgr.store_peer_tls(key="PK", cert="PC", ca="PCA")
    key, ca, cert = mgr.get_peer_tls_files()
    assert (key, cert) == ("PK", "PC")
    assert ca == "PCA"  # bundle = current-ca only here


def test_get_peer_tls_files_falls_back_to_internal(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    peer = charm.state.peer
    peer.internal_key = "IK"
    peer.internal_cert = "IC"
    charm.state.set_secret("app", "internal-ca", "ICA")

    assert charm.tls_manager.get_peer_tls_files() == ("IK", "ICA", "IC")


def test_clear_client_tls_removes_operator_fields(harness):
    mgr = harness.charm.tls_manager
    mgr.store_client_tls(key="K", cert="C", ca="A")
    mgr.clear_client_tls()

    assert mgr.get_client_tls_files() == (None, None, None)


def test_clear_peer_tls_rotates_ca_and_removes_fields(harness):
    mgr = harness.charm.tls_manager
    mgr.store_peer_tls(key="K", cert="C", ca="CA")
    mgr.clear_peer_tls()

    peer = harness.charm.state.peer
    assert peer.old_ca == "CA"
    assert peer.current_ca is None
    assert peer.operator_peer_cert is None
    assert peer.operator_peer_key is None


def test_clear_peer_tls_falls_back_to_internal(harness):
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    peer = charm.state.peer
    peer.internal_key = "IK"
    peer.internal_cert = "IC"
    charm.state.set_secret("app", "internal-ca", "ICA")

    charm.tls_manager.store_peer_tls(key="K", cert="C", ca="CA")
    charm.tls_manager.clear_peer_tls()

    assert charm.tls_manager.get_peer_tls_files() == ("IK", "ICA", "IC")


def test_push_tls_files_writes_expected_files(harness):
    mgr = harness.charm.tls_manager
    mgr.store_client_tls(key="CK", cert="CC", ca="CA")
    mgr.store_peer_tls(key="PK", cert="PC", ca="PCA")

    mgr.workload.write_text = MagicMock()
    mgr.push_tls_files()

    written = {call.args[1].name: call.args[0] for call in mgr.workload.write_text.call_args_list}
    assert written["key.pem"] == "CK"
    assert written["ca.pem"] == "CA"
    assert written["cert.pem"] == "CC"
    assert written["peer_key.pem"] == "PK"
    assert written["peer_cert.pem"] == "PC"
    assert written["peer_ca.pem"] == "PCA"
    assert written["peer_ca_bundle.pem"] == "PCA"
    # all TLS files are written with 0o600
    assert all(call.args[2] == 0o600 for call in mgr.workload.write_text.call_args_list)
