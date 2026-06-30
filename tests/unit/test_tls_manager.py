# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, sentinel


class _FakePrivateKey:
    """Minimal stand-in for PrivateKey: str(key) returns the raw PEM string."""

    def __init__(self, raw: str) -> None:
        self._raw = raw

    def __str__(self) -> str:
        return self._raw


def _fake_assigned(cert, ca, key):
    """Mimic TLSCertificatesRequiresV4.get_assigned_certificates() -> (list, PrivateKey|None)."""
    return [SimpleNamespace(certificate=cert, ca=ca)], _FakePrivateKey(key)


def _set_client(mgr, cert, ca, key):
    mgr.client_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned(cert, ca, key)
    )


def _set_peer(mgr, cert, ca, key):
    mgr.peer_certificate.get_assigned_certificates = MagicMock(
        return_value=_fake_assigned(cert, ca, key)
    )


def _set_empty(requirer):
    requirer.get_assigned_certificates = MagicMock(return_value=([], None))


# --- CA rotation (current-ca / old-ca are 16/edge secrets, kept; cert/key are NOT stored) ---


def test_rotate_peer_ca_sets_current(harness):
    mgr = harness.charm.tls_manager
    mgr.rotate_peer_ca("CA1")

    peer = harness.charm.state.peer
    assert peer.current_ca == "CA1"
    assert peer.old_ca is None


def test_rotate_peer_ca_rotates_on_change(harness):
    mgr = harness.charm.tls_manager
    mgr.rotate_peer_ca("CA1")
    mgr.rotate_peer_ca("CA2")

    peer = harness.charm.state.peer
    assert peer.current_ca == "CA2"
    assert peer.old_ca == "CA1"


def test_rotate_peer_ca_no_rotation_when_unchanged(harness):
    mgr = harness.charm.tls_manager
    mgr.rotate_peer_ca("CA1")
    mgr.rotate_peer_ca("CA1")

    peer = harness.charm.state.peer
    assert peer.current_ca == "CA1"
    assert peer.old_ca is None


def test_clear_peer_ca_rotates_current_to_old(harness):
    mgr = harness.charm.tls_manager
    mgr.rotate_peer_ca("CA")
    mgr.clear_peer_ca()

    peer = harness.charm.state.peer
    assert peer.old_ca == "CA"
    assert peer.current_ca is None


def test_rotate_peer_ca_clears_stale_old_ca_on_reenable(harness):
    """Re-enabling after a disable must not leave a stale pre-disable CA in the bundle."""
    mgr = harness.charm.tls_manager
    mgr.rotate_peer_ca("A")
    mgr.rotate_peer_ca("B")  # rotate A -> B
    assert mgr.state.peer.current_ca == "B"
    assert mgr.state.peer.old_ca == "A"
    mgr.clear_peer_ca()  # disable stashes B as old and clears current
    assert mgr.state.peer.current_ca is None
    assert mgr.state.peer.old_ca == "B"
    mgr.rotate_peer_ca("C")  # re-enable with a fresh CA
    assert mgr.state.peer.current_ca == "C"
    assert mgr.state.peer.old_ca is None


# --- operator cert/key are fetched LIVE from the requirer, never persisted ---


def test_get_client_tls_files_none_when_absent(harness):
    mgr = harness.charm.tls_manager
    _set_empty(mgr.client_certificate)
    assert mgr.get_client_tls_files() == (None, None, None)


def test_get_client_tls_files_returns_live(harness):
    mgr = harness.charm.tls_manager
    _set_client(mgr, cert="C", ca="A", key="K")
    assert mgr.get_client_tls_files() == ("K", "A", "C")


def test_get_peer_ca_bundle_composes_live_old_internal(harness):
    charm = harness.charm
    # leadership is required to write the app-scoped internal-ca secret
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    mgr = charm.tls_manager
    _set_peer(mgr, cert="PC", ca="CUR", key="PK")  # live operator CA = CUR
    charm.state.peer.old_ca = "OLD"
    charm.state.set_secret("app", "internal-ca", "INT")

    assert mgr.get_peer_ca_bundle() == "CUR\nOLD\nINT"


def test_get_peer_tls_files_prefers_live_operator(harness):
    mgr = harness.charm.tls_manager
    _set_peer(mgr, cert="PC", ca="PCA", key="PK")
    key, ca, cert = mgr.get_peer_tls_files()
    assert (key, cert) == ("PK", "PC")
    assert ca == "PCA"  # bundle = live operator CA only (no old, no internal here)


def test_get_peer_tls_files_falls_back_to_internal(harness):
    charm = harness.charm
    # leadership is required to write the app-scoped internal-ca secret
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    mgr = charm.tls_manager
    _set_empty(mgr.peer_certificate)
    peer = charm.state.peer
    peer.internal_key = "IK"
    peer.internal_cert = "IC"
    charm.state.set_secret("app", "internal-ca", "ICA")

    assert mgr.get_peer_tls_files() == ("IK", "ICA", "IC")


def test_push_tls_files_writes_expected_files(harness):
    mgr = harness.charm.tls_manager
    _set_client(mgr, cert="CC", ca="CA", key="CK")
    _set_peer(mgr, cert="PC", ca="PCA", key="PK")

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
    # all TLS files are written with the workload's substrate-specific mode
    # (VM 0o600, K8s 0o400), user, and group
    expected_mode = mgr.workload.tls_file_mode
    expected_user = mgr.workload.user
    expected_group = mgr.workload.group
    for call in mgr.workload.write_text.call_args_list:
        assert call.args[2] == expected_mode
        assert call.kwargs["user"] == expected_user
        assert call.kwargs["group"] == expected_group
    # every TLS file is written under the substrate-correct TLS dir (paths.tls)
    for call in mgr.workload.write_text.call_args_list:
        assert call.args[1].parent == mgr.workload.paths.tls


def test_tls_manager_constructs_without_client(harness):
    """TLSManager must not require a PostgreSQL client (it never uses one)."""
    from single_kernel_postgresql.managers.tls import TLSManager

    mgr = TLSManager(harness.charm.state, harness.charm.tls_manager.workload)
    assert mgr.postgresql_client is None
    # requirers are wired by the TLS events handler, not the manager itself
    assert mgr.client_certificate is None
    assert mgr.peer_certificate is None


def test_client_tls_files_on_disk(harness):
    """Reports presence of the client key/cert/ca files; container errors count as absent."""
    from single_kernel_postgresql.config.exceptions import PostgreSQLFileOperationError

    mgr = harness.charm.tls_manager
    with patch.object(mgr.workload, "exists", return_value=True) as _exists:
        assert mgr.client_tls_files_on_disk() is True
        assert _exists.call_count == 3
    with patch.object(mgr.workload, "exists", side_effect=[True, False]):
        assert mgr.client_tls_files_on_disk() is False
    with patch.object(mgr.workload, "exists", side_effect=PostgreSQLFileOperationError("down")):
        assert mgr.client_tls_files_on_disk() is False


def test_generate_internal_peer_ca_stores_secrets(harness):
    """generate_internal_peer_ca persists the generated CA cert/key into app state."""
    charm = harness.charm
    # leadership is required to write the app-scoped internal-ca secrets
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    with (
        patch(
            "single_kernel_postgresql.managers.tls.generate_private_key",
            return_value=sentinel.ca_key,
        ),
        patch(
            "single_kernel_postgresql.managers.tls.generate_ca",
            return_value=sentinel.ca,
        ),
    ):
        charm.tls_manager.generate_internal_peer_ca()

    assert charm.state.application.internal_ca_key == str(sentinel.ca_key)
    assert charm.state.application.internal_ca == str(sentinel.ca)


def test_generate_internal_peer_cert_stores_material(harness):
    """generate_internal_peer_cert persists internal key/cert into peer state and pushes nothing.

    Writing the internal peer cert/CA to disk is owned by the (not-yet-migrated)
    config subsystem, so generating the material must not touch the workload.
    """
    charm = harness.charm
    mgr = charm.tls_manager
    # leadership is required to write/read the app-scoped internal-ca secrets the
    # internal peer cert is signed against
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)
    charm.state.set_secret("app", "internal-ca-key", "ca-key-content")
    charm.state.set_secret("app", "internal-ca", "ca-content")

    # spy on the only file-writing primitive to prove no push happens
    mgr.workload.write_text = MagicMock()
    with (
        patch(
            "single_kernel_postgresql.managers.tls.generate_private_key",
            return_value=sentinel.cert_key,
        ),
        patch(
            "single_kernel_postgresql.managers.tls.generate_csr",
            return_value=sentinel.cert_csr,
        ),
        patch(
            "single_kernel_postgresql.managers.tls.generate_certificate",
            return_value=sentinel.cert,
        ) as _generate_certificate,
        patch("single_kernel_postgresql.managers.tls.PrivateKey") as _private_key,
        patch("single_kernel_postgresql.managers.tls.Certificate") as _certificate,
    ):
        _private_key.from_string.return_value = sentinel.ca_key
        _certificate.from_string.return_value = sentinel.ca_cert

        mgr.generate_internal_peer_cert()

        # the CSR is signed by the stored internal CA (cert + key), valid 20 years
        _generate_certificate.assert_called_once_with(
            sentinel.cert_csr, sentinel.ca_cert, sentinel.ca_key, validity=timedelta(days=7300)
        )

    # generated material is persisted in peer-unit state (round-trip)
    assert charm.state.peer.internal_cert == str(sentinel.cert)
    assert charm.state.peer.internal_key == str(sentinel.cert_key)
    # writing the internal peer cert/CA to disk is owned by the config subsystem
    mgr.workload.write_text.assert_not_called()
