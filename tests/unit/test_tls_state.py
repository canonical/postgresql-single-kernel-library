# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for operator-cert TLS state accessors on PostgreSQLPeer."""

from single_kernel_postgresql.config.enums import Substrates  # noqa: F401  (substrate fixture)


def test_common_hosts_k8s_includes_service_endpoints(substrate, harness):
    """On K8s the cert-SAN source must carry the primary/replicas Service DNS."""
    state = harness.charm.state
    app = state.model.app.name
    namespace = state.model.name
    hosts = state.common_hosts

    if substrate == "k8s":
        assert f"{app}-primary.{namespace}.svc.cluster.local" in hosts
        assert f"{app}-replicas.{namespace}.svc.cluster.local" in hosts
        # host/fqdn still present
        assert state.host in hosts
        assert state.fqdn in hosts
    else:
        # VM: only host/fqdn — no k8s service DNS leaks in
        assert not any(h.endswith(".svc.cluster.local") for h in hosts)
        assert hosts == {state.host, state.fqdn}


def test_operator_client_material_roundtrips(harness):
    peer = harness.charm.state.peer
    peer.operator_client_key = "CLIENT-KEY"
    peer.operator_client_cert = "CLIENT-CERT"
    peer.operator_client_ca = "CLIENT-CA"

    assert peer.operator_client_key == "CLIENT-KEY"
    assert peer.operator_client_cert == "CLIENT-CERT"
    assert peer.operator_client_ca == "CLIENT-CA"


def test_operator_peer_material_roundtrips(harness):
    peer = harness.charm.state.peer
    peer.operator_peer_key = "PEER-KEY"
    peer.operator_peer_cert = "PEER-CERT"

    assert peer.operator_peer_key == "PEER-KEY"
    assert peer.operator_peer_cert == "PEER-CERT"


def test_ca_rotation_slots_roundtrip(harness):
    peer = harness.charm.state.peer
    peer.current_ca = "CA-1"
    peer.old_ca = "CA-0"

    assert peer.current_ca == "CA-1"
    assert peer.old_ca == "CA-0"


def test_unset_operator_material_is_none(harness):
    peer = harness.charm.state.peer
    assert peer.operator_client_cert is None
    assert peer.operator_peer_cert is None
    assert peer.current_ca is None
