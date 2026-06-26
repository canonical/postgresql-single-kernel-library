# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for operator-cert TLS state accessors on PostgreSQLPeer."""

import socket
from unittest.mock import patch

from single_kernel_postgresql.config.enums import Substrates  # noqa: F401  (substrate fixture)


def _set_unit_db(harness, key, value):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, {key: value})


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
        # the resolved per-pod FQDN is also included (parity with the original charm)
        assert socket.getfqdn() in hosts
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


# -- G1: substrate-aware cert common name (parity with the original charm) ----


def test_cert_common_name_is_endpoints_fqdn_on_k8s(substrate, harness):
    """K8s operator-cert CN must be `<app>-<unit_id>.<app>-endpoints` (original charm parity)."""
    state = harness.charm.state
    app = state.model.app.name
    expected = f"{app}-0.{app}-endpoints"

    if substrate == "k8s":
        assert state.client_common_name == expected
        assert state.peer_common_name == expected
    else:
        # VM unchanged: host-derived, not the endpoints FQDN.
        assert state.client_common_name != expected
        assert state.peer_common_name != expected


def test_cert_common_name_wildcards_when_too_long_on_k8s(substrate, harness):
    """K8s CN collapses to `*.<app>-endpoints` when the endpoints FQDN exceeds 64 chars."""
    if substrate != "k8s":
        return  # wildcard rule is K8s-only

    state = harness.charm.state
    app = state.model.app.name
    # Force the endpoints FQDN past the 64-char CN limit; the suffix stays app-derived.
    long_fqdn = "x" * 80
    with patch.object(state, "_get_hostname_from_unit", return_value=long_fqdn):
        assert state._k8s_cert_common_name == f"*.{app}-endpoints"


def test_cert_common_name_vm_unchanged(substrate, harness):
    """VM CN stays host-derived: client reads database-address, peer reads database-peers-address."""
    if substrate != "vm":
        return

    _set_unit_db(harness, "database-address", "10.1.2.3")
    _set_unit_db(harness, "database-peers-address", "10.4.5.6")
    state = harness.charm.state
    assert state.client_common_name == "10.1.2.3"
    assert state.peer_common_name == "10.4.5.6"


# -- G2: substrate-aware peer_addresses (no `ip` SAN on K8s) -----------------


def test_peer_addresses_excludes_ip_on_k8s(substrate, harness):
    """K8s peer SAN set must omit the `ip` databag key (original K8s charm never added it)."""
    _set_unit_db(harness, "ip", "10.0.0.1")
    _set_unit_db(harness, "private-address", "10.0.0.2")
    _set_unit_db(harness, "database-peers-address", "10.0.0.3")

    addrs = harness.charm.state.peer_addresses

    if substrate == "k8s":
        assert "10.0.0.1" not in addrs  # `ip` excluded
        assert "10.0.0.2" in addrs  # private-address kept
        assert "10.0.0.3" in addrs  # database-peers-address kept
    else:
        assert "10.0.0.1" in addrs  # VM keeps `ip`


def test_peer_addresses_includes_ip_on_vm(substrate, harness):
    """VM peer SAN set keeps `ip` (unchanged from pre-migration behavior)."""
    if substrate != "vm":
        return

    _set_unit_db(harness, "ip", "10.0.0.1")
    addrs = harness.charm.state.peer_addresses
    assert "10.0.0.1" in addrs  # `ip` retained on VM (the G2 regression was K8s-only)


def test_charmstate_accepts_plain_charmbase():
    """CharmState must accept any ops.CharmBase, not only AbstractPostgreSQLCharm."""
    import inspect

    import ops
    from single_kernel_postgresql.core.state import CharmState

    ann = inspect.signature(CharmState.__init__).parameters["charm"].annotation
    assert ann is ops.CharmBase
