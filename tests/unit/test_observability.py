# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""The library-owned observability surface (the obs-1 move).

The substrate charm classes construct the COS requirers from the vendored
charm libs, with the payload data served from the package; the abstract charm
owns the shared pieces (rolling-ops restart manager + seam, TLS transfer,
tracing). The consuming charms carry no wiring at all after the move.
"""

from pathlib import Path

import pytest
from ops_tracing import Tracing
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.events.tls_transfer import TLS_TRANSFER_RELATION, TLSTransfer
from single_kernel_postgresql.lib.charms.grafana_agent.v0.cos_agent import COSAgentProvider
from single_kernel_postgresql.lib.charms.grafana_k8s.v0.grafana_dashboard import (
    GrafanaDashboardProvider,
)
from single_kernel_postgresql.lib.charms.loki_k8s.v1.loki_push_api import LogProxyConsumer
from single_kernel_postgresql.lib.charms.prometheus_k8s.v0.prometheus_scrape import (
    MetricsEndpointProvider,
)
from single_kernel_postgresql.lib.charms.rolling_ops.v0.rollingops import RollingOpsManager
from single_kernel_postgresql.utils.payloads import payload_dir


def test_payload_trees_are_substrate_specific():
    """The k8s and vm payload copies exist in the package and diverge."""
    for substrate in ("k8s", "vm"):
        dashboards = payload_dir(substrate, "grafana_dashboards")
        rules = payload_dir(substrate, "prometheus_alert_rules")
        assert dashboards.is_dir() and rules.is_dir()
        assert len(list(dashboards.glob("*.json"))) == 3
        assert len(list(rules.glob("*.yaml"))) == 4
    k8s_dashboard = payload_dir("k8s", "grafana_dashboards", "postgresql-metrics.json")
    vm_dashboard = payload_dir("vm", "grafana_dashboards", "postgresql-metrics.json")
    assert Path(k8s_dashboard).read_bytes() != Path(vm_dashboard).read_bytes()


def test_abstract_charm_owns_the_shared_surface(harness):
    """Restart manager + seam, TLS transfer and tracing are lib-owned."""
    charm = harness.charm
    assert isinstance(charm.restart_manager, RollingOpsManager)
    assert callable(charm.restart_lock_acquire)
    assert isinstance(charm.tls_transfer, TLSTransfer)
    assert isinstance(charm.tracing, Tracing)


def test_tls_transfer_binds_the_ca_requirer(harness):
    """The TLS-transfer handler keeps the peer relation and the CA requirer."""
    charm = harness.charm
    assert charm.tls_transfer.charm is charm
    assert charm.tls_transfer.peer_relation == PEER_RELATION
    assert charm.tls_transfer.certs_transfer is not None
    assert TLS_TRANSFER_RELATION == "receive-ca-cert"


def test_k8s_charm_owns_the_cos_requirers(harness, substrate):
    """The K8s substrate builds scrape/dashboards/loki from the vendored libs."""
    if substrate != Substrates.K8S:
        pytest.skip("the K8s requirers are K8s-only")
    charm = harness.charm
    assert isinstance(charm.metrics_endpoint, MetricsEndpointProvider)
    assert isinstance(charm.grafana_dashboards, GrafanaDashboardProvider)
    assert isinstance(charm.loki_push, LogProxyConsumer)


def test_vm_charm_owns_the_cos_agent(harness, substrate):
    """The VM substrate builds the COS-agent provider from the vendored lib."""
    if substrate != Substrates.VM:
        pytest.skip("the COS-agent provider is VM-only")
    charm = harness.charm
    assert isinstance(charm._grafana_agent, COSAgentProvider)


def test_request_restart_acquires_the_lib_owned_lock(harness, monkeypatch):
    """request_restart routes through the lib's lock seam."""
    charm = harness.charm
    acquired = []
    monkeypatch.setattr(charm, "restart_lock_acquire", lambda: acquired.append(True))
    charm.request_restart()
    assert acquired == [True]
