# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""The K8s charm constructor carries no COS/TLS-transfer parameters.

The consuming charm builds those objects itself and assigns them as
attributes; the lib-side parameters had zero call sites.
"""

import inspect

from single_kernel_postgresql.charms.k8s_charm import PostgreSQLK8sCharm

DEAD_PARAMS = {"tls_transfer", "metrics_endpoint", "grafana_dashboards", "loki_push"}


def test_k8s_charm_ctor_has_no_dead_cos_params():
    params = set(inspect.signature(PostgreSQLK8sCharm.__init__).parameters)
    assert params & DEAD_PARAMS == set()
