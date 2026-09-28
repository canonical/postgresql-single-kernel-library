#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for PostgreSQL."""

from ops import ActionEvent
from ops.main import main
from single_kernel_postgresql.charms.k8s_charm import PostgreSQLK8sCharm


class PostgreSQLK8sTestCharm(PostgreSQLK8sCharm):
    """Test charm: dispatches the cluster-scope promote action."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.promote_to_primary_action, self._on_promote_to_primary)

    def _on_promote_to_primary(self, event: ActionEvent) -> None:
        """Dispatch the cluster-scope promotion to the async-replication handler."""
        if event.params.get("scope") == "cluster":
            return self.async_replication.promote_to_primary(event)
        event.fail("Only the cluster scope is supported by the test charm.")


if __name__ == "__main__":
    main(PostgreSQLK8sTestCharm)
