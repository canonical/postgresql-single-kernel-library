#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for PostgreSQL."""

from ops import ActionEvent
from ops.main import main
from single_kernel_postgresql.charms.vm_charm import PostgreSQLVMCharm


class PostgreSQLVMTestCharm(PostgreSQLVMCharm):
    """Test charm: dispatches the promote action like the production charm."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.promote_to_primary_action, self._on_promote_to_primary)

    def _on_promote_to_primary(self, event: ActionEvent) -> None:
        """Dispatch the cluster-scope promotion to the async-replication handler."""
        if event.params.get("scope") == "cluster":
            return self.async_replication.promote_to_primary(event)
        event.fail("Only the cluster scope is supported by the test charm.")


if __name__ == "__main__":
    main(PostgreSQLVMTestCharm)
