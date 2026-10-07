#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for PostgreSQL.

Test-only variant of the library VM test charm: adds the composition-root
config-changed wiring that canonical/postgresql-operator#1941 restores in the
real charm, so the exact issue flow (relation joined first, subscription
request configured afterwards) can be exercised end to end, and the
cluster-scope promote dispatch for the async-replication suite.
"""

from ops import ActionEvent, EventBase
from ops.main import main
from single_kernel_postgresql.charms.vm_charm import PostgreSQLVMCharm


class PostgreSQLVMTestCharm(PostgreSQLVMCharm):
    """VM test charm with the logical-replication config-changed observer wired."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_lr_config_changed)
        self.framework.observe(self.on.promote_to_primary_action, self._on_promote_to_primary)

    def _on_lr_config_changed(self, event: EventBase) -> None:
        self.logical_replication.apply_changed_config(event)

    def _on_promote_to_primary(self, event: ActionEvent) -> None:
        """Dispatch the cluster-scope promotion to the async-replication handler."""
        if event.params.get("scope") == "cluster":
            return self.async_replication.promote_to_primary(event)
        event.fail("Only the cluster scope is supported by the test charm.")


if __name__ == "__main__":
    main(PostgreSQLVMTestCharm)
