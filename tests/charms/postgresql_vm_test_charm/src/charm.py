#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for PostgreSQL.

Test-only variant of the library VM test charm: adds the composition-root
config-changed wiring that canonical/postgresql-operator#1941 restores in the
real charm, so the exact issue flow (relation joined first, subscription
request configured afterwards) can be exercised end to end.
"""

from ops import EventBase
from ops.main import main
from single_kernel_postgresql.charms.vm_charm import PostgreSQLVMCharm


class PostgreSQLVMTestCharm(PostgreSQLVMCharm):
    """VM test charm with the logical-replication config-changed observer wired."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_lr_config_changed)

    def _on_lr_config_changed(self, event: EventBase) -> None:
        self.logical_replication.apply_changed_config(event)


if __name__ == "__main__":
    main(PostgreSQLVMTestCharm)
