#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for PostgreSQL."""

from ops.main import main

# First platform-specific import; on wrong architectures fall back to a
# blocked warning charm (the wheel for this arch is unavailable).
try:
    from single_kernel_postgresql.charms.vm_charm import PostgreSQLVMCharm
except ModuleNotFoundError:
    from single_kernel_postgresql.utils.arch import (
        WrongArchitectureWarningCharm,
        is_wrong_architecture,
    )

    if is_wrong_architecture() and __name__ == "__main__":
        main(WrongArchitectureWarningCharm)
    raise


class PostgreSQLVMTestCharm(PostgreSQLVMCharm):
    """Test charm."""


if __name__ == "__main__":
    main(PostgreSQLVMTestCharm)
