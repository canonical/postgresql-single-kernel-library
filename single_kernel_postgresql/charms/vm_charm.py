#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL VM Charm."""

import logging

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import SYSTEM_USERS, USER
from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


class PostgreSQLVMCharm(AbstractPostgreSQLCharm):
    """PostgreSQL VM Charm."""

    def __init__(self, *args):
        """Initialize the PostgreSQL VM Charm."""
        super().__init__(*args)
        self.state.resource_provider = self.workload

    @property
    def postgresql(self) -> PostgreSQL:
        """Return a PostgreSQL client."""
        return PostgreSQL(
            substrate=Substrates.VM,
            primary_host="localhost",
            current_host="localhost",
            user=USER,
            # The password is hardcoded because this is an abstract charm and
            # it meant to be used only in unit tests.
            password="test-password",  # noqa S106
            database="test-database",
            system_users=SYSTEM_USERS,
        )

    @property
    def workload(self) -> VMWorkload:
        """Access current workload instance.

        Returns the workload object.

        Returns:
            VMWorkload: The VMWorkload instance for this charm
        """
        return VMWorkload(charm_dir=self.charm_dir)

    @property
    def substrate(self) -> Substrates:
        """Access current substrate type.

        Returns:
            Substrates: always Substrates.VM for this charm
        """
        return Substrates.VM

    # The concrete production charm owns these bridges (pops postgresql_restarted +
    # acquire_lock, update_endpoints, snap metrics/ldap restarts). This abstract charm
    # is unit-test only, so they are no-ops here.
    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""

    def refresh_endpoints(self) -> None:
        """Refresh the client relation endpoints."""

    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""
