#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL VM Charm."""

import logging

from ops import StatusBase

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
    # acquire_lock, snap metrics/ldap restarts, the refresh-aware status write, the
    # Patroni-derived primary lookup and the config re-render), so they are minimal here.
    def get_resource_provider(self) -> VMWorkload:
        """Return the substrate's (cpu_cores, memory_bytes) introspector."""
        return self.workload

    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""

    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""

    def set_unit_status(self, status: StatusBase) -> None:
        """Set the unit status without overriding a higher-priority refresh status."""
        self.unit.status = status

    def update_config(self) -> bool:
        """Re-render the Patroni configuration and apply it."""
        return self.config_manager.update_config(self.postgresql)

    def set_app_status(self, status: StatusBase) -> None:
        """Set the application status; the production charm gates this on its own state."""
        self.app.status = status

    def set_primary_status_message(self) -> None:
        """Recompute the unit's primary/standby status message."""

    @property
    def primary_endpoint(self) -> str | None:
        """Address of the cluster primary, or None when there is not one."""
        primary = self.patroni_manager.get_primary()
        return self.patroni_manager.get_member_ip(primary) if primary else None
