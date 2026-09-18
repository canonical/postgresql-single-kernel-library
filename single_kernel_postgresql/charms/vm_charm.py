#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL VM Charm."""

import logging
from typing import TYPE_CHECKING

from ops import ActiveStatus, StatusBase

from single_kernel_postgresql.charms.abstract_charm import AbstractPostgreSQLCharm, PostgreSQL
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import SYSTEM_USERS, USER
from single_kernel_postgresql.workload.vm import VMWorkload

if TYPE_CHECKING:
    import charm_refresh

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
            # Test-charm-only bridge: mirrors the real charm's construction — the
            # unit-test hardcoded credentials ("localhost"/"test-password") cannot
            # authenticate against a real cluster. The primary endpoint comes from
            # Patroni and the operator password from the app secret, exactly as in
            # the real VM charm.
            primary_host=self.primary_endpoint,
            # The snap's own runtime tmp dir (not a tempfile): bandit's S108
            # shared-temp-dir rule doesn't model snap runtime dirs.
            current_host="/tmp/snap-private-tmp/snap.charmed-postgresql/tmp/",  # noqa: S108
            user=USER,
            password=str(self.state.application.user_password or ""),
            database="postgres",
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
    # acquire_lock, snap metrics/ldap restarts, the async app status, the Patroni-derived
    # primary lookup and the config re-render), so they are minimal here.
    def get_resource_provider(self) -> VMWorkload:
        """Return the substrate's (cpu_cores, memory_bytes) introspector."""
        return self.workload

    def request_restart(self) -> None:
        """Run the substrate pre-restart side effect and acquire the restart lock."""

    def restart_services(self) -> None:
        """Restart the monitoring and LDAP-sync sidecar services."""

    def set_unit_status(
        self,
        status: StatusBase,
        /,
        *,
        refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None,
    ) -> None:
        """Set the unit status without overriding a higher-priority refresh status."""
        self.refresh_manager.set_unit_status(status, refresh=refresh)

    def set_default_unit_status(self) -> None:
        """Set the unit status that applies when no refresh status is active."""
        self.unit.status = ActiveStatus()

    def post_refresh_side_effects(self) -> None:
        """Set up the exporters, pgBackRest service, and watcher unit address."""

    def has_async_replication_relation(self) -> bool:
        """Whether this unit is related to an async replication partner."""
        return False

    def update_config(
        self, *, refresh: "charm_refresh.Machines | charm_refresh.Kubernetes | None" = None
    ) -> bool:
        """Re-render the Patroni configuration and apply it."""
        if refresh is None:
            refresh = self.refresh_manager.refresh
        return self.config_manager.update_config(self.postgresql, refresh=refresh)
        # The real charm collects the per-user hba map in its composition root
        # (charm.py relations_user_databases_map) and passes it on every render;
        # the rel-handler wiring is an un-ported TODO in the library.
        return self.config_manager.update_config(
            self.postgresql,
            relations_user_databases_map=self.relations_user_databases_map(),
        )

    def relations_user_databases_map(self) -> dict[str, str]:
        """Build the user -> accessible-databases map for the pg_hba render.

        Mirrors the real charm's charm.py relations_user_databases_map: non-system
        users get a per-user hba rule for the databases they can access, and the
        internal users fall back to "all" while the access groups are missing.
        """
        postgresql = self.postgresql
        user_database_map: dict[str, str] = {}
        skip = {
            "backup",
            "monitoring",
            USER,
            "postgres",
            "replication",
            "rewind",
            "charmed_databases_owner",
        }
        try:
            for user in postgresql.list_users(current_host=True):
                if user in skip:
                    continue
                if databases := ",".join(
                    sorted(postgresql.list_accessible_databases_for_user(user, current_host=True))
                ):
                    user_database_map[user] = databases
            if postgresql.list_access_groups(current_host=True) != {
                "identity_access",
                "internal_access",
                "relation_access",
            }:
                user_database_map.update({USER: "all", "replication": "all", "rewind": "all"})
        except Exception as e:
            logger.debug(f"Failed to build the relations user databases map: {e}")
            user_database_map.update({USER: "all", "replication": "all", "rewind": "all"})
        return user_database_map

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

    def get_async_primary_cluster_endpoint(self) -> str | None:
        """Endpoint of the primary cluster of the async replication partner, if any."""
        return None

    def update_relation_endpoints(self) -> None:
        """Refresh the client and async relation endpoints after a switchover."""

    def update_pebble_layers(self) -> None:
        """Reconcile the workload's Pebble layers (K8s only)."""

    def ensure_pgdata_dirs_and_symlinks(self) -> None:
        """Create the storage directories and symlinks for the PostgreSQL data paths (K8s only)."""
