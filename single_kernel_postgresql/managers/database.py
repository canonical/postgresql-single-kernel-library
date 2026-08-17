#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database Manager.

Responsible for the ``database`` (``postgresql_client``) client relation: the user and
database lifecycle, the username/prefix-database mapping caches, and the user-databases
map the Patroni config renders its pg_hba rules from.
"""

import json
import logging
from collections.abc import Callable
from hashlib import shake_128
from typing import TypedDict

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from ops import StatusBase

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    DATABASE,
    DATABASE_MAPPING_LABEL,
    PLUGIN_OVERRIDES,
    SPI_MODULE,
    USERNAME_MAPPING_LABEL,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
)
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)


class PrefixDatabaseCacheType(TypedDict):
    """Type definition for the prefix database cached mapping."""

    username: str
    prefix: str
    databases: list[str]


class DatabaseManager(BaseManager):
    """PostgreSQL Database Manager.

    This manager is responsible for handling the client relation user and database
    lifecycle. It owns no events; :class:`~single_kernel_postgresql.events.database`
    drives it and maps its outcomes onto defer/status.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        database_provides: DatabaseProvides,
        patroni_manager: PatroniManager,
        tls_manager: TLSManager,
        set_unit_status: Callable[[StatusBase], None],
        relation_name: str = DATABASE,
    ) -> None:
        super().__init__(state, workload, "database_manager")
        # Constructor-injected by events.database.DatabaseEventsHandler, which owns the
        # provider interface and the ops observers.
        self.database_provides = database_provides
        self.patroni_manager = patroni_manager
        self.tls_manager = tls_manager
        # Charm bridge: both charms gate unit-status writes on charm_refresh priority, and
        # charm_refresh is not a migration target, so this one stays.
        self.set_unit_status = set_unit_status
        self.relation_name = relation_name

    # -- Relation user naming

    @property
    def _username_prefix(self) -> str:
        """The prefix of the PostgreSQL role generated for a client relation.

        The two substrates ship different live role names and deployed clusters hold
        roles under whichever their charm created, so the name is substrate-derived
        rather than converged. The SQL layer already matches both
        (``utils.postgresql`` filters on ``relation-%`` OR ``relation_id_%``).
        """
        return "relation_id_" if self.state.substrate == Substrates.K8S else "relation-"

    def relation_username(self, relation_id: int) -> str:
        """Return the default PostgreSQL role name for a client relation."""
        return f"{self._username_prefix}{relation_id}"

    # -- Mapping caches (app peer secrets; labels are upgrade-compatible, do not rename)

    def get_username_mapping(self) -> dict[str, str]:
        """Get a mapping of custom usernames by a relation ID."""
        if username_mapping := self.state.get_secret(APP_SCOPE, USERNAME_MAPPING_LABEL):
            return json.loads(username_mapping)
        return {}

    def update_username_mapping(self, relation_id: int, username: str | None) -> None:
        """Update a mapping of custom usernames in the application peer secret."""
        if username == self.relation_username(relation_id):
            return

        username_mapping = self.get_username_mapping()
        if username and username_mapping.get(str(relation_id)) != username:
            username_mapping[str(relation_id)] = username
        elif not username and str(relation_id) in username_mapping:
            del username_mapping[str(relation_id)]
        else:
            # Cache is up to date
            return
        self.state.set_secret(APP_SCOPE, USERNAME_MAPPING_LABEL, json.dumps(username_mapping))

    def get_databases_prefix_mapping(self) -> dict[str, PrefixDatabaseCacheType]:
        """Get a mapping of prefixed databases by relation ID."""
        if database_mapping := self.state.get_secret(APP_SCOPE, DATABASE_MAPPING_LABEL):
            return json.loads(database_mapping)
        return {}

    def set_databases_prefix_mapping(
        self,
        relation_id: int,
        username: str | None,
        prefix: str | None,
        databases: list[str] | None,
    ) -> None:
        """Set the initial mapping of prefix databases."""
        database_mapping = self.get_databases_prefix_mapping()
        # Empty databases is valid
        if prefix and username and databases is not None:
            database_mapping[str(relation_id)] = {
                "prefix": prefix,
                "username": username,
                "databases": databases,
            }
        elif not prefix and str(relation_id) in database_mapping:
            del database_mapping[str(relation_id)]
        else:
            # Cache is up to date
            return
        self.state.set_secret(APP_SCOPE, DATABASE_MAPPING_LABEL, json.dumps(database_mapping))

    def add_database_to_prefix_mapping(self, database: str) -> list[str]:
        """Add a new database to all fitting prefixes."""
        usernames = []
        dirty = False
        database_mapping = self.get_databases_prefix_mapping()
        for value in database_mapping.values():
            if database.startswith(value["prefix"]):
                if database not in value["databases"]:
                    value["databases"].append(database)
                    value["databases"].sort()
                    dirty = True
                usernames.append(value["username"])
        if dirty:
            self.state.set_secret(APP_SCOPE, DATABASE_MAPPING_LABEL, json.dumps(database_mapping))
        return usernames

    def remove_database_from_prefix_mapping(self, database: str) -> list[str]:
        """Remove a database from all fitting prefixes."""
        usernames = []
        database_mapping = self.get_databases_prefix_mapping()
        for value in database_mapping.values():
            if database in value["databases"]:
                value["databases"].remove(database)
                usernames.append(value["username"])
        if usernames:
            self.state.set_secret(APP_SCOPE, DATABASE_MAPPING_LABEL, json.dumps(database_mapping))
        return usernames

    def set_rel_to_db_mapping(self) -> None:
        """Set mapping between relation and database."""
        if self.state.peer.is_app_leader:
            self.state.application.data["rel_databases"] = json.dumps({
                key: val["database"]
                for key, val in self.database_provides.fetch_relation_data(
                    None, ["database"]
                ).items()
                if val.get("database")
            })

    def get_rel_to_db_mapping(self) -> dict[str, str] | None:
        """Get mapping between relation and database."""
        if self.state.peer.is_app_leader:
            return json.loads(self.state.application.data.get("rel_databases", "{}"))

    # -- User/database map consumed by the Patroni pg_hba render

    def collect_user_relations(self) -> dict[str, str]:
        """Return the user->databases pairs the established client relations imply."""
        user_db_pairs = {}
        custom_username_mapping = self.get_username_mapping()
        prefix_database_mapping = self.get_databases_prefix_mapping()

        for relation in self.state.model.relations[self.relation_name]:
            if database := self.database_provides.fetch_relation_field(relation.id, "database"):
                user = custom_username_mapping.get(
                    str(relation.id), self.relation_username(relation.id)
                )
                database = ",".join(prefix_database_mapping.get(str(relation.id), [database]))
                user_db_pairs[user] = database
        return user_db_pairs

    @property
    def user_hash(self) -> str:
        """Hash of the expected users and databases, used to detect unsynced peers."""
        return shake_128(str(self.collect_user_relations()).encode()).hexdigest(16)

    def are_units_in_sync(self) -> bool:
        """Whether every peer unit has applied the current user hash."""
        expected = self.user_hash
        if not self.state.peer_relation:
            return True
        for key in self.state.peer_relation.data:
            # We skip the leader so we don't have to wait on the defer
            if (
                key != self.state.model.app
                and key != self.state.model.unit
                and self.state.peer_relation.data[key].get("user_hash", "") != expected
            ):
                return False
        return True

    # -- Plugins

    def get_plugins(self) -> list[str]:
        """Return a list of installed plugins."""
        config = self.state.config
        plugins = [
            "_".join(plugin.split("_")[1:-1])
            for plugin in config.plugin_keys()
            if getattr(config, plugin)
        ]
        plugins = [PLUGIN_OVERRIDES.get(plugin, plugin) for plugin in plugins]
        if "spi" in plugins:
            plugins.remove("spi")
            plugins.extend(SPI_MODULE)
        return plugins

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        if not recompute:
            return self.state.statuses.get(scope, self.name).root or [
                GeneralStatuses.ACTIVE_IDLE.value
            ]
        return [GeneralStatuses.ACTIVE_IDLE.value]
