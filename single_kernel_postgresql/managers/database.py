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
from collections.abc import Callable, Mapping
from functools import cached_property
from hashlib import shake_128
from typing import TypedDict

from ops import ActiveStatus, BlockedStatus, ModelError, Relation, StatusBase, Unit

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    DATABASE,
    DATABASE_MAPPING_LABEL,
    DATABASE_PORT,
    PLUGIN_OVERRIDES,
    SPI_MODULE,
    SYSTEM_USERS,
    USERNAME_MAPPING_LABEL,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
)
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.managers.patroni import PatroniManager
from single_kernel_postgresql.managers.tls import TLSManager
from single_kernel_postgresql.utils import label2name, new_password
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_RELATION,
    ACCESS_GROUPS,
    INVALID_DATABASE_NAME_BLOCKING_MESSAGE,
    INVALID_DATABASE_NAMES,
    INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE,
    PostgreSQLBaseError,
    PostgreSQLCreateDatabaseError,
    PostgreSQLCreateUserError,
    PostgreSQLDeleteUserError,
    PostgreSQLGetPostgreSQLVersionError,
)
from single_kernel_postgresql.utils.postgresql import PostgreSQL as PostgreSQLClient
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)

# Label not a secret
NO_ACCESS_TO_SECRET_MSG = "Missing grant to requested entity secret"  # noqa: S105
FORBIDDEN_USER_MSG = "Requesting an existing username"
PREFIX_TOO_SHORT_MSG = "Prefix too short"

# The shortest prefixed database request that is accepted: three characters plus the "*".
MINIMUM_PREFIX_REQUEST_LENGTH = 4


class PrefixDatabaseCacheType(TypedDict):
    """Type definition for the prefix database cached mapping."""

    username: str
    prefix: str
    databases: list[str]


class DatabaseRequest(TypedDict):
    """The parts of a database-requested event the manager acts on."""

    relation_id: int
    database: str
    extra_user_roles: str | None
    prefix_matching: str | None
    requested_entity_secret_content: Mapping[str, str | None] | None


class DatabaseManager(BaseManager):
    """PostgreSQL Database Manager.

    This manager is responsible for handling the client relation user and database
    lifecycle. It owns no events; :class:`~single_kernel_postgresql.events.database.DatabaseEventsHandler`
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
        # Injected only while the charm_refresh priority gate is still charm-side; once
        # the refresh logic migrates into the library the manager owns its status writes.
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

    @cached_property
    def user_hash(self) -> str:
        """Hash of the expected users and databases, used to detect unsynced peers.

        Frozen at first read for the manager's lifetime, as both charms cached it per
        hook: the request flow's second update_config stores the same hash as the first.
        """
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

    # -- Request handling

    @staticmethod
    def sanitize_extra_roles(extra_roles: str | None) -> list[str]:
        """Standardize and sanitize user extra-roles."""
        if extra_roles is None:
            return []

        # Make sure the access-groups are not in the list
        extra_roles_list = [role.lower() for role in extra_roles.split(",")]
        return [role for role in extra_roles_list if role not in ACCESS_GROUPS]

    def build_extra_user_roles(self, extra_roles: str | None) -> list[str]:
        """Sanitize the requested roles and add the relation access-group."""
        return [*self.sanitize_extra_roles(extra_roles), ACCESS_GROUP_RELATION]

    def get_credentials(
        self, postgresql_client: PostgreSQLClient, request: DatabaseRequest
    ) -> tuple[str, str] | None:
        """Resolve the user and password for a request, or block and return None."""
        try:
            if requested_entities := request["requested_entity_secret_content"]:
                for key, val in requested_entities.items():
                    if key in SYSTEM_USERS or key in postgresql_client.list_users():
                        self.set_unit_status(BlockedStatus(FORBIDDEN_USER_MSG))
                        return None
                    return key, val or new_password()
        except ModelError:
            self.set_unit_status(BlockedStatus(NO_ACCESS_TO_SECRET_MSG))
            return None
        return self.relation_username(request["relation_id"]), new_password()

    def collect_databases(
        self, postgresql_client: PostgreSQLClient, user: str, request: DatabaseRequest
    ) -> tuple[str, list[str]] | None:
        """Resolve the requested database(s), or block and return None."""
        database = request["database"] or ""
        if database and database[-1] == "*":
            if len(database) < MINIMUM_PREFIX_REQUEST_LENGTH:
                self.set_unit_status(BlockedStatus(PREFIX_TOO_SHORT_MSG))
                return None
            prefix_matching = request["prefix_matching"]
            if prefix_matching and prefix_matching != "all":
                logger.warning("Only all prefix matching is supported")
            databases = sorted(postgresql_client.list_databases(database[:-1]))
            self.set_databases_prefix_mapping(
                request["relation_id"], user, database[:-1], databases
            )
        else:
            databases = [database]
            # Add to cached field to be able to generate hba rules
            self.add_database_to_prefix_mapping(database)
        return database, databases

    @property
    def request_errors(self) -> tuple[type[PostgreSQLBaseError], ...]:
        """The PostgreSQL errors a failed request blocks on, per substrate.

        VM blocks on the whole hierarchy; K8s only on the three it named. Kept apart
        because widening K8s would newly block requests that today raise through.
        """
        if self.state.substrate == Substrates.K8S:
            return (
                PostgreSQLCreateDatabaseError,
                PostgreSQLCreateUserError,
                PostgreSQLGetPostgreSQLVersionError,
            )
        return (PostgreSQLBaseError,)

    def create_relation_user_and_database(
        self,
        postgresql_client: PostgreSQLClient,
        user: str,
        password: str,
        database: str,
        databases: list[str],
        extra_user_roles: list[str],
    ) -> None:
        """Create the user and database backing one client relation."""
        plugins = self.get_plugins()

        if database[-1] != "*":
            postgresql_client.create_database(database, plugins=plugins)
            postgresql_client.create_user(
                user, password, extra_user_roles=extra_user_roles, database=database
            )
            # Get the prefixed users again, to add db level grants
            for prefixed_user in self.add_database_to_prefix_mapping(database):
                postgresql_client.add_user_to_databases(prefixed_user, databases, extra_user_roles)
        else:
            postgresql_client.create_user(user, password, extra_user_roles=extra_user_roles)
            postgresql_client.add_user_to_databases(user, databases, extra_user_roles)

    def delete_relation_user(self, postgresql_client: PostgreSQLClient, relation_id: int) -> None:
        """Delete the relation user and drop it from every mapping cache."""
        user = self.get_username_mapping().get(
            str(relation_id), self.relation_username(relation_id)
        )
        try:
            postgresql_client.delete_user(user)
        except PostgreSQLDeleteUserError as e:
            logger.exception(e)
            self.set_unit_status(
                BlockedStatus(
                    f"Failed to delete user during {self.relation_name} relation broken event"
                )
            )

        self.update_username_mapping(relation_id, None)
        self.set_databases_prefix_mapping(relation_id, None, None, None)
        if (
            (dbs := self.get_rel_to_db_mapping())
            and (database := dbs.get(str(relation_id)))
            and database[-1] != "*"
        ):
            for prefixed_user in self.remove_database_from_prefix_mapping(database):
                postgresql_client.remove_user_from_databases(prefixed_user, [database])

    def oversee_users(self, postgresql_client: PostgreSQLClient) -> None:
        """Remove users from database if their relations were broken."""
        if not self.state.peer.is_app_leader:
            return

        delete_user = "suppress-oversee-users" not in self.state.application.data

        # Retrieve database users.
        try:
            database_users = {
                user
                for user in postgresql_client.list_users()
                if user.startswith(self._username_prefix)
            }
        except PostgreSQLBaseError as e:
            logger.error("Early-exit, failed to oversee users: %r", e)
            return

        # Retrieve the users from the active relations.
        relation_users = {
            self.relation_username(relation.id)
            for relation in self.state.model.relations[self.relation_name]
        }

        # Delete that users that exist in the database but not in the active relations.
        for user in database_users - relation_users:
            if delete_user:
                try:
                    logger.info("Remove relation user: %s", user)
                    self.state.set_secret(APP_SCOPE, user, None)
                    self.state.set_secret(APP_SCOPE, f"{user}-database", None)
                    postgresql_client.delete_user(user)
                except PostgreSQLDeleteUserError:
                    logger.error("Failed to delete user %s", user)
            else:
                logger.info("Stale relation user detected: %s", user)

    # -- Endpoint publishing

    def _unit_ip(self, unit: Unit) -> str | None:
        """The client-facing address a peer unit published for this relation."""
        if not self.state.peer_relation:
            return None
        try:
            return self.state.peer_relation.data[unit].get(f"{self.relation_name}-address")
        except KeyError:
            return None

    def _vm_endpoints(self) -> tuple[str, str, str]:
        """(rw_endpoint, ro_endpoints, ro_hosts) from the online Patroni members."""
        online_members = [
            member
            for member in self.patroni_manager.online_cluster_members()
            if not member.get("tags", {}).get("nosync", False)
        ]

        primary_unit_ip, rw_endpoint, ro_hosts, ro_endpoints = "", "", "", ""
        for member in online_members:
            unit = self.state.model.get_unit(label2name(member["name"]))
            if member["role"] == "leader":
                primary_unit_ip = self._unit_ip(unit) or ""
                rw_endpoint = f"{primary_unit_ip}:{DATABASE_PORT}"
            else:
                replica_ip = self._unit_ip(unit)
                if not replica_ip:
                    continue
                if ro_hosts:
                    ro_hosts = f"{ro_hosts},{replica_ip}"
                    ro_endpoints = f"{ro_endpoints},{replica_ip}:{DATABASE_PORT}"
                else:
                    ro_hosts = replica_ip
                    ro_endpoints = f"{replica_ip}:{DATABASE_PORT}"
        if not ro_hosts and primary_unit_ip:
            # If there are no replicas, fallback to primary
            ro_endpoints = rw_endpoint
            ro_hosts = primary_unit_ip
        return rw_endpoint, ro_endpoints, ro_hosts

    def _k8s_endpoints(self) -> tuple[str, str, str]:
        """(rw_endpoint, ro_endpoints, ro_hosts) from the primary/replicas Services."""
        peer_relation = self.state.peer_relation
        ro_hosts = (
            self.state.replicas_endpoint
            if peer_relation and len(peer_relation.units) > 0
            else self.state.primary_endpoint
        )
        return (
            f"{self.state.primary_endpoint}:{DATABASE_PORT}",
            f"{ro_hosts}:{DATABASE_PORT}",
            ro_hosts,
        )

    def update_endpoints(self, relation_id: int | None = None) -> None:  # noqa: C901
        """Set the read/write and read-only endpoints on the client relations.

        Args:
            relation_id: restrict the update to one relation; all of them when None.
        """
        if not self.state.peer.is_app_leader:
            return

        relations_ids = [relation_id] if relation_id is not None else None
        rel_data = self.database_provides.fetch_relation_data(
            relations_ids, ["external-node-connectivity", "database"]
        )

        # skip if no relation data
        if not rel_data:
            return

        secret_data = (
            self.database_provides.fetch_my_relation_data(relations_ids, ["username", "password"])
            or {}
        )

        if self.state.substrate == Substrates.K8S:
            rw_endpoint, ro_endpoints, ro_hosts = self._k8s_endpoints()
        else:
            rw_endpoint, ro_endpoints, ro_hosts = self._vm_endpoints()

        tls = "True" if self.is_tls_enabled else "False"
        ca = None
        if tls == "True":
            _, ca, _ = self.tls_manager.get_client_tls_files()
        if not ca:
            ca = ""

        prefix_database_mapping = self.get_databases_prefix_mapping()

        for current_id in rel_data:
            database = rel_data[current_id].get("database")
            databases = None
            prefix_def = prefix_database_mapping.get(str(current_id))
            if prefix_def is not None:
                databases = prefix_def["databases"]
                self.database_provides.set_prefix_databases(current_id, databases)
                database = databases[0] if len(databases) else database
            user = secret_data.get(current_id, {}).get("username")
            password = secret_data.get(current_id, {}).get("password")
            if not database or not password:
                continue

            self.database_provides.set_endpoints(current_id, rw_endpoint)
            self.database_provides.set_read_only_endpoints(current_id, ro_endpoints)
            self.database_provides.set_tls(current_id, tls)
            self.database_provides.set_tls_ca(current_id, ca)
            if databases is None or len(databases):
                # Set connection string URI.
                self.database_provides.set_uris(
                    current_id,
                    f"postgresql://{user}:{password}@{rw_endpoint}/{database}",
                )
                # Make sure that the URI will be a secret
                if (
                    secret_fields := self.database_provides.fetch_relation_field(
                        current_id, "requested-secrets"
                    )
                ) and "read-only-uris" in secret_fields:
                    self.database_provides.set_read_only_uris(
                        current_id,
                        f"postgresql://{user}:{password}@{ro_hosts}:{DATABASE_PORT}/{database}",
                    )
            else:
                # No database matches prefix, no valid URI
                self.database_provides.delete_relation_data(current_id, ["uris", "read-only-uris"])
            self.set_rel_to_db_mapping()

    @property
    def is_tls_enabled(self) -> bool:
        """Whether the client-facing TLS files have been issued."""
        return all(self.tls_manager.get_client_tls_files())

    # -- Blocking-status validation

    def check_for_invalid_extra_user_roles(
        self, postgresql_client: PostgreSQLClient, relation_id: int
    ) -> bool:
        """Checks if there are relations with invalid extra user roles.

        Args:
            postgresql_client: the PostgreSQL client to list valid privileges with.
            relation_id: current relation to be skipped.
        """
        valid_privileges, valid_roles = postgresql_client.list_valid_privileges_and_roles()
        # VM also accepts "createdb", which is not a privilege or role PostgreSQL reports.
        extra_valid = {"createdb"} if self.state.substrate == Substrates.VM else set()
        for relation in self.state.model.relations.get(self.relation_name, []):
            if relation.id == relation_id:
                continue
            for data in relation.data.values():
                for extra_user_role in self.sanitize_extra_roles(data.get("extra-user-roles")):
                    if (
                        extra_user_role not in valid_privileges
                        and extra_user_role not in valid_roles
                        and extra_user_role not in extra_valid
                    ):
                        return True
        return False

    def check_for_invalid_database_name(self, relation_id: int) -> bool:
        """Checks if there are relations with invalid database names.

        Args:
            relation_id: current relation to be skipped.
        """
        for relation in self.state.model.relations.get(self.relation_name, []):
            if relation.id == relation_id:
                continue
            for data in relation.data.values():
                database = data.get("database")
                if database is not None and (
                    len(database) > 49 or database in INVALID_DATABASE_NAMES
                ):
                    return True
        return False

    def unblock_custom_user_errors(
        self, postgresql_client: PostgreSQLClient, relation: Relation
    ) -> None:
        """Clear a custom-user blocking status once no relation still requests one."""
        if self.check_for_invalid_extra_user_roles(postgresql_client, relation.id):
            self.set_unit_status(BlockedStatus(INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE))
            return
        existing_users = postgresql_client.list_users()
        for other in self.state.model.relations.get(self.relation_name, []):
            try:
                # Relation is not established and custom user was requested
                if not self.database_provides.fetch_my_relation_field(
                    other.id, "secret-user"
                ) and (
                    secret_uri := self.database_provides.fetch_relation_field(
                        other.id, "requested-entity-secret"
                    )
                ):
                    content = self.state.model.get_secret(id=secret_uri).get_content()
                    for key in content:
                        if key in SYSTEM_USERS or key in existing_users:
                            logger.warning(
                                f"Relation {other.id} is still requesting a forbidden user"
                            )
                            self.set_unit_status(BlockedStatus(FORBIDDEN_USER_MSG))
                            return
            except ModelError:
                logger.warning(f"Relation {other.id} still cannot access the set secret")
                self.set_unit_status(BlockedStatus(NO_ACCESS_TO_SECRET_MSG))
                return
        self.set_unit_status(ActiveStatus())

    def update_unit_status(self, postgresql_client: PostgreSQLClient, relation: Relation) -> None:
        """Clean up Blocked status if it's due to extensions request."""
        unit = self.state.model.unit
        if (
            (
                self.state.peer.is_blocked
                and (
                    unit.status.message == INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE
                    or unit.status.message == INVALID_DATABASE_NAME_BLOCKING_MESSAGE
                )
            )
            and not self.check_for_invalid_extra_user_roles(postgresql_client, relation.id)
            and not self.check_for_invalid_database_name(relation.id)
        ):
            self.set_unit_status(ActiveStatus())
        # Matches the handler's failed-init message; the charms' longer substring never
        # matched their K8s wording, leaving the block unclearable there.
        if self.state.peer.is_blocked and "Failed to initialize" in unit.status.message:
            self.set_unit_status(ActiveStatus())
        if self.state.peer.is_blocked and unit.status.message == PREFIX_TOO_SHORT_MSG:
            for other in self.state.model.relations.get(self.relation_name, []):
                # Relation is not established and custom user was requested
                if (
                    (database := self.database_provides.fetch_relation_field(other.id, "database"))
                    and database[-1] == "*"
                    and len(database) < MINIMUM_PREFIX_REQUEST_LENGTH
                ):
                    return
                self.set_unit_status(ActiveStatus())
                return
        if self.state.peer.is_blocked and unit.status.message in [
            INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE,
            NO_ACCESS_TO_SECRET_MSG,
            FORBIDDEN_USER_MSG,
        ]:
            self.unblock_custom_user_errors(postgresql_client, relation)
