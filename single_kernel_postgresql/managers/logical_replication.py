# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Logical replication manager.

Owns the logical-replication data plane: publication and privilege management for
the offer side, the offer secret lifecycle, and the published-resources
bookkeeping. Ported from the PostgreSQL VM and K8s charms' logical replication
module. The substrate-specific primary lookup stays behind the injected
``primary_endpoint`` bridge, Patroni re-render stays behind ``update_config``, and
the charm-side events handler owns the observers and event-flow guards.
"""

import json
import logging
import re
from collections.abc import Callable, Mapping
from typing import Any, cast

from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from ops import ActiveStatus, BlockedStatus, Relation, Secret, SecretNotFoundError, StatusBase

from single_kernel_postgresql.config.literals import (
    LOGICAL_REPLICATION_OFFER_RELATION,
    LOGICAL_REPLICATION_RELATION,
    LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS,
    SECRET_LABEL,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import new_password
from single_kernel_postgresql.utils.postgresql import PostgreSQL
from single_kernel_postgresql.workload.base import BaseWorkload

logger = logging.getLogger(__name__)

CIRCULAR_REPLICATION_STATUS = "Circular replication detected"
PUBLISHED_RESOURCES_KEY = "logical-replication-published-resources"
SUBSCRIPTIONS_KEY = "logical-replication-subscriptions"
APPLIED_REQUEST_KEY = "logical-replication-applied-request"
VALIDATION_KEY = "logical-replication-validation"
VALIDATION_STATUS_MESSAGE_KEY = "logical-replication-validation-status-message"
# Never cleared: the charm's update-status allowlist gate compares the unit's
# (frozen) Blocked message against this field, so the marker must survive the
# healing validation that clears VALIDATION_KEY/VALIDATION_STATUS_MESSAGE_KEY --
# otherwise the stale blocked message matches nothing and the gate early-exits
# forever, keeping the unit blocked despite a healthy, replicating flow.
LAST_BLOCK_MESSAGE_KEY = "logical-replication-last-block-message"

# Publisher error prose for circular rejections, singular or plural:
# "circular replication detected for table public.t1 in database db1"
# "circular replication detected for tables public.t1, public.t2 in database db1"
_CIRCULAR_ERROR_PATTERN = re.compile(
    r"circular replication detected for tables? (?P<tables>.*) in database (?P<database>\S+)"
)


def safe_databag_json(databag: Mapping[str, str], key: str, default: Any) -> Any:
    """Read a JSON databag field, treating unreadable content as the default.

    Foreign or older writers may leave malformed (e.g. empty) values behind;
    readers must behave as if the field were absent instead of crashing the hook.
    """
    try:
        return json.loads(databag.get(key) or default)
    except json.JSONDecodeError:
        return json.loads(default)


# The charm-side hooks the data plane needs; the composition root injects them (the
# manager never touches the charm directly). The PostgreSQL client is constructed
# fresh per access (Patroni primary lookup + app secret), per the events
# handlers' convention.
type PostgreSQLClientFunction = Callable[[], PostgreSQL]
type PrimaryEndpointFunction = Callable[[], str | None]
type UpdateConfigFunction = Callable[..., bool]
type SetUnitStatusFunction = Callable[[StatusBase], None]


class LogicalReplicationManager(BaseManager):
    """Defines the logical-replication management logic."""

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        postgresql: PostgreSQLClientFunction,
        primary_endpoint: PrimaryEndpointFunction,
        update_config: UpdateConfigFunction,
        set_unit_status: SetUnitStatusFunction,
    ):
        """Constructor.

        Args:
            state: the charm state.
            workload: the substrate workload.
            postgresql: bridge returning a freshly constructed PostgreSQL client.
            primary_endpoint: bridge returning the current primary endpoint, or None.
            update_config: the charm's config re-render bridge.
            set_unit_status: the charm's status-write bridge, routed through the
                charm_refresh priority gate.
        """
        super().__init__(state, workload, "logical_replication")
        self.postgresql = postgresql
        self.primary_endpoint = primary_endpoint
        self.update_config = update_config
        self.set_unit_status = set_unit_status

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Surface the logical-replication validation state.

        The module's own BlockedStatus writes are transient (any later status
        write replaces them); this gate re-surfaces the persisted validation
        state on every collect so a blocked setup stays visible until it is
        fixed, and mirrors the publisher's current complaint otherwise.

        Args:
            scope: the status scope (unit or app); only unit statuses apply.
            recompute: unused; the gate reads cheap peer-data state.

        Returns:
            The blocked status when validation failed or the publisher
            reported a current error, otherwise the base statuses.
        """
        if scope != "unit":
            return super().get_statuses(scope, recompute)
        if self.state.application.data.get(VALIDATION_KEY) == "error":
            return [
                StatusObject(
                    status="blocked",
                    message=self.state.application.data.get(VALIDATION_STATUS_MESSAGE_KEY)
                    or LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS,
                )
            ]
        if remote_error := self.remote_publisher_error_message():
            return [StatusObject(status="blocked", message=remote_error)]
        return super().get_statuses(scope, recompute)

    # region Helpers

    def _publication_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def _replication_slot_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def _subscription_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def save_published_resources_info(
        self,
        relation_id: str,
        secret_id: str,
        publications: dict[str, dict[str, str | list[str]]],
    ) -> None:
        """Record the relation's published resources in the application peer databag."""
        published_resources = json.loads(
            self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
        )
        published_resources[relation_id] = {
            "secret-id": secret_id,
            "publications": publications,
        }
        self.state.application.data[PUBLISHED_RESOURCES_KEY] = json.dumps(published_resources)

    def replication_slots(self) -> dict[str, str]:
        """Get list of all managed replication slots.

        Returns: dictionary in <slot>: <database> format.
        """
        return {
            publication["replication-slot-name"]: database
            for resources in json.loads(
                self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
            ).values()
            for database, publication in resources["publications"].items()
        }

    # endregion

    # region Offer

    def get_offer_secret(self, relation_id: int) -> tuple[Secret, str]:
        """Returns the logical replication secret and its id. Updates, if content changed."""
        secret_label = f"{SECRET_LABEL}-{relation_id}"
        # The events handlers defer/exit while the primary endpoint is unavailable;
        # the manager is only invoked with a primary present.
        primary = cast("str", self.primary_endpoint())
        try:
            # Avoid recreating the secret.
            secret = self.state.model.get_secret(label=secret_label)
            if not secret.id:
                # Workaround for the secret id not being set with model uuid.
                secret._id = (
                    f"secret://{self.state.model.uuid}/{secret.get_info().id.split(':')[1]}"
                )
            if (content := secret.peek_content())["primary"] != primary:
                logger.debug(
                    f"Updating secret for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
                )
                content["primary"] = primary
                secret.set_content(content)
        except SecretNotFoundError:
            logger.debug(
                f"Creating new secret for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
            )
            username, password = self._create_user(relation_id)
            secret = self.state.model.app.add_secret(
                content={
                    "primary": primary,
                    "username": username,
                    "password": password,
                },
                label=secret_label,
            )
        # The id resolves on both paths (the workaround above / a fresh add_secret);
        # the callers write it straight into the relation databag, as the references do.
        return secret, cast("str", secret.id)

    def _create_user(self, relation_id: int) -> tuple[str, str]:
        user = f"logical_replication_relation_{relation_id}"
        password = new_password()
        logger.info(
            f"Creating new user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
        )
        self.postgresql().create_user(user, password, replication=True)
        # The real charm renders per-relation-user pg_hba rules from
        # relations_user_databases_map (an un-ported TODO here); grant the internal
        # access group so the subscriber's replication worker matches the
        # `host all +internal_access` rule on the publisher.
        self.postgresql().grant_internal_access_group_membership(user)
        return user, password

    def clean_up_published_resources(self, relation_id: int) -> None:
        """Drop the publications, users, secrets and slots of broken offer relations."""
        published_resources = json.loads(
            self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
        )
        active_relation_ids = [
            str(relation.id)
            for relation in self.state.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ())
        ]

        # Deterministic slot cleanup independent of published-resources state: the
        # slot name is derived from the relation id and database, so a slot left
        # behind by a subscriber whose bookkeeping entry was lost (or whose app
        # was removed) is dropped by name — Patroni never auto-removes permanent
        # slots when their config entry disappears.
        candidate_databases = set(
            json.loads(self.state.config.logical_replication_subscription_request or "{}")
        ) | {
            database
            for relation_resources in published_resources.values()
            for database in relation_resources["publications"]
        }

        # Freshly constructed per access (Patroni primary lookup + app secret); the
        # candidate-database loop drops one slot per database.
        postgresql = self.postgresql()

        for database in candidate_databases:
            postgresql.drop_replication_slot(
                self._replication_slot_name(relation_id, database), database
            )

        for stale_relation_id, relation_resources in published_resources.copy().items():
            if stale_relation_id in active_relation_ids:
                continue
            logger.info(
                f"Cleaning up published logical replication resources for the redundant {LOGICAL_REPLICATION_OFFER_RELATION} #{stale_relation_id}"
            )
            try:
                secret = self.state.model.get_secret(id=relation_resources["secret-id"])
                postgresql.delete_user(secret.peek_content()["username"])
                secret.remove_all_revisions()
            except SecretNotFoundError:
                pass
            for database, publication in relation_resources["publications"].items():
                postgresql.drop_publication(database, publication["publication-name"])
                postgresql.drop_replication_slot(publication["replication-slot-name"], database)
            del published_resources[stale_relation_id]
            self.state.application.data[PUBLISHED_RESOURCES_KEY] = json.dumps(published_resources)

        self.update_config()

    def process_offer(self, relation: Relation) -> None:
        """Reconcile the offered publications with the subscriber's request."""
        logger.debug(
            f"Started processing offer for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )

        subscriptions_request = json.loads(
            relation.data[relation.app].get("subscription-request", "{}")
        )
        publications = json.loads(relation.data[self.state.model.app].get("publications", "{}"))
        secret, secret_id = self.get_offer_secret(relation.id)
        user = secret.peek_content()["username"]
        errors = []

        for database, publication in publications.copy().items():
            if database in subscriptions_request:
                continue
            logger.info(
                f"Dropping redundant publication {publication['publication-name']} in database {database} from {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
            )
            self.postgresql().drop_publication(database, publication["publication-name"])
            del publications[database]
            logger.info(
                f"Revoking replication privileges on database {database} from user {user} from {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
            )
            self.postgresql().revoke_replication_privileges(user, database, publication["tables"])

        for database, tables in subscriptions_request.items():
            # Check for circular replication on publisher side
            circular_tables = self._check_publisher_circular_replication(
                relation, database, tables
            )
            if circular_tables:
                error = (
                    f"circular replication detected for tables {', '.join(circular_tables)} "
                    f"in database {database}"
                )
                errors.append(error)
                logger.error(
                    f"Cannot create/update publication for "
                    f"{LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {error}"
                )
                continue

            if database not in publications:
                publication_error = self._create_new_publication(
                    relation, user, database, tables, publications
                )
                if publication_error:
                    errors.append(publication_error)
                    continue
            elif sorted(publication_tables := publications[database]["tables"]) != sorted(tables):
                publication_name = publications[database]["publication-name"]
                if validation_error := self._validate_new_publication(
                    database, tables, publication_tables
                ):
                    errors.append(validation_error)
                    logger.error(
                        f"Cannot alter publication {publication_name} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {validation_error}"
                    )
                    continue
                if not self.postgresql().publication_exists(database, publication_name):
                    errors.append(
                        f"managed publication {publication_name} in database {database} can't be found"
                    )
                    logger.error(
                        f"Can't find managed publication {publication_name} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                    )
                    continue
                logger.info(
                    f"Altering replication privileges on database {database} for user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                )
                self.postgresql().grant_replication_privileges(
                    user, database, tables, publication_tables
                )
                logger.info(
                    f"Altering publication {publication_name} tables from {','.join(publication_tables)} to {','.join(tables)} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                )
                self.postgresql().alter_publication(database, publication_name, tables)
                publications[database]["tables"] = tables
                publications[database]["replication-chains"] = self._build_replication_chains(
                    database, tables
                )
            self.save_published_resources_info(str(relation.id), secret_id, publications)
            relation.data[self.state.model.app]["publications"] = json.dumps(publications)

        self.save_published_resources_info(str(relation.id), secret_id, publications)
        relation.data[self.state.model.app].update({
            "errors": json.dumps(errors),
            "publications": json.dumps(publications),
        })
        self.update_config()

        logger.debug(
            f"Successfully processed offer for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )

    def _validate_new_publication(
        self,
        database: str,
        schematables: list[str],
        publication_schematables: list[str] | None = None,
    ) -> str | None:
        if not self.postgresql().database_exists(database):
            return f"database {database} doesn't exist"
        for schematable in schematables:
            if publication_schematables is not None and schematable in publication_schematables:
                continue
            schema, table = schematable.split(".")
            if not self.postgresql().table_exists(database, schema, table):
                return f"table {schematable} in database {database} doesn't exist"
        return None

    def _create_new_publication(
        self,
        relation: Relation,
        user: str,
        database: str,
        tables: list[str],
        publications: dict[str, dict],
    ) -> str | None:
        """Create a new publication for the requester; return the error message, if any."""
        if validation_error := self._validate_new_publication(database, tables):
            logger.error(
                f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {validation_error}"
            )
            return validation_error
        publication_name = self._publication_name(relation.id, database)
        if self.postgresql().publication_exists(database, publication_name):
            error = f"conflicting publication {publication_name} in database {database}"
            logger.error(
                f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {error}"
            )
            return error
        logger.info(
            f"Granting replication privileges on database {database} for user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )
        self.postgresql().grant_replication_privileges(user, database, tables)
        logger.info(
            f"Creating new publication {publication_name} for tables {', '.join(tables)} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )
        self.postgresql().create_publication(database, publication_name, tables)
        publications[database] = {
            "publication-name": publication_name,
            "replication-slot-name": self._replication_slot_name(relation.id, database),
            "tables": tables,
            "replication-chains": self._build_replication_chains(database, tables),
        }
        return None

    def _check_publisher_circular_replication(
        self, offer_relation: Relation, database: str, tables: list[str]
    ) -> list[str]:
        """Check if we (publisher) are subscribed to the requester.

        This prevents circular replication where:
        - Direct circular: App A is subscribed to App B for table X, and App B tries to
          subscribe to App A for the same table X
        - Multi-hop circular: App A -> B -> C -> A, where the chain eventually loops back

        The check works by examining:
        1. If we have an active subscription to the same app (direct circular)
        2. If we're subscribed to any table and the requester's app is in its replication
           chain (multi-hop circular)

        Args:
            offer_relation: The offer relation being processed
            database: The database being requested
            tables: List of tables being requested

        Returns:
            List of tables that would create circular replication (empty if none)
        """
        circular_tables = []

        # Get our subscription relation (limit: 1)
        subscription_relation = self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)

        if not subscription_relation:
            # No subscription relation, can't have circular replication
            return circular_tables

        # Get the publications to see what tables we're actually subscribed to
        publications = json.loads(
            subscription_relation.data[subscription_relation.app].get("publications", "{}")
        )

        # Check for direct circular replication (we're subscribed to the same app)
        if subscription_relation.app.name == offer_relation.app.name:
            # We're subscribed to the same app that's trying to subscribe to us!
            # Check if we have active subscriptions to this database
            subscriptions = self._subscriptions_info()

            if database not in subscriptions or database not in publications:
                # Fresh mutual setups race: our own bookkeeping or the remote's
                # publications can lag while both sides configure each other. Fall back
                # to our own outgoing subscription-request: if we already asked this app
                # for any of the same tables, accepting their mirror request would close
                # the direct cycle.
                outgoing_request = safe_databag_json(
                    subscription_relation.data[self.state.model.app],
                    "subscription-request",
                    "{}",
                )
                overlap = set(tables) & set(outgoing_request.get(database, []))
                if overlap:
                    circular_tables.extend(sorted(overlap))
                    logger.warning(
                        f"Direct circular replication detected: we already requested tables "
                        f"{sorted(overlap)} in database {database} from "
                        f"{subscription_relation.app.name}, and they are trying to subscribe "
                        f"to us for the same tables"
                    )
                return circular_tables

            # Check for overlap in tables
            subscribed_tables = publications[database].get("tables", [])
            overlap = set(tables) & set(subscribed_tables)

            if overlap:
                circular_tables.extend(sorted(overlap))
                logger.warning(
                    f"Direct circular replication detected: subscribed to {subscription_relation.app.name} "
                    f"for tables {subscribed_tables}, and they are trying to subscribe to us "
                    f"for tables {tables}. Overlapping tables: {circular_tables}"
                )

            return circular_tables

        # Check for multi-hop circular replication
        # If we're subscribed to any table in this database, check if the requester's
        # app is in the replication chain for that table
        if database not in publications:
            # Not subscribed to this database, can't have multi-hop circular replication
            return circular_tables

        # Get replication chains from our subscription
        replication_chains = publications[database].get("replication-chains", {})

        for table in tables:
            if table not in replication_chains:
                # We're not subscribed to this table, so no circular replication for it
                continue

            # Check if the requester's app is in the replication chain
            chain = replication_chains[table]
            if offer_relation.app.name in chain:
                circular_tables.append(table)
                logger.warning(
                    f"Multi-hop circular replication detected: subscribed to {table} "
                    f"in {database} with chain {chain}, and {offer_relation.app.name} "
                    f"(which is in the chain) is trying to subscribe to us for the same table"
                )

        return sorted(circular_tables)

    def _build_replication_chains(self, database: str, tables: list[str]) -> dict[str, list[str]]:
        """Build replication chains for tables being published.

        This checks if we're subscribed to any of these tables. If so, we extend
        their replication chain. Otherwise, we're the origin.

        Chains are rebuilt only when a publication is created or altered: upstream
        subscription changes do not refresh already-published chains until the
        subscriber alters its request.

        Args:
            database: The database name
            tables: List of tables being published

        Returns:
            Dictionary mapping table names to their replication chains
        """
        chains: dict[str, list[str]] = {}

        # Get our subscription relation (limit: 1, so only one relation possible)
        subscription_relation = self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)

        if not subscription_relation:
            # No subscription, we're the origin for all tables
            for table in tables:
                chains[table] = [self.state.model.app.name]
            return chains

        # Get the remote publications we're subscribed to
        remote_publications = json.loads(
            subscription_relation.data[subscription_relation.app].get("publications", "{}")
        )

        if database not in remote_publications:
            # Not subscribed to this database, we're the origin
            for table in tables:
                chains[table] = [self.state.model.app.name]
            return chains

        # Get the replication chains from our subscription
        remote_chains = remote_publications[database].get("replication-chains", {})

        for table in tables:
            if table in remote_chains:
                # Extend the chain - we're republishing data we subscribed to
                chains[table] = remote_chains[table] + [self.state.model.app.name]
            else:
                # We're the origin for this table
                chains[table] = [self.state.model.app.name]

        return chains

    # endregion

    # region Subscription

    def push_subscription_request(self, relation: Relation) -> None:
        """Push the configured subscription request to the publisher, pre-validated.

        The request is pushed before local validation so the publisher's circular
        guard can reject it; only syntactically valid JSON is ever pushed, so a
        malformed config cannot crash the remote publisher's hook.
        """
        raw_request = self.state.config.logical_replication_subscription_request or "{}"
        try:
            parsed = json.loads(raw_request)
        except json.JSONDecodeError as err:
            self._fail_validation(f"JSON decode error {err}")
            return
        relation.data[self.state.model.app]["subscription-request"] = json.dumps(parsed)

    def validate_subscription_request(
        self,
        previous_request: dict[str, list[str]] | None = None,
        empty_tables: str = "enforce",
    ) -> bool:
        """Validate the logical-replication-subscription-request config parameter."""
        try:
            subscription_request_config = json.loads(
                self.state.config.logical_replication_subscription_request or "{}"
            )
        except json.JSONDecodeError as err:
            return self._fail_validation(f"JSON decode error {err}")

        relation = self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)

        # Check for errors from the publisher first
        if self._check_publisher_errors(relation, subscription_request_config):
            return False

        # The applied baseline lives in the peer data: the relation data holds the
        # just-pushed request (pushes happen before validating so the publisher's
        # chain checks can run), so deriving from it would mark every table as
        # already subscribed and skip the empty-table guard.
        if previous_request is None:
            previous_request = json.loads(
                self.state.application.data.get(APPLIED_REQUEST_KEY, "{}")
            )

        for database, schematables in subscription_request_config.items():
            if not self.postgresql().database_exists(database):
                return self._fail_validation(f"database {database} doesn't exist")
            for schematable in schematables:
                if not self._validate_table_for_subscription(
                    relation, database, schematable, previous_request, empty_tables
                ):
                    return False

        self.state.application.data[VALIDATION_KEY] = ""
        self.state.application.data[VALIDATION_STATUS_MESSAGE_KEY] = ""
        return True

    def _validate_table_for_subscription(
        self,
        relation: Relation | None,
        database: str,
        schematable: str,
        previous_request: dict[str, list[str]],
        empty_tables: str = "enforce",
    ) -> bool:
        """Validate a single table for subscription.

        Args:
            relation: The subscription relation
            database: The database name
            schematable: The table name in schema.table format
            previous_request: The previously applied subscription request
            empty_tables: "enforce" always guards; "auto" only guards extensions

        Returns:
            True if validation passes, False otherwise
        """
        try:
            schema, table = schematable.split(".")
        except ValueError:
            return self._fail_validation(f"table format isn't right at {schematable}")

        if not self.postgresql().table_exists(database, schema, table):
            return self._fail_validation(
                f"table {schematable} in database {database} doesn't exist"
            )

        # Check for circular replication FIRST before checking if table is empty
        # This is important because:
        # 1. If we're already publishing to the remote app, we can't subscribe from them
        # 2. The table might not be empty because of existing data (not from replication)
        if relation and self._check_subscriber_circular_replication(
            relation, database, schematable
        ):
            return self._fail_validation(
                f"circular replication detected for table {schematable} in database {database}",
                status_msg=f"Circular replication detected for table {schematable}",
            )

        # Also check replication chains (for multi-hop scenarios)
        if relation and self._would_create_circular_replication(relation, database, schematable):
            return self._fail_validation(
                f"circular replication detected for table {schematable} in database {database}",
                status_msg=f"Circular replication detected for table {schematable}",
            )

        return not self._enforce_empty_table_guard(
            database, schema, table, schematable, previous_request, empty_tables
        )

    def _enforce_empty_table_guard(
        self,
        database: str,
        schema: str,
        table: str,
        schematable: str,
        previous_request: dict[str, list[str]],
        empty_tables: str,
    ) -> bool:
        """Enforce the empty-table guard for a table about to be replicated.

        The empty-table check must be skipped only for tables ALREADY being
        replicated by this subscription; it must fire for tables being NEWLY
        added (their local data is stale or absent, and copy_data would
        duplicate it). The comparison baseline is the PREVIOUSLY APPLIED
        request -- captured before the push in apply_changed_config -- which
        restores the original #982 semantics
        (canonical/postgresql-k8s-operator#982 comment 3019811325;
        test_pg2_dynamic_error vs test_pg3_extend_subscription).
        The truthful "already replicated" test: the LIVE subscription's actual
        table set (pg_publication_tables, via subscription_table_set), per
        TABLE — the database-level bookkeeping cannot see tables added to an
        already-subscribed database, which silently passed the guard and
        re-subscribed with copy_data over non-empty tables
        (test_pg2_dynamic_error; the #982 comment 3019811325 duplication).
        `previous_request` stays as the secondary signal for bookkeeping-only
        states (no live subscription yet).

        Args:
            database: The database name
            schema: The table schema
            table: The table name
            schematable: The table name in schema.table format
            previous_request: The previously applied subscription request
            empty_tables: "enforce" always guards; "auto" only fires the guard
                for extensions of an already-subscribed database

        Returns:
            True when validation failed, False when the table may be replicated.
        """
        live_table_set = set()
        for _, subscription in self._subscriptions_info().items():
            live_table_set |= self.postgresql().subscription_table_set(database, subscription)
        already_subscribed = (schema, table) in live_table_set or (
            not live_table_set
            and database in previous_request
            and schematable in previous_request[database]
        )
        if already_subscribed:
            return False
        if self.postgresql().is_table_empty(database, schema, table):
            return False
        # "auto" (config-changed validation): the guard only fires for
        # EXTENSIONS of an already-subscribed database -- the local block
        # must not preempt the request round-trip the multi-hop circular
        # detection needs (canonical/postgresql-operator#1085). For NEW
        # databases the guard is enforced at subscription-creation time
        # (_on_relation_changed), which still blocks the copy_data
        # duplication. "enforce" (creation gate, retries, publisher-error
        # re-validation) always guards.
        if empty_tables == "enforce" or database in self._subscriptions_info():
            self._fail_validation(f"table {schematable} in database {database} isn't empty")
            # True = validation failed: _validate_table_for_subscription flips this
            # with `not`, so the blocked table stops the request.
            return True
        return False

    def _guard_subscription_refresh(
        self,
        database: str,
        subscription_name: str,
        subscription_request_config: dict[str, list[str]],
    ) -> bool:
        """Empty-table guard for the subscription REFRESH path.

        A table added to the request that the subscription does not already
        replicate would be copy_data-ed by REFRESH PUBLICATION (copy_data
        defaults to true) on top of the subscriber's local rows — the #982
        duplication. Block the refresh when any newly-requested, non-replicated
        table is locally non-empty; a newly-requested table that does not exist
        locally blocks with a truthful message instead of failing at REFRESH
        time. The truth source is the CURRENT publication membership
        (pg_publication_tables), not pg_subscription_rel: the latter lags a
        publication ALTER until the next REFRESH, which would misfire the guard
        on safe re-widens.

        Args:
            database: The database name
            subscription_name: The subscription being refreshed
            subscription_request_config: The configured subscription request

        Returns:
            True when the refresh may proceed, False when blocked.
        """
        # Freshly constructed per access (Patroni primary lookup + app secret);
        # capture once for the three calls below, per events/database.py.
        postgresql = self.postgresql()
        requested = {
            tuple(schematable.partition(".")[::2])
            for schematable in subscription_request_config.get(database, [])
        }
        live_table_set = postgresql.subscription_table_set(database, subscription_name)
        for database_table in requested - live_table_set:
            schema, table = database_table
            if not postgresql.table_exists(database, schema, table):
                self._fail_validation(
                    f"table {schema}.{table} in database {database} doesn't exist",
                    status_msg=f"table {schema}.{table} doesn't exist",
                )
                return False
            if not postgresql.is_table_empty(database, schema, table):
                self._fail_validation(
                    f"table {schema}.{table} in database {database} isn't empty",
                    status_msg=f"table {schema}.{table} isn't empty",
                )
                return False
        return True

    def persist_applied_request_baseline(self) -> None:
        """Persist logical-replication-applied-request = configured ∩ live.

        The baseline is the "already-subscribed" comparison point for the
        empty-table guard (_validate_table_for_subscription). It may only
        contain databases replicated by a live subscription: a database whose
        subscription does not exist yet is created later by the creation gate,
        which re-derives `previous` from this peer key — a phantom entry would
        silence the guard and duplicate non-empty tables with copy_data=true.
        """
        applied_request = json.loads(
            self.state.config.logical_replication_subscription_request or "{}"
        )
        live_databases = self._subscriptions_info()
        self.state.application.data[APPLIED_REQUEST_KEY] = json.dumps({
            database: tables
            for database, tables in applied_request.items()
            if database in live_databases
        })

    def _is_error_relevant_to_request(
        self, error: str, subscription_request: dict[str, list[str]]
    ) -> bool:
        """Check if a publisher error is relevant to the current subscription request.

        Args:
            error: The error message from the publisher
            subscription_request: The subscription request being validated (database -> tables)

        Returns:
            True if the error is relevant to this request, False otherwise
        """
        # Non-circular errors apply to the whole request
        if "circular replication" not in error.lower():
            return True

        # For circular replication errors, match the reported tables and database as
        # whole tokens: a substring check would let "public.t" match "public.t2" and
        # wrongly fail validation on a stale error about a different table.
        match = _CIRCULAR_ERROR_PATTERN.search(error)
        if not match:
            # Unparsable circular error - treat it as relevant (fail-safe)
            return True
        if match.group("database") not in subscription_request:
            return False
        reported_tables = {table.strip() for table in match.group("tables").split(",")}
        requested_tables = {
            schematable for tables in subscription_request.values() for schematable in tables
        }
        return bool(reported_tables & requested_tables)

    def _configured_subscription_request(self) -> dict[str, list[str]]:
        """Parse the configured subscription request, treating malformed JSON as empty.

        Local readers must not crash hooks on a malformed config; the validation
        path reports malformed JSON via _fail_validation instead.
        """
        try:
            return json.loads(self.state.config.logical_replication_subscription_request or "{}")
        except json.JSONDecodeError:
            return {}

    def _current_publisher_error(self) -> str | None:
        """Return the publisher's first CURRENT error, or None.

        Mirrors _check_publisher_errors() staleness semantics: errors only
        count when the relation request matches the configured request and
        the error is relevant to it. Otherwise the publisher simply hasn't
        reprocessed the new request yet and its errors are stale.
        """
        relation = self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)
        if not relation:
            return None
        subscription_request = self._configured_subscription_request()
        current_relation_request = safe_databag_json(
            relation.data[self.state.model.app], "subscription-request", "{}"
        )
        if current_relation_request != subscription_request:
            return None
        publisher_errors = json.loads(relation.data[relation.app].get("errors", "[]"))
        relevant_errors = [
            error
            for error in publisher_errors
            if self._is_error_relevant_to_request(error, subscription_request)
        ]
        return relevant_errors[0] if relevant_errors else None

    def remote_publisher_error_message(self) -> str | None:
        """Return the remote publisher's first current error verbatim, if any.

        The composition-root status gate surfaces this so the user sees the
        publisher's exact complaint (e.g. "circular replication detected for
        tables public.users in database testdb") instead of a generic one.
        """
        return self._current_publisher_error()

    def _check_publisher_errors(
        self, relation: Relation | None, subscription_request: dict[str, list[str]]
    ) -> bool:
        """Check if the publisher has reported errors for the current subscription request.

        Args:
            relation: The subscription relation
            subscription_request: The subscription request being validated (database -> tables)

        Returns:
            True if validation should fail, False to continue validation
        """
        if not relation:
            return False

        publisher_errors = json.loads(relation.data[relation.app].get("errors", "[]"))
        if not publisher_errors:
            return False

        # Check if we have the same subscription request in relation data
        # If the request has changed, old errors may not be relevant
        current_relation_request = safe_databag_json(
            relation.data[self.state.model.app], "subscription-request", "{}"
        )

        # If requests don't match, publisher errors are stale - ignore them
        # The publisher will re-validate when we update the subscription-request
        if current_relation_request != subscription_request:
            return False

        # Filter to only errors relevant to the tables we're trying to subscribe to
        relevant_errors = [
            error
            for error in publisher_errors
            if self._is_error_relevant_to_request(error, subscription_request)
        ]

        # Only fail if we have relevant errors
        if not relevant_errors:
            return False

        # Check if any relevant error mentions circular replication
        for error in relevant_errors:
            if "circular replication" in error.lower():
                self._fail_validation(
                    f"Publisher rejected subscription: {error}",
                    status_msg=CIRCULAR_REPLICATION_STATUS,
                )
                return True

        # Generic publisher error
        self._fail_validation(f"Publisher errors: {', '.join(relevant_errors)}")
        return True

    def apply_updated_subscription_request(self) -> None:
        """Apply a validated subscription request to the active subscription relation."""
        if not (relation := self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)):
            return
        logger.debug(
            "Logical replication config validation is passed, applying config to the active relations"
        )
        subscription_request_config = json.loads(
            self.state.config.logical_replication_subscription_request or "{}"
        )
        subscriptions = self._subscriptions_info()
        relation.data[self.state.model.app]["subscription-request"] = (
            self.state.config.logical_replication_subscription_request or "{}"
        )
        for database, subscription in subscriptions.copy().items():
            if database in subscription_request_config:
                continue
            self.postgresql().drop_subscription(database, subscription)
            logger.info(f"Dropped redundant subscription {subscription} from database {database}")
            del subscriptions[database]
        self.state.application.data[SUBSCRIPTIONS_KEY] = json.dumps({
            str(relation.id): subscriptions
        })

    def retry_validations(self) -> None:
        """Run recurrent logical replication validation attempt.

        For subscribers - try to validate & apply subscription request.
        For publishers - try to validate & process all the offer relations.
        """
        if self.state.application.data.get(
            VALIDATION_KEY
        ) == "error" and self.validate_subscription_request(
            # Re-validate against the CURRENT config: the blocked request
            # was already pushed (push-before-validate), so every
            # configured table counts as in-flight and the empty-table
            # guard must not re-fire on the retry -- otherwise a mid-flight
            # blocked extend can never unblock once the local blocker is
            # fixed (the refresh copies nothing for already-replicated
            # tables, so no duplication either).
            self._configured_subscription_request()
        ):
            self.apply_updated_subscription_request()
            # NOTE: no applied-request baseline update here. The retry
            # re-validates against the configured (in-flight) request;
            # persisting it would mark never-replicated tables as already
            # subscribed and silence the creation gate's empty-table guard on
            # the next relation (the remove/re-integrate duplication). The
            # baseline only advances in apply_changed_config, where the
            # previously applied request was captured before the push.
            # Clear any previous blocked status from validation errors
            self.set_unit_status(ActiveStatus())
        for relation in self.state.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ()):
            if json.loads(relation.data[self.state.model.app].get("errors", "[]")):
                self.process_offer(relation)

    def has_remote_publisher_errors(self) -> bool:
        """Check if remote publisher in logical-replication relation has any errors."""
        return bool(
            relation := self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)
        ) and json.loads(relation.data[relation.app].get("errors", "[]"))

    def handle_publisher_errors(self, relation: Relation) -> bool:
        """Surface publisher errors on the unit status; drop the stale ones.

        Returns:
            False when relation processing must stop, True to continue.
        """
        errors = json.loads(relation.data[relation.app].get("errors", "[]"))
        if not errors:
            return True

        our_request = safe_databag_json(
            relation.data[self.state.model.app], "subscription-request", "{}"
        )

        # If we have a subscription-request, re-validate to check if these errors are
        # current; _check_publisher_errors() handles the stale-error detection.
        if our_request:
            logger.debug(
                f"Publisher reported errors: {errors}. Re-validating to check if errors are current."
            )
            if not self.validate_subscription_request():
                # Validation failed with current errors
                return False
            # Validation passed, errors were stale - continue processing
            logger.debug("Publisher errors were stale, continuing with relation processing")
            self.set_unit_status(ActiveStatus())
            return True

        # No subscription-request yet - process errors as-is
        for error in errors:
            logger.error(
                f"Got logical replication error from the publisher in {LOGICAL_REPLICATION_RELATION} #{relation.id}: {error}"
            )
            # Set specific message for circular replication errors
            if "circular replication" in error.lower():
                self.set_unit_status(BlockedStatus(CIRCULAR_REPLICATION_STATUS))
            else:
                self.set_unit_status(BlockedStatus(LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS))
        return False

    def reconcile_subscriptions(self, relation: Relation) -> None:
        """Reconcile the local subscriptions with the publisher's publications."""
        secret_content = self.state.model.get_secret(
            id=relation.data[relation.app]["secret-id"]
        ).get_content(refresh=True)
        # Capture the PostgreSQL client once: the bridge is freshly
        # constructed per access (Patroni primary lookup + app secret), and
        # this loop performs several calls per subscribed database — the
        # same per-event capture events/database.py uses.
        postgresql = self.postgresql()
        subscriptions = self._subscriptions_info()
        subscription_request_config = self._configured_subscription_request()
        publications = json.loads(relation.data[relation.app].get("publications", "{}"))

        # The publisher may create publications for a request that failed our local
        # validation (apply_changed_config pushes the request before validating).
        # Creating a subscription here would bypass the empty-table guard and
        # re-subscribe with copy_data=true, duplicating rows
        # (canonical/postgresql-k8s-operator#982 comment 3019811325). Existing
        # subscriptions keep refreshing; only NEW subscriptions are gated.
        # The gate is the LIVE creation-time validation below -- NOT the
        # persisted VALIDATION_KEY flag: the flag is only cleared by the
        # update-status retry, and the publisher may clear its errors and
        # publish in between, so a flag-gated skip deadlocks the resolve path
        # (the relation-changed arrives while the flag is still "error" and
        # nothing ever creates the subscription). The creation-time
        # validation re-checks the publisher's CURRENT errors, the local
        # tables and the empty-table guard, so the #982 protection is
        # unchanged; on success it also clears the stale flag.
        for database, publication in publications.items():
            subscription_name = self._subscription_name(relation.id, database)
            if database in subscriptions:
                # The REFRESH path must respect the same empty-table guard as
                # the creation path; block on any newly-requested, locally
                # non-empty table (the #982 duplication).
                if not self._guard_subscription_refresh(
                    database, subscription_name, subscription_request_config
                ):
                    return
                postgresql.refresh_subscription(database, subscription_name)
                logger.info(
                    f"Refreshed subscription {subscription_name} in database {database} due to relation change"
                )
                continue
            # Re-validate at creation time: the validations that ran on
            # config-changed predate the publisher's publication, and with both
            # relations established first (canonical/postgresql-k8s-operator#1052
            # exact order) a cycle can form in between. The guards read the
            # CURRENT relation data, so a re-run sees the publications that now
            # exist and blocks the subscribe.
            if not self.validate_subscription_request():
                logger.debug(
                    f"Skipping subscription {subscription_name}: validation failed at creation time"
                )
                continue
            publication_name = publication["publication-name"]
            # Transient publisher races are retried inside create_subscription
            # (fresh connection per attempt); anything else surfaces
            # immediately as PostgreSQLCreateSubscriptionError.
            postgresql.create_subscription(
                subscription_name,
                secret_content["primary"],
                database,
                secret_content["username"],
                secret_content["password"],
                publication_name,
                publication["replication-slot-name"],
            )
            logger.info(
                f"Created new subscription {subscription_name} for publication {publication_name} in database {database}"
            )
            subscriptions[database] = subscription_name

        for database, subscription in subscriptions.copy().items():
            if database in publications:
                continue
            postgresql.drop_subscription(database, subscription)
            logger.info(f"Dropped redundant subscription {subscription} from database {database}")
            # The subscriber's DROP SUBSCRIPTION with slot_name=NONE leaves the
            # publisher-side slot orphaned; drop it so the slot count returns to
            # one-per-active-relation (test_pg2_remove asserts this).
            slot_name = self._replication_slot_name(relation.id, database)
            postgresql.drop_replication_slot(slot_name, database)
            del subscriptions[database]

        self.state.application.data[SUBSCRIPTIONS_KEY] = json.dumps({
            str(relation.id): subscriptions
        })
        # Live replication state changed here: a database got a subscription
        # (creation loop above) or lost one (drop loop above). Re-derive the
        # baseline so the empty-table guard stays armed exactly for tables not
        # replicated by a live subscription; this advances the baseline for
        # databases subscribed via the creation gate.
        self.persist_applied_request_baseline()

    def drop_subscriptions(self) -> None:
        """Drop every local subscription; the subscription relation is gone."""
        for database, subscription in self._subscriptions_info().items():
            self.postgresql().drop_subscription(database, subscription)
            logger.info(
                f"Dropped subscription {subscription} from database {database} due to relation break"
            )
        self.state.application.data[SUBSCRIPTIONS_KEY] = "{}"
        # Clear the applied-request baseline too: nothing is being replicated
        # anymore, so the next validation must treat every configured table as
        # newly added -- the empty-table guard then blocks re-subscribing onto
        # a non-empty table (the original remove/re-integrate semantics;
        # canonical/postgresql-k8s-operator#982 comment 3019811325).
        self.state.application.data[APPLIED_REQUEST_KEY] = "{}"

    def update_subscriptions_from_secret(self, relation: Relation) -> None:
        """Rotate the subscription credentials after a publisher secret change."""
        secret_content = self.state.model.get_secret(
            id=relation.data[relation.app]["secret-id"], label=SECRET_LABEL
        ).get_content(refresh=True)
        for database, subscription in self._subscriptions_info().items():
            self.postgresql().update_subscription(
                database,
                subscription,
                secret_content["primary"],
                secret_content["username"],
                secret_content["password"],
            )

    def _would_create_circular_replication(
        self, relation: Relation | None, database: str, table: str
    ) -> bool:
        """Check if subscribing to a table would create circular replication.

        This checks the replication chain in the remote publication to see if our app
        is already in the chain, which would mean the data originated from us.

        Args:
            relation: The logical-replication relation we're subscribing on
            database: The database name
            table: The table name (schema.table format)

        Returns:
            True if subscribing would create a circle, False otherwise
        """
        if not relation:
            return False

        # Get the publications from the remote app
        remote_publications = json.loads(relation.data[relation.app].get("publications", "{}"))

        if database not in remote_publications:
            return False

        # Get the replication chains for this database
        replication_chains = remote_publications[database].get("replication-chains", {})

        if table not in replication_chains:
            return False

        # Check if our app name is in the chain
        chain = replication_chains[table]
        if self.state.model.app.name in chain:
            logger.warning(
                f"Circular replication detected: table {table} in database {database} "
                f"has replication chain {chain} which includes this app ({self.state.model.app.name})"
            )
            return True

        return False

    def _check_subscriber_circular_replication(
        self, relation: Relation, database: str, table: str
    ) -> bool:
        """Check if we're already publishing this table to the remote app.

        This prevents circular replication where:
        - App A is publishing table X to App B (via offer relation)
        - App A tries to subscribe to table X from App B (via subscription relation)

        This check runs on the subscriber side during validation, before the
        subscription is applied; the request itself is pushed to the publisher first
        so the publisher's guard can also reject the mirrored setup.

        Args:
            relation: The subscription relation we're trying to create
            database: The database name
            table: The table name (schema.table format)

        Returns:
            True if we're already publishing this table to the remote app
        """
        # Get our offer relation (limit: 1, so only one relation possible)
        offer_relation = self.state.model.get_relation(LOGICAL_REPLICATION_OFFER_RELATION)

        if not offer_relation:
            # No offer relation, so we're not publishing anything
            return False

        # Check if the offer relation is to the same app we want to subscribe from
        if offer_relation.app.name != relation.app.name:
            return False

        # We have an offer relation to the same app! Check if we're publishing this table
        publications = json.loads(
            offer_relation.data[self.state.model.app].get("publications", "{}")
        )

        if database not in publications:
            return False

        # Check if the table is in our publications
        published_tables = publications[database].get("tables", [])
        if table in published_tables:
            logger.warning(
                f"Circular replication detected: we are publishing {table} in {database} "
                f"to {offer_relation.app.name}, and trying to subscribe to the same table from them"
            )
            return True

        return False

    def _fail_validation(self, message: str | None = None, status_msg: str | None = None) -> bool:
        if message:
            logger.error(f"Logical replication validation: {message}")
        self.state.application.data[VALIDATION_KEY] = "error"
        # Persist the specific reason so the composition-root status gate can
        # re-surface it after any later transient status write: the gate's
        # allowlist compares the unit status message against THIS field (or
        # the generic literal), so it must mirror what the unit actually
        # shows -- the update-status retry is the path that clears
        # VALIDATION_KEY while the unit stays blocked, and an empty/stale
        # message here makes the gate early-exit forever (the resolve
        # deadlock).
        self.state.application.data[VALIDATION_STATUS_MESSAGE_KEY] = status_msg or message or ""
        # Persistent marker for the charm's update-status allowlist gate (see
        # the literal's comment): written at every failure, NEVER cleared.
        self.state.application.data[LAST_BLOCK_MESSAGE_KEY] = status_msg or message or ""
        self.set_unit_status(
            BlockedStatus(status_msg or LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS)
        )
        return False

    def _subscriptions_info(self) -> dict[str, str]:
        for subscriptions_info in json.loads(
            self.state.application.data.get(SUBSCRIPTIONS_KEY) or "{}"
        ).values():
            return subscriptions_info
        return {}

    # endregion
