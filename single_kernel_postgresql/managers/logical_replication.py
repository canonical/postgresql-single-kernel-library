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
from collections.abc import Callable
from typing import cast

from ops import BlockedStatus, Relation, Secret, SecretNotFoundError, StatusBase
from tenacity import Retrying, stop_after_delay, wait_fixed

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
            self.state.application.data.get("logical-replication-published-resources", "{}")
        )
        published_resources[relation_id] = {
            "secret-id": secret_id,
            "publications": publications,
        }
        self.state.application.data["logical-replication-published-resources"] = json.dumps(
            published_resources
        )

    def replication_slots(self) -> dict[str, str]:
        """Get list of all managed replication slots.

        Returns: dictionary in <slot>: <database> format.
        """
        return {
            publication["replication-slot-name"]: database
            for resources in json.loads(
                self.state.application.data.get("logical-replication-published-resources", "{}")
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
        return user, password

    def clean_up_published_resources(self, relation_id: int) -> None:
        """Drop the publications, users, secrets and slots of broken offer relations."""
        published_resources = json.loads(
            self.state.application.data.get("logical-replication-published-resources", "{}")
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
            self.state.application.data["logical-replication-published-resources"] = json.dumps(
                published_resources
            )

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
                # Fresh mutual setups race: our bookkeeping or the remote's
                # publications can lag while both sides configure each other. Fall back
                # to our own outgoing request: if we already asked for the same tables,
                # accepting their mirror request would close the direct cycle.
                outgoing_request = json.loads(
                    subscription_relation.data[self.state.model.app].get(
                        "subscription-request", "{}"
                    )
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

        # Multi-hop circular detection: if we're subscribed to any table in this
        # database, check whether the requester's app is in its replication chain.
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

    def validate_subscription_request(self) -> bool:
        """Validate the logical-replication-subscription-request config parameter."""
        try:
            subscription_request_config = json.loads(
                self.state.config.logical_replication_subscription_request or "{}"
            )
        except json.JSONDecodeError as err:
            return self._fail_validation(f"JSON decode error {err}")

        relation = self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)
        subscription_request_relation = (
            json.loads(relation.data[self.state.model.app].get("subscription-request", "{}"))
            if relation
            else {}
        )

        for database, schematables in subscription_request_config.items():
            if not self.postgresql().database_exists(database):
                return self._fail_validation(f"database {database} doesn't exist")
            for schematable in schematables:
                try:
                    schema, table = schematable.split(".")
                except ValueError:
                    return self._fail_validation(f"table format isn't right at {schematable}")
                if not self.postgresql().table_exists(database, schema, table):
                    return self._fail_validation(
                        f"table {schematable} in database {database} doesn't exist"
                    )
                already_subscribed = (
                    database in subscription_request_relation
                    and schematable in subscription_request_relation[database]
                )
                if not already_subscribed and not self.postgresql().is_table_empty(
                    database, schema, table
                ):
                    return self._fail_validation(
                        f"table {schematable} in database {database} isn't empty"
                    )

        self.state.application.data["logical-replication-validation"] = ""
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
        self.state.application.data["logical-replication-subscriptions"] = json.dumps({
            str(relation.id): subscriptions
        })

    def retry_validations(self) -> None:
        """Run recurrent logical replication validation attempt.

        For subscribers - try to validate & apply subscription request.
        For publishers - try to validate & process all the offer relations.
        """
        if (
            self.state.application.data.get("logical-replication-validation") == "error"
            and self.validate_subscription_request()
        ):
            self.apply_updated_subscription_request()
        for relation in self.state.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ()):
            if json.loads(relation.data[self.state.model.app].get("errors", "[]")):
                self.process_offer(relation)

    def has_remote_publisher_errors(self) -> bool:
        """Check if remote publisher in logical-replication relation has any errors."""
        return bool(
            relation := self.state.model.get_relation(LOGICAL_REPLICATION_RELATION)
        ) and json.loads(relation.data[relation.app].get("errors", "[]"))

    def reconcile_subscriptions(self, relation: Relation) -> None:
        """Reconcile the local subscriptions with the publisher's publications."""
        for error in json.loads(relation.data[relation.app].get("errors", "[]")):
            logger.error(
                f"Got logical replication error from the publisher in {LOGICAL_REPLICATION_RELATION} #{relation.id}: {error}"
            )
            self.set_unit_status(BlockedStatus(LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS))

        secret_content = self.state.model.get_secret(
            id=relation.data[relation.app]["secret-id"]
        ).get_content(refresh=True)
        subscriptions = self._subscriptions_info()
        publications = json.loads(relation.data[relation.app].get("publications", "{}"))

        for database, publication in publications.items():
            subscription_name = self._subscription_name(relation.id, database)
            if database in subscriptions:
                self.postgresql().refresh_subscription(database, subscription_name)
                logger.info(
                    f"Refreshed subscription {subscription_name} in database {database} due to relation change"
                )
            else:
                publication_name = publication["publication-name"]
                for attempt in Retrying(
                    stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True
                ):
                    with attempt:
                        self.postgresql().create_subscription(
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
            self.postgresql().drop_subscription(database, subscription)
            logger.info(f"Dropped redundant subscription {subscription} from database {database}")
            del subscriptions[database]

        self.state.application.data["logical-replication-subscriptions"] = json.dumps({
            str(relation.id): subscriptions
        })

    def drop_subscriptions(self) -> None:
        """Drop every local subscription; the subscription relation is gone."""
        for database, subscription in self._subscriptions_info().items():
            self.postgresql().drop_subscription(database, subscription)
            logger.info(
                f"Dropped subscription {subscription} from database {database} due to relation break"
            )
        self.state.application.data["logical-replication-subscriptions"] = ""

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
        subscription request is even sent to the publisher.

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

    def _fail_validation(self, message: str | None = None) -> bool:
        if message:
            logger.error(f"Logical replication validation: {message}")
        self.state.application.data["logical-replication-validation"] = "error"
        self.set_unit_status(BlockedStatus(LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS))
        return False

    def _subscriptions_info(self) -> dict[str, str]:
        for subscriptions_info in json.loads(
            self.state.application.data.get("logical-replication-subscriptions", "{}")
        ).values():
            return subscriptions_info
        return {}

    # endregion
