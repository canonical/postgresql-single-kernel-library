# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Logical replication events handler — owns the two ``logical-replication`` relations.

Ported from the PostgreSQL VM and K8s charms' logical replication module. The
substrate-specific primary lookup stays behind the charm's ``primary_endpoint`` bridge,
Patroni re-render stays behind ``update_config``, and status writes go through
``set_unit_status``.
"""

import json
import logging

from ops import (
    ActiveStatus,
    BlockedStatus,
    EventBase,
    LeaderElectedEvent,
    Relation,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
    Secret,
    SecretChangedEvent,
    SecretNotFoundError,
)
from ops.framework import Object

from single_kernel_postgresql.config.literals import (
    LOGICAL_REPLICATION_OFFER_RELATION,
    LOGICAL_REPLICATION_RELATION,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.utils import new_password

logger = logging.getLogger(__name__)

LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS = "Logical replication setup is invalid. Check logs"
SECRET_LABEL = "logical-replication-relation"  # noqa: S105
PUBLISHED_RESOURCES_KEY = "logical-replication-published-resources"
SUBSCRIPTIONS_KEY = "logical-replication-subscriptions"
APPLIED_REQUEST_KEY = "logical-replication-applied-request"
VALIDATION_KEY = "logical-replication-validation"
VALIDATION_STATUS_MESSAGE_KEY = "logical-replication-validation-status-message"


class PostgreSQLLogicalReplication(Object):
    """Defines the logical-replication logic."""

    def __init__(self, charm, state: CharmState):
        super().__init__(charm, key="logical_replication")
        self.charm = charm
        self.state = state

        # The offer relation publishes resources to the subscriber cluster; the
        # subscription relation consumes the publisher's publications.
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_OFFER_RELATION].relation_joined,
            self._on_offer_relation_joined,
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_OFFER_RELATION].relation_changed,
            self._on_offer_relation_changed,
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_OFFER_RELATION].relation_departed,
            self._on_offer_relation_departed,
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_OFFER_RELATION].relation_broken,
            self._on_offer_relation_broken,
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_RELATION].relation_joined, self._on_relation_joined
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_RELATION].relation_changed, self._on_relation_changed
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_RELATION].relation_departed,
            self._on_relation_departed,
        )
        self.charm.framework.observe(
            self.charm.on[LOGICAL_REPLICATION_RELATION].relation_broken, self._on_relation_broken
        )
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)
        # Topology-driven secret refresh: the VM charms expose a custom
        # cluster_topology_change event; leader_elected covers the same refresh on
        # K8s, where the custom event does not exist.
        self.charm.framework.observe(
            self.charm.on.leader_elected, self._on_cluster_topology_change
        )
        if hasattr(self.charm.on, "cluster_topology_change"):
            self.charm.framework.observe(
                self.charm.on.cluster_topology_change, self._on_cluster_topology_change
            )

    # region Relations

    def _on_offer_relation_joined(self, event: RelationJoinedEvent) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} join early exit due to unit not being a leader"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} join due to primary unavailability"
            )
            event.defer()
            return

        secret = self._get_secret(event.relation.id)
        logger.debug(
            f"Sharing logical replication secret to the {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id}"
        )
        secret.grant(event.relation)

        self._save_published_resources_info(str(event.relation.id), secret.id, {})
        event.relation.data[self.model.app]["secret-id"] = secret.id

    def _on_offer_relation_changed(self, event: RelationChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} change early exit due to unit not being a leader"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} change due to primary unavailability"
            )
            event.defer()
            return
        self._process_offer(event.relation)

    def _on_offer_relation_departed(self, event: RelationDepartedEvent) -> None:
        if event.departing_unit == self.charm.unit and self.state.peer_relation is not None:
            logger.debug(
                f"Marking unit as departed for {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} to skip break"
            )
            self.state.peer.update({"departing": "True"})

    def _on_offer_relation_broken(self, event: RelationBrokenEvent) -> None:
        if not self.state.peer_relation or self.state.peer.is_unit_departing:
            logger.debug(
                f"{LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} break early exit due to unit departure"
            )
            return
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} break early exit due to unit not being a leader"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id} break due to primary unavailability"
            )
            event.defer()
            return

        published_resources = json.loads(
            self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
        )
        active_relation_ids = [
            str(relation.id)
            for relation in self.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ())
        ]

        # Deterministic slot cleanup independent of published-resources state: the
        # slot name is derived from the relation id and database, so a slot left
        # behind by a subscriber whose bookkeeping entry was lost (or whose app
        # was removed) is dropped by name — Patroni never auto-removes permanent
        # slots when their config entry disappears.
        candidate_databases = set(
            json.loads(self.state.config.logical_replication_subscription_request or "{}")
        ) | set(
            database
            for relation_resources in published_resources.values()
            for database in relation_resources["publications"]
        )
        for database in candidate_databases:
            self.charm.postgresql.drop_replication_slot(
                self._replication_slot_name(event.relation.id, database), database
            )

        for relation_id, relation_resources in published_resources.copy().items():
            if relation_id in active_relation_ids:
                continue
            logger.info(
                f"Cleaning up published logical replication resources for the redundant {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
            )
            try:
                secret = self.model.get_secret(id=relation_resources["secret-id"])
                self.charm.postgresql.delete_user(secret.peek_content()["username"])
                secret.remove_all_revisions()
            except SecretNotFoundError:
                pass
            for database, publication in relation_resources["publications"].items():
                self.charm.postgresql.drop_publication(database, publication["publication-name"])
                self.charm.postgresql.drop_replication_slot(
                    publication["replication-slot-name"], database
                )
            del published_resources[relation_id]
            self.state.application.data[PUBLISHED_RESOURCES_KEY] = json.dumps(
                published_resources
            )

        self.charm.update_config()

    # endregion

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} join early exit due to unit not being a leader"
            )
            return
        if self.state.application.data.get(VALIDATION_KEY) == "ongoing":
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_RELATION} #{event.relation.id} join due to still ongoing logical replication config validation"
            )
            event.defer()
            return
        if self.state.application.data.get(VALIDATION_KEY) == "error":
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} join early exit due to validation error"
            )
            return
        if not self._validate_subscription_request():
            return
        event.relation.data[self.model.app]["subscription-request"] = (
            self.state.config.logical_replication_subscription_request or ""
        )

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        if not self._relation_changed_checks(event):
            return

        if not self._handle_publisher_errors(event):
            return

        secret_content = self.model.get_secret(
            id=event.relation.data[event.app]["secret-id"]
        ).get_content(refresh=True)
        subscriptions = self._subscriptions_info()
        subscription_request_config = json.loads(
            self.state.config.logical_replication_subscription_request or "{}"
        )
        publications = json.loads(event.relation.data[event.app].get("publications", "{}"))

        # The publisher may create publications for a request that failed our local
        # validation (apply_changed_config pushes the request before validating).
        # Creating a subscription here would bypass the empty-table guard and
        # re-subscribe with copy_data=true, duplicating rows
        # (canonical/postgresql-k8s-operator#982 comment 3019811325). Existing
        # subscriptions keep refreshing; only NEW subscriptions are gated.
        validation_error = (
            self.state.application.data.get(VALIDATION_KEY) == "error"
        )
        for database, publication in publications.items():
            subscription_name = self._subscription_name(event.relation.id, database)
            if database in subscriptions:
                # The REFRESH path must respect the same empty-table guard as
                # the creation path: a table added to the request that the
                # subscription does not already replicate would be copy_data-ed
                # by REFRESH PUBLICATION (copy_data defaults to true) on top of
                # the subscriber's local rows — the #982 duplication. Block the
                # refresh when any newly-requested, non-replicated table is
                # locally non-empty. The truth source is the CURRENT publication
                # membership (pg_publication_tables), not pg_subscription_rel:
                # the latter lags a publication ALTER until the next REFRESH,
                # which would misfire the guard on safe re-widens.
                requested = {
                    tuple(schematable.partition(".")[::2])
                    for schematable in subscription_request_config.get(database, [])
                }
                live_table_set = self.charm.postgresql.subscription_table_set(
                    database, subscription_name
                )
                for database_table in requested - live_table_set:
                    schema, table = database_table
                    if not self.charm.postgresql.table_exists(
                        database, schema, table
                    ) or self.charm.postgresql.is_table_empty(database, schema, table):
                        self._fail_validation(
                            f"table {schema}.{table} in database {database} isn't empty",
                            status_msg=f"table {schema}.{table} isn't empty",
                        )
                        return
                self.charm.postgresql.refresh_subscription(database, subscription_name)
                logger.info(
                    f"Refreshed subscription {subscription_name} in database {database} due to relation change"
                )
                continue
            if validation_error:
                logger.debug(
                    f"Skipping subscription {subscription_name}: the current subscription request failed validation"
                )
                continue
            # Re-validate at creation time: the validations that ran on
            # config-changed predate the publisher's publication, and with both
            # relations established first (canonical/postgresql-k8s-operator#1052
            # exact order) a cycle can form in between. The guards read the
            # CURRENT relation data, so a re-run sees the publications that now
            # exist and blocks the subscribe.
            if not self._validate_subscription_request():
                logger.debug(
                    f"Skipping subscription {subscription_name}: validation failed at creation time"
                )
                continue
            publication_name = publication["publication-name"]
            # Transient publisher races are retried inside create_subscription
            # (fresh connection per attempt); anything else surfaces
            # immediately as PostgreSQLCreateSubscriptionError.
            self.charm.postgresql.create_subscription(
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
            self.charm.postgresql.drop_subscription(database, subscription)
            logger.info(f"Dropped redundant subscription {subscription} from database {database}")
            # The subscriber's DROP SUBSCRIPTION with slot_name=NONE leaves the
            # publisher-side slot orphaned; drop it so the slot count returns to
            # one-per-active-relation (test_pg2_remove asserts this).
            slot_name = self._replication_slot_name(event.relation.id, database)
            self.charm.postgresql.drop_replication_slot(slot_name, database)
            del subscriptions[database]

        self.state.application.data[SUBSCRIPTIONS_KEY] = json.dumps({
            str(event.relation.id): subscriptions
        })
        # Live replication state changed here: a database got a subscription
        # (creation loop above) or lost one (drop loop above). Re-derive the
        # baseline so the empty-table guard stays armed exactly for tables not
        # replicated by a live subscription; this advances the baseline for
        # databases subscribed via the creation gate.
        self._persist_applied_request_baseline()

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        if event.departing_unit == self.charm.unit and self.state.peer_relation is not None:
            self.state.peer.update({"departing": "True"})

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        if not self.state.peer_relation or self.state.peer.is_unit_departing:
            logger.debug(f"{LOGICAL_REPLICATION_RELATION} break skipped due to departing unit")
            return
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} break early exit due to unit not being a leader"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_RELATION} break until primary is available"
            )
            event.defer()
            return

        for database, subscription in self._subscriptions_info().items():
            self.charm.postgresql.drop_subscription(database, subscription)
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

    # endregion

    # region Events

    def _handle_publisher_errors(self, event: RelationChangedEvent) -> bool:
        """Surface publisher errors on the unit status; drop the stale ones.

        Returns:
            False when relation processing must stop, True to continue.
        """
        errors = json.loads(event.relation.data[event.app].get("errors", "[]"))
        if not errors:
            return True

        our_request = json.loads(
            event.relation.data[self.model.app].get("subscription-request", "{}")
        )

        # If we have a subscription-request, re-validate to check if these errors are
        # current; _check_publisher_errors() handles the stale-error detection.
        if our_request and self.charm.unit.is_leader():
            logger.debug(
                f"Publisher reported errors: {errors}. Re-validating to check if errors are current."
            )
            if not self._validate_subscription_request():
                # Validation failed with current errors
                return False
            # Validation passed, errors were stale - continue processing
            logger.debug("Publisher errors were stale, continuing with relation processing")
            return True

        # No subscription-request yet, or not leader - process errors as-is
        for error in errors:
            logger.error(
                f"Got logical replication error from the publisher in {LOGICAL_REPLICATION_RELATION} #{event.relation.id}: {error}"
            )
            # Set specific message for circular replication errors
            if "circular replication" in error.lower():
                self.charm.set_unit_status(BlockedStatus("Circular replication detected"))
            else:
                self.charm.set_unit_status(
                    BlockedStatus(LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS)
                )
        return False

    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                "Logical replication secret change early exit due to unit not being a leader"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug("Deferring logical replication secret change until primary is available")
            event.defer()
            return

        if (
            (relation := self.model.get_relation(LOGICAL_REPLICATION_RELATION))
            and event.secret.label
            and event.secret.label.startswith(SECRET_LABEL)
        ):
            logger.info("Logical replication secret changed, updating subscriptions")
            secret_content = self.model.get_secret(
                id=relation.data[relation.app]["secret-id"], label=SECRET_LABEL
            ).get_content(refresh=True)
            for database, subscription in self._subscriptions_info().items():
                self.charm.postgresql.update_subscription(
                    database,
                    subscription,
                    secret_content["primary"],
                    secret_content["username"],
                    secret_content["password"],
                )

    def _on_cluster_topology_change(self, event: LeaderElectedEvent | EventBase) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                "Logical replication topology change early exit due to unit not being a leader"
            )
            return
        if not self.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ()):
            logger.debug(
                f"Logical replication topology change early exit due to {LOGICAL_REPLICATION_OFFER_RELATION} connections absence"
            )
            return
        if not self.charm.primary_endpoint:
            logger.debug(
                "Deferring logical replication topology change until primary is available"
            )
            event.defer()
            return
        for relation in self.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ()):
            self._get_secret(relation.id)

    # endregion

    def apply_changed_config(self, event: EventBase) -> bool:
        """Validate & apply (relation) logical-replication-subscription-request config parameter."""
        if not self.charm.unit.is_leader():
            return True
        if not self.charm.primary_endpoint:
            logger.debug(
                "Marking logical replication config validation as ongoing and deferring event until primary as available"
            )
            self.state.application.data[VALIDATION_KEY] = "ongoing"
            event.defer()
            return False
        # Clear any previous error state when config changes
        # This prevents retry_validations() from validating stale config
        self.state.application.data[VALIDATION_KEY] = "ongoing"

        # Capture the PREVIOUSLY APPLIED request from the peer data: the empty-table
        # check must fire for tables being NEWLY added to the subscription (their
        # local data is stale or absent), while tables already being replicated
        # keep skipping it (canonical/postgresql-k8s-operator#1052;
        # test_pg2_dynamic_error vs test_pg3_extend_subscription).
        previous_request = json.loads(
            self.state.application.data.get(APPLIED_REQUEST_KEY, "{}")
        )

        # Push the request to the relation BEFORE validating: the publisher's
        # replication-chain checks read this request, and the multi-hop circular
        # detection only works after the round-trip (the chain data lives in the
        # publisher's publications, which don't exist until it sees a request).
        # A local validation failure below leaves the request pushed: the
        # publisher may create publications, but the subscriber's validation gate
        # and the creation-time check in _on_relation_changed keep the empty-table
        # guard intact (canonical/postgresql-operator#1085 exact order).
        if relation := self.model.get_relation(LOGICAL_REPLICATION_RELATION):
            relation.data[self.model.app]["subscription-request"] = (
                self.state.config.logical_replication_subscription_request or "{}"
            )

        if self._validate_subscription_request(previous_request, empty_tables="auto"):
            self._apply_updated_subscription_request()
            # The baseline means "replicated by a LIVE subscription". A
            # newly-added database has no subscription yet (creation is
            # deferred to _on_relation_changed); persisting its tables here
            # would make the creation gate see them as already-subscribed
            # (previous=None re-derives from this peer key), skip the
            # empty-table guard and re-subscribe with copy_data=true over a
            # non-empty table (the config-cycle duplication;
            # canonical/postgresql-k8s-operator#982 comment 3019811325).
            self._persist_applied_request_baseline()
            # Clear any previous blocked status from validation errors
            self.charm.set_unit_status(ActiveStatus())
        return True

    def retry_validations(self) -> None:
        """Run recurrent logical replication validation attempt.

        For subscribers - try to validate & apply subscription request.
        For publishers - try to validate & process all the offer relations.
        """
        if not self.charm.unit.is_leader() or not self.charm.primary_endpoint:
            return
        if self.state.application.data.get(
            VALIDATION_KEY
        ) == "error" and self._validate_subscription_request(
            # Re-validate against the CURRENT config: the blocked request
            # was already pushed (push-before-validate), so every
            # configured table counts as in-flight and the empty-table
            # guard must not re-fire on the retry -- otherwise a mid-flight
            # blocked extend can never unblock once the local blocker is
            # fixed (the refresh copies nothing for already-replicated
            # tables, so no duplication either).
            json.loads(self.state.config.logical_replication_subscription_request or "{}")
        ):
            self._apply_updated_subscription_request()
            # NOTE: no applied-request baseline update here. The retry
            # re-validates against the configured (in-flight) request;
            # persisting it would mark never-replicated tables as already
            # subscribed and silence the creation gate's empty-table guard on
            # the next relation (the remove/re-integrate duplication). The
            # baseline only advances in apply_changed_config, where the
            # previously applied request was captured before the push.
            # Clear any previous blocked status from validation errors
            self.charm.set_unit_status(ActiveStatus())
        for relation in self.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ()):
            if json.loads(relation.data[self.model.app].get("errors", "[]")):
                self._process_offer(relation)

    def _current_publisher_error(self) -> str | None:
        """Return the publisher's first CURRENT error, or None.

        Mirrors _check_publisher_errors() staleness semantics: errors only
        count when the relation request matches the configured request and
        the error is relevant to it. Otherwise the publisher simply hasn't
        reprocessed the new request yet and its errors are stale.
        """
        relation = self.model.get_relation(LOGICAL_REPLICATION_RELATION)
        if not relation:
            return None
        subscription_request = json.loads(
            self.state.config.logical_replication_subscription_request or "{}"
        )
        current_relation_request = json.loads(
            relation.data[self.model.app].get("subscription-request", "{}")
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

    def has_remote_publisher_errors(self) -> bool:
        """Check if the remote publisher has any errors for the current request."""
        return self._current_publisher_error() is not None

    def remote_publisher_error_message(self) -> str | None:
        """Return the remote publisher's first current error verbatim, if any.

        The composition-root status gate surfaces this so the user sees the
        publisher's exact complaint (e.g. "circular replication detected for
        tables public.users in database testdb") instead of a generic one.
        """
        return self._current_publisher_error()

    def _apply_updated_subscription_request(self) -> None:
        if not (relation := self.model.get_relation(LOGICAL_REPLICATION_RELATION)):
            return
        logger.debug(
            "Logical replication config validation is passed, applying config to the active relations"
        )
        subscription_request_config = json.loads(
            self.state.config.logical_replication_subscription_request or "{}"
        )
        subscriptions = self._subscriptions_info()
        relation.data[self.model.app]["subscription-request"] = (
            self.state.config.logical_replication_subscription_request
        )
        for database, subscription in subscriptions.copy().items():
            if database in subscription_request_config:
                continue
            self.charm.postgresql.drop_subscription(database, subscription)
            logger.info(f"Dropped redundant subscription {subscription} from database {database}")
            del subscriptions[database]
        self.state.application.data[SUBSCRIPTIONS_KEY] = json.dumps({
            str(relation.id): subscriptions
        })

    def _persist_applied_request_baseline(self) -> None:
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

        # For circular replication errors, check if they mention any of our tables
        for database, tables in subscription_request.items():
            for table in tables:
                # Check if this specific table is mentioned in the error
                if table in error and database in error:
                    return True

        return False

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
        current_relation_request = json.loads(
            relation.data[self.model.app].get("subscription-request", "{}")
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
                    status_msg="Circular replication detected",
                )
                return True

        # Generic publisher error
        self._fail_validation(f"Publisher errors: {', '.join(relevant_errors)}")
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

        Returns:
            True if validation passes, False otherwise
        """
        try:
            schema, table = schematable.split(".")
        except ValueError:
            return self._fail_validation(f"table format isn't right at {schematable}")

        if not self.charm.postgresql.table_exists(database, schema, table):
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

        # The empty-table check must be skipped only for tables ALREADY being
        # replicated by this subscription; it must fire for tables being NEWLY
        # added (their local data is stale or absent, and copy_data would
        # duplicate it). The comparison baseline is the PREVIOUSLY APPLIED
        # request -- captured before the push in apply_changed_config -- which
        # restores the original #982 semantics
        # (canonical/postgresql-k8s-operator#982 comment 3019811325;
        # test_pg2_dynamic_error vs test_pg3_extend_subscription).
        # The truthful "already replicated" test: the LIVE subscription's actual
        # table set (pg_subscription_rel), per TABLE — the database-level
        # bookkeeping cannot see tables added to an already-subscribed database,
        # which silently passed the guard and re-subscribed with copy_data over
        # non-empty tables (test_pg2_dynamic_error; the #982 comment 3019811325
        # duplication). `previous_request` stays as the secondary signal for
        # bookkeeping-only states (no live subscription yet).
        live_table_set = set()
        for _, subscription in self._subscriptions_info().items():
            live_table_set |= self.charm.postgresql.subscription_table_set(database, subscription)
        already_subscribed = (schema, table) in live_table_set or (
            not live_table_set
            and database in previous_request
            and schematable in previous_request[database]
        )
        if not already_subscribed and not self.charm.postgresql.is_table_empty(
            database, schema, table
        ):
            # "auto" (config-changed validation): the guard only fires for
            # EXTENSIONS of an already-subscribed database -- the local block
            # must not preempt the request round-trip the multi-hop circular
            # detection needs (canonical/postgresql-operator#1085). For NEW
            # databases the guard is enforced at subscription-creation time
            # (_on_relation_changed), which still blocks the copy_data
            # duplication. "enforce" (creation gate, retries, publisher-error
            # re-validation) always guards.
            if empty_tables == "enforce" or database in self._subscriptions_info():
                return self._fail_validation(
                    f"table {schematable} in database {database} isn't empty"
                )

        return True

    def _validate_subscription_request(
        self,
        previous_request: dict[str, list[str]] | None = None,
        empty_tables: str = "enforce",
    ) -> bool:
        try:
            subscription_request_config = json.loads(
                self.state.config.logical_replication_subscription_request or "{}"
            )
        except json.JSONDecodeError as err:
            return self._fail_validation(f"JSON decode error {err}")

        relation = self.model.get_relation(LOGICAL_REPLICATION_RELATION)

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
            if not self.charm.postgresql.database_exists(database):
                return self._fail_validation(f"database {database} doesn't exist")
            for schematable in schematables:
                if not self._validate_table_for_subscription(
                    relation, database, schematable, previous_request, empty_tables
                ):
                    return False

        self.state.application.data[VALIDATION_KEY] = ""
        self.state.application.data[VALIDATION_STATUS_MESSAGE_KEY] = ""
        return True

    def _fail_validation(self, message: str | None = None, status_msg: str | None = None) -> bool:
        if message:
            logger.error(f"Logical replication validation: {message}")
        self.state.application.data[VALIDATION_KEY] = "error"
        blocked_message = status_msg or LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS
        # Persist the exact message: the composition root's status gate re-sets the
        # unit status on update-status and must surface THIS text, not a generic one
        # (canonical/postgresql-k8s-operator#1052 follow-up).
        self.state.application.data[VALIDATION_STATUS_MESSAGE_KEY] = (
            blocked_message
        )
        self.charm.set_unit_status(BlockedStatus(blocked_message))
        return False

    def _relation_changed_checks(self, event: RelationChangedEvent) -> bool:
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} change early exit due to unit not being a leader"
            )
            return False
        if not event.relation.data[event.app].get("secret-id"):
            logger.warning(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} change early exit due to secret absence in remote application bag (unusual behavior)"
            )
            return False
        if not self.charm.primary_endpoint:
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_RELATION} #{event.relation.id} change due to primary unavailability"
            )
            event.defer()
            return False
        return True

    def _subscriptions_info(self) -> dict[str, str]:
        for subscriptions_info in json.loads(
            self.state.application.data.get(SUBSCRIPTIONS_KEY) or "{}"
        ).values():
            return subscriptions_info
        return {}

    # region Offer

    def _process_offer(self, relation: Relation) -> None:
        logger.debug(
            f"Started processing offer for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )

        subscriptions_request = json.loads(
            relation.data[relation.app].get("subscription-request", "{}")
        )
        publications = json.loads(relation.data[self.model.app].get("publications", "{}"))
        secret = self._get_secret(relation.id)
        user = secret.peek_content()["username"]
        errors = []

        for database, publication in publications.copy().items():
            if database in subscriptions_request:
                continue
            logger.info(
                f"Dropping redundant publication {publication['publication-name']} in database {database} from {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
            )
            self.charm.postgresql.drop_publication(database, publication["publication-name"])
            del publications[database]
            logger.info(
                f"Revoking replication privileges on database {database} from user {user} from {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
            )
            self.charm.postgresql.revoke_replication_privileges(
                user, database, publication["tables"]
            )

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
                if not self.charm.postgresql.publication_exists(database, publication_name):
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
                self.charm.postgresql.grant_replication_privileges(
                    user, database, tables, publication_tables
                )
                logger.info(
                    f"Altering publication {publication_name} tables from {','.join(publication_tables)} to {','.join(tables)} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                )
                self.charm.postgresql.alter_publication(database, publication_name, tables)
                publications[database]["tables"] = tables
                publications[database]["replication-chains"] = self._build_replication_chains(
                    database, tables
                )
            self._save_published_resources_info(str(relation.id), secret.id, publications)
            relation.data[self.model.app]["publications"] = json.dumps(publications)

        self._save_published_resources_info(str(relation.id), secret.id, publications)
        relation.data[self.model.app].update({
            "errors": json.dumps(errors),
            "publications": json.dumps(publications),
        })
        self.charm.update_config()

        logger.debug(
            f"Successfully processed offer for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )

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
        if self.charm.postgresql.publication_exists(database, publication_name):
            error = f"conflicting publication {publication_name} in database {database}"
            logger.error(
                f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {error}"
            )
            return error
        logger.info(
            f"Granting replication privileges on database {database} for user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )
        self.charm.postgresql.grant_replication_privileges(user, database, tables)
        logger.info(
            f"Creating new publication {publication_name} for tables {', '.join(tables)} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
        )
        self.charm.postgresql.create_publication(database, publication_name, tables)
        slot_name = self._replication_slot_name(relation.id, database)
        # The subscriber creates its subscription with create_slot=false, so the
        # slot must exist on this publisher before it connects. Patroni only
        # creates the slots: block entries at startup, which the charm cannot
        # rely on mid-flow -- create it here (canonical/postgresql-operator#1085).
        self.charm.postgresql.create_replication_slot(slot_name, database, plugin="pgoutput")
        publications[database] = {
            "publication-name": publication_name,
            "replication-slot-name": slot_name,
            "tables": tables,
            "replication-chains": self._build_replication_chains(database, tables),
        }
        return None

    def _validate_new_publication(
        self,
        database: str,
        schematables: list[str],
        publication_schematables: list[str] | None = None,
    ) -> str | None:
        if not self.charm.postgresql.database_exists(database):
            return f"database {database} doesn't exist"
        for schematable in schematables:
            if publication_schematables is not None and schematable in publication_schematables:
                continue
            schema, table = schematable.split(".")
            if not self.charm.postgresql.table_exists(database, schema, table):
                return f"table {schematable} in database {database} doesn't exist"
        return None

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
        if self.model.app.name in chain:
            logger.warning(
                f"Circular replication detected: table {table} in database {database} "
                f"has replication chain {chain} which includes this app ({self.model.app.name})"
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
        offer_relation = self.model.get_relation(LOGICAL_REPLICATION_OFFER_RELATION)

        if not offer_relation:
            # No offer relation, so we're not publishing anything
            return False

        # Check if the offer relation is to the same app we want to subscribe from
        if offer_relation.app.name != relation.app.name:
            return False

        # We have an offer relation to the same app! Check if we're publishing this table
        publications = json.loads(offer_relation.data[self.model.app].get("publications", "{}"))

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
        subscription_relation = self.model.get_relation(LOGICAL_REPLICATION_RELATION)

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

            if database not in subscriptions:
                # We're subscribed to the same app but not this database, so no circular replication
                return circular_tables

            if database not in publications:
                # Subscription exists but publications not yet set up
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

        Args:
            database: The database name
            tables: List of tables being published

        Returns:
            Dictionary mapping table names to their replication chains
        """
        chains: dict[str, list[str]] = {}

        # Get our subscription relation (limit: 1, so only one relation possible)
        subscription_relation = self.model.get_relation(LOGICAL_REPLICATION_RELATION)

        if not subscription_relation:
            # No subscription, we're the origin for all tables
            for table in tables:
                chains[table] = [self.model.app.name]
            return chains

        # Get the remote publications we're subscribed to
        remote_publications = json.loads(
            subscription_relation.data[subscription_relation.app].get("publications", "{}")
        )

        if database not in remote_publications:
            # Not subscribed to this database, we're the origin
            for table in tables:
                chains[table] = [self.model.app.name]
            return chains

        # Get the replication chains from our subscription
        remote_chains = remote_publications[database].get("replication-chains", {})

        for table in tables:
            if table in remote_chains:
                # Extend the chain - we're republishing data we subscribed to
                chains[table] = remote_chains[table] + [self.model.app.name]
            else:
                # We're the origin for this table
                chains[table] = [self.model.app.name]

        return chains

    # endregion

    # region Helpers

    def _publication_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def _replication_slot_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def _subscription_name(self, relation_id: int, database: str) -> str:
        return f"relation_{relation_id}_{database}"

    def _save_published_resources_info(
        self,
        relation_id: str,
        secret_id: str,
        publications: dict[str, dict[str, str | list[str]]],
    ) -> None:
        published_resources = json.loads(
            self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
        )
        published_resources[relation_id] = {
            "secret-id": secret_id,
            "publications": publications,
        }
        self.state.application.data[PUBLISHED_RESOURCES_KEY] = json.dumps(
            published_resources
        )

    def replication_slots(self) -> dict[str, str]:
        """Get list of all managed replication slots.

        Returns: dictionary in <slot>: <database> format.
        """
        raw = self.state.application.data.get(PUBLISHED_RESOURCES_KEY, "{}")
        slots = {
            publication["replication-slot-name"]: database
            for resources in json.loads(raw).values()
            for database, publication in resources["publications"].items()
        }
        return slots

    def _create_user(self, relation_id: int) -> tuple[str, str]:
        user = f"logical_replication_relation_{relation_id}"
        password = new_password()
        logger.info(
            f"Creating new user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
        )
        self.charm.postgresql.create_user(user, password, replication=True)
        # The real charm renders per-relation-user pg_hba rules from
        # relations_user_databases_map (an un-ported TODO here); grant the internal
        # access group so the subscriber's replication worker matches the
        # `host all +internal_access` rule on the publisher.
        self.charm.postgresql.grant_internal_access_group_membership(user)
        return user, password

    def _get_secret(self, relation_id: int) -> Secret:
        """Returns logical replication secret. Updates, if content changed."""
        secret_label = f"{SECRET_LABEL}-{relation_id}"
        primary = self.charm.primary_endpoint
        try:
            # Avoid recreating the secret.
            secret = self.charm.model.get_secret(label=secret_label)
            if not secret.id:
                # Workaround for the secret id not being set with model uuid.
                secret._id = f"secret://{self.model.uuid}/{secret.get_info().id.split(':')[1]}"
            if (content := secret.peek_content())["primary"] != primary:
                logger.debug(
                    f"Updating secret for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
                )
                content["primary"] = primary
                secret.set_content(content)
            return secret
        except SecretNotFoundError:
            logger.debug(
                f"Creating new secret for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
            )
        username, password = self._create_user(relation_id)
        return self.charm.model.app.add_secret(
            content={
                "primary": primary,
                "username": username,
                "password": password,
            },
            label=secret_label,
        )

    # endregion
