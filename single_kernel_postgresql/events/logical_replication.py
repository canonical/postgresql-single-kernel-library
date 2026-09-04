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
    Relation,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
    Secret,
    SecretNotFoundError,
)
from ops.framework import Object

from single_kernel_postgresql.config.literals import LOGICAL_REPLICATION_OFFER_RELATION
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.utils import new_password

logger = logging.getLogger(__name__)

LOGICAL_REPLICATION_VALIDATION_ERROR_STATUS = "Logical replication setup is invalid. Check logs"
SECRET_LABEL = "logical-replication-relation"  # noqa: S105


class PostgreSQLLogicalReplication(Object):
    """Defines the logical-replication logic."""

    def __init__(self, charm, state: CharmState):
        super().__init__(charm, key="logical_replication")
        self.charm = charm
        self.state = state

        # The offer relation publishes resources to the subscriber cluster; the
        # subscription relation handlers land with the subscriber-side PR.
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
            self.state.application.data.get("logical-replication-published-resources", "{}")
        )
        active_relation_ids = [
            str(relation.id)
            for relation in self.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ())
        ]

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
            del published_resources[relation_id]
            self.state.application.data["logical-replication-published-resources"] = json.dumps(
                published_resources
            )

        self.charm.update_config()

    # endregion

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
            if database not in publications:
                if validation_error := self._validate_new_publication(database, tables):
                    errors.append(validation_error)
                    logger.error(
                        f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {validation_error}"
                    )
                    continue
                publication_name = self._publication_name(relation.id, database)
                if self.charm.postgresql.publication_exists(database, publication_name):
                    error = f"conflicting publication {publication_name} in database {database}"
                    errors.append(error)
                    logger.error(
                        f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {error}"
                    )
                    continue
                logger.info(
                    f"Granting replication privileges on database {database} for user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                )
                self.charm.postgresql.grant_replication_privileges(user, database, tables)
                logger.info(
                    f"Creating new publication {publication_name} for tables {', '.join(tables)} in database {database} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}"
                )
                self.charm.postgresql.create_publication(database, publication_name, tables)
                publications[database] = {
                    "publication-name": publication_name,
                    "replication-slot-name": self._replication_slot_name(relation.id, database),
                    "tables": tables,
                }
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

    def _create_user(self, relation_id: int) -> tuple[str, str]:
        user = f"logical_replication_relation_{relation_id}"
        password = new_password()
        logger.info(
            f"Creating new user {user} for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
        )
        self.charm.postgresql.create_user(user, password, replication=True)
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
