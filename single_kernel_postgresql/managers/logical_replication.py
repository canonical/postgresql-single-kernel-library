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

from ops import Relation, Secret, SecretNotFoundError

from single_kernel_postgresql.config.literals import (
    LOGICAL_REPLICATION_OFFER_RELATION,
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


class LogicalReplicationManager(BaseManager):
    """Defines the logical-replication management logic."""

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
        postgresql: PostgreSQLClientFunction,
        primary_endpoint: PrimaryEndpointFunction,
        update_config: UpdateConfigFunction,
    ):
        """Constructor.

        Args:
            state: the charm state.
            workload: the substrate workload.
            postgresql: bridge returning a freshly constructed PostgreSQL client.
            primary_endpoint: bridge returning the current primary endpoint, or None.
            update_config: the charm's config re-render bridge.
        """
        super().__init__(state, workload, "logical_replication")
        self.postgresql = postgresql
        self.primary_endpoint = primary_endpoint
        self.update_config = update_config

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

    def clean_up_published_resources(self) -> None:
        """Drop the publications, users and secrets of relations that no longer exist."""
        published_resources = json.loads(
            self.state.application.data.get("logical-replication-published-resources", "{}")
        )
        active_relation_ids = [
            str(relation.id)
            for relation in self.state.model.relations.get(LOGICAL_REPLICATION_OFFER_RELATION, ())
        ]

        for relation_id, relation_resources in published_resources.copy().items():
            if relation_id in active_relation_ids:
                continue
            logger.info(
                f"Cleaning up published logical replication resources for the redundant {LOGICAL_REPLICATION_OFFER_RELATION} #{relation_id}"
            )
            try:
                secret = self.state.model.get_secret(id=relation_resources["secret-id"])
                self.postgresql().delete_user(secret.peek_content()["username"])
                secret.remove_all_revisions()
            except SecretNotFoundError:
                pass
            for database, publication in relation_resources["publications"].items():
                self.postgresql().drop_publication(database, publication["publication-name"])
            del published_resources[relation_id]
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
            if database not in publications:
                if validation_error := self._validate_new_publication(database, tables):
                    errors.append(validation_error)
                    logger.error(
                        f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {validation_error}"
                    )
                    continue
                publication_name = self._publication_name(relation.id, database)
                if self.postgresql().publication_exists(database, publication_name):
                    error = f"conflicting publication {publication_name} in database {database}"
                    errors.append(error)
                    logger.error(
                        f"Cannot create new publication for {LOGICAL_REPLICATION_OFFER_RELATION} #{relation.id}: {error}"
                    )
                    continue
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

    # endregion
