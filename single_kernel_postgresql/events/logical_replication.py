# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Logical replication events handler — owns the two ``logical-replication`` relations.

Ported from the PostgreSQL VM and K8s charms' logical replication module. The events
handler owns the observers and the event-flow guards; the data plane (publications,
privileges, the offer secret, published-resources bookkeeping, the subscription
reconcile/validation flow) lives in the ``LogicalReplicationManager``. The
substrate-specific primary lookup stays behind the charm's ``primary_endpoint``
bridge, Patroni re-render stays behind ``update_config``, and status writes go
through ``set_unit_status``.
"""

import logging

from ops import (
    EventBase,
    LeaderElectedEvent,
    Object,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
    SecretChangedEvent,
)

from single_kernel_postgresql.config.literals import (
    LOGICAL_REPLICATION_OFFER_RELATION,
    LOGICAL_REPLICATION_RELATION,
    SECRET_LABEL,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.logical_replication import LogicalReplicationManager

logger = logging.getLogger(__name__)


class PostgreSQLLogicalReplication(Object):
    """Defines the logical-replication logic."""

    def __init__(
        self,
        charm,
        state: CharmState,
        manager: LogicalReplicationManager,
    ):
        super().__init__(charm, key="logical_replication")
        self.charm = charm
        self.state = state
        self.manager = manager

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

    # -- Public surface the composition root and the charms consume

    def replication_slots(self) -> dict[str, str]:
        """Get list of all managed replication slots.

        Returns: dictionary in <slot>: <database> format.
        """
        return self.manager.replication_slots()

    def apply_changed_config(self, event: EventBase) -> bool:
        """Validate & apply (relation) logical-replication-subscription-request config parameter."""
        if not self.charm.unit.is_leader():
            return True
        if not self.charm.primary_endpoint:
            logger.debug(
                "Marking logical replication config validation as ongoing and deferring event until primary as available"
            )
            self.state.application.data["logical-replication-validation"] = "ongoing"
            event.defer()
            return False
        if self.manager.validate_subscription_request():
            self.manager.apply_updated_subscription_request()
        return True

    def retry_validations(self) -> None:
        """Run recurrent logical replication validation attempt.

        For subscribers - try to validate & apply subscription request.
        For publishers - try to validate & process all the offer relations.
        """
        if not self.charm.unit.is_leader() or not self.charm.primary_endpoint:
            return
        self.manager.retry_validations()

    def has_remote_publisher_errors(self) -> bool:
        """Check if remote publisher in logical-replication relation has any errors."""
        return self.manager.has_remote_publisher_errors()

    # region Relations — subscription side

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        if not self.charm.unit.is_leader():
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} join early exit due to unit not being a leader"
            )
            return
        if self.state.application.data.get("logical-replication-validation") == "ongoing":
            logger.debug(
                f"Deferring {LOGICAL_REPLICATION_RELATION} #{event.relation.id} join due to still ongoing logical replication config validation"
            )
            event.defer()
            return
        if self.state.application.data.get("logical-replication-validation") == "error":
            logger.debug(
                f"{LOGICAL_REPLICATION_RELATION} #{event.relation.id} join early exit due to validation error"
            )
            return
        if not self.manager.validate_subscription_request():
            return
        event.relation.data[self.model.app]["subscription-request"] = (
            self.state.config.logical_replication_subscription_request or ""
        )

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        if not self._relation_changed_checks(event):
            return
        self.manager.reconcile_subscriptions(event.relation)

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

        self.manager.drop_subscriptions()

    # endregion

    # region Events

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
            self.manager.update_subscriptions_from_secret(relation)

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
            self.manager.get_offer_secret(relation.id)

    # endregion

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

    # region Relations — offer side

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

        secret, secret_id = self.manager.get_offer_secret(event.relation.id)
        logger.debug(
            f"Sharing logical replication secret to the {LOGICAL_REPLICATION_OFFER_RELATION} #{event.relation.id}"
        )
        secret.grant(event.relation)

        self.manager.save_published_resources_info(str(event.relation.id), secret_id, {})
        event.relation.data[self.model.app]["secret-id"] = secret_id

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
        self.manager.process_offer(event.relation)

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

        self.manager.clean_up_published_resources()

    # endregion
