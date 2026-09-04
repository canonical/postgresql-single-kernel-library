# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the logical replication events handler.

Ports the circular-replication coverage from the K8s charm's
``tests/unit/test_logical_replication.py`` (the issue-1085 fix) onto the library's
test charms.
"""

import json
from unittest.mock import Mock, PropertyMock, patch

from single_kernel_postgresql.config.literals import PEER_RELATION

TESTING_DATABASE = "testdb"


def _add_logical_relation(harness, relation_name: str, remote_app: str) -> int:
    """Add a logical replication relation with its remote unit, hooks disabled."""
    with harness.hooks_disabled():
        rel_id = harness.add_relation(relation_name, remote_app)
        harness.add_relation_unit(rel_id, f"{remote_app}/0")
    return rel_id


def _set_peer_data(harness, data: dict[str, str]) -> None:
    """Write to the application peer databag the handler persists its state in."""
    with harness.hooks_disabled():
        peer_rel_id = harness.model.get_relation(PEER_RELATION).id
        harness.update_relation_data(peer_rel_id, harness.charm.app.name, data)


def _patch_config(request: dict):
    """Stub CharmState.config; the harness fixture does not load config.yaml."""
    config = Mock(
        logical_replication_subscription_request=json.dumps(request) if request else None
    )
    return patch(
        "single_kernel_postgresql.core.state.CharmState.config",
        new_callable=PropertyMock,
        return_value=config,
    )


def _patch_postgresql(harness, database_exists=True, table_exists=True, is_table_empty=True):
    """Stub the PostgreSQL client the validation consults."""
    postgresql = Mock()
    postgresql.database_exists.return_value = database_exists
    postgresql.table_exists.return_value = table_exists
    postgresql.is_table_empty.return_value = is_table_empty
    return patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql))


def test_would_create_circular_replication_no_relation(harness):
    """Circular detection returns False when there's no subscription relation."""
    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            None, TESTING_DATABASE, "public.test_table"
        )
        is False
    )


def test_would_create_circular_replication_no_database_published(harness):
    """Circular detection returns False when the database is not published yet."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps({})})

    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is False
    )


def test_would_create_circular_replication_table_not_published(harness):
    """Circular detection returns False when the table is not in the publication."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {"public.other_table": ["remote-app"]},
        }
    }
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is False
    )


def test_would_create_circular_replication_simple_bidirectional(harness):
    """A chain already containing this app makes the subscription circular (A <-> B)."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {"public.test_table": [harness.charm.app.name, "remote-app"]},
        }
    }
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is True
    )


def test_would_create_circular_replication_multihop(harness):
    """A multi-hop chain A -> B -> C -> A is detected as circular."""
    rel_id = _add_logical_relation(harness, "logical-replication", "cluster-c")
    relation = harness.model.get_relation("logical-replication", rel_id)
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {
                "public.test_table": [harness.charm.app.name, "cluster-b", "cluster-c"]
            },
        }
    }
    harness.update_relation_data(rel_id, "cluster-c", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is True
    )


def test_would_create_circular_replication_different_table_ok(harness):
    """Subscribing to a different table than the circular one is allowed."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {"public.table1": [harness.charm.app.name, "remote-app"]},
        }
    }
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.table2"
        )
        is False
    )


def test_check_publisher_circular_replication_no_subscription(harness):
    """The publisher check returns no circular tables without a subscription relation."""
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert (
        harness.charm.logical_replication._check_publisher_circular_replication(
            offer_relation, TESTING_DATABASE, ["public.test_table"]
        )
        == []
    )


def test_check_publisher_circular_replication_different_database(harness):
    """No circular tables when the existing subscription is for another database."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {"publications": json.dumps({"otherdb": {"tables": ["public.test_table"]}})},
    )
    _set_peer_data(
        harness,
        {
            "logical-replication-subscriptions": json.dumps({
                str(rel_id): {"otherdb": "subscription_name"}
            })
        },
    )
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert (
        harness.charm.logical_replication._check_publisher_circular_replication(
            offer_relation, TESTING_DATABASE, ["public.test_table"]
        )
        == []
    )


def test_check_publisher_circular_replication_detects_cycle(harness):
    """The publisher refuses to publish a table it is subscribed to from the requester."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {"tables": ["public.test_table", "public.other_table"]}
            })
        },
    )
    _set_peer_data(
        harness,
        {
            "logical-replication-subscriptions": json.dumps({
                str(rel_id): {TESTING_DATABASE: "subscription_name"}
            })
        },
    )
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert harness.charm.logical_replication._check_publisher_circular_replication(
        offer_relation, TESTING_DATABASE, ["public.test_table", "public.another_table"]
    ) == ["public.test_table"]


def test_build_replication_chains_no_subscription(harness):
    """Without a subscription, this app is the origin for every published table."""
    chains = harness.charm.logical_replication._build_replication_chains(
        TESTING_DATABASE, ["public.table1", "public.table2"]
    )

    assert chains == {
        "public.table1": [harness.charm.app.name],
        "public.table2": [harness.charm.app.name],
    }


def test_build_replication_chains_extends_chain(harness):
    """Publishing republished tables extends the chains of the remote publication."""
    rel_id = _add_logical_relation(harness, "logical-replication", "cluster-b")
    harness.update_relation_data(
        rel_id,
        "cluster-b",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {
                    "replication-chains": {
                        "public.table1": ["cluster-a", "cluster-b"],
                        "public.table2": ["cluster-b"],
                    }
                }
            })
        },
    )

    chains = harness.charm.logical_replication._build_replication_chains(
        TESTING_DATABASE, ["public.table1", "public.table2", "public.table3"]
    )

    assert chains == {
        "public.table1": ["cluster-a", "cluster-b", harness.charm.app.name],
        "public.table2": ["cluster-b", harness.charm.app.name],
        "public.table3": [harness.charm.app.name],
    }


def test_validate_subscription_request_blocks_circular(harness):
    """Validation fails and marks the peer state when the request is circular."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {
                    "replication-chains": {
                        "public.test_table": [harness.charm.app.name, "remote-app"]
                    }
                }
            })
        },
    )
    request = {TESTING_DATABASE: ["public.test_table"]}
    with (
        _patch_config(request),
        _patch_postgresql(harness),
        harness.hooks_disabled(),
    ):
        result = harness.charm.logical_replication._validate_subscription_request()

    assert result is False
    assert harness.charm.state.application.data.get("logical-replication-validation") == "error"


def test_validate_subscription_request_passes_non_circular(harness):
    """Validation passes for a request that does not loop back on this app."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {
                    "replication-chains": {
                        "public.test_table": [harness.charm.app.name, "remote-app"]
                    }
                }
            })
        },
    )
    request = {TESTING_DATABASE: ["public.other_table"]}
    with (
        _patch_config(request),
        _patch_postgresql(harness),
        harness.hooks_disabled(),
    ):
        result = harness.charm.logical_replication._validate_subscription_request()

    assert result is True
    # Juju clears peer relation data on empty-string writes: the validation marker
    # is absent after a successful validation.
    assert not harness.charm.state.application.data.get("logical-replication-validation")


def test_stale_publisher_errors_do_not_block(harness):
    """Publisher errors for a different request are stale and do not surface."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    stale_error = (
        f"circular replication detected for table public.test_table in database {TESTING_DATABASE}"
    )
    harness.update_relation_data(rel_id, "remote-app", {"errors": json.dumps([stale_error])})
    request = {TESTING_DATABASE: ["public.other_table"]}
    with (
        _patch_config(request),
        _patch_postgresql(harness),
        harness.hooks_disabled(),
    ):
        harness.set_leader(True)
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"subscription-request": json.dumps(request)}
        )
        result = harness.charm.logical_replication._handle_publisher_errors(
            Mock(relation=relation, app=relation.app)
        )

    assert result is True
