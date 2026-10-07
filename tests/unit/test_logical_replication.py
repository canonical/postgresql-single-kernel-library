# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the logical replication events handler.

Ports the circular-replication coverage from the K8s charm's
``tests/unit/test_logical_replication.py`` (the issue-1085 fix) onto the library's
test charms.
"""

import json
from unittest.mock import Mock, PropertyMock, patch

from single_kernel_postgresql.config.literals import PEER_RELATION, SECRET_LABEL
from single_kernel_postgresql.managers.logical_replication import (
    APPLIED_REQUEST_KEY,
    SUBSCRIPTIONS_KEY,
    VALIDATION_KEY,
    VALIDATION_STATUS_MESSAGE_KEY,
)

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
        harness.charm.logical_replication_manager._would_create_circular_replication(
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
        harness.charm.logical_replication_manager._would_create_circular_replication(
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
        harness.charm.logical_replication_manager._would_create_circular_replication(
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
        harness.charm.logical_replication_manager._would_create_circular_replication(
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
        harness.charm.logical_replication_manager._would_create_circular_replication(
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
        harness.charm.logical_replication_manager._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.table2"
        )
        is False
    )


def test_would_create_circular_replication_identity_token(harness):
    """A chain carrying this app's identity token makes the subscription circular."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    self_token = f"{harness.model.uuid}/{harness.charm.app.name}"
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {"public.test_table": [self_token]},
        }
    }
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication_manager._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is True
    )


def test_would_create_circular_replication_same_name_other_model_ok(harness):
    """A same-named app in another model in the chain does NOT make it circular.

    The chain token carries the origin's model UUID; a same-named app here has
    a different token, so the subscription is legitimate (the cross-model
    false positive the name-based check had).
    """
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    other_model_token = f"00000000-0000-0000-0000-000000000001/{harness.charm.app.name}"
    publications = {
        TESTING_DATABASE: {
            "publication-name": "test_pub",
            "replication-chains": {"public.test_table": [other_model_token]},
        }
    }
    harness.update_relation_data(rel_id, "remote-app", {"publications": json.dumps(publications)})

    assert (
        harness.charm.logical_replication_manager._would_create_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is False
    )


def test_check_publisher_circular_replication_no_subscription(harness):
    """The publisher check returns no circular tables without a subscription relation."""
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert (
        harness.charm.logical_replication_manager._check_publisher_circular_replication(
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
        harness.charm.logical_replication_manager._check_publisher_circular_replication(
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

    assert harness.charm.logical_replication_manager._check_publisher_circular_replication(
        offer_relation, TESTING_DATABASE, ["public.test_table", "public.another_table"]
    ) == ["public.test_table"]


def test_check_publisher_circular_replication_identity_cycle(harness):
    """Matching identity stamps on both remotes detect the direct cycle."""
    identity = json.dumps({"model-uuid": "uuid-b", "app-name": "remote-app"})
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {"tables": ["public.test_table", "public.other_table"]}
            }),
            "replication-identity": identity,
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
    harness.update_relation_data(offer_rel_id, "remote-app", {"replication-identity": identity})
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert harness.charm.logical_replication_manager._check_publisher_circular_replication(
        offer_relation, TESTING_DATABASE, ["public.test_table"]
    ) == ["public.test_table"]


def test_check_publisher_circular_replication_same_name_other_models_no_cycle(harness):
    """Same-named apps with different model UUIDs are NOT the same app.

    Regression test for the name-based direct check: two same-named apps in
    different models must not be treated as a direct circular replication.
    """
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {"tables": ["public.test_table", "public.other_table"]}
            }),
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-1",
                "app-name": "remote-app",
            }),
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
    harness.update_relation_data(
        offer_rel_id,
        "remote-app",
        {
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-2",
                "app-name": "remote-app",
            })
        },
    )
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert (
        harness.charm.logical_replication_manager._check_publisher_circular_replication(
            offer_relation, TESTING_DATABASE, ["public.test_table"]
        )
        == []
    )


def test_check_publisher_circular_replication_multihop_identity_chain(harness):
    """The requester's identity token in the inherited chain blocks the offer."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        rel_id,
        "remote-app",
        {
            "publications": json.dumps({
                TESTING_DATABASE: {
                    "replication-chains": {
                        "public.test_table": ["uuid-model-1/remote-app"],
                    }
                }
            }),
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-b",
                "app-name": "remote-app",
            }),
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
    harness.update_relation_data(
        offer_rel_id,
        "remote-app",
        {
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-1",
                "app-name": "remote-app",
            })
        },
    )
    offer_relation = harness.model.get_relation("logical-replication-offer", offer_rel_id)

    assert harness.charm.logical_replication_manager._check_publisher_circular_replication(
        offer_relation, TESTING_DATABASE, ["public.test_table"]
    ) == ["public.test_table"]


def test_build_replication_chains_no_subscription(harness):
    """Without a subscription, this app is the origin for every published table."""
    chains = harness.charm.logical_replication_manager._build_replication_chains(
        TESTING_DATABASE, ["public.table1", "public.table2"]
    )

    self_token = f"{harness.model.uuid}/{harness.charm.app.name}"
    assert chains == {
        "public.table1": [self_token],
        "public.table2": [self_token],
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

    chains = harness.charm.logical_replication_manager._build_replication_chains(
        TESTING_DATABASE, ["public.table1", "public.table2", "public.table3"]
    )

    self_token = f"{harness.model.uuid}/{harness.charm.app.name}"
    assert chains == {
        "public.table1": ["cluster-a", "cluster-b", self_token],
        "public.table2": ["cluster-b", self_token],
        "public.table3": [self_token],
    }


def test_check_subscriber_circular_replication_same_name_other_models_ok(harness):
    """Same-named apps in different models must not count as the same app.

    Regression test for the name-based same-app check on the subscriber side:
    a same-named publisher in another model is a different application, so
    publishing to it must not block subscribing from it.
    """
    sub_rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(
        sub_rel_id,
        "remote-app",
        {
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-1",
                "app-name": "remote-app",
            })
        },
    )
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    harness.update_relation_data(
        offer_rel_id,
        "remote-app",
        {
            "replication-identity": json.dumps({
                "model-uuid": "uuid-model-2",
                "app-name": "remote-app",
            })
        },
    )
    relation = harness.model.get_relation("logical-replication", sub_rel_id)

    assert (
        harness.charm.logical_replication_manager._check_subscriber_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is False
    )


def test_check_subscriber_circular_replication_identity_same_app(harness):
    """Matching identity stamps on both relations keep the mutual-setup block."""
    identity = json.dumps({"model-uuid": "uuid-b", "app-name": "remote-app"})
    sub_rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(sub_rel_id, "remote-app", {"replication-identity": identity})
    offer_rel_id = _add_logical_relation(harness, "logical-replication-offer", "remote-app")
    harness.update_relation_data(offer_rel_id, "remote-app", {"replication-identity": identity})
    harness.update_relation_data(
        offer_rel_id,
        harness.charm.app.name,
        {"publications": json.dumps({TESTING_DATABASE: {"tables": ["public.test_table"]}})},
    )
    relation = harness.model.get_relation("logical-replication", sub_rel_id)

    assert (
        harness.charm.logical_replication_manager._check_subscriber_circular_replication(
            relation, TESTING_DATABASE, "public.test_table"
        )
        is True
    )


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
        result = harness.charm.logical_replication_manager.validate_subscription_request()

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
        result = harness.charm.logical_replication_manager.validate_subscription_request()

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
        result = harness.charm.logical_replication_manager.handle_publisher_errors(relation)

    assert result is True


def test_guard_subscription_refresh_blocks_non_empty_new_table(harness):
    """The refresh guard blocks a newly-requested locally non-empty table (#982)."""
    _set_peer_data(harness, {SUBSCRIPTIONS_KEY: json.dumps({"1": {TESTING_DATABASE: "sub"}})})
    postgresql = Mock()
    postgresql.subscription_table_set.return_value = set()
    postgresql.is_table_empty.return_value = False
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)):
        result = harness.charm.logical_replication_manager._guard_subscription_refresh(
            TESTING_DATABASE, "sub", {TESTING_DATABASE: ["public.fresh"]}
        )

    assert result is False
    assert harness.charm.state.application.data.get(VALIDATION_KEY) == "error"
    assert (
        harness.charm.state.application.data.get(VALIDATION_STATUS_MESSAGE_KEY)
        == "table public.fresh isn't empty"
    )


def test_guard_subscription_refresh_allows_empty_new_table(harness):
    """The refresh guard lets a locally empty newly-requested table through."""
    _set_peer_data(harness, {SUBSCRIPTIONS_KEY: json.dumps({"1": {TESTING_DATABASE: "sub"}})})
    postgresql = Mock()
    postgresql.subscription_table_set.return_value = set()
    postgresql.is_table_empty.return_value = True
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)):
        result = harness.charm.logical_replication_manager._guard_subscription_refresh(
            TESTING_DATABASE, "sub", {TESTING_DATABASE: ["public.fresh"]}
        )

    assert result is True


def test_guard_subscription_refresh_reports_missing_table(harness):
    """The refresh guard blocks a missing table with a truthful message."""
    _set_peer_data(harness, {SUBSCRIPTIONS_KEY: json.dumps({"1": {TESTING_DATABASE: "sub"}})})
    postgresql = Mock()
    postgresql.subscription_table_set.return_value = set()
    postgresql.table_exists.return_value = False
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)):
        result = harness.charm.logical_replication_manager._guard_subscription_refresh(
            TESTING_DATABASE, "sub", {TESTING_DATABASE: ["public.ghost"]}
        )

    assert result is False
    assert (
        harness.charm.state.application.data.get(VALIDATION_STATUS_MESSAGE_KEY)
        == "table public.ghost doesn't exist"
    )


def test_enforce_empty_table_guard_skips_already_replicated(harness):
    """The empty-table guard passes tables the live subscription replicates."""
    _set_peer_data(harness, {SUBSCRIPTIONS_KEY: json.dumps({"1": {TESTING_DATABASE: "sub"}})})
    postgresql = Mock()
    postgresql.subscription_table_set.return_value = {("public", "replicated")}
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)):
        failed = harness.charm.logical_replication_manager._enforce_empty_table_guard(
            TESTING_DATABASE, "public", "replicated", "public.replicated", {}, "enforce"
        )

    assert failed is False


def test_enforce_empty_table_guard_blocks_non_empty_new_table(harness):
    """The empty-table guard fires for a newly-added locally non-empty table."""
    _set_peer_data(harness, {SUBSCRIPTIONS_KEY: json.dumps({"1": {TESTING_DATABASE: "sub"}})})
    postgresql = Mock()
    postgresql.subscription_table_set.return_value = set()
    postgresql.is_table_empty.return_value = False
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)):
        failed = harness.charm.logical_replication_manager._enforce_empty_table_guard(
            TESTING_DATABASE, "public", "fresh", "public.fresh", {}, "enforce"
        )

    assert failed is True


def test_apply_changed_config_pushes_then_persists_baseline(harness):
    """apply_changed_config pushes the request, validates and persists the baseline."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    request = {TESTING_DATABASE: ["public.other_table"]}
    with (
        _patch_config(request),
        _patch_postgresql(harness),
        patch.object(
            type(harness.charm), "primary_endpoint", PropertyMock(return_value="10.0.0.1")
        ),
        harness.hooks_disabled(),
    ):
        harness.set_leader(True)
        result = harness.charm.logical_replication.apply_changed_config(Mock())

    assert result is True
    assert json.loads(relation.data[harness.charm.app]["subscription-request"]) == request
    baseline = json.loads(harness.charm.state.application.data.get(APPLIED_REQUEST_KEY, "{}"))
    # No live subscription yet: nothing counts as replicated by the baseline.
    assert baseline == {}


def test_apply_changed_config_rejects_malformed_json(harness):
    """Malformed subscription-request config fails validation and pushes nothing."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    relation = harness.model.get_relation("logical-replication", rel_id)
    config = Mock(logical_replication_subscription_request="not-json")
    with (
        patch(
            "single_kernel_postgresql.core.state.CharmState.config",
            new_callable=PropertyMock,
            return_value=config,
        ),
        _patch_postgresql(harness),
        patch.object(
            type(harness.charm), "primary_endpoint", PropertyMock(return_value="10.0.0.1")
        ),
        harness.hooks_disabled(),
    ):
        harness.set_leader(True)
        result = harness.charm.logical_replication.apply_changed_config(Mock())

    assert result is True
    assert harness.charm.state.application.data.get(VALIDATION_KEY) == "error"
    assert "subscription-request" not in relation.data[harness.charm.app]


def test_secret_changed_updates_subscriptions(harness):
    """A publisher secret change rotates the credentials of live subscriptions."""
    rel_id = _add_logical_relation(harness, "logical-replication", "remote-app")
    harness.update_relation_data(rel_id, "remote-app", {"secret-id": "secret:1"})
    _set_peer_data(
        harness, {SUBSCRIPTIONS_KEY: json.dumps({str(rel_id): {TESTING_DATABASE: "sub"}})}
    )
    secret_content = {"primary": "10.0.0.1", "username": "u", "password": "p"}
    secret = Mock()
    secret.get_content.return_value = secret_content
    model = Mock()
    model.get_secret.return_value = secret
    model.get_relation.return_value = harness.model.get_relation(PEER_RELATION)
    model.app = harness.charm.app
    postgresql = Mock()
    event = Mock()
    event.secret.label = f"{SECRET_LABEL}-1"
    with (
        patch(
            "single_kernel_postgresql.core.state.CharmState.model",
            PropertyMock(return_value=model),
        ),
        patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)),
        patch.object(
            type(harness.charm), "primary_endpoint", PropertyMock(return_value="10.0.0.1")
        ),
        harness.hooks_disabled(),
    ):
        harness.set_leader(True)
        harness.charm.logical_replication._on_secret_changed(event)

    postgresql.update_subscription.assert_called_once_with(
        TESTING_DATABASE, "sub", "10.0.0.1", "u", "p"
    )


def test_reconcile_creates_subscription_after_publisher_clears_errors(harness):
    """The stale VALIDATION_KEY flag must not deadlock subscription creation.

    The publisher may clear its errors and publish after this unit last
    validated (the relation-changed then arrives while the flag is still
    "error"). reconcile_subscriptions must trust the CURRENT state: the
    creation-time validation re-checks publisher errors, local tables and
    the empty-table guard, so a passing re-validation must proceed to
    create_subscription instead of skipping on the stale flag (the
    blocked-forever resolve deadlock).
    """
    rel_id = _add_logical_relation(harness, "logical-replication", "publisher")
    relation = harness.model.get_relation("logical-replication", rel_id)

    secret_id = harness.add_model_secret(
        owner=harness.charm.app.name,
        content={"primary": "10.0.0.1", "username": "u", "password": "p"},
    )
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            "publisher",
            {
                "errors": "[]",
                "publications": json.dumps({
                    TESTING_DATABASE: {
                        "publication-name": "relation_15_testdb",
                        "replication-slot-name": "slot_testdb",
                    }
                }),
                "secret-id": secret_id,
            },
        )
    _set_peer_data(
        harness,
        {
            VALIDATION_KEY: "error",
            VALIDATION_STATUS_MESSAGE_KEY: "stale error",
            SUBSCRIPTIONS_KEY: "{}",
        },
    )

    postgresql = Mock()
    postgresql.database_exists.return_value = True
    postgresql.table_exists.return_value = True
    postgresql.is_table_empty.return_value = True
    postgresql.subscription_table_set.return_value = set()
    with (
        _patch_config({TESTING_DATABASE: ["public.test_table"]}),
        patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)),
    ):
        harness.charm.logical_replication_manager.reconcile_subscriptions(relation)

    postgresql.create_subscription.assert_called_once()
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    peer_data = harness.get_relation_data(peer_rel_id, harness.charm.app.name)
    assert peer_data.get(VALIDATION_KEY) != "error"


def test_fail_validation_persists_reason_for_the_status_gate(harness):
    """A message-only failure must persist the reason in the peer databag.

    The charm's update-status self-heal gate recognizes a validation block by
    comparing the unit status message against THIS peer field (or the generic
    literal). Persisting "" for message-only failures defeats that allowlist:
    the unit's message gets frozen to a third string (the publisher's raw
    error surfaced by the status gate) that matches neither, and the
    update-status retry that clears VALIDATION_KEY can never run.
    """
    with harness.hooks_disabled():
        harness.set_leader(True)
    manager = harness.charm.logical_replication_manager

    manager._fail_validation(
        "Publisher errors: table public.test_table in database testdb doesn't exist"
    )
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    peer_data = harness.get_relation_data(peer_rel_id, harness.charm.app.name)
    assert peer_data.get(VALIDATION_STATUS_MESSAGE_KEY) == (
        "Publisher errors: table public.test_table in database testdb doesn't exist"
    )

    manager._fail_validation("guard reason", status_msg="short status")
    peer_data = harness.get_relation_data(peer_rel_id, harness.charm.app.name)
    assert peer_data.get(VALIDATION_STATUS_MESSAGE_KEY) == "short status"


def test_fail_validation_persists_marker_that_survives_healing(harness):
    """The last-block marker must persist after a successful validation.

    The charm's update-status gate allowlist compares the unit's (frozen)
    Blocked message against this field: once a validation SUCCEEDS it clears
    the flag and the peer status message, so a marker that only mirrors them
    would re-break the allowlist exactly when the self-heal needs to run
    (update-status sets active on the healed flow). The marker is therefore
    written at every failure and NEVER cleared.
    """
    rel_id = _add_logical_relation(harness, "logical-replication", "publisher")
    relation = harness.model.get_relation("logical-replication", rel_id)
    with harness.hooks_disabled():
        harness.set_leader(True)
    manager = harness.charm.logical_replication_manager

    manager._fail_validation(
        "Publisher errors: table public.test_table in database testdb doesn't exist"
    )
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    peer_data = harness.get_relation_data(peer_rel_id, harness.charm.app.name)
    assert peer_data.get("logical-replication-last-block-message") == (
        "Publisher errors: table public.test_table in database testdb doesn't exist"
    )

    postgresql = Mock()
    postgresql.database_exists.return_value = True
    postgresql.table_exists.return_value = True
    postgresql.is_table_empty.return_value = True
    postgresql.subscription_table_set.return_value = set()
    secret_id = harness.add_model_secret(
        owner=harness.charm.app.name,
        content={"primary": "10.0.0.1", "username": "u", "password": "p"},
    )
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            "publisher",
            {
                "errors": "[]",
                "publications": json.dumps({
                    TESTING_DATABASE: {
                        "publication-name": "relation_15_testdb",
                        "replication-slot-name": "slot_testdb",
                    }
                }),
                "secret-id": secret_id,
            },
        )
        _set_peer_data(
            harness,
            {SUBSCRIPTIONS_KEY: "{}"},
        )
    with (
        _patch_config({TESTING_DATABASE: ["public.test_table"]}),
        patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=postgresql)),
    ):
        # The healing validation succeeds and clears the flag + status message.
        harness.charm.logical_replication_manager.reconcile_subscriptions(relation)
    peer_data = harness.get_relation_data(peer_rel_id, harness.charm.app.name)
    assert peer_data.get(VALIDATION_KEY) != "error"
    # ...but the marker survives, so the charm's gate keeps recognizing the
    # (now stale) blocked message and lets update-status set active.
    assert peer_data.get("logical-replication-last-block-message") == (
        "Publisher errors: table public.test_table in database testdb doesn't exist"
    )
