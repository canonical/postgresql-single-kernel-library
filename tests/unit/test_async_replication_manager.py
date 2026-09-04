# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the async-replication data-plane manager."""

import json
from unittest.mock import Mock, PropertyMock, call, patch

import pytest
from ops import ModelError, SecretNotFoundError
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    ASYNC_SHARED_SECRET_ID_KEY,
    PEER_RELATION,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.async_replication import (
    AsyncReplicationError,
    AsyncReplicationManager,
    _safe_databag_get,
)

REMOTE_APP = "other-cluster"
REMOTE_UNIT_ADDRESS = "10.9.9.9"
PRIMARY_ENDPOINT = "10.1.1.1"
PEER_IPS = {"10.0.0.2", "10.0.0.1"}


def _set_leader(harness, leader=True):
    """Set leadership without triggering the heavy leader-elected side effects."""
    with (
        harness.hooks_disabled(),
        patch.object(harness.charm.cluster_manager, "configure_system_passwords"),
        patch.object(harness.charm.config_manager, "update_config"),
    ):
        harness.set_leader(leader)


def _add_relation(harness, relation_name):
    """Add an async-replication relation with one remote unit."""
    rel_id = harness.add_relation(relation_name, REMOTE_APP)
    harness.add_relation_unit(rel_id, f"{REMOTE_APP}/0")
    return rel_id


def _update_peer_app_data(harness, data):
    """Write into this application's peer databag."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    harness.update_relation_data(peer_rel_id, harness.charm.app.name, data)


def _update_peer_unit_data(harness, data):
    """Write into this unit's peer databag."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    harness.update_relation_data(peer_rel_id, harness.charm.model.unit.name, data)


def _create_app_secret(harness, content=None):
    """Create the peer app secret holding the internal credentials."""
    label = f"{PEER_RELATION}.{harness.charm.app.name}.app"
    return harness.charm.model.app.add_secret(
        content=content or {"operator-password": "pw", "user-password": "pw"}, label=label
    )


@pytest.fixture
def manager(harness):
    """A leader charm with the cluster initialised and the manager under test.

    Framework hooks stay disabled: these tests exercise the data plane directly, so
    relation-data updates must not drive the events handler flow.
    """
    harness.disable_hooks()
    _set_leader(harness)
    with harness.hooks_disabled():
        _update_peer_app_data(harness, {"cluster_initialised": "True"})
    with patch.object(AsyncReplicationManager, "_get_unit_ip", return_value="10.0.0.5"):
        # The K8s unit IP comes from /etc/hosts, which the test host does not model.
        yield AsyncReplicationManager(
            state=harness.charm.state,
            workload=harness.charm.workload,
            patroni_manager=harness.charm.patroni_manager,
            update_config=Mock(return_value=True),
        )


def _unreadable(databag):
    """Make a databag's reads raise ModelError, as a force-removed dead DC does."""
    return patch.object(databag, "get", side_effect=ModelError)


# _safe_databag_get


def test_safe_databag_get_returns_the_value():
    assert _safe_databag_get({"key": "value"}, "key") == "value"


def test_safe_databag_get_returns_default_when_key_is_missing():
    assert _safe_databag_get({}, "key", "fallback") == "fallback"
    assert _safe_databag_get({}, "key") is None


def test_safe_databag_get_treats_unreadable_databag_as_key_absent():
    databag = Mock()
    databag.get.side_effect = ModelError
    assert _safe_databag_get(databag, "key", "fallback") == "fallback"


# primary-cluster resolution


def test_primary_cluster_is_the_highest_counter_application(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    _update_peer_app_data(harness, {"promoted-cluster-counter": "0"})

    primary = manager.get_primary_cluster()
    assert primary.name == REMOTE_APP
    assert not manager.is_primary_cluster()
    assert manager.get_highest_promoted_cluster_counter_value() == "3"


def test_primary_cluster_resolution_favors_the_local_counter(harness, manager):
    _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(
        harness.model.get_relation(REPLICATION_OFFER_RELATION).id,
        REMOTE_APP,
        {"promoted-cluster-counter": "3"},
    )
    _update_peer_app_data(harness, {"promoted-cluster-counter": "5"})

    assert manager.get_primary_cluster() is harness.charm.model.app
    assert manager.is_primary_cluster()
    assert manager.get_highest_promoted_cluster_counter_value() == "5"


def test_primary_cluster_resolution_skips_unreadable_remote(harness, manager):
    _add_relation(harness, REPLICATION_OFFER_RELATION)
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    harness.update_relation_data(relation.id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    _update_peer_app_data(harness, {"promoted-cluster-counter": "5"})
    with _unreadable(relation.data[relation.app]):
        assert manager.get_primary_cluster() is harness.charm.model.app
        assert manager.is_primary_cluster()
        assert manager.get_highest_promoted_cluster_counter_value() == "5"


# endpoints


def test_standby_sees_the_primary_cluster_endpoints(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    harness.update_relation_data(rel_id, f"{REMOTE_APP}/0", {"unit-address": REMOTE_UNIT_ADDRESS})

    assert manager.get_all_primary_cluster_endpoints() == [REMOTE_UNIT_ADDRESS]
    assert manager.get_standby_endpoints() == []
    assert manager.get_primary_cluster_endpoint() is None


def test_primary_sees_the_standby_endpoints(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    harness.update_relation_data(rel_id, f"{REMOTE_APP}/0", {"unit-address": REMOTE_UNIT_ADDRESS})
    _update_peer_app_data(harness, {"promoted-cluster-counter": "5"})

    assert manager.get_standby_endpoints() == [REMOTE_UNIT_ADDRESS]
    assert manager.get_all_primary_cluster_endpoints() == []


def test_standby_reads_the_primary_cluster_endpoint(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    harness.update_relation_data(
        rel_id, REMOTE_APP, {"primary-cluster-data": json.dumps({"endpoint": PRIMARY_ENDPOINT})}
    )

    assert manager.get_primary_cluster_endpoint() == PRIMARY_ENDPOINT


def test_remote_unit_addresses_skip_unreadable_units(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.add_relation_unit(rel_id, f"{REMOTE_APP}/1")
    harness.update_relation_data(rel_id, f"{REMOTE_APP}/0", {"unit-address": REMOTE_UNIT_ADDRESS})
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    dead_unit = next(u for u in relation.units if u.name == f"{REMOTE_APP}/1")

    with _unreadable(relation.data[dead_unit]):
        assert manager._remote_unit_addresses() == [REMOTE_UNIT_ADDRESS]


def test_endpoints_without_relation(harness, manager):
    assert manager.get_standby_endpoints() == []
    assert manager.get_primary_cluster_endpoint() is None
    with pytest.raises(AsyncReplicationError):
        manager.get_all_primary_cluster_endpoints()


# partner addresses


def test_get_partner_addresses_following_the_primary(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    _update_peer_unit_data(harness, {"unit-promoted-cluster-counter": "3"})

    with patch.object(
        CharmState, "peer_members_ips", new_callable=PropertyMock, return_value=PEER_IPS
    ):
        assert manager.get_partner_addresses() == ["10.0.0.1", "10.0.0.2"]


def test_get_partner_addresses_not_following_the_primary(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    _update_peer_unit_data(harness, {"unit-promoted-cluster-counter": "2"})

    with patch.object(
        CharmState, "peer_members_ips", new_callable=PropertyMock, return_value=PEER_IPS
    ):
        assert manager.get_partner_addresses() == []


# secrets


def test_get_secret_references_the_persisted_id(harness, manager):
    _create_app_secret(harness)
    secret_id = harness.add_model_secret(
        owner=harness.charm.app.name,
        content={"operator-password": "pw", "user-password": "pw"},
    )
    _update_peer_app_data(harness, {ASYNC_SHARED_SECRET_ID_KEY: secret_id})

    secret = manager._get_secret()

    assert secret.id == secret_id
    assert secret.peek_content() == {"operator-password": "pw", "user-password": "pw"}


def test_get_secret_leader_creates_the_shared_secret(harness, manager):
    _create_app_secret(harness)

    secret = manager._get_secret()

    assert secret is not None
    assert harness.charm.state.application.data[ASYNC_SHARED_SECRET_ID_KEY] == secret.id
    assert secret.peek_content() == {"operator-password": "pw", "user-password": "pw"}


def test_get_secret_non_leader_without_id_returns_none(harness, manager):
    _set_leader(harness, leader=False)
    _create_app_secret(harness)

    assert manager._get_secret() is None
    assert ASYNC_SHARED_SECRET_ID_KEY not in harness.charm.state.application.data


def test_get_secret_adopts_the_own_published_id(harness, manager):
    _create_app_secret(harness)
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    secret_id = harness.add_model_secret(
        owner=harness.charm.app.name, content={"operator-password": "pw"}
    )
    harness.update_relation_data(
        rel_id,
        harness.charm.app.name,
        {"primary-cluster-data": json.dumps({"secret-id": secret_id})},
    )

    secret = manager._get_secret()

    assert secret.id == secret_id
    assert harness.charm.state.application.data[ASYNC_SHARED_SECRET_ID_KEY] == secret_id


def test_update_internal_secret_syncs_credentials(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    secret_id = harness.add_model_secret(
        owner=harness.charm.app.name,
        content={"replication-password": "pw", "operator-password": "pw2"},
    )
    harness.update_relation_data(
        rel_id,
        REMOTE_APP,
        {
            "primary-cluster-data": json.dumps({
                "endpoint": PRIMARY_ENDPOINT,
                "secret-id": secret_id,
            })
        },
    )
    with patch.object(harness.charm.state, "set_secret") as set_secret:
        assert manager._update_internal_secret() is True

    set_secret.assert_has_calls([
        call(APP_SCOPE, "replication-password", "pw"),
        call(APP_SCOPE, "operator-password", "pw2"),
    ])


def test_update_internal_secret_without_published_secret_id(harness, manager):
    _add_relation(harness, REPLICATION_OFFER_RELATION)
    with patch.object(harness.charm.state, "set_secret") as set_secret:
        assert manager._update_internal_secret() is False
    set_secret.assert_not_called()


def test_update_internal_secret_missing_secret(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(
        rel_id,
        REMOTE_APP,
        {"primary-cluster-data": json.dumps({"secret-id": "secret:missing"})},
    )
    with patch.object(type(harness.charm.model), "get_secret", side_effect=SecretNotFoundError):
        assert manager._update_internal_secret() is False


# stale promotion recovery


def _peer_app_databag(harness):
    """The raw peer databag of this application."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    return harness.get_relation_data(peer_rel_id, harness.charm.app.name)


def test_clear_stale_promotion_is_a_noop_for_non_leaders(harness, manager):
    _set_leader(harness, leader=False)
    _update_peer_app_data(harness, {"promoted-cluster-counter": "2"})

    manager.clear_stale_promotion()

    assert _peer_app_databag(harness)["promoted-cluster-counter"] == "2"
    manager.update_config.assert_not_called()


def test_clear_stale_promotion_ignores_standby_counters(harness, manager):
    for counter, expected in [("", None), ("0", "0")]:
        _update_peer_app_data(harness, {"promoted-cluster-counter": counter})
        manager.clear_stale_promotion()
        databag = _peer_app_databag(harness)
        if expected is None:
            # Juju drops empty values: a cleared counter reads as key-absent.
            assert "promoted-cluster-counter" not in databag
        else:
            assert databag["promoted-cluster-counter"] == expected
        manager.update_config.assert_not_called()


def test_clear_stale_promotion_clears_unmirrored_counter(harness, manager):
    _update_peer_app_data(harness, {"promoted-cluster-counter": "2"})

    manager.clear_stale_promotion()

    assert "promoted-cluster-counter" not in _peer_app_databag(harness)
    manager.update_config.assert_called_once()


def test_clear_stale_promotion_keeps_mirrored_counter(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    _update_peer_app_data(harness, {"promoted-cluster-counter": "2"})
    harness.update_relation_data(rel_id, harness.charm.app.name, {"promoted-cluster-counter": "2"})

    manager.clear_stale_promotion()

    assert _peer_app_databag(harness)["promoted-cluster-counter"] == "2"
    manager.update_config.assert_not_called()


def test_clear_stale_promotion_clears_when_mirror_is_unreadable(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    _update_peer_app_data(harness, {"promoted-cluster-counter": "2"})
    harness.update_relation_data(rel_id, harness.charm.app.name, {"promoted-cluster-counter": "2"})
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)

    with _unreadable(relation.data[harness.charm.model.app]):
        manager.clear_stale_promotion()

    assert "promoted-cluster-counter" not in _peer_app_databag(harness)
    manager.update_config.assert_called_once()


# relation data publication


def test_update_async_replication_data_publishes_the_unit_address(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)

    with patch.object(AsyncReplicationManager, "_get_unit_ip", return_value="10.1.1.5"):
        manager.update_async_replication_data()

    expected_ip = (
        "10.1.1.5"
        if harness.charm.state.substrate.value == "k8s"
        else (harness.charm.state.replication_offer_ip)
    )
    assert relation.data[harness.charm.model.unit]["unit-address"] == expected_ip
    assert "primary-cluster-data" not in relation.data[harness.charm.model.app]


def test_update_async_replication_data_publishes_the_consumer_address(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_CONSUMER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    relation = harness.model.get_relation(REPLICATION_CONSUMER_RELATION)

    with patch.object(AsyncReplicationManager, "_get_unit_ip", return_value="10.1.1.5"):
        manager.update_async_replication_data()

    expected_ip = (
        "10.1.1.5"
        if harness.charm.state.substrate.value == "k8s"
        else (harness.charm.state.replication_consumer_ip)
    )
    assert relation.data[harness.charm.model.unit]["unit-address"] == expected_ip


def test_update_async_replication_data_as_primary(harness, manager):
    rel_id = _add_relation(harness, REPLICATION_OFFER_RELATION)
    harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": "3"})
    _update_peer_app_data(harness, {"promoted-cluster-counter": "5"})
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    secret = Mock(id="secret:x")

    with (
        patch.object(AsyncReplicationManager, "_get_unit_ip", return_value="10.1.1.5"),
        patch.object(
            AsyncReplicationManager,
            "_primary_cluster_endpoint",
            new_callable=PropertyMock,
            return_value=PRIMARY_ENDPOINT,
        ),
        patch.object(AsyncReplicationManager, "_get_secret", return_value=secret),
    ):
        manager.update_async_replication_data()

    data = json.loads(relation.data[harness.charm.model.app]["primary-cluster-data"])
    assert data["endpoint"] == PRIMARY_ENDPOINT
    assert data["secret-id"] == "secret:x"
    secret.grant.assert_called_once_with(relation)


def test_update_primary_cluster_data_writes_the_counter_everywhere(harness, manager):
    _add_relation(harness, REPLICATION_OFFER_RELATION)
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    secret = Mock(id="secret:x")

    with (
        patch.object(
            AsyncReplicationManager,
            "_primary_cluster_endpoint",
            new_callable=PropertyMock,
            return_value=PRIMARY_ENDPOINT,
        ),
        patch.object(AsyncReplicationManager, "_get_secret", return_value=secret),
    ):
        manager._update_primary_cluster_data(promoted_cluster_counter=4, system_identifier="sys-1")

    app_databag = relation.data[harness.charm.model.app]
    assert app_databag["promoted-cluster-counter"] == "4"
    peer_relation = harness.model.get_relation(PEER_RELATION)
    assert peer_relation.data[harness.charm.app]["promoted-cluster-counter"] == "4"
    assert json.loads(app_databag["primary-cluster-data"]) == {
        "endpoint": PRIMARY_ENDPOINT,
        "secret-id": "secret:x",
        "system-id": "sys-1",
    }
    secret.grant.assert_called_once_with(relation)


def test_update_primary_cluster_data_on_the_consumer_side(harness, manager):
    _add_relation(harness, REPLICATION_CONSUMER_RELATION)
    relation = harness.model.get_relation(REPLICATION_CONSUMER_RELATION)

    with (
        patch.object(
            AsyncReplicationManager,
            "_primary_cluster_endpoint",
            new_callable=PropertyMock,
            return_value=PRIMARY_ENDPOINT,
        ),
        patch.object(AsyncReplicationManager, "_get_secret") as get_secret,
    ):
        manager._update_primary_cluster_data()

    data = json.loads(relation.data[harness.charm.model.app]["primary-cluster-data"])
    assert data == {"endpoint": PRIMARY_ENDPOINT}
    get_secret.assert_not_called()
