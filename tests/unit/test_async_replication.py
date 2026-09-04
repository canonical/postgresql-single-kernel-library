# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the async-replication events handler."""

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, ModelError, WaitingStatus
from ops.testing import ActionFailed
from single_kernel_postgresql.config.literals import (
    PEER_RELATION,
    REPLICATION_CONSUMER_RELATION,
    REPLICATION_OFFER_RELATION,
)
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.events.async_replication import _same_secret_id
from single_kernel_postgresql.managers.async_replication import (
    READ_ONLY_MODE_BLOCKING_MESSAGE,
    AsyncReplicationManager,
)
from single_kernel_postgresql.managers.patroni import PatroniManager

SYSTEM_IDENTIFIER = "7000000001"
UNIT_IP = "10.0.0.5"
REMOTE_UNIT_ADDRESS = "10.1.0.1"
REMOTE_APP = "other-cluster"
PEER_APP = "postgresql-single-kernel"


def _set_leader(harness):
    """Elect the leader, skipping the incidental password/config side effects."""
    with (
        patch.object(harness.charm.cluster_manager, "configure_system_passwords"),
        patch.object(harness.charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)


def _peer_rel_id(harness):
    return harness.model.get_relation(PEER_RELATION).id


def _initialise_cluster(harness):
    """Mark the peer cluster as initialised."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    harness.update_relation_data(
        peer_rel_id, harness.charm.app.name, {"cluster_initialised": "True"}
    )
    return peer_rel_id


@pytest.fixture
def async_rel(harness):
    """A leader charm with an initialised cluster and a replication-offer relation."""
    _set_leader(harness)
    _initialise_cluster(harness)
    with patch.object(
        AsyncReplicationManager, "_unit_ip", new_callable=PropertyMock, return_value=UNIT_IP
    ):
        rel_id = harness.add_relation(REPLICATION_OFFER_RELATION, REMOTE_APP)
        harness.add_relation_unit(rel_id, f"{REMOTE_APP}/0")
    harness.update_relation_data(rel_id, f"{REMOTE_APP}/0", {"unit-address": REMOTE_UNIT_ADDRESS})
    return rel_id


def _add_consumer_relation(harness):
    """Add a replication (consumer-side) relation with one remote unit."""
    with patch.object(
        AsyncReplicationManager, "_unit_ip", new_callable=PropertyMock, return_value=UNIT_IP
    ):
        rel_id = harness.add_relation(REPLICATION_CONSUMER_RELATION, REMOTE_APP)
        harness.add_relation_unit(rel_id, f"{REMOTE_APP}/0")
    harness.update_relation_data(rel_id, f"{REMOTE_APP}/0", {"unit-address": REMOTE_UNIT_ADDRESS})
    return rel_id


def _set_primary_cluster(harness, rel_id, counter):
    """Make the remote cluster the primary without triggering the changed-event flow."""
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, REMOTE_APP, {"promoted-cluster-counter": counter})


@contextmanager
def _action_flow_patches(harness):
    """Patch everything the promotion action flow touches beyond its own state."""
    with (
        patch.object(
            type(harness.charm.workload),
            "get_system_identifier",
            return_value=(SYSTEM_IDENTIFIER, None),
        ),
        patch.object(
            AsyncReplicationManager,
            "_primary_cluster_endpoint",
            new_callable=PropertyMock,
            return_value=UNIT_IP,
        ),
        patch.object(
            AsyncReplicationManager, "_get_secret", return_value=MagicMock(id="secret:abc")
        ),
        patch.object(harness.charm, "update_config"),
        patch.object(PatroniManager, "get_standby_leader", return_value=None),
    ):
        yield


def _emit_broken(harness):
    """Emit relation-broken for the offer relation through the framework."""
    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    harness.charm.on[REPLICATION_OFFER_RELATION].relation_broken.emit(relation, app=relation.app)


def _peer_app_data(harness):
    return harness.get_relation_data(_peer_rel_id(harness), harness.charm.app.name)


def _unit_peer_data(harness):
    return harness.get_relation_data(_peer_rel_id(harness), harness.charm.unit.name)


# -- create-replication action


def test_create_replication_fails_when_a_primary_cluster_exists(harness, async_rel):
    _set_primary_cluster(harness, async_rel, "1")
    with pytest.raises(ActionFailed) as exc:
        harness.run_action("create-replication", {"name": "default"})
    assert exc.value.message == "There is already a replication set up."


def test_create_replication_sets_up_replication(harness, async_rel):
    with (
        _action_flow_patches(harness),
        patch.object(harness.charm.config_manager, "update_config"),
    ):
        harness.run_action("create-replication", {"name": "async-replication"})

    peer_data = _peer_app_data(harness)
    assert peer_data["promoted-cluster-counter"] == "1"
    offer_data = harness.get_relation_data(async_rel, harness.charm.app.name)
    assert offer_data["name"] == "async-replication"
    assert json.loads(offer_data["primary-cluster-data"])["endpoint"] == UNIT_IP
    # our data is published on our side of the relation only
    assert "primary-cluster-data" not in harness.get_relation_data(async_rel, REMOTE_APP)
    assert harness.model.unit.status == MaintenanceStatus("Creating replication...")


def test_create_replication_clears_a_stale_promotion_counter(harness, async_rel):
    """A dead-DC teardown without relation-broken leaves an orphaned counter (DPE-10203)."""
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "2"}
        )
    with (
        _action_flow_patches(harness),
        patch.object(harness.charm.config_manager, "update_config"),
    ):
        harness.run_action("create-replication", {"name": "async-replication"})

    # the stale counter was cleared before the guard, and the successful setup re-promotes
    assert _peer_app_data(harness)["promoted-cluster-counter"] == "1"


def test_create_replication_keeps_a_live_promotion_counter(harness, async_rel):
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "2"}
        )
        harness.update_relation_data(
            async_rel, harness.charm.app.name, {"promoted-cluster-counter": "2"}
        )
    with pytest.raises(ActionFailed) as exc:
        harness.run_action("create-replication", {"name": "async-replication"})
    assert exc.value.message == "There is already a replication set up."
    assert _peer_app_data(harness)["promoted-cluster-counter"] == "2"


def test_create_replication_fails_on_the_consumer_side(harness):
    _set_leader(harness)
    _initialise_cluster(harness)
    _add_consumer_relation(harness)
    with pytest.raises(ActionFailed) as exc:
        harness.run_action("create-replication", {"name": "async-replication"})
    assert (
        exc.value.message == "This action must be run in the cluster where the offer was created."
    )


# -- promote_to_primary action


def test_promote_to_primary_fails_without_a_primary_cluster(harness, async_rel):
    event = MagicMock()
    event.params = {"force": True}
    event.fail = Mock()
    harness.charm.async_replication.promote_to_primary(event)
    event.fail.assert_called_once_with(
        "No primary cluster found. Run `create-replication` action in the cluster where the "
        "offer was created."
    )


def test_promote_to_primary_promotes_a_read_only_standby(substrate, harness, async_rel):
    harness.charm.app.status = BlockedStatus(READ_ONLY_MODE_BLOCKING_MESSAGE)
    event = MagicMock()
    event.params = {"force": True}
    event.fail = Mock()
    with (
        _action_flow_patches(harness),
        patch.object(harness.charm.config_manager, "update_config"),
        patch.object(AsyncReplicationManager, "get_primary_cluster", return_value=None),
    ):
        harness.charm.async_replication.promote_to_primary(event)

    event.fail.assert_not_called()
    assert _peer_app_data(harness)["promoted-cluster-counter"] == "1"
    message = "Promoting cluster..." if substrate == "k8s" else "Creating replication..."
    assert harness.model.unit.status == MaintenanceStatus(message)


def test_promote_to_primary_clears_a_stale_promotion_counter(harness, async_rel):
    """A stale counter must not mask the read-only standby promotion path (DPE-10203)."""
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "2"}
        )
    harness.charm.app.status = BlockedStatus(READ_ONLY_MODE_BLOCKING_MESSAGE)
    event = MagicMock()
    event.params = {"force": True}
    event.fail = Mock()
    with (
        _action_flow_patches(harness),
        patch.object(harness.charm.config_manager, "update_config"),
        patch.object(AsyncReplicationManager, "get_primary_cluster", return_value=None),
    ):
        harness.charm.async_replication.promote_to_primary(event)

    event.fail.assert_not_called()
    assert _peer_app_data(harness)["promoted-cluster-counter"] == "1"


# -- relation joined / created


def test_relation_joined_publishes_the_unit_address_and_counter(substrate, harness, async_rel):
    assert harness.get_relation_data(async_rel, harness.charm.unit.name) == {
        "unit-address": UNIT_IP
    }

    relation = harness.model.get_relation(REPLICATION_OFFER_RELATION)
    with patch.object(
        AsyncReplicationManager, "_unit_ip", new_callable=PropertyMock, return_value=UNIT_IP
    ):
        _set_primary_cluster(harness, async_rel, "4")
        # the VM charm observes relation_joined, the K8s charm relation_created
        join_event = "relation_joined" if substrate == "vm" else "relation_created"
        getattr(harness.charm.on[REPLICATION_OFFER_RELATION], join_event).emit(
            relation, app=relation.app
        )

    assert _unit_peer_data(harness)["unit-promoted-cluster-counter"] == "4"


# -- relation changed


def test_relation_changed_exits_early_for_a_following_non_leader(substrate, harness, async_rel):
    harness.set_leader(False)
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.unit.name, {"unit-promoted-cluster-counter": "1"}
        )
    with (
        patch.object(
            PatroniManager, "member_started", new_callable=PropertyMock, return_value=True
        ),
        patch.object(harness.charm, "update_config") as update_config,
        patch.object(PatroniManager, "stop_patroni") as stop_patroni,
    ):
        harness.update_relation_data(async_rel, REMOTE_APP, {"promoted-cluster-counter": "1"})

    if substrate == "k8s":
        # the member is already running as a standby: nothing to do
        update_config.assert_not_called()
    else:
        update_config.assert_called_once()
    stop_patroni.assert_not_called()


def test_relation_changed_leader_stops_and_defers_until_all_units_stopped(
    substrate, harness, async_rel
):
    harness.add_relation_unit(_peer_rel_id(harness), f"{PEER_APP}/1")

    @contextmanager
    def _standby_teardown_patches():
        if substrate == "k8s":
            with (
                patch.object(type(harness.charm.workload), "stop"),
                patch.object(harness.charm.k8s_manager, "delete_patroni_cluster_resources"),
            ):
                yield
        else:
            with patch.object(type(harness.charm.workload), "remove_raft_state"):
                yield

    with (
        patch.object(
            type(harness.charm.workload), "get_system_identifier", return_value=("7001", None)
        ),
        patch.object(
            type(harness.charm.workload),
            "create_data_backup_tarball",
            return_value="backup.tar.gz",
        ),
        patch.object(type(harness.charm.workload), "clear_data_directories"),
        patch.object(PatroniManager, "stop_patroni", return_value=True),
        patch.object(PatroniManager, "get_standby_leader", return_value=None),
        patch.object(harness.charm, "update_config"),
        _standby_teardown_patches(),
    ):
        harness.update_relation_data(async_rel, REMOTE_APP, {"promoted-cluster-counter": "1"})

    assert _unit_peer_data(harness)["stopped"] == "True"
    if substrate == "vm":
        # The VM flow publishes the counter for the demoted cluster's pre-check.
        assert harness.get_relation_data(async_rel, harness.model.unit.name)["stopped"] == "1"
    assert harness.model.unit.status == WaitingStatus(
        "Waiting for the database to be stopped in all units"
    )


# -- relation broken


def test_relation_broken_marks_the_standby_cluster_read_only(harness, async_rel):
    _set_primary_cluster(harness, async_rel, "3")
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness),
            harness.charm.unit.name,
            {"stopped": "True", "unit-promoted-cluster-counter": "3"},
        )
    with (
        patch.object(PatroniManager, "get_standby_leader", return_value="postgresql-standby-0"),
        patch.object(harness.charm, "update_config") as update_config,
        patch.object(harness.charm.async_replication, "set_app_status") as set_app_status,
    ):
        _emit_broken(harness)

    assert _peer_app_data(harness)["promoted-cluster-counter"] == "0"
    set_app_status.assert_called_once()
    update_config.assert_not_called()


def test_relation_broken_clears_the_counter_of_the_primary_cluster(harness, async_rel):
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "3"}
        )
    with (
        patch.object(PatroniManager, "get_standby_leader", return_value=None),
        patch.object(harness.charm, "update_config") as update_config,
    ):
        _emit_broken(harness)

    assert _peer_app_data(harness).get("promoted-cluster-counter") in (None, "")
    update_config.assert_called_once()


def test_relation_broken_treats_an_unreadable_standby_check_as_primary(harness, async_rel):
    """A force-removed dead DC can make the standby check fail; treat as primary (DPE-10203)."""
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "3"}
        )
    with (
        patch.object(PatroniManager, "get_standby_leader", side_effect=ModelError("gone")),
        patch.object(harness.charm, "update_config") as update_config,
    ):
        _emit_broken(harness)

    assert _peer_app_data(harness).get("promoted-cluster-counter") in (None, "")
    update_config.assert_called_once()


def test_relation_broken_skips_a_departing_unit(harness, async_rel):
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "3"}
        )
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.unit.name, {"departing": "True"}
        )
    with (
        patch.object(PatroniManager, "get_standby_leader", return_value=None),
        patch.object(harness.charm, "update_config") as update_config,
    ):
        _emit_broken(harness)

    assert _peer_app_data(harness)["promoted-cluster-counter"] == "3"
    update_config.assert_not_called()


# -- secret changed


def test_secret_changed_syncs_the_internal_secret(harness):
    _set_leader(harness)
    _initialise_cluster(harness)
    rel_id = _add_consumer_relation(harness)
    secret_id = harness.add_model_secret(owner=REMOTE_APP, content={"replication-password": "pw"})
    harness.grant_secret(secret_id, harness.charm.app.name)
    primary_cluster_data = {"endpoint": REMOTE_UNIT_ADDRESS, "secret-id": secret_id}
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, REMOTE_APP, {"primary-cluster-data": json.dumps(primary_cluster_data)}
        )

    with patch.object(CharmState, "set_secret") as set_secret:
        harness.charm.async_replication._on_secret_changed(
            MagicMock(secret=MagicMock(id=secret_id))
        )

    set_secret.assert_called_once_with("app", "replication-password", "pw")


# -- app status


def test_set_app_status_blocks_a_read_only_standby_cluster(harness, async_rel):
    with harness.hooks_disabled():
        harness.update_relation_data(
            _peer_rel_id(harness), harness.charm.app.name, {"promoted-cluster-counter": "0"}
        )
    with patch.object(harness.charm, "set_app_status") as set_app_status:
        harness.charm.async_replication.set_app_status()

    set_app_status.assert_called_once_with(BlockedStatus(READ_ONLY_MODE_BLOCKING_MESSAGE))


def test_set_app_status_is_active_without_an_async_relation(harness):
    _set_leader(harness)
    _initialise_cluster(harness)
    with patch.object(harness.charm, "set_app_status") as set_app_status:
        harness.charm.async_replication.set_app_status()

    set_app_status.assert_called_once_with(ActiveStatus())


def test_set_app_status_reports_primary_and_standby(harness, async_rel):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with (
        harness.hooks_disabled(),
        patch.object(harness.charm, "set_app_status") as set_app_status,
    ):
        _set_primary_cluster(harness, async_rel, "1")
        harness.update_relation_data(
            peer_rel_id, harness.charm.app.name, {"promoted-cluster-counter": "3"}
        )
        harness.charm.async_replication.set_app_status()
        set_app_status.assert_called_once_with(ActiveStatus("Primary"))

        harness.update_relation_data(
            peer_rel_id, harness.charm.app.name, {"promoted-cluster-counter": "1"}
        )
        harness.charm.async_replication.set_app_status()
        set_app_status.assert_called_with(ActiveStatus("Standby"))


# -- read-only mode


def test_handle_read_only_mode_recomputes_the_status_messages(harness):
    _set_leader(harness)
    _initialise_cluster(harness)
    handler = harness.charm.async_replication
    with (
        patch.object(harness.charm, "set_primary_status_message") as set_primary_message,
        patch.object(harness.charm, "set_app_status") as set_app_status,
    ):
        handler.handle_read_only_mode()
        assert set_primary_message.call_count == 1
        assert set_app_status.call_count == 1

        harness.charm.unit.status = BlockedStatus(READ_ONLY_MODE_BLOCKING_MESSAGE)
        handler.handle_read_only_mode()
        assert set_primary_message.call_count == 1
        assert set_app_status.call_count == 2


def test_same_secret_id():
    assert _same_secret_id("secret:abc123", "secret://model-uuid/abc123") is True
    assert not _same_secret_id(None, "secret:abc123")
    assert not _same_secret_id("", "secret:abc123")
    assert not _same_secret_id("secret:abc123", None)
    assert not _same_secret_id("secret:abc123", "secret:def456")
