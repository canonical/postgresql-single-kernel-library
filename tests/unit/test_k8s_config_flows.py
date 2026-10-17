# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the K8s config flows: the Patroni on-failure override and secret rotation.

Ports of the K8s charm's override_patroni_on_failure_condition repeat-cause guard
and the secret-changed rotation path.
"""

from unittest.mock import Mock, patch

import pytest

ORIGINAL = "always"


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != "k8s":
        pytest.skip("the Patroni on-failure override is K8s-only")


@pytest.fixture
def handler(harness):
    handler = harness.charm.postgresql_events_handler
    handler.observer_manager = Mock()
    handler.backup_manager = Mock()
    handler.restore_manager = Mock()
    return handler


@pytest.fixture
def patroni(harness):
    return harness.charm.patroni_manager


@pytest.fixture
def pebble_patch():
    with patch(
        "single_kernel_postgresql.workload.k8s.K8sWorkload.update_pebble_layers"
    ) as _update:
        yield _update


def test_override_sets_condition_and_repeat_cause(patroni, harness, pebble_patch):
    result = patroni.override_on_failure_condition("on-failure: ignore", repeat_cause="restore")
    assert result is True
    data = harness.charm.state.peer.data
    assert data["patroni-on-failure-condition-override"] == "on-failure: ignore"
    assert data["overridden-patroni-on-failure-condition-repeat-cause"] == "restore"
    pebble_patch.assert_called_once()


def test_override_blocks_distinct_repeat_cause(patroni, harness, pebble_patch):
    patroni.override_on_failure_condition("on-failure: ignore", repeat_cause="restore")
    pebble_patch.reset_mock()
    assert (
        patroni.override_on_failure_condition("on-failure: restart", repeat_cause="other") is False
    )
    data = harness.charm.state.peer.data
    assert data["patroni-on-failure-condition-override"] == "on-failure: ignore"
    pebble_patch.assert_not_called()


def test_override_allows_same_repeat_cause(patroni, harness, pebble_patch):
    patroni.override_on_failure_condition("on-failure: ignore", repeat_cause="restore")
    pebble_patch.reset_mock()
    assert (
        patroni.override_on_failure_condition("on-failure: restart", repeat_cause="restore")
        is True
    )
    assert (
        harness.charm.state.peer.data["patroni-on-failure-condition-override"]
        == "on-failure: restart"
    )
    pebble_patch.assert_called_once()


def test_override_blocks_when_already_overridden_without_cause(patroni, harness, pebble_patch):
    patroni.override_on_failure_condition("on-failure: ignore", repeat_cause="restore")
    pebble_patch.reset_mock()
    assert patroni.override_on_failure_condition("on-failure: restart", repeat_cause=None) is False
    pebble_patch.assert_not_called()


def test_secret_changed_rotates_passwords_on_k8s(handler, harness):
    """The K8s secret-changed flow rotates the system users like the VM flow."""
    harness.set_leader(True)
    handler.state.config = Mock()
    handler.state.config.system_users = "secret:1"
    with (
        patch.object(
            type(handler.state),
            "get_secret_from_id",
            return_value={"operator-password": "changed"},
        ),
        patch.object(
            type(handler.state),
            "get_secret",
            side_effect=lambda scope, key: "old",
        ),
        patch.object(handler.cluster_manager, "rotate_system_user_passwords") as _rotate,
    ):
        handler._on_secret_changed(Mock(secret=Mock(id="secret:1")))
    _rotate.assert_called_once()
