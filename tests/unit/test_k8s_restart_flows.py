# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the K8s restart flow (pebble layer reconciliation)."""

from unittest.mock import Mock, patch

import pytest
from ops.pebble import ChangeError


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != "k8s":
        pytest.skip("K8s flow")


@pytest.fixture
def handler(harness):
    return harness.charm.postgresql_events_handler


def test_restart_reconciles_the_pebble_layers(harness, handler):
    event = Mock()
    with (
        patch.object(
            type(handler.patroni_manager),
            "are_all_members_ready",
            lambda self: True,
        ),
        patch.object(handler.patroni_manager, "restart_postgresql"),
        patch.object(handler.cluster_manager, "can_connect_to_postgresql", return_value=True),
        patch.object(handler.backup_manager, "start_stop_pgbackrest_service"),
        patch.object(harness.charm, "update_pebble_layers") as update,
    ):
        handler._restart(event)
    update.assert_called_once()
    event.defer.assert_not_called()


def test_update_pebble_layers_survives_a_replan_change_error(harness, caplog):
    replan_error = ChangeError("boom", Mock(name="change-id", tasks=[]))
    replan_error.args = ("boom",)
    with patch.object(
        type(harness.charm.workload),
        "update_pebble_layers",
        side_effect=replan_error,
    ):
        harness.charm.update_pebble_layers()
    assert "Failed to replan pebble services" in caplog.text
