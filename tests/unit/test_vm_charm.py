# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the VM charm class bridges (status setters, update_config)."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from single_kernel_postgresql.config.enums import Substrates


@pytest.fixture(autouse=True)
def _vm_only(substrate):
    if substrate != Substrates.VM:
        pytest.skip("the VM charm class is VM-only")


@pytest.fixture
def charm(harness):
    return harness.charm


def test_set_unit_status_writes_when_no_refresh(charm):
    charm.refresh_manager.refresh = None
    charm.set_unit_status(MaintenanceStatus("reconfiguring cluster"))
    assert isinstance(charm.unit.status, MaintenanceStatus)


def test_set_unit_status_skips_lower_priority_refresh(charm):
    charm.refresh_manager.refresh = Mock(unit_status_higher_priority=True)
    unit = Mock()
    with patch.object(type(charm), "unit", new_callable=PropertyMock, return_value=unit):
        charm.set_unit_status(MaintenanceStatus("reconfiguring cluster"))
    unit.status.assert_not_called()


def test_update_config_clears_stale_standby_cluster(charm):
    """DPE-10203: a primary cluster with no async endpoint clears the DCS standby."""
    charm.async_replication = Mock(
        get_primary_cluster_endpoint=Mock(return_value=None),
        get_partner_addresses=Mock(return_value=[]),
        get_standby_endpoints=Mock(return_value=[]),
    )
    charm.watcher_handler.is_active = False
    charm.patroni_manager.member_started = True
    with (
        patch.object(charm.config_manager, "update_config", return_value=True) as _update,
        patch.object(
            charm.patroni_manager, "bulk_update_parameters_controller_by_patroni"
        ) as _bulk,
        patch.object(type(charm), "postgresql", new_callable=PropertyMock, return_value=Mock()),
        patch.object(charm.config_manager, "build_relations_user_databases_map", return_value={}),
    ):
        assert charm.update_config() is True
    _bulk.assert_called_once_with({}, {"standby_cluster": None})


def test_set_primary_status_message_degraded(charm):
    """A primary with fewer running members than planned shows the degraded state."""
    charm.patroni_manager.get_primary = Mock(return_value=charm.unit.name)
    charm.patroni_manager.get_running_cluster_members = Mock(return_value=["postgresql-0"])
    charm.raft_manager.has_raft_quorum = Mock(return_value=True)
    with (
        patch.object(type(charm.unit), "is_leader", return_value=True),
        patch.object(
            type(charm.state.application),
            "planned_units",
            new_callable=PropertyMock,
            return_value=3,
        ),
    ):
        charm.set_primary_status_message()
    assert isinstance(charm.unit.status, ActiveStatus)
    assert charm.unit.status.message == "Primary (degraded)"


def test_set_primary_status_message_read_only(charm):
    charm.patroni_manager.get_primary = Mock(return_value=charm.unit.name)
    charm.raft_manager.has_raft_quorum = Mock(return_value=False)
    with (
        patch.object(type(charm.unit), "is_leader", return_value=True),
        patch.object(
            type(charm.state.application),
            "planned_units",
            new_callable=PropertyMock,
            return_value=1,
        ),
    ):
        charm.set_primary_status_message()
    assert charm.unit.status.message == "Primary (read-only)"


def test_set_primary_status_message_blocked_by_s3(charm):
    """The leader surfaces the s3-initialization block message as blocked."""
    charm.state.application.data["s3-initialization-block-message"] = "S3 is not ready"
    with patch.object(type(charm.unit), "is_leader", return_value=True):
        charm.set_primary_status_message()
    assert isinstance(charm.unit.status, BlockedStatus)
    assert charm.unit.status.message == "S3 is not ready"
