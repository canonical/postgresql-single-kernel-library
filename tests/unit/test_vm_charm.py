# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the VM charm class bridges (status setters, update_config)."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.statuses import GeneralStatuses


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


def test_collect_unit_status_reconciles_the_refresh_status(charm):
    # Patch the status collectors: the emit walks every manager's get_statuses,
    # which would reach the snap/charm_refresh surface the unit env cannot build.
    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status") as reconcile,
    ):
        charm.on.collect_unit_status.emit()
    reconcile.assert_called_once()


def test_config_error_persists_through_collect(charm):
    """The invalid-config blocked must come from the status machinery.

    A directly-written blocked status is overwritten by the collect handler's
    all-active recompute on the same hook, so the "Configuration Error"
    message never reaches juju status; persisting it in the status-peers
    databag makes collect replay blocked+message.
    """
    charm.state.application.data["cluster_initialised"] = "True"
    with (
        patch.object(charm.membership_manager, "update_member_ip", return_value=False),
        patch.object(
            charm.config_manager,
            "validate_config_options",
            side_effect=ValueError("instance_max_locks_per_transaction: -1 < 64"),
        ),
    ):
        charm.postgresql_events_handler._vm_config_changed(Mock())

    records = charm.state.statuses.get("unit", charm.config_manager.name).root
    assert any(
        r.status == "blocked" and "Configuration Error" in (r.message or "") for r in records
    )
    assert charm.state.peer.data["configuration-error"] == "1"


def test_config_error_survives_collect_and_recompute(charm, harness):
    """The config-error blocked must survive the collect AND the recompute.

    Live repro (2026-10-13): the persisted record sat in the real
    status-peers databag while the deployed collect kept reporting active -
    ConfigManager.get_statuses returned ACTIVE_IDLE unconditionally, so ops
    overwrote the direct write at hook end and nothing polling juju status
    (test_config's blocked wait) ever saw the blocked state.
    """
    charm.state.application.data["cluster_initialised"] = "True"
    with (
        patch.object(charm.membership_manager, "update_member_ip", return_value=False),
        patch.object(
            charm.config_manager,
            "validate_config_options",
            side_effect=ValueError("instance_max_locks_per_transaction: -1 < 64"),
        ),
    ):
        charm.postgresql_events_handler._vm_config_changed(Mock())

    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus("Configuration Error. Please check the logs")

        # The update-status recompute clears the store per manager and re-adds
        # whatever get_statuses(recompute=True) returns; the durable flag must
        # bring the blocked record back.
        charm.status_handler._on_update_status(Mock())
        records = charm.state.statuses.get("unit", charm.config_manager.name).root
        assert any(
            r.status == "blocked" and "Configuration Error" in (r.message or "") for r in records
        )
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus("Configuration Error. Please check the logs")


def test_config_error_clears_when_config_validates(charm):
    """The persisted config-error blocked is removed once config validates."""
    charm.state.application.data["cluster_initialised"] = "True"
    charm.state.peer.data["configuration-error"] = "1"
    charm.state.add_status_if_not_present(
        GeneralStatuses.CONFIGURATION_ERROR.value,
        scope="unit",
        component=charm.config_manager.name,
    )
    with (
        patch.object(charm.membership_manager, "update_member_ip", return_value=False),
        patch.object(charm.config_manager, "validate_config_options", return_value=None),
        patch.object(charm, "update_config", return_value=True),
    ):
        charm.postgresql_events_handler._vm_config_changed(Mock())

    records = charm.state.statuses.get("unit", charm.config_manager.name).root
    assert not any(
        r.status == "blocked" and "Configuration Error" in (r.message or "") for r in records
    )
    assert "configuration-error" not in charm.state.peer.data


def test_start_primary_defers_without_a_patroni_primary(charm):
    """The bootstrap gate must use the Patroni-derived primary endpoint.

    state.primary_endpoint is the K8s service DNS name and is always truthy;
    the charm gates setup_instance_users on the Patroni-derived endpoint and
    defers "awaiting start of the primary" until Patroni elects one.
    """
    charm.patroni_manager.bootstrap_cluster = Mock(return_value=True)
    charm.patroni_manager.member_started = True
    charm.cluster_manager.can_connect_to_postgresql = Mock(return_value=True)
    charm.primary_endpoint = None
    charm.cluster_manager.setup_instance_users = Mock()
    charm.database_manager.oversee_users = Mock()
    event = Mock()

    charm.postgresql_events_handler._start_primary(event)

    event.defer.assert_called_once()
    charm.cluster_manager.setup_instance_users.assert_not_called()


def test_start_primary_bootstraps_with_a_patroni_primary(charm):
    """A Patroni-elected primary lets the bootstrap flow reach setup_instance_users."""
    charm.patroni_manager.bootstrap_cluster = Mock(return_value=True)
    charm.patroni_manager.member_started = True
    charm.cluster_manager.can_connect_to_postgresql = Mock(return_value=True)
    charm.primary_endpoint = "10.1.2.3"
    charm.cluster_manager.setup_instance_users = Mock()
    charm.database_manager.oversee_users = Mock()
    event = Mock()

    charm.postgresql_events_handler._start_primary(event)

    event.defer.assert_not_called()
    charm.cluster_manager.setup_instance_users.assert_called_once()


def test_cluster_get_statuses_survives_unreadable_peer_secrets(charm):
    """The status handler evaluates get_statuses on every hook, on every unit.

    Real Juju answers ``secret-get --label`` with permission denied on units
    that do not own the app-scope peer secrets (until the leader creates
    them); the charms treat an unreadable password as "not yet generated".
    """
    from ops import ModelError

    with (
        patch.object(
            charm.model._backend,
            "secret_get",
            side_effect=ModelError("ERROR permission denied"),
        ),
        patch.object(
            type(charm.cluster_manager.workload),
            "workload_present",
            new_callable=PropertyMock,
            return_value=True,
        ),
    ):
        statuses = charm.cluster_manager.get_statuses("unit")

    assert statuses == [GeneralStatuses.WAITING_PASSWORDS_GENERATION.value]
