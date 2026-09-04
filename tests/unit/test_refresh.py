# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the refresh module."""

import json
import pathlib
from unittest.mock import MagicMock, patch

import charm_refresh
import pytest
from charm_refresh import CharmVersion, PrecheckFailed
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import SwitchoverFailedError
from single_kernel_postgresql.managers.refresh import (
    PostgreSQLRefreshK8s,
    RefreshManager,
)
from tenacity import RetryError

CHARM_VERSION = "16/1.0.0"


@pytest.fixture
def charm():
    """A mock charm with the surfaces the K8s pre-refresh checks touch."""
    return MagicMock(name="charm")


@pytest.fixture
def state():
    """A mock charm state on the K8s substrate."""
    state = MagicMock(name="state")
    state.substrate = Substrates.K8S
    return state


@pytest.fixture
def set_default_status():
    return MagicMock(name="set_default_status")


@pytest.fixture
def refresh_k8s(charm) -> PostgreSQLRefreshK8s:
    """The K8s charm-specific refresh class wired to the mock charm."""
    return PostgreSQLRefreshK8s(
        workload_name="PostgreSQL",
        charm_name="postgresql-k8s",
        oci_resource_name="postgresql-image",
        _charm=charm,
    )


@pytest.fixture
def refresh_manager(charm, state, set_default_status) -> RefreshManager:
    return RefreshManager(
        state=state,
        workload=MagicMock(name="workload"),
        charm=charm,
        set_default_status=set_default_status,
    )


@pytest.mark.parametrize(
    "old_charm,new_charm,expected",
    [
        # Released, same track, upgrade: compatible.
        ("16/1.0.0", "16/1.1.0", True),
        # Charm code downgrade: incompatible.
        ("16/1.1.0", "16/1.0.0", False),
        # Track change: incompatible.
        ("16/1.0.0", "14/1.1.0", False),
        # Unreleased charm versions: incompatible.
        ("16/1.0.0.dev0+abc", "16/1.1.0", False),
        ("16/1.0.0", "16/1.1.0.post1.dev0+abc", False),
    ],
)
def test_is_compatible_charm_version(old_charm, new_charm, expected, refresh_k8s):
    assert (
        PostgreSQLRefreshK8s.is_compatible(
            old_charm_version=CharmVersion(old_charm),
            new_charm_version=CharmVersion(new_charm),
            old_workload_version="16.9",
            new_workload_version="16.14",
        )
        is expected
    )


@pytest.mark.parametrize(
    "old_workload,new_workload,expected",
    [
        # Same major, minor upgrade: compatible.
        ("16.9", "16.14", True),
        # Same major, same version: compatible.
        ("16.14", "16.14", True),
        # Same major, minor downgrade: incompatible.
        ("16.14", "16.9", False),
        # Major upgrade: incompatible (dump/restore required instead).
        ("16.14", "17.0", False),
    ],
)
def test_is_compatible_workload_version(old_workload, new_workload, expected, refresh_k8s):
    assert (
        PostgreSQLRefreshK8s.is_compatible(
            old_charm_version=CharmVersion("16/1.0.0"),
            new_charm_version=CharmVersion("16/1.1.0"),
            old_workload_version=old_workload,
            new_workload_version=new_workload,
        )
        is expected
    )


def test_pre_refresh_check_after_1_unit_refreshed_backup_in_progress(refresh_k8s, charm):
    charm.patroni_manager.is_creating_backup = True
    with pytest.raises(PrecheckFailed, match=r"Backup in progress"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_after_1_unit_refreshed_member_not_running(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-2",
    ]
    with pytest.raises(PrecheckFailed, match=r"PostgreSQL is not running on unit 1"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_after_1_unit_refreshed_switches_primary(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/2"
    charm.get_async_primary_cluster_endpoint.return_value = None

    refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()

    charm.patroni_manager.switchover.assert_called_once_with(
        candidate="postgresql-k8s/0", async_cluster=False
    )


def test_pre_refresh_check_after_1_unit_refreshed_already_primary(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/0"

    refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()

    charm.patroni_manager.switchover.assert_not_called()


def test_pre_refresh_check_after_1_unit_refreshed_switchover_failed(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/2"
    charm.patroni_manager.switchover.side_effect = SwitchoverFailedError

    with pytest.raises(PrecheckFailed, match=r"Unable to switch primary"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_before_any_units_refreshed_members_not_ready(refresh_k8s, charm):
    charm.patroni_manager.are_all_members_ready.return_value = False
    with pytest.raises(PrecheckFailed, match=r"PostgreSQL is not running on 1\+ units"):
        refresh_k8s.run_pre_refresh_checks_before_any_units_refreshed()


def test_pre_refresh_check_before_any_units_refreshed_delegates(refresh_k8s, charm):
    charm.patroni_manager.are_all_members_ready.return_value = True
    charm.app.planned_units.return_value = 1
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = ["postgresql-k8s-0"]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/0"

    with patch.object(
        PostgreSQLRefreshK8s, "run_pre_refresh_checks_after_1_unit_refreshed"
    ) as delegate:
        refresh_k8s.run_pre_refresh_checks_before_any_units_refreshed()

    delegate.assert_called_once()


def test_refresh_manager_constructs_the_refresh_object(refresh_manager):
    assert refresh_manager.refresh is not None
    assert refresh_manager.can_set_app_status is True


def test_refresh_manager_k8s_untrusted_disables_app_status(state, charm, set_default_status):
    state.substrate = Substrates.K8S
    with patch("charm_refresh.Kubernetes", side_effect=charm_refresh.KubernetesJujuAppNotTrusted):
        manager = RefreshManager(
            state=state,
            workload=MagicMock(),
            charm=charm,
            set_default_status=set_default_status,
        )
    assert manager.refresh is None
    assert manager.can_set_app_status is False


def test_refresh_manager_peer_relation_not_ready(state, charm, set_default_status):
    state.substrate = Substrates.K8S
    with patch("charm_refresh.Kubernetes", side_effect=charm_refresh.PeerRelationNotReady):
        manager = RefreshManager(
            state=state,
            workload=MagicMock(),
            charm=charm,
            set_default_status=set_default_status,
        )
    assert manager.refresh is None
    assert manager.can_set_app_status is True


def test_set_unit_status_suppressed_by_higher_priority(refresh_manager, charm):
    refresh_manager.refresh.unit_status_higher_priority = MaintenanceStatus("refreshing")
    charm.unit.status = ActiveStatus("prior status")

    refresh_manager.set_unit_status(ActiveStatus("would override"))

    assert charm.unit.status == ActiveStatus("prior status")


def test_set_unit_status_writes_lower_priority_for_active_status(refresh_manager, charm):
    lower = ActiveStatus("PostgreSQL 16.14 running")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(return_value=lower)

    refresh_manager.set_unit_status(ActiveStatus())

    assert charm.unit.status == lower
    assert pathlib.Path(".last_refresh_unit_status.json").read_text() == json.dumps(lower.message)


def test_set_unit_status_writes_non_active_status_directly(refresh_manager, charm):
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(
        return_value=ActiveStatus("should not be used")
    )
    blocked = BlockedStatus("blocked for a reason")

    refresh_manager.set_unit_status(blocked)

    assert charm.unit.status == blocked
    refresh_manager.refresh.unit_status_lower_priority.assert_not_called()


def test_set_unit_status_without_refresh_object_sets_directly(refresh_manager, charm):
    refresh_manager.refresh = None
    waiting = WaitingStatus("waiting")

    refresh_manager.set_unit_status(waiting)

    assert charm.unit.status == waiting


def test_set_unit_status_explicit_refresh_argument_wins(refresh_manager, charm):
    lower = ActiveStatus("explicit refresh lower priority")
    explicit = MagicMock(name="explicit_refresh")
    explicit.unit_status_higher_priority = None
    explicit.unit_status_lower_priority = MagicMock(return_value=lower)
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(
        return_value=ActiveStatus("default refresh lower priority")
    )

    refresh_manager.set_unit_status(ActiveStatus(), refresh=explicit)

    assert charm.unit.status == lower
    refresh_manager.refresh.unit_status_lower_priority.assert_not_called()


def test_reconcile_refresh_status_sets_higher_priority_status(refresh_manager, charm):
    higher = MaintenanceStatus("refresh in progress")
    refresh_manager.refresh.unit_status_higher_priority = higher

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == higher
    assert pathlib.Path(".last_refresh_unit_status.json").read_text() == json.dumps(higher.message)


def test_reconcile_refresh_status_clears_stale_cached_status(
    refresh_manager, charm, set_default_status
):
    pathlib.Path(".last_refresh_unit_status.json").write_text(json.dumps("PostgreSQL 16.14"))
    charm.unit.status = ActiveStatus("PostgreSQL 16.14")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(return_value=None)

    refresh_manager.reconcile_refresh_status()

    set_default_status.assert_called_once()
    assert pathlib.Path(".last_refresh_unit_status.json").read_text() == json.dumps(None)


def test_reconcile_refresh_status_restores_lower_priority_from_cached_status(
    refresh_manager, charm
):
    lower = ActiveStatus("PostgreSQL 16.14 running")
    pathlib.Path(".last_refresh_unit_status.json").write_text(json.dumps("PostgreSQL 16.14"))
    charm.unit.status = ActiveStatus("PostgreSQL 16.14")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(return_value=lower)

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == lower


def test_reconcile_refresh_status_ignores_unrelated_status(refresh_manager, charm):
    pathlib.Path(".last_refresh_unit_status.json").write_text(json.dumps(None))
    charm.unit.status = BlockedStatus("unrelated")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(
        return_value=ActiveStatus("nope")
    )

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == BlockedStatus("unrelated")
    refresh_manager.refresh.unit_status_lower_priority.assert_not_called()


def test_reconcile_updates_layers_and_allows_next_unit(refresh_manager, charm):
    charm.patroni_manager.member_started = True
    charm.unit.is_leader.return_value = False
    charm.unit.name = "postgresql/0"
    charm.patroni_manager.cluster_members = {"postgresql-0"}
    charm.patroni_manager.is_replication_healthy.return_value = True

    refresh_manager.reconcile()

    charm.ensure_pgdata_dirs_and_symlinks.assert_called_once()
    charm.update_pebble_layers.assert_called_once()
    assert refresh_manager.refresh.next_unit_allowed_to_refresh is True
    assert charm.unit.status == ActiveStatus()


def test_reconcile_exits_early_when_patroni_has_not_started(refresh_manager, charm):
    charm.patroni_manager.member_started = False

    refresh_manager.reconcile()

    charm.ensure_pgdata_dirs_and_symlinks.assert_called_once()
    charm.update_pebble_layers.assert_called_once()
    charm.patroni_manager.is_replication_healthy.assert_not_called()


def test_reconcile_exits_early_when_primary_endpoint_not_ready(refresh_manager, charm):
    charm.patroni_manager.member_started = True
    charm.unit.is_leader.return_value = True
    charm.patroni_manager.primary_endpoint_ready = False

    refresh_manager.reconcile()

    assert charm.unit.status == MaintenanceStatus("starting services")


def test_reconcile_blocks_when_retries_exhausted(refresh_manager, charm):
    charm.patroni_manager.member_started = True
    charm.unit.is_leader.return_value = False
    charm.unit.name = "postgresql/0"
    charm.patroni_manager.cluster_members = set()
    refresh_manager.refresh.next_unit_allowed_to_refresh = False

    with patch(
        "single_kernel_postgresql.managers.refresh.Retrying",
        side_effect=RetryError("last attempt"),
    ):
        refresh_manager.reconcile()

    assert charm.unit.status == BlockedStatus(
        "upgrade failed. Check logs for rollback instruction"
    )
    assert refresh_manager.refresh.next_unit_allowed_to_refresh is False


def test_on_init_reconciles_when_in_progress(refresh_manager):
    refresh_manager.refresh.in_progress = True
    refresh_manager.refresh.workload_allowed_to_start = True
    refresh_manager.refresh.next_unit_allowed_to_refresh = False

    with patch.object(refresh_manager, "reconcile") as reconcile:
        refresh_manager.on_init()

    reconcile.assert_called_once()


def test_on_init_marks_next_unit_allowed_when_not_in_progress(refresh_manager):
    refresh_manager.refresh.in_progress = False
    refresh_manager.refresh.workload_allowed_to_start = True
    refresh_manager.refresh.next_unit_allowed_to_refresh = False

    with patch.object(refresh_manager, "reconcile") as reconcile:
        refresh_manager.on_init()

    reconcile.assert_not_called()
    assert refresh_manager.refresh.next_unit_allowed_to_refresh is True


def test_on_init_noop_without_refresh_object(refresh_manager):
    refresh_manager.refresh = None

    with patch.object(refresh_manager, "reconcile") as reconcile:
        refresh_manager.on_init()

    reconcile.assert_not_called()


def test_pebble_ready_defers_while_refresh_in_progress(harness):
    charm = harness.charm
    charm.refresh_manager.refresh.in_progress = True
    charm.refresh_manager.refresh.workload_allowed_to_start = False
    event = MagicMock()

    charm.postgresql_events_handler._on_postgresql_pebble_ready(event)

    event.defer.assert_called_once()
