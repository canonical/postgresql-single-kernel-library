# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the refresh module."""

import json
import pathlib
from unittest.mock import MagicMock, patch

import charm_refresh
import psycopg2
import pytest
from charm_refresh import CharmVersion, PrecheckFailed
from data_platform_helpers.advanced_statuses import StatusObject
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, UnknownStatus, WaitingStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import SwitchoverFailedError
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.managers.refresh import (
    PostgreSQLRefreshK8s,
    RefreshManager,
)
from tenacity import RetryError


@pytest.fixture
def charm():
    """A mock charm with the surfaces the K8s pre-refresh checks touch."""
    charm = MagicMock(name="charm")
    charm.unit.status = ActiveStatus()
    return charm


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
    cached = pathlib.Path(".last_refresh_unit_status.json").read_text()

    refresh_manager.set_unit_status(ActiveStatus("would override"))

    assert charm.unit.status == ActiveStatus("prior status")
    assert pathlib.Path(".last_refresh_unit_status.json").read_text() == cached


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
    charm.set_app_status.reset_mock()

    refresh_manager.reconcile_refresh_status()

    charm.set_app_status.assert_called_once()
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
    refresh_manager.workload.is_patroni_running.return_value = False

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == lower
    refresh_manager.refresh.unit_status_lower_priority.assert_called_once_with(
        workload_is_running=False
    )


def test_reconcile_refresh_status_ignores_unrelated_status(refresh_manager, charm):
    pathlib.Path(".last_refresh_unit_status.json").write_text(json.dumps(None))
    charm.unit.status = BlockedStatus("unrelated")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(
        return_value=ActiveStatus("nope")
    )

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == BlockedStatus("unrelated")
    refresh_manager.refresh.unit_status_lower_priority.assert_not_called()


def test_reconcile_refresh_status_substitutes_active_status_without_cached_message(
    refresh_manager, charm
):
    lower = ActiveStatus("PostgreSQL 16.14 running")
    pathlib.Path(".last_refresh_unit_status.json").write_text(json.dumps(None))
    charm.unit.status = ActiveStatus("stale message")
    refresh_manager.refresh.unit_status_lower_priority = MagicMock(return_value=lower)

    refresh_manager.reconcile_refresh_status()

    assert charm.unit.status == lower
    assert pathlib.Path(".last_refresh_unit_status.json").read_text() == json.dumps(lower.message)


def test_get_statuses_replays_the_reconciliation_on_recompute(refresh_manager, charm):
    higher = MaintenanceStatus("refresh in progress")
    refresh_manager.refresh.unit_status_higher_priority = higher
    record = StatusObject(status="maintenance", message="refresh in progress")
    refresh_manager.state.statuses.set.reset_mock()

    statuses = refresh_manager.get_statuses("unit", recompute=True)

    assert charm.unit.status == higher
    assert statuses == [record]
    refresh_manager.state.statuses.set.assert_called_once_with(record, "unit", "refresh_manager")


def test_get_statuses_persists_the_reconciled_active_status(refresh_manager, charm):
    refresh_manager.refresh = None
    refresh_manager.state.statuses.set.reset_mock()

    statuses = refresh_manager.get_statuses("unit", recompute=True)

    assert statuses == [GeneralStatuses.ACTIVE_IDLE.value]
    refresh_manager.state.statuses.set.assert_called_once_with(
        GeneralStatuses.ACTIVE_IDLE.value, "unit", "refresh_manager"
    )


def test_get_statuses_persists_active_idle_for_unsettled_unit_status(refresh_manager, charm):
    charm.unit.status = UnknownStatus()
    refresh_manager.state.statuses.set.reset_mock()

    statuses = refresh_manager.get_statuses("unit", recompute=True)

    assert statuses == [GeneralStatuses.ACTIVE_IDLE.value]
    refresh_manager.state.statuses.set.assert_called_once_with(
        GeneralStatuses.ACTIVE_IDLE.value, "unit", "refresh_manager"
    )


def test_get_statuses_returns_cached_active_idle_for_app_scope_without_recompute(
    refresh_manager,
):
    refresh_manager.state.statuses.get.return_value.root = []

    assert refresh_manager.get_statuses("app") == [GeneralStatuses.ACTIVE_IDLE.value]


def test_get_statuses_returns_the_cached_records_without_recompute(refresh_manager):
    record = StatusObject(status="blocked", message="upgrade failed")
    refresh_manager.state.statuses.get.return_value.root = [record]

    assert refresh_manager.get_statuses("unit") == [record]


def test_get_statuses_defaults_to_active_idle_without_cached_records(refresh_manager):
    refresh_manager.state.statuses.get.return_value.root = []

    assert refresh_manager.get_statuses("unit") == [GeneralStatuses.ACTIVE_IDLE.value]


def test_get_statuses_returns_active_idle_for_app_scope(refresh_manager, charm):
    refresh_manager.state.statuses.set.reset_mock()

    statuses = refresh_manager.get_statuses("app", recompute=True)

    assert statuses == [GeneralStatuses.ACTIVE_IDLE.value]
    charm.set_app_status.assert_called()
    refresh_manager.state.statuses.set.assert_not_called()


@pytest.fixture
def vm_manager(charm, set_default_status):
    """A refresh manager on the VM substrate wired to the mock charm."""
    state = MagicMock(name="state")
    state.substrate = Substrates.VM
    return RefreshManager(
        state=state,
        workload=MagicMock(name="workload"),
        charm=charm,
        set_default_status=set_default_status,
    )


@pytest.fixture
def refresh_vm(charm):
    from single_kernel_postgresql.managers.refresh import PostgreSQLRefreshVM

    return PostgreSQLRefreshVM(workload_name="PostgreSQL", charm_name="postgresql", _charm=charm)


def test_refresh_snap_runs_the_post_refresh_flow(refresh_vm, charm):
    refresh = MagicMock(name="refresh")
    charm.refresh_manager = MagicMock(name="refresh_manager")

    refresh_vm.refresh_snap(snap_name="charmed-postgresql", snap_revision="366", refresh=refresh)

    charm.update_config.assert_called_once_with(refresh=refresh)
    charm.workload.install_snap_package.assert_called_once_with(revision="366", refresh=refresh)
    charm.refresh_manager.post_snap_refresh.assert_called_once_with(refresh)


def test_post_snap_refresh_blocks_when_patroni_fails_to_start(vm_manager, charm):
    charm.patroni_manager.start_patroni.return_value = False
    vm_manager.refresh.next_unit_allowed_to_refresh = False

    vm_manager.post_snap_refresh(vm_manager.refresh)

    assert charm.unit.status == BlockedStatus("Failed to start PostgreSQL")
    charm.post_refresh_side_effects.assert_not_called()
    assert vm_manager.refresh.next_unit_allowed_to_refresh is False


def test_post_snap_refresh_allows_next_unit_when_healthy(vm_manager, charm):
    charm.patroni_manager.start_patroni.return_value = True
    charm.patroni_manager.member_started = True
    charm.patroni_manager.cluster_members = {"postgresql-2"}
    charm.patroni_manager.is_replication_healthy.return_value = True
    charm.unit.name = "postgresql/2"
    lower_peer, middle_peer = MagicMock(), MagicMock()
    lower_peer.name = "postgresql/0"
    middle_peer.name = "postgresql/1"
    charm.state.peer_relation = MagicMock(units=[lower_peer, middle_peer])

    vm_manager.post_snap_refresh(vm_manager.refresh)

    charm.post_refresh_side_effects.assert_called_once()
    assert vm_manager.refresh.next_unit_allowed_to_refresh is True
    assert charm.unit.status == ActiveStatus()


def test_post_snap_refresh_retries_exhausted_keeps_unit_blocked_from_refresh(vm_manager, charm):
    charm.patroni_manager.start_patroni.return_value = True
    charm.patroni_manager.member_started = False
    vm_manager.refresh.next_unit_allowed_to_refresh = False

    with patch(
        "single_kernel_postgresql.managers.refresh.Retrying",
        side_effect=RetryError("last attempt"),
    ):
        vm_manager.post_snap_refresh(vm_manager.refresh)

    charm.post_refresh_side_effects.assert_called_once()
    assert vm_manager.refresh.next_unit_allowed_to_refresh is False


def test_on_init_marks_next_unit_allowed_when_not_in_progress(vm_manager):
    vm_manager.refresh.next_unit_allowed_to_refresh = False
    vm_manager.refresh.in_progress = False

    with patch.object(vm_manager, "migrate_temp_tablespace_location") as migrate:
        vm_manager.on_init()

    migrate.assert_called_once()
    assert vm_manager.refresh.next_unit_allowed_to_refresh is True


def test_on_init_runs_post_refresh_when_in_progress(vm_manager):
    vm_manager.refresh.next_unit_allowed_to_refresh = False
    vm_manager.refresh.in_progress = True

    with patch.object(vm_manager, "post_snap_refresh") as post_snap_refresh:
        vm_manager.on_init()

    post_snap_refresh.assert_called_once_with(vm_manager.refresh)


def test_migrate_temp_tablespace_skips_without_primary_endpoint(vm_manager, charm):
    charm.primary_endpoint = None
    assert vm_manager.migrate_temp_tablespace_location() is True
    assert vm_manager.migrate_temp_tablespace_location(required=True) is False


def test_migrate_temp_tablespace_skips_for_async_relation(vm_manager, charm):
    charm.primary_endpoint = "10.1.0.1"
    charm.has_async_replication_relation.return_value = True

    assert vm_manager.migrate_temp_tablespace_location() is True


def test_migrate_temp_tablespace_skips_when_tablespace_missing(vm_manager, charm):
    """When the tablespace doesn't exist in pg_catalog, no migration is needed."""
    temp_data_dir = MagicMock()
    temp_data_dir.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp/16/main"
    temp_root = MagicMock()
    temp_root.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp"
    charm.workload.paths.temp = temp_data_dir
    charm.workload.paths.temp.parent = temp_root
    charm.primary_endpoint = "10.1.0.1"
    charm.has_async_replication_relation.return_value = False
    with patch.object(vm_manager, "_resolve_primary_host", return_value="10.1.0.1"):
        cursor = charm.postgresql._connect_to_database.return_value.cursor.return_value
        cursor.fetchone.return_value = None

        assert vm_manager.migrate_temp_tablespace_location() is True

    cursor.execute.assert_called_once_with(
        "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='temp';"
    )


def test_migrate_temp_tablespace_returns_false_on_db_error(vm_manager, charm):
    """When a psycopg2 error occurs, the migration reports failure."""
    charm.primary_endpoint = "10.1.0.1"
    charm.has_async_replication_relation.return_value = False
    charm.postgresql._connect_to_database.side_effect = psycopg2.Error("connection failed")

    assert vm_manager.migrate_temp_tablespace_location() is False


def test_execute_temp_tablespace_migration_noop_when_already_migrated(vm_manager, charm):
    temp_data_dir = MagicMock()
    temp_data_dir.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp/16/main"
    temp_root = MagicMock()
    temp_root.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp"
    charm.workload.paths.temp = temp_data_dir
    charm.workload.paths.temp.parent = temp_root
    cursor = charm.postgresql._connect_to_database.return_value.cursor.return_value
    cursor.fetchone.return_value = ("/var/snap/charmed-postgresql/common/data/temp/16/main",)

    assert vm_manager._execute_temp_tablespace_migration("10.1.0.1") is True
    cursor.execute.assert_called_once_with(
        "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='temp';"
    )


def test_execute_temp_tablespace_migration_performs_the_ddl(vm_manager, charm):
    temp_data_dir = MagicMock()
    temp_data_dir.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp/16/main"
    temp_root = MagicMock()
    temp_root.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp"
    charm.workload.paths.temp = temp_data_dir
    charm.workload.paths.temp.parent = temp_root
    cursor = charm.postgresql._connect_to_database.return_value.cursor.return_value
    cursor.fetchone.return_value = ("/var/snap/charmed-postgresql/common/data/temp",)

    assert vm_manager._execute_temp_tablespace_migration("10.1.0.1") is True
    cursor.execute.assert_any_call("DROP TABLESPACE temp;")
    cursor.execute.assert_any_call(
        "CREATE TABLESPACE temp LOCATION '/var/snap/charmed-postgresql/common/data/temp/16/main';"
    )
    cursor.execute.assert_any_call("GRANT CREATE ON TABLESPACE temp TO public;")
    cursor.execute.assert_any_call("CHECKPOINT;")


def test_execute_temp_tablespace_migration_skips_unexpected_location(vm_manager, charm):
    temp_data_dir = MagicMock()
    temp_data_dir.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp/16/main"
    temp_root = MagicMock()
    temp_root.__str__.return_value = "/var/snap/charmed-postgresql/common/data/temp"
    charm.workload.paths.temp = temp_data_dir
    charm.workload.paths.temp.parent = temp_root
    cursor = charm.postgresql._connect_to_database.return_value.cursor.return_value
    cursor.fetchone.return_value = ("/somewhere/else",)

    assert vm_manager._execute_temp_tablespace_migration("10.1.0.1") is True
    cursor.execute.assert_called_once()


def test_check_and_update_internal_cert_regenerates_on_cn_mismatch(vm_manager, charm):
    cert = MagicMock()
    cert.subject.get_attributes_for_oid.return_value = [MagicMock(value="10.9.9.9")]
    charm.state.get_secret.return_value = "raw-cert"
    charm.state.unit_ip = "10.1.0.1"

    with patch(
        "single_kernel_postgresql.managers.refresh.load_pem_x509_certificate",
        return_value=cert,
    ):
        vm_manager.check_and_update_internal_cert()

    charm.tls_manager.generate_internal_peer_cert.assert_called_once()
    charm.tls_manager.push_tls_files.assert_called_once()
    charm.update_config.assert_called_once()


def test_check_and_update_internal_cert_keeps_matching_cert(vm_manager, charm):
    cert = MagicMock()
    cert.subject.get_attributes_for_oid.return_value = [MagicMock(value="10.1.0.1")]
    charm.state.get_secret.return_value = "raw-cert"
    charm.state.unit_ip = "10.1.0.1"

    with patch(
        "single_kernel_postgresql.managers.refresh.load_pem_x509_certificate",
        return_value=cert,
    ):
        vm_manager.check_and_update_internal_cert()

    charm.tls_manager.generate_internal_peer_cert.assert_not_called()
    charm.update_config.assert_not_called()
