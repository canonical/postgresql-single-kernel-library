# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the migrated K8s lifecycle flows in the events handler.

Ports of the K8s charm's pebble-ready, peer-relation, update-status, and
action flows; the pinned status strings must match charm 16/edge exactly.
"""

import json
from sys import modules
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest
from lightkube.core.exceptions import ApiError

# Mock the charm-lib module before importing TLSTransfer (mirrors test_tls_transfer);
# the requirer fake must expose real ops BoundEvents for framework.observe.
from ops import (
    ActiveStatus,
    BlockedStatus,
    EventBase,
    EventSource,
    MaintenanceStatus,
    Object,
    ObjectEvents,
    WaitingStatus,
)
from ops.pebble import ChangeError, PathError, ServiceStatus


class _FakeTransferEvents(ObjectEvents):
    certificate_available = EventSource(EventBase)
    certificate_removed = EventSource(EventBase)


class _FakeCertificateTransferRequires(Object):
    on = _FakeTransferEvents()

    def __init__(self, charm, relation_name):
        super().__init__(charm, "fake-certificate-transfer-requires")


_cert_transfer_lib = MagicMock()
_cert_transfer_lib.CertificateTransferRequires = _FakeCertificateTransferRequires
modules["charms.certificate_transfer_interface.v0.certificate_transfer"] = _cert_transfer_lib

from single_kernel_postgresql.config.enums import Substrates  # noqa: E402
from single_kernel_postgresql.config.exceptions import (  # noqa: E402
    DeployedWithoutTrustError,
    SwitchoverFailedError,
    SwitchoverNotSyncError,
)
from single_kernel_postgresql.config.literals import (  # noqa: E402
    APP_SCOPE,
    EXTENSION_OBJECT_MESSAGE,
    INSUFFICIENT_SIZE_WARNING,
    MONITORING_PASSWORD_KEY,
    PATRONI_PASSWORD_KEY,
    PEER_RELATION,
    REPLICATION_PASSWORD_KEY,
    REWIND_PASSWORD_KEY,
    USER_PASSWORD_KEY,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses  # noqa: E402
from single_kernel_postgresql.core.peer_relation import PostgreSQLApplication  # noqa: E402
from single_kernel_postgresql.events.tls_transfer import TLSTransfer  # noqa: E402
from single_kernel_postgresql.utils.backup import CANNOT_RESTORE_PITR  # noqa: E402
from single_kernel_postgresql.utils.postgresql import (  # noqa: E402
    PostgreSQLCreatePredefinedRolesError,
    PostgreSQLCreateUserError,
    PostgreSQLGrantDatabasePrivilegesToUserError,
    PostgreSQLListUsersError,
)
from tenacity import RetryError  # noqa: E402

TIMELINE_PATCH = "single_kernel_postgresql.utils.postgresql.PostgreSQL.get_current_timeline"


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the K8s lifecycle flows are K8s-only")


@pytest.fixture
def handler(harness):
    handler = harness.charm.postgresql_events_handler
    handler.workload = Mock()
    handler.backup_manager = Mock()
    handler.restore_manager = Mock()
    handler.k8s_manager = Mock()
    handler.charm.tls_transfer = MagicMock()
    handler.charm.tls_transfer.get_ca_secret_names.return_value = iter(())
    return handler


@pytest.fixture
def container(handler):
    return handler.workload.container


def _fake_api_error():
    return ApiError(status=SimpleNamespace(code=403, message="Forbidden"))


def _set_cluster_initialised(harness, rel_id):
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, harness.charm.app.name, {"cluster_initialised": "T"})


def _set_endpoints_with_self(harness, rel_id):
    unit_id = harness.charm.unit.name.split("/")[1]
    endpoints = [f"{harness.charm.app.name}-{unit_id}.{harness.charm.app.name}-endpoints"]
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            harness.charm.app.name,
            {"cluster_initialised": "T", "endpoints": json.dumps(endpoints)},
        )


# -- pebble-ready ------------------------------------------------------------------


def test_pebble_ready_defers_while_refresh_in_progress(handler):
    event = Mock(defer=Mock())
    handler.refresh_manager.refresh = Mock(in_progress=True, workload_allowed_to_start=False)
    handler._on_postgresql_pebble_ready(event)
    event.defer.assert_called_once()


def test_pebble_ready_fixes_pod_when_in_endpoints(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_endpoints_with_self(harness, rel_id)
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = False
    with patch.object(type(handler.charm), "fix_pod") as fix_pod:
        handler._on_postgresql_pebble_ready(event)
    fix_pod.assert_called_once_with()
    event.defer.assert_called_once()


def test_pebble_ready_creates_pgdata_and_defers_for_unready_replica(handler, container):
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = True
    with (
        patch.object(type(handler.charm), "create_pgdata") as create_pgdata,
        patch.object(handler, "_replica_can_start", return_value=False),
    ):
        handler._on_postgresql_pebble_ready(event)
    create_pgdata.assert_called_once_with()
    event.defer.assert_called_once()


def test_pebble_ready_replica_membership_wait_persists_through_collect(
    handler, harness, container
):
    """A replica stuck on the members-list gate surfaces a collect-surviving waiting.

    The silent defer+return left juju reporting active while the replica had
    not joined yet, releasing the deploy's all-active gate mid-join
    (test_symlinks pg_wal symlink failure, CI 2026-10-15).
    """
    rel_id = harness.model.get_relation(PEER_RELATION).id
    harness.set_leader(False)
    _set_cluster_initialised(harness, rel_id)
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = True
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(
            type(handler.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="10.1.1.1",
        ),
    ):
        handler._on_postgresql_pebble_ready(event)
    event.defer.assert_called_once()
    records = handler.state.statuses.get("unit", handler.cluster_manager.name).root
    assert records == [GeneralStatuses.WAITING_FOR_CLUSTER_MEMBERSHIP.value]
    with patch.object(handler.refresh_manager, "reconcile_refresh_status"):
        harness.evaluate_status()
    assert harness.charm.unit.status == WaitingStatus("Waiting for cluster membership")

    # The leader adds this unit's endpoint: re-firing clears the wait.
    _set_endpoints_with_self(harness, rel_id)
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "fix_pod"),
        patch.object(
            type(handler.charm),
            "primary_endpoint",
            new_callable=PropertyMock,
            return_value="10.1.1.1",
        ),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(handler, "_set_active_status"),
        patch.object(
            type(handler.async_replication_manager),
            "async_relation",
            new_callable=PropertyMock,
            return_value=None,
        ),
        patch.object(type(handler.patroni_manager), "is_replication_hba_ready", return_value=True),
    ):
        assert handler._replica_can_start() is True
        handler._on_postgresql_pebble_ready(event)
    event.defer.assert_called_once()
    records = handler.state.statuses.get("unit", handler.cluster_manager.name).root
    assert GeneralStatuses.WAITING_FOR_CLUSTER_MEMBERSHIP.value not in records
    with patch.object(handler.refresh_manager, "reconcile_refresh_status"):
        harness.evaluate_status()
    assert harness.charm.unit.status != WaitingStatus("Waiting for cluster membership")


def test_pebble_ready_generates_internal_cert_chain(handler, harness, container):
    harness.set_leader(False)
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = True
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(handler, "_replica_can_start", return_value=True),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(
            type(handler.state.peer),
            "internal_cert",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(type(handler.tls_manager), "generate_internal_peer_cert") as gen_cert,
        patch.object(type(handler.tls_manager), "push_tls_files") as push_tls,
        patch.object(type(handler.charm), "sync_tls_trust_store_and_bundle") as sync_tls,
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.charm), "update_config", return_value=True),
    ):
        handler._on_postgresql_pebble_ready(event)
    gen_cert.assert_called_once_with()
    push_tls.assert_called_once_with()
    sync_tls.assert_called_once_with()


def test_pebble_ready_defers_when_ca_push_fails(handler, harness, container):
    harness.set_leader(False)
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = True
    handler.charm.tls_transfer.get_ca_secret_names.return_value = iter(["ca-secret"])
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(handler, "_replica_can_start", return_value=True),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.charm),
            "push_ca_file_into_workload",
            side_effect=PathError("not-found", "x"),
        ),
    ):
        handler._on_postgresql_pebble_ready(event)
    event.defer.assert_called_once()


def test_pebble_ready_member_gate_and_leader_bootstrap(handler, harness, container):
    with harness.hooks_disabled():
        harness.set_leader(True)
    event = Mock(workload=container, defer=Mock())
    container.can_connect.return_value = True
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        handler._on_postgresql_pebble_ready(event)
    set_status.assert_called_once_with(WaitingStatus("awaiting for cluster to start"))
    event.defer.assert_called_once()

    event = Mock(workload=container, defer=Mock())
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(handler, "_initialize_cluster", return_value=True) as initialize,
        patch.object(type(handler.charm), "update_config", return_value=True) as update_config,
        patch.object(type(handler.config_manager), "reconcile_extensions") as reconcile,
        patch.object(handler.backup_manager, "start_stop_pgbackrest_service") as pgbackrest,
        patch.object(handler, "_set_active_status") as set_active,
    ):
        handler._on_postgresql_pebble_ready(event)
    initialize.assert_called_once_with(event)
    update_config.assert_called()
    reconcile.assert_called_once()
    pgbackrest.assert_called_once_with()
    set_active.assert_called_once_with()


def test_pebble_ready_rerenders_pgbackrest_conf_when_stanza_exists(handler, harness, container):
    with harness.hooks_disabled():
        harness.set_leader(True)
    event = Mock(workload=container, defer=Mock())
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(type(handler.state), "cluster_stanza", "some-stanza", create=True),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.state.application),
            "is_cluster_restoring_backup",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            type(handler.state.application),
            "is_cluster_restoring_to_time",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(handler, "_initialize_cluster", return_value=True),
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(type(handler.config_manager), "reconcile_extensions"),
        patch.object(handler.backup_manager, "start_stop_pgbackrest_service"),
        patch.object(handler, "_set_active_status"),
    ):
        handler._on_postgresql_pebble_ready(event)
    handler.backup_manager.remove_departed_endpoints.assert_called_once_with()
    handler.backup_manager._render_pgbackrest_conf_file.assert_called_once_with()


def test_pebble_ready_skips_pgbackrest_rerender_without_stanza(handler, harness, container):
    with harness.hooks_disabled():
        harness.set_leader(True)
    event = Mock(workload=container, defer=Mock())
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(type(handler.state), "cluster_stanza", None, create=True),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(handler, "_initialize_cluster", return_value=True),
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(type(handler.config_manager), "reconcile_extensions"),
        patch.object(handler.backup_manager, "start_stop_pgbackrest_service"),
        patch.object(handler, "_set_active_status"),
    ):
        handler._on_postgresql_pebble_ready(event)
    handler.backup_manager.remove_departed_endpoints.assert_not_called()
    handler.backup_manager._render_pgbackrest_conf_file.assert_not_called()


def test_pebble_ready_skips_pgbackrest_rerender_while_restoring(handler, harness, container):
    with harness.hooks_disabled():
        harness.set_leader(True)
    event = Mock(workload=container, defer=Mock())
    with (
        patch.object(type(handler.charm), "create_pgdata"),
        patch.object(type(handler.charm), "update_pebble_layers"),
        patch.object(type(handler.state), "get_secret", return_value="ca"),
        patch.object(type(handler.state), "cluster_stanza", "some-stanza", create=True),
        patch.object(
            type(handler.state.peer), "internal_cert", new_callable=PropertyMock, return_value=True
        ),
        patch.object(
            type(handler.state.application),
            "is_cluster_restoring_backup",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(handler, "_initialize_cluster", return_value=True),
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(type(handler.config_manager), "reconcile_extensions"),
        patch.object(handler.backup_manager, "start_stop_pgbackrest_service"),
        patch.object(handler, "_set_active_status"),
    ):
        handler._on_postgresql_pebble_ready(event)
    handler.backup_manager.remove_departed_endpoints.assert_called_once_with()
    handler.backup_manager._render_pgbackrest_conf_file.assert_not_called()


# -- update-status family ----------------------------------------------------------


def test_update_status_blocked_non_s3_fixes_leader_annotation(handler, container):
    handler.state.model.unit.status = BlockedStatus()
    with patch.object(type(handler.charm), "fix_leader_annotation") as fix_annotation:
        assert handler._on_update_status_early_exit_checks(container) is False
    fix_annotation.assert_called_once_with()


def test_update_status_retries_extension_disable(handler, container):
    handler.state.model.unit.status = BlockedStatus(EXTENSION_OBJECT_MESSAGE)
    with patch.object(type(handler.config_manager), "reconcile_extensions") as reconcile:
        assert handler._on_update_status_early_exit_checks(container) is True
    reconcile.assert_called_once()


def test_check_pgdata_storage_size_blocks_below_ten_percent(handler):
    handler.state.model.unit.status = ActiveStatus()
    with (
        patch(
            "single_kernel_postgresql.events.postgresql.shutil.disk_usage",
            return_value=(100, 40, 5),
        ),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        handler._check_pgdata_storage_size()
    set_status.assert_called_once_with(BlockedStatus(INSUFFICIENT_SIZE_WARNING))


def test_set_active_status_pins_charm_strings(handler):
    handler.state.model.unit.status = ActiveStatus()
    patroni = handler.patroni_manager
    with (
        patch.object(type(handler.state.model.unit), "is_leader", return_value=True),
        patch.object(type(patroni), "get_primary", return_value=handler.state.model.unit.name),
        patch.object(type(patroni), "get_standby_leader", return_value=None),
        patch.object(type(patroni), "get_running_cluster_members", return_value=["a", "b"]),
        patch.object(
            PostgreSQLApplication, "planned_units", new_callable=PropertyMock, return_value=2
        ),
    ):
        handler._set_active_status()
    assert handler.state.model.unit.status == ActiveStatus("Primary")


def test_set_active_status_degraded_when_members_below_planned(handler):
    handler.state.model.unit.status = ActiveStatus()
    patroni = handler.patroni_manager
    with (
        patch.object(type(handler.state.model.unit), "is_leader", return_value=True),
        patch.object(type(patroni), "get_primary", return_value=handler.state.model.unit.name),
        patch.object(type(patroni), "get_standby_leader", return_value=None),
        patch.object(type(patroni), "get_running_cluster_members", return_value=["a"]),
        patch.object(
            PostgreSQLApplication, "planned_units", new_callable=PropertyMock, return_value=2
        ),
    ):
        handler._set_active_status()
    assert handler.state.model.unit.status == ActiveStatus("Primary (degraded)")


def test_set_active_status_error_logs_and_keeps_status(handler):
    handler.state.model.unit.status = ActiveStatus()
    patroni = handler.patroni_manager
    with (
        patch.object(type(handler.state.model.unit), "is_leader", return_value=False),
        patch.object(type(patroni), "get_primary", side_effect=RetryError("boom")),
    ):
        handler._set_active_status()
    assert handler.state.model.unit.status == ActiveStatus()


def test_update_status_inactive_service_restart_order(handler, container):
    container.pebble.get_services.return_value = [Mock(current=ServiceStatus.INACTIVE)]
    container.can_connect.return_value = True
    order = Mock()
    with (
        patch.object(type(handler.charm), "ensure_pgdata_dirs_and_symlinks") as ensure_dirs,
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(handler.patroni_manager), "get_primary", return_value=None),
    ):
        order.attach_mock(ensure_dirs, "ensure_dirs")
        order.attach_mock(container.restart, "restart")
        handler._on_update_status(Mock())
    assert order.mock_calls == [call.ensure_dirs(), call.restart("postgresql")]


def test_update_status_restarts_and_sets_maintenance_when_member_down(handler, container):
    container.pebble.get_services.return_value = [Mock(current=ServiceStatus.INACTIVE)]
    container.can_connect.return_value = True
    with (
        patch.object(type(handler.charm), "ensure_pgdata_dirs_and_symlinks"),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        container.restart.side_effect = ChangeError(change=None, err=None)
        handler._on_update_status(Mock())
    set_status.assert_called_once_with(MaintenanceStatus("Database service inactive, restarting"))


def test_update_status_restore_gate_runs_completion(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"restoring-backup": "20230101-090000F"}
        )
    service = Mock(current=ServiceStatus.ACTIVE)
    container.pebble.get_services.return_value = [service]
    container.can_connect.return_value = True
    with patch.object(handler, "_was_restore_successful_k8s", return_value=False) as restore:
        handler._on_update_status(Mock())
    restore.assert_called_once_with(container, service)


def test_was_restore_successful_clears_flags_and_revalidates_s3(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            harness.charm.app.name,
            {
                "restoring-backup": "20230101-090000F",
                "restore-stanza": "stanza",
                "restore-timeline": "2",
            },
        )
    service = Mock(current=ServiceStatus.ACTIVE)
    with (
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch(TIMELINE_PATCH, return_value="3"),
        patch.object(type(handler.charm), "update_config", return_value=True) as update_config,
        patch.object(handler.restore_manager, "restore_patroni_restart_condition") as restore_cond,
        patch.object(
            handler.backup_manager, "can_use_s3_repository", return_value=(True, None)
        ) as can_use,
        patch.object(type(handler.cluster_manager), "setup_instance_users_k8s") as setup_users,
    ):
        assert handler._was_restore_successful_k8s(container, service) is True
    update_config.assert_called_once()
    restore_cond.assert_called_once_with()
    can_use.assert_called_once()
    setup_users.assert_called_once()
    assert handler.state.application.data.get("restoring-backup", "") == ""


def test_was_restore_successful_blocks_on_pitr_failure(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"restore-to-time": "2023-01-01"}
        )
    service = Mock(current=ServiceStatus.INACTIVE)
    with (
        patch.object(handler.restore_manager, "is_pitr_failed", return_value=(True, True)),
        patch.object(handler.restore_manager, "log_pitr_last_transaction_time") as log_pitr,
    ):
        assert handler._was_restore_successful_k8s(container, service) is False
    log_pitr.assert_called_once_with()
    # The handler fixture replaces restore_manager with a Mock: the blocked
    # CANNOT_RESTORE_PITR must be persisted through the status machinery
    # (RestoreManager.block_on_pitr_failure), not written directly.
    handler.restore_manager.block_on_pitr_failure.assert_called_once_with()


# -- peer-relation flows -----------------------------------------------------------


def test_peer_relation_changed_initialises_on_leader(handler, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    event = Mock(defer=Mock())
    # A successful bootstrap defers: the full flow re-runs on the re-emitted event.
    with patch.object(handler, "_initialize_cluster", return_value=True) as initialize:
        handler._on_peer_relation_changed(event)
    initialize.assert_called_once_with(event)
    event.defer.assert_called_once()


def test_peer_relation_changed_non_leader_uninitialised_is_silent(handler):
    with patch.object(handler, "_initialize_cluster") as initialize:
        handler._on_peer_relation_changed(Mock(defer=Mock()))
    initialize.assert_not_called()


def test_peer_relation_changed_leader_adds_members(handler, harness):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_cluster_initialised(harness, rel_id)
    with harness.hooks_disabled():
        harness.set_leader(True)
    with patch.object(type(handler.membership_manager), "add_members_k8s") as add_members:
        handler._on_peer_relation_changed(Mock(defer=Mock()))
    add_members.assert_called_once()


def test_peer_relation_changed_endpoint_gate_and_update_config(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_endpoints_with_self(harness, rel_id)
    container.can_connect.return_value = True
    with (
        patch.object(
            type(handler.charm), "update_config", side_effect=ValueError("bad")
        ) as update_config,
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        handler._on_peer_relation_changed(Mock(defer=Mock()))
    update_config.assert_called_once()
    set_status.assert_called_once_with(BlockedStatus("Configuration Error. Please check the logs"))


def test_peer_relation_changed_member_start_gate_defers(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_endpoints_with_self(harness, rel_id)
    container.can_connect.return_value = True
    event = Mock(defer=Mock())
    with (
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        handler._on_peer_relation_changed(event)
    set_status.assert_called_once_with(WaitingStatus("awaiting for member to start"))
    event.defer.assert_called_once()


def test_peer_relation_changed_tls_server_ping_roundtrip(handler, harness, container):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_endpoints_with_self(harness, rel_id)
    container.can_connect.return_value = True
    handler.backup_manager.start_stop_pgbackrest_service.return_value = False
    with (
        patch.object(type(handler.charm), "update_config", return_value=True),
        patch.object(
            type(handler.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(handler.patroni_manager), "get_primary", return_value=None),
        patch.object(handler.membership_manager, "set_unit_status"),
        patch.object(handler.database_manager, "update_endpoints"),
    ):
        event = Mock(defer=Mock())
        handler._on_peer_relation_changed(event)
        assert handler.state.peer.data.get("start-tls-server") == "True"
        event.defer.assert_called_once()

        handler.backup_manager.start_stop_pgbackrest_service.return_value = True
        event = Mock(defer=Mock())
        handler._on_peer_relation_changed(event)
        assert "start-tls-server" not in handler.state.peer.data
        handler.backup_manager.coordinate_stanza_fields.assert_called_with()


def test_peer_relation_departed_leader_removes_endpoints(handler, harness):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_cluster_initialised(harness, rel_id)
    with harness.hooks_disabled():
        harness.set_leader(True)
    membership = handler.membership_manager
    event = Mock(departing_unit=MagicMock(__eq__=MagicMock(return_value=False)))
    with (
        patch.object(type(membership), "updated_synchronous_node_count", return_value=True),
        patch.object(type(membership), "get_endpoints_to_remove", return_value=["ep-1"]),
        patch.object(type(membership), "remove_endpoint") as remove_endpoint,
        patch.object(handler.database_manager, "update_endpoints") as update_endpoints,
        patch.object(
            type(handler.async_replication_manager), "update_async_replication_data"
        ) as update_async,
    ):
        handler._on_peer_relation_departed(event)
    update_endpoints.assert_called_once()
    remove_endpoint.assert_called_once_with("ep-1")
    update_async.assert_called_once_with()


def test_pgdata_storage_detaching_switches_over_then_updates_endpoints(handler):
    membership = handler.membership_manager
    patroni = handler.patroni_manager
    unit_name = handler.state.model.unit.name
    with (
        patch.object(type(patroni), "get_primary", return_value=unit_name) as get_primary,
        patch.object(type(patroni), "are_all_members_ready", return_value=True),
        patch.object(type(patroni), "switchover") as switchover,
        patch.object(type(patroni), "primary_changed", return_value=True),
        patch.object(type(membership), "get_endpoints_to_remove", return_value=["ep-1"]),
        patch.object(type(membership), "remove_endpoint") as remove_endpoint,
        patch.object(handler.database_manager, "update_endpoints") as update_endpoints,
        patch.object(
            type(handler.tls_manager), "get_client_tls_files", return_value=(None, None, None)
        ),
    ):
        handler._on_pgdata_storage_detaching(Mock())
    get_primary.assert_called_once_with(unit_name_pattern=True)
    switchover.assert_called_once_with()
    update_endpoints.assert_called_once()
    remove_endpoint.assert_called_once_with("ep-1")


# -- initialization ----------------------------------------------------------------


def test_initialize_cluster_patches_pod_and_creates_services(handler):
    handler.refresh_manager.refresh = Mock(in_progress=False)
    k8s_manager = handler.k8s_manager
    with (
        patch.object(k8s_manager, "patch_pod_labels") as patch_pod,
        patch.object(k8s_manager, "create_services") as create_services,
        patch.object(
            type(handler.async_replication_manager), "get_primary_cluster", return_value=None
        ),
        patch.object(
            type(handler.patroni_manager),
            "primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(handler.cluster_manager), "setup_instance_users_k8s") as setup_users,
    ):
        assert handler._initialize_cluster(Mock()) is True
    patch_pod.assert_called_once()
    create_services.assert_called_once_with()
    setup_users.assert_called_once()
    assert "cluster_initialised" in handler.state.application.data


def test_initialize_cluster_blocked_on_pod_patch_failure(handler):
    handler.refresh_manager.refresh = Mock(in_progress=False)
    with (
        patch.object(handler.k8s_manager, "patch_pod_labels", side_effect=_fake_api_error()),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        assert handler._initialize_cluster(Mock()) is False
    status = set_status.call_args[0][0]
    assert isinstance(status, BlockedStatus)
    assert status.message.startswith("failed to patch pod with error")


def test_initialize_cluster_defers_until_primary_endpoint_ready(handler):
    handler.refresh_manager.refresh = Mock(in_progress=False)
    event = Mock(defer=Mock())
    with (
        patch.object(handler.k8s_manager, "patch_pod_labels"),
        patch.object(
            type(handler.async_replication_manager), "get_primary_cluster", return_value=None
        ),
        patch.object(
            type(handler.patroni_manager),
            "primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(handler.membership_manager, "set_unit_status") as set_status,
    ):
        assert handler._initialize_cluster(event) is False
    set_status.assert_called_once_with(WaitingStatus("awaiting for primary endpoint to be ready"))
    event.defer.assert_called_once()


def test_initialize_cluster_setup_user_error_statuses(handler):
    handler.refresh_manager.refresh = Mock(in_progress=False)
    for exception, expected in (
        (PostgreSQLCreatePredefinedRolesError, "Failed to create pre-defined roles"),
        (
            PostgreSQLGrantDatabasePrivilegesToUserError,
            "Failed to grant database privileges to user",
        ),
        (PostgreSQLCreateUserError, "Failed to create postgres user"),
    ):
        with (
            patch.object(handler.k8s_manager, "patch_pod_labels"),
            patch.object(
                type(handler.async_replication_manager), "get_primary_cluster", return_value=None
            ),
            patch.object(
                type(handler.patroni_manager),
                "primary_endpoint_ready",
                new_callable=PropertyMock,
                return_value=True,
            ),
            patch.object(
                type(handler.cluster_manager), "setup_instance_users_k8s", side_effect=exception
            ),
            patch.object(handler.membership_manager, "set_unit_status") as set_status,
        ):
            assert handler._initialize_cluster(Mock()) is False
        assert set_status.call_args[0][0] == BlockedStatus(expected)


def test_initialize_cluster_defers_when_users_cannot_be_listed(handler):
    handler.refresh_manager.refresh = Mock(in_progress=False)
    event = Mock(defer=Mock())
    with (
        patch.object(handler.k8s_manager, "patch_pod_labels"),
        patch.object(
            type(handler.async_replication_manager), "get_primary_cluster", return_value=None
        ),
        patch.object(
            type(handler.patroni_manager),
            "primary_endpoint_ready",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            type(handler.cluster_manager),
            "setup_instance_users_k8s",
            side_effect=PostgreSQLListUsersError,
        ),
        patch.object(handler.membership_manager, "set_unit_status"),
    ):
        assert handler._initialize_cluster(event) is False
    event.defer.assert_called_once()


def test_replica_can_start_gates(handler, harness):
    patroni = handler.patroni_manager
    rel_id = harness.model.get_relation(PEER_RELATION).id
    assert handler._replica_can_start() is False
    _set_endpoints_with_self(harness, rel_id)
    with (
        patch.object(
            type(handler.async_replication_manager),
            "async_relation",
            new_callable=PropertyMock,
            return_value=None,
        ),
        patch.object(type(patroni), "is_replication_hba_ready", return_value=True),
    ):
        assert handler._replica_can_start() is True


# -- actions and misc ----------------------------------------------------------------


def test_promote_cluster_scope_delegates_to_async_manager(handler):
    with patch.object(type(handler.async_replication_manager), "promote_to_primary") as promote:
        handler._on_promote_to_primary(Mock(params={"scope": "cluster"}))
    promote.assert_called_once()


def test_promote_unit_scope_switches_over(handler):
    with patch.object(type(handler.patroni_manager), "switchover") as switchover:
        handler._on_promote_to_primary(Mock(params={"scope": "unit"}))
    switchover.assert_called_once_with(handler.state.model.unit.name, wait=False)


def test_promote_unit_scope_failure_messages(handler):
    for switchover_side_effect, params, expected in (
        (SwitchoverNotSyncError, {"scope": "unit"}, "Unit is not sync standby"),
        (
            SwitchoverFailedError,
            {"scope": "unit"},
            "Switchover failed or timed out, check the logs for details",
        ),
        (None, {"scope": "unit", "force": True}, "Suprerfluous force flag with unit scope"),
    ):
        event = Mock(params=params)
        with patch.object(
            type(handler.patroni_manager), "switchover", side_effect=switchover_side_effect
        ):
            handler._on_promote_to_primary(event)
        event.fail.assert_called_once_with(expected)

    event = Mock(params={"scope": "bad"})
    handler._on_promote_to_primary(event)
    event.fail.assert_called_once_with("Scope should be either cluster or unit")


def test_get_primary_action_reports_primary(handler):
    with patch.object(type(handler.patroni_manager), "get_primary", return_value="app-0"):
        event = Mock(set_results=Mock())
        handler._on_get_primary(event)
    event.set_results.assert_called_once_with({"primary": "app-0"})


def test_get_primary_action_logs_retry_error(handler):
    with patch.object(type(handler.patroni_manager), "get_primary", side_effect=RetryError("x")):
        event = Mock(set_results=Mock())
        handler._on_get_primary(event)
    event.set_results.assert_not_called()


def test_upgrade_charm_defers_on_fix_pod_failure(handler):
    event = Mock(defer=Mock())
    with patch.object(type(handler.charm), "fix_pod", side_effect=RuntimeError("boom")):
        handler._on_upgrade_charm(event)
    event.defer.assert_called_once()


def test_on_stop_reparents_services(handler):
    with (
        patch.object(
            PostgreSQLApplication, "planned_units", new_callable=PropertyMock, return_value=0
        ),
        patch.object(handler.k8s_manager, "reparent_services_on_stop") as reparent,
    ):
        handler.state.peer.data["stopped-marker"] = "1"
        handler._on_stop(Mock())
    reparent.assert_called_once_with()
    assert "stopped-marker" not in handler.state.peer.data


def test_on_start_touches_ca_bundle_only(handler):
    with (
        patch("single_kernel_postgresql.events.postgresql.Path") as path,
        patch.object(handler, "_start_primary") as start_primary,
    ):
        handler._on_start(Mock())
    path.assert_called_once()
    path.return_value.touch.assert_called_once_with()
    start_primary.assert_not_called()


# -- leader-elected ----------------------------------------------------------------


def _seed_leader(handler, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    handler.charm.fix_leader_annotation = Mock(return_value=True)


def test_leader_elected_k8s_runs_the_full_flow(patch_crypto, handler, harness):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _seed_leader(handler, harness)
    with patch.object(handler.membership_manager, "add_members_k8s") as add_members:
        handler._on_leader_elected(Mock())

    for key in (
        USER_PASSWORD_KEY,
        REPLICATION_PASSWORD_KEY,
        REWIND_PASSWORD_KEY,
        MONITORING_PASSWORD_KEY,
        PATRONI_PASSWORD_KEY,
    ):
        assert handler.state.get_secret(APP_SCOPE, key)
    endpoints = json.loads(harness.get_relation_data(rel_id, harness.charm.app.name)["endpoints"])
    assert handler.state.endpoint in endpoints
    handler.k8s_manager.cleanup_old_cluster_resources.assert_called_once()
    handler.k8s_manager.create_services.assert_called_once()
    add_members.assert_called_once()


def test_leader_elected_k8s_blocks_when_service_creation_fails(handler, harness):
    _seed_leader(handler, harness)
    handler.k8s_manager.create_services.side_effect = _fake_api_error()
    with patch.object(handler.membership_manager, "add_members_k8s") as add_members:
        handler._on_leader_elected(Mock())
    assert harness.charm.unit.status.name == "blocked"
    assert harness.charm.unit.status.message == "failed to create k8s services"
    add_members.assert_not_called()


def test_leader_elected_k8s_stops_before_services_when_annotation_fix_fails(handler, harness):
    _seed_leader(handler, harness)
    handler.charm.fix_leader_annotation = Mock(return_value=False)
    with patch.object(handler.membership_manager, "add_members_k8s") as add_members:
        handler._on_leader_elected(Mock())
    handler.charm.fix_leader_annotation.assert_called_once()
    handler.k8s_manager.create_services.assert_not_called()
    add_members.assert_not_called()


def test_leader_elected_k8s_skips_service_creation_during_refresh(handler, harness):
    _seed_leader(handler, harness)
    handler.refresh_manager.refresh.in_progress = True
    event = Mock()
    with patch.object(handler.membership_manager, "add_members_k8s") as add_members:
        handler._on_leader_elected(event)
    handler.k8s_manager.create_services.assert_not_called()
    add_members.assert_called_once()
    event.defer.assert_not_called()


# -- restart -----------------------------------------------------------------------


def test_restart_k8s_reconciles_the_pebble_layers(handler):
    handler.patroni_manager = Mock()
    handler.cluster_manager = Mock()
    handler.postgresql = Mock()
    event = Mock()
    handler._restart(event)
    handler.workload.update_pebble_layers.assert_called_once()
    assert handler.state.peer.data.get("postgresql_restarted") is None
    event.defer.assert_not_called()


def test_leader_elected_k8s_blocks_without_trust_and_continues(handler, harness):
    """A 403 during the resource cleanup blocks the unit; the flow still proceeds."""
    _seed_leader(handler, harness)
    handler.k8s_manager.cleanup_old_cluster_resources.side_effect = DeployedWithoutTrustError()
    on_deployed = patch.object(handler.charm, "on_deployed_without_trust").start()
    with patch.object(handler.membership_manager, "add_members_k8s") as add_members:
        handler._on_leader_elected(Mock())
    on_deployed.assert_called_once()
    handler.k8s_manager.create_services.assert_called_once()
    add_members.assert_called_once()


# -- replica-can-start -------------------------------------------------------------


def test_replica_can_start_probes_the_primary_endpoint(handler, harness):
    """The HBA probe targets the primary's endpoint (trunk parity).

    The port originally passed the primary Service DNS name; trunk probes the
    primary member's endpoint, and the Service form wedged replica starts
    during the scaled-up relation churn (test_new_relations_1, CI 2026-10-13).
    """
    rel_id = harness.model.get_relation(PEER_RELATION).id
    _set_endpoints_with_self(harness, rel_id)
    handler.patroni_manager = Mock()
    handler.patroni_manager.is_replication_hba_ready.return_value = True
    with patch.object(
        type(handler.charm), "primary_endpoint", new_callable=PropertyMock, return_value="10.1.1.1"
    ):
        assert handler._replica_can_start() is True
    handler.patroni_manager.is_replication_hba_ready.assert_called_once_with("10.1.1.1")
