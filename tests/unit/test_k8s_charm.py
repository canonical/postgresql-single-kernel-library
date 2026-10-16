# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the completed K8s charm class bridges."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest
from lightkube.core.exceptions import ApiError
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from ops.pebble import ChangeError, ServiceStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import DeployedWithoutTrustError
from single_kernel_postgresql.config.literals import (
    CONFIGURATION_ERROR_MESSAGE,
    METRICS_PORT,
    PEER_RELATION,
    PGBACKREST_METRICS_PORT,
)
from single_kernel_postgresql.utils.backup import CANNOT_RESTORE_PITR


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the K8s charm class tests are K8s-only")


@pytest.fixture
def charm(harness):
    return harness.charm


@pytest.fixture
def container(charm):
    workload = MagicMock()
    with patch.object(type(charm), "workload", new_callable=PropertyMock, return_value=workload):
        yield workload.container


def _fake_api_error(code):
    return ApiError(status=SimpleNamespace(code=code, message="fake"))


def _set_initialised(harness):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, harness.charm.app.name, {"cluster_initialised": "T"})
    return rel_id


@pytest.fixture
def resuming_refresh(monkeypatch):
    """Construct charm_refresh.Kubernetes so its resume runs during charm init."""
    seen = {"lock_calls": 0}

    class ResumingKubernetes:
        in_progress = True
        next_unit_allowed_to_refresh = True
        workload_allowed_to_start = True
        pinned_oci_image = "charmed-postgresql-16.0"
        app_status_higher_priority = None
        unit_status_higher_priority = None

        def __init__(self, charm_specific, /):
            seen["lock_calls"] += 1
            # Reproduce the action path: the construction-time state machine may
            # reach request_restart before the subclass assigned its bridges.
            charm_specific._charm.request_restart()

        def unit_status_lower_priority(self, *, workload_is_running=True):
            return None

    monkeypatch.setattr("charm_refresh.Kubernetes", ResumingKubernetes)
    return seen


# -- status bridges ----------------------------------------------------------------


def test_refresh_resume_during_init_request_restart_needs_no_lock_yet(resuming_refresh, charm):
    """charm_refresh.Kubernetes runs its state machine AT construction time.

    A force-refresh-start reaches request_restart INSIDE the abstract
    __init__ (via the refresh manager), before the substrate subclass
    assigns restart_lock_acquire: the guarded read must not raise
    AttributeError, and the injected lock (assigned post-init) must be the
    only invocation.
    """
    assert resuming_refresh["lock_calls"] == 1  # constructor ran request_restart
    assert charm.restart_lock_acquire is None  # class default, nothing injected yet

    lock = Mock()
    charm.restart_lock_acquire = lock
    with patch.object(
        type(charm.tls_manager), "get_client_tls_files", return_value=("k", "ca", "c")
    ):
        charm.request_restart()
    lock.assert_called_once_with()


def test_set_unit_status_respects_higher_refresh_priority(charm):
    charm.unit.status = ActiveStatus()
    charm.refresh_manager.refresh = Mock(unit_status_higher_priority=MaintenanceStatus("r"))
    charm.set_unit_status(ActiveStatus())
    assert charm.unit.status == ActiveStatus()


def test_async_subsystem_watcher_stays_none_on_k8s(charm):
    """The K8s substrate has no watcher bridge for the async subsystem (trunk parity)."""
    assert charm.async_replication_manager.watcher is None
    assert charm.async_replication.watcher is None


def test_set_app_status_gates(charm, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    charm.refresh_manager.can_set_app_status = False
    charm.set_app_status(BlockedStatus("nope"))
    assert charm.app.status != BlockedStatus("nope")

    charm.refresh_manager.can_set_app_status = True
    charm.refresh_manager.refresh = Mock(app_status_higher_priority=ActiveStatus("refresh"))
    charm.set_app_status(BlockedStatus("x"))
    assert charm.app.status == ActiveStatus("refresh")


def test_set_primary_status_message_delegates_to_events(charm):
    with patch.object(charm.postgresql_events_handler, "_set_active_status") as set_active:
        charm.set_primary_status_message()
    set_active.assert_called_once_with()


# -- update_config -----------------------------------------------------------------


def test_update_config_delegates_to_config_manager(charm):
    charm.refresh_manager.refresh = None
    with (
        patch.object(type(charm.config_manager), "update_config", return_value=True) as update,
        patch.object(
            type(charm.config_manager),
            "build_relations_user_databases_map",
            return_value={"u": "db"},
        ),
        patch.object(
            type(charm.async_replication_manager),
            "get_primary_cluster_endpoint",
            return_value="endpoint-1",
        ),
        patch.object(
            type(charm.async_replication_manager), "get_standby_endpoints", return_value=["sb"]
        ),
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
    ):
        assert charm.update_config() is True
    kwargs = update.call_args.kwargs
    assert kwargs["relations_user_databases_map"] == {"u": "db"}
    assert kwargs["async_primary_cluster_endpoint"] == "endpoint-1"
    assert kwargs["async_standby_endpoints"] == ["sb"]


def test_update_config_catches_deployed_without_trust(charm):
    charm.refresh_manager.refresh = None
    with patch.object(
        type(charm.config_manager), "update_config", side_effect=DeployedWithoutTrustError
    ):
        assert charm.update_config() is False
    assert isinstance(charm.unit.status, BlockedStatus)
    assert "juju trust" in charm.unit.status.message


def test_update_config_clears_stale_standby_when_primary(charm):
    charm.refresh_manager.refresh = None
    with (
        patch.object(type(charm.config_manager), "update_config", return_value=True),
        patch.object(
            type(charm.async_replication_manager),
            "get_primary_cluster_endpoint",
            return_value=None,
        ),
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            type(charm.patroni_manager), "bulk_update_parameters_controller_by_patroni"
        ) as bulk,
    ):
        charm.update_config()
    bulk.assert_called_once_with({}, {"standby_cluster": None})


def test_update_config_keeps_standby_when_not_primary(charm):
    charm.refresh_manager.refresh = None
    with (
        patch.object(type(charm.config_manager), "update_config", return_value=True),
        patch.object(
            type(charm.async_replication_manager),
            "get_primary_cluster_endpoint",
            return_value="other-cluster",
        ),
        patch.object(
            type(charm.patroni_manager), "bulk_update_parameters_controller_by_patroni"
        ) as bulk,
    ):
        charm.update_config()
    bulk.assert_not_called()


# -- restart seams -----------------------------------------------------------------


def test_request_restart_refreshes_scrape_jobs_and_acquires_lock(charm):
    charm.metrics_endpoint = Mock()
    lock = Mock()
    charm.restart_lock_acquire = lock
    with patch.object(
        type(charm.tls_manager), "get_client_tls_files", return_value=("k", "ca", "c")
    ):
        charm.request_restart()
    jobs = charm.metrics_endpoint.update_scrape_job_spec.call_args[0][0]
    assert jobs == [
        {"static_configs": [{"targets": [f"*:{METRICS_PORT}"]}]},
        {"static_configs": [{"targets": [f"*:{PGBACKREST_METRICS_PORT}"]}]},
        {
            "static_configs": [{"targets": ["*:8008"]}],
            "scheme": "https",
            "tls_config": {"insecure_skip_verify": True},
        },
    ]
    lock.assert_called_once_with()


def test_request_restart_noops_without_injections(charm):
    charm.metrics_endpoint = None
    charm.restart_lock_acquire = None
    charm.request_restart()


def test_restart_services_restarts_rotated_metrics(charm, container):
    charm.metrics_endpoint = Mock()
    service = Mock()
    service.environment = {"DATA_SOURCE_PASS": "stale"}
    container.get_plan.return_value = MagicMock(services={"metrics_server": service})
    with (
        patch.object(type(charm.state), "get_secret", return_value="fresh"),
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=False,
        ),
    ):
        charm.restart_services()
    container.restart.assert_called_once_with("metrics_server")


def test_restart_services_stops_ldap_on_replica(charm, container):
    replica_service = MagicMock()
    replica_service.is_running.return_value = True
    container.pebble.get_services.return_value = [replica_service]
    container.get_plan.return_value = MagicMock(
        services={"metrics_server": Mock(environment={"DATA_SOURCE_PASS": "same"})}
    )
    with (
        patch.object(type(charm.state), "get_secret", return_value="same"),
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(charm.patroni_manager), "get_primary", return_value="other/0"),
    ):
        charm.restart_services()
    container.stop.assert_called_once_with("ldap-sync")


# -- config-changed / databases-changed --------------------------------------------


def test_on_databases_change_updates_config_and_records_timestamp(charm):
    """The shared observer handler records the K8s pg-hba timestamp key."""
    with patch.object(type(charm), "update_config", return_value=True) as update:
        charm.on.databases_change.emit()
    update.assert_called_once_with()
    assert "pg_hba_needs_update_timestamp" in charm.state.peer.data


def test_on_config_changed_defers_until_initialised(charm):
    event = Mock(defer=Mock())
    charm._on_config_changed(event)
    event.defer.assert_called_once_with()


def test_on_config_changed_blocks_on_invalid_option(charm, harness):
    _set_initialised(harness)
    charm.refresh_manager.refresh = None
    with patch.object(
        type(charm.config_manager), "validate_config_options", side_effect=ValueError("bad")
    ):
        charm._on_config_changed(Mock(defer=Mock()))
    assert charm.unit.status == BlockedStatus("Configuration Error. Please check the logs")
    assert charm.state.peer.data["configuration-error"] == CONFIGURATION_ERROR_MESSAGE
    records = charm.state.statuses.get("unit", charm.config_manager.name).root
    assert any(
        r.status == "blocked" and "Configuration Error" in (r.message or "") for r in records
    )


def test_config_error_survives_collect_and_recompute(charm, harness):
    """The config-error blocked must survive the collect AND the recompute.

    Mirror of the VM test: ops applies the collect result last, so the
    durable peer-databag flag is what keeps the blocked visible in juju
    status until the config validates.
    """
    _set_initialised(harness)
    charm.refresh_manager.refresh = None
    with patch.object(
        type(charm.config_manager), "validate_config_options", side_effect=ValueError("bad")
    ):
        charm._on_config_changed(Mock(defer=Mock()))

    # Patch the live collectors: the emit walks every manager's get_statuses,
    # which would reach the snap/charm_refresh surface the unit env cannot build.
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
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus("Configuration Error. Please check the logs")


def test_on_config_changed_gates_on_apply_changed_config(charm, harness):
    """Config-changed must run the logical-replication config gate and honor its veto."""
    _set_initialised(harness)
    charm.refresh_manager.refresh = None
    with harness.hooks_disabled():
        harness.set_leader(True)
    with (
        patch.object(type(charm), "update_config"),
        patch.object(type(charm.config_manager), "validate_config_options"),
        patch.object(type(charm.config_manager), "reconcile_extensions") as reconcile,
        patch.object(
            charm.logical_replication, "apply_changed_config", return_value=False
        ) as apply_config,
    ):
        charm._on_config_changed(Mock(defer=Mock()))
    apply_config.assert_called_once()
    reconcile.assert_not_called()

    apply_config.return_value = True
    with (
        patch.object(type(charm), "update_config"),
        patch.object(type(charm.config_manager), "validate_config_options"),
        patch.object(type(charm.config_manager), "reconcile_extensions") as reconcile,
        patch.object(charm.logical_replication, "apply_changed_config", return_value=True),
    ):
        charm._on_config_changed(Mock(defer=Mock()))
    reconcile.assert_called_once()


# -- leader annotation / pebble layers ---------------------------------------------


def test_fix_leader_annotation_patches_missing_annotation(charm):
    endpoint = Mock()
    endpoint.metadata.annotations = {}
    client = Mock()
    client.get.return_value = endpoint
    with patch("single_kernel_postgresql.charms.k8s_charm.Client", return_value=client):
        assert charm.fix_leader_annotation() is True
    client.patch.assert_called_once()


def test_fix_leader_annotation_blocks_without_trust(charm):
    client = Mock()
    client.get.side_effect = _fake_api_error(403)
    with patch("single_kernel_postgresql.charms.k8s_charm.Client", return_value=client):
        assert charm.fix_leader_annotation() is False
    assert isinstance(charm.unit.status, BlockedStatus)


def test_fix_leader_annotation_ignores_missing_resource(charm):
    client = Mock()
    client.get.side_effect = _fake_api_error(404)
    with patch("single_kernel_postgresql.charms.k8s_charm.Client", return_value=client):
        assert charm.fix_leader_annotation() is True


def test_update_pebble_layers_guards_replan_failure(charm):
    workload = MagicMock()
    workload.update_pebble_layers.side_effect = ChangeError(
        "replan failed", Mock(tasks=[Mock(log=[])])
    )
    with patch.object(type(charm), "workload", new_callable=PropertyMock, return_value=workload):
        charm.update_pebble_layers()


def test_reload_tls_after_push_syncs_then_reloads(charm):
    order = Mock()
    event = Mock(defer=Mock())
    with (
        patch.object(charm, "sync_tls_trust_store_and_bundle") as sync,
        patch.object(type(charm), "update_config", return_value=True) as update,
    ):
        order.attach_mock(sync, "sync")
        order.attach_mock(update, "update")
        charm._reload_tls_after_push(event)
    assert order.mock_calls == [call.sync(), call.update()]
    event.defer.assert_not_called()

    with (
        patch.object(charm, "sync_tls_trust_store_and_bundle"),
        patch.object(type(charm), "update_config", return_value=False),
    ):
        charm._reload_tls_after_push(event)
    event.defer.assert_called_once_with()


def test_install_is_not_observed_on_k8s(charm):
    """The K8s charm has no install hook; the observe is VM-only."""
    with patch.object(charm.cluster_manager, "install_workload") as install:
        charm.on.install.emit()
    install.assert_not_called()


def test_pitr_failure_persists_blocked_through_restore_check(charm, harness):
    """The failed-PITR blocked must surface through the status machinery.

    Mirror of the VM test: trunk (c4f378dda) wrote the blocked directly; the
    migration's collect replay wiped it with the restore manager's maintenance
    "restoring backup", so the unit never reached CANNOT_RESTORE_PITR.
    """
    _set_initialised(harness)
    charm.state.application.data["restore-to-time"] = "20251015T120000Z"
    service = Mock(current=ServiceStatus.ACTIVE)
    with (
        patch.object(charm.restore_manager, "is_pitr_failed", return_value=(True, True)),
        patch.object(charm.restore_manager, "log_pitr_last_transaction_time"),
    ):
        assert (
            charm.postgresql_events_handler._was_restore_successful_k8s(Mock(), service) is False
        )

    assert charm.state.peer.data["pitr-failed"] == "True"
    records = charm.state.statuses.get("unit", charm.restore_manager.name).root
    assert any(r.status == "blocked" and r.message == CANNOT_RESTORE_PITR for r in records)

    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus(CANNOT_RESTORE_PITR)
