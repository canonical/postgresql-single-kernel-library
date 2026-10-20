# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the VM charm class bridges (status setters, update_config)."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus, Object
from single_kernel_postgresql.compat.postgresql import PostgreSQLCreateDatabaseError
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    CONFIGURATION_ERROR_MESSAGE,
    EXTENSION_OBJECT_MESSAGE,
    PEER_RELATION,
    RAFT_MAJORITY_LOSS_MESSAGE,
    RAFT_PARTNER_PREFIX,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.events.watcher import WatcherEventsHandler
from single_kernel_postgresql.utils.backup import CANNOT_RESTORE_PITR


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
    assert charm.state.peer.data["configuration-error"] == CONFIGURATION_ERROR_MESSAGE


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
    charm.state.peer.data["configuration-error"] = CONFIGURATION_ERROR_MESSAGE
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


def test_database_request_error_survives_collect(charm, harness):
    """The invalid-database blocked must survive the collect replay.

    Live repro (2026-10-13): after relating data-integrator with an invalid
    database-name, the charm raised PostgreSQLCreateDatabaseError and wrote the
    blocked directly - which the collect handler's all-active recompute wiped at
    hook end, so nothing polling juju status ever saw the block. The durable
    peer-databag flag re-surfaces it via DatabaseManager.get_statuses.
    """
    charm.state.application.data["cluster_initialised"] = "True"
    with harness.hooks_disabled():
        harness.set_leader(True)
    with (
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            type(charm), "primary_endpoint", new_callable=PropertyMock, return_value="10.1.1.1"
        ),
        patch.object(charm.database_manager, "get_username_mapping", return_value={}),
        patch.object(charm.database_manager, "get_credentials", return_value=("relation-5", "pw")),
        patch.object(
            charm.database_manager,
            "collect_databases",
            return_value=("databases", ["databases"]),
        ),
        patch.object(charm.database_manager, "are_units_in_sync", return_value=True),
        patch.object(type(charm), "postgresql", new_callable=PropertyMock, return_value=Mock()),
        patch.object(charm.database_manager, "update_username_mapping"),
        patch.object(charm.database_manager, "update_unit_status"),
        patch.object(
            charm.database_manager,
            "create_relation_user_and_database",
            side_effect=PostgreSQLCreateDatabaseError("invalid database name"),
        ),
        patch.object(charm, "update_config", return_value=True),
    ):
        charm.database._on_database_requested(
            Mock(
                relation=Mock(id=5),
                database="databases",
                extra_user_roles="",
                prefix_matching=False,
                requested_entity_secret_content=None,
            )
        )

    assert charm.state.peer.data["database-request-error"] == "invalid database name"
    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus("invalid database name")

        # The update-status recompute wipes per-manager store records; the flag
        # must bring the blocked back.
        charm.status_handler._on_update_status(Mock())
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus("invalid database name")


@pytest.fixture
def resuming_refresh(monkeypatch):
    """Construct charm_refresh.Machines so its resume runs during charm init."""
    seen = {"membership_manager_seen": False}

    class ResumingMachines:
        in_progress = True
        next_unit_allowed_to_refresh = True
        workload_allowed_to_start = True
        pinned_snap_revision = "428"
        app_status_higher_priority = None
        unit_status_higher_priority = None

        def __init__(self, charm_specific, /):
            seen["membership_manager_seen"] = hasattr(charm_specific._charm, "membership_manager")
            # Reproduce the action path: the construction-time state machine may
            # reach request_restart before the subclass assigned its bridges.
            charm_specific._charm.request_restart()

        def unit_status_lower_priority(self, *, workload_is_running=True):
            return None

    monkeypatch.setattr("charm_refresh.Machines", ResumingMachines)
    return seen


def test_refresh_resume_during_init_sees_the_membership_manager(resuming_refresh, charm):
    """charm_refresh resume callbacks run INSIDE RefreshManager.__init__.

    A mid-refresh unit's force-refresh-start constructs the charm and the
    resume chain walks update_config -> postgresql -> primary_endpoint ->
    membership_manager.units_ips() (all five VM upgrade legs +
    test_snap_refresh, CI 2026-10-13): the refresh manager must be
    constructed after the membership manager, or the hook dies with
    AttributeError.
    """
    assert resuming_refresh["membership_manager_seen"] is True


def test_refresh_resume_during_init_request_restart_needs_no_lock_yet(resuming_refresh, charm):
    """charm_refresh.Machines runs its state machine AT construction time.

    A force-refresh-start reaches request_restart INSIDE the abstract
    __init__ (via the refresh manager), before the substrate subclass
    assigns restart_lock_acquire: the guarded read must not raise
    AttributeError, and no lock may be invoked during construction.
    """
    assert charm.restart_lock_acquire is None  # class default, nothing injected yet

    lock = Mock()
    charm.restart_lock_acquire = lock
    charm.request_restart()
    lock.assert_called_once_with()


def test_async_subsystem_receives_the_live_watcher_bridge(charm):
    """Trunk parity (trunk charm.py:391/399): live WatcherEventsHandler lands in the async subsystem.

    With the watcher bridge dead, the standby conversion never disables/re-points/re-enables
    the watcher, which stays a stale voting raft partner on the re-bootstrapped cluster —
    the DCS never elects a leader and the standby-leader assertion fails
    (test_async_replication_dead_dc_recovery.py::test_relate_and_replicate).
    """
    assert isinstance(charm.watcher_handler, WatcherEventsHandler)  # not None-vacuous
    assert charm.async_replication_manager.watcher is charm.watcher_handler
    assert charm.async_replication.watcher is charm.watcher_handler


def test_watcher_update_unit_address_writes_the_pinned_snap(charm, harness):
    """watcher.update_unit_address dereferences charm.refresh (trunk parity)."""
    charm.refresh_manager.refresh = Mock(pinned_snap_revision="428")
    rel_id = harness.add_relation("watcher-offer", "external-watcher")
    harness.add_relation_unit(rel_id, "external-watcher/0")
    relation = charm.model.relations["watcher-offer"][0]
    with patch.object(
        type(charm.state), "unit_ip", new_callable=PropertyMock, return_value="10.1.1.1"
    ):
        charm.watcher_handler.update_unit_address(relation)
    assert relation.data[charm.state.peer.unit]["snap"] == "428"


def test_watcher_relation_changed_reads_charm_is_cluster_initialised(charm, harness):
    """The watcher relation-changed gate goes through the charm property.

    test_build_and_deploy_stereo_mode crashed the relation-changed hook on
    units without an initialised cluster: the lib watcher handler reads
    self.charm.is_cluster_initialised (trunk charm.py parity) but no charm
    attribute exposed it.
    """
    rel_id = harness.add_relation("watcher-offer", "external-watcher")
    with harness.hooks_disabled():
        harness.add_relation_unit(rel_id, "external-watcher/0")
    event = Mock(relation=charm.model.relations["watcher-offer"][0], defer=Mock())
    assert charm.is_cluster_initialised is False
    assert charm.is_tls_enabled is False  # no certs assigned yet (trunk parity)
    with patch.object(charm.watcher_handler, "update_unit_address"):
        charm.watcher_handler._on_watcher_relation_changed(event)
    event.defer.assert_called_once()  # not initialised yet -> defer, no crash


def test_watcher_offer_joined_leader_cleans_up_raft_cluster(charm, harness):
    """The leader's watcher-offer cleanup goes through the RAFT manager.

    test_build_and_deploy_stereo_mode (arm64, run 38496136894) crashed the
    watcher-offer-relation-joined hook with AttributeError
    'PatroniManager' object has no attribute 'cleanup_raft_cluster':
    trunk's _patroni property is the RAFT object (src/cluster.py Patroni),
    not the REST-API PatroniManager.
    """
    peer_rel = charm.model.relations[PEER_RELATION][0]
    rel_id = harness.add_relation("watcher-offer", "external-watcher")
    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel.id, charm.app.name, {"cluster_initialised": "True"})
        harness.add_relation_unit(rel_id, "external-watcher/0")
        harness.update_relation_data(rel_id, "external-watcher/0", {"unit-address": "10.0.0.9"})
    event = Mock(relation=charm.model.relations["watcher-offer"][0], defer=Mock())
    with (
        patch.object(type(charm.state.peer), "is_app_leader", PropertyMock(return_value=True)),
        patch.object(charm.raft_manager, "cleanup_raft_cluster") as cleanup,
        patch.object(charm.watcher_handler, "update_unit_address"),
        patch.object(charm.watcher_handler, "_ensure_watcher_user"),
        patch.object(charm.watcher_handler, "_update_relation_data"),
        patch.object(charm, "update_config"),
    ):
        charm.watcher_handler._on_watcher_relation_changed(event)
    cleanup.assert_called_once()


def test_remove_raft_member_stuck_cluster_re_emits_peer_changed(charm, harness):
    """The stuck-raft re-emit goes through the real relation-changed event.

    test_scaling / test_scaling_three_units(+async) crashed the
    database-peers-relation-departed hook (run 38505392160 attempt 3): the
    ported _set_stuck_raft_flag called .emit() on the ctor-injected FUNCTION
    bridge (AttributeError 'function' object has no attribute 'emit'); trunk
    emits charm.on[PEER_RELATION].relation_changed (src/cluster.py:174-178).
    """
    peer_rel = charm.model.relations[PEER_RELATION][0]
    with harness.hooks_disabled():
        harness.update_relation_data(peer_rel.id, charm.app.name, {"cluster_initialised": "True"})
    with (
        patch.object(
            type(charm.state.application), "raft_password", PropertyMock(return_value="pw")
        ),
        patch.object(type(charm.state.peer), "is_app_leader", PropertyMock(return_value=True)),
        patch("single_kernel_postgresql.managers.raft.TcpUtility") as tcp_utility,
    ):
        tcp_utility.return_value.executeCommand.return_value = {
            "has_quorum": False,
            "leader": None,
            f"{RAFT_PARTNER_PREFIX}2.2.2.2:2222": 0,
        }
        emitted = []

        class _Capture(Object):
            def capture(self, event):
                emitted.append(event)

        capture = _Capture(charm, "capture")
        charm.framework.observe(charm.on[PEER_RELATION].relation_changed, capture.capture)
        charm.raft_manager.remove_raft_member("2.2.2.2:2222")
    assert len(emitted) == 1
    assert emitted[0].relation == peer_rel
    assert charm.unit.status == BlockedStatus(RAFT_MAJORITY_LOSS_MESSAGE)
    assert charm.state.peer.data["raft_stuck"] == "True"


def test_raft_stuck_block_survives_collect(charm, harness):
    """The raft-majority-loss blocked must survive the collect.

    test_removing_stereo_primary waits for blocked + "Raft majority loss"
    (CI 2026-10-13): the direct write died at hook end, so the wait timed
    out. The raft_stuck peer flag now replays through RaftManager.get_statuses.
    """
    charm.state.peer.data["raft_stuck"] = "True"
    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus(RAFT_MAJORITY_LOSS_MESSAGE)

        charm.status_handler._on_update_status(Mock())
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus(RAFT_MAJORITY_LOSS_MESSAGE)

    # The promote-to-primary flow clears the flag and the unit unblocks.
    charm.state.peer.data.pop("raft_stuck")
    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
    assert charm.unit.status == ActiveStatus()


def test_config_validation_clear_does_not_lift_a_plugin_block(charm, harness):
    """The config-validation clear is message-scoped.

    Non-leaders run the config-changed validation but never the extension
    reconcile; an unscoped clear would silently lift a plugin-object block
    that trunk keeps visible.
    """
    charm.state.peer.data["configuration-error"] = EXTENSION_OBJECT_MESSAGE
    with harness.hooks_disabled():
        harness.set_leader(False)
    with (
        patch.object(charm.membership_manager, "update_member_ip", return_value=False),
        patch.object(charm.config_manager, "validate_config_options", return_value=None),
        patch.object(charm, "update_config", return_value=True),
    ):
        charm.postgresql_events_handler._vm_config_changed(Mock())
    assert charm.state.peer.data["configuration-error"] == EXTENSION_OBJECT_MESSAGE


def test_pitr_failure_persists_blocked_through_collect(charm, harness):
    """The failed-PITR blocked must surface through the status machinery.

    CI 2026-10-15 trunk-parity regression: trunk wrote the CANNOT_RESTORE_PITR
    blocked directly and it survived (no collect handler); the migration's
    collect replay re-added the restore manager's maintenance "restoring
    backup" over that direct write, so the unit never reached blocked and
    stayed "restoring backup" forever.
    """
    charm.state.application.data["cluster_initialised"] = "True"
    charm.state.application.data["restore-to-time"] = "20251015T120000Z"
    charm.refresh_manager.refresh = Mock(in_progress=False)
    with (
        patch.object(charm.restore_manager, "is_pitr_failed", return_value=(True, True)),
        patch.object(charm.restore_manager, "log_pitr_last_transaction_time"),
    ):
        charm.postgresql_events_handler._on_update_status(Mock())

    assert charm.state.peer.data["pitr-failed"] == "True"
    records = charm.state.statuses.get("unit", charm.restore_manager.name).root
    assert any(r.status == "blocked" and r.message == CANNOT_RESTORE_PITR for r in records)

    # Patch the live collectors: the emit walks every manager's get_statuses,
    # which would reach the snap/charm_refresh surface the unit env cannot build.
    with (
        patch.object(charm.cluster_manager, "get_statuses", return_value=[]),
        patch.object(charm.refresh_manager, "reconcile_refresh_status"),
    ):
        harness.evaluate_status()
        assert charm.unit.status == BlockedStatus(CANNOT_RESTORE_PITR)
