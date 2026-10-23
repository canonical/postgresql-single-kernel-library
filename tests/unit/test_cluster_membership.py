# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the VM cluster-membership flows and the RAFT recovery state machine.

Ports of the VM charm's tests/unit/test_charm.py cases for _reconfigure_cluster,
_update_member_ip, add_cluster_member, the stuck-raft family, _raft_reinitialisation
and _on_peer_relation_departed, against the lib managers.
"""

import json
from contextlib import ExitStack
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from ops import BlockedStatus, MaintenanceStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import (
    NotReadyError,
    RemoveRaftMemberFailedError,
)
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.managers.cluster_membership import (
    COMPLETED,
    DEFER,
    ClusterMembershipManager,
)
from single_kernel_postgresql.utils.backup import CANNOT_RESTORE_PITR
from tenacity import RetryError, stop_after_attempt, wait_fixed

PEER_ADDRESS = f"{PEER_RELATION}-address"


@pytest.fixture(autouse=True)
def _vm_only(substrate):
    """The membership subsystem is a VM substrate feature."""
    if substrate != Substrates.VM:
        pytest.skip("the membership subsystem is VM-only")


@pytest.fixture
def membership(harness):
    """A ClusterMembershipManager wired to the harness state with mock collaborators."""
    return ClusterMembershipManager(
        state=harness.charm.state,
        workload=harness.charm.workload,
        patroni_manager=Mock(),
        update_config=Mock(),
        set_unit_status=Mock(),
        watcher_handler=Mock(),
        async_replication_manager=Mock(),
        update_relation_endpoints=Mock(),
        update_certificate=Mock(),
        raft_manager=Mock(),
        primary_endpoint=lambda: harness.charm.primary_endpoint,
    )


@pytest.fixture
def charm(harness):
    return harness.charm


def _rel_id(harness) -> int:
    return harness.model.get_relation(PEER_RELATION).id


def test_get_ips_to_remove(harness, membership):
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"members_ips": json.dumps(["1.1.1.1", "2.2.2.2"])}
        )
        harness.update_relation_data(rel_id, harness.charm.unit.name, {PEER_ADDRESS: "1.1.1.1"})
    with patch.object(
        type(harness.charm.state), "unit_ip", new_callable=PropertyMock, return_value="1.1.1.1"
    ):
        assert membership.get_ips_to_remove() == {"2.2.2.2"}


def test_add_and_remove_members_ips_leader_gated(harness, membership):
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.set_leader(False)
        membership.add_member_ip("1.1.1.1")
    assert (
        json.loads(
            harness.get_relation_data(rel_id, harness.charm.app.name).get("members_ips", "[]")
        )
        == []
    )

    with harness.hooks_disabled():
        harness.set_leader(True)
        membership.add_member_ip("1.1.1.1")
        membership.add_member_ip("1.1.1.1")
    assert json.loads(
        harness.get_relation_data(rel_id, harness.charm.app.name)["members_ips"]
    ) == ["1.1.1.1"]
    membership.add_member_ip("2.2.2.2")
    membership.remove_member_ip("1.1.1.1")
    assert json.loads(
        harness.get_relation_data(rel_id, harness.charm.app.name)["members_ips"]
    ) == ["2.2.2.2"]


def test_reconfigure_cluster(harness, membership):
    event = MagicMock(spec_set=["unit", "relation"])
    event.unit = harness.charm.unit

    # Cluster not initialised: cleanup is skipped, members are added.
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(membership, "add_members") as _add_members,
        patch.object(membership.raft_manager, "cleanup_raft_cluster", return_value=False),
    ):
        assert membership.reconfigure_cluster(event) is True
        membership.raft_manager.cleanup_raft_cluster.assert_not_called()
        _add_members.assert_called_once_with(event)

    # Cleanup fails when initialised.
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(membership, "add_members") as _add_members,
        patch.object(
            membership.raft_manager, "cleanup_raft_cluster", return_value=False
        ) as _cleanup,
    ):
        assert membership.reconfigure_cluster(event) is False
        _cleanup.assert_called_once_with()
        _add_members.assert_not_called()

    # Happy scenario.
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(membership, "add_members") as _add_members,
        patch.object(
            membership.raft_manager, "cleanup_raft_cluster", return_value=True
        ) as _cleanup,
    ):
        assert membership.reconfigure_cluster(event) is True
        _cleanup.assert_called_once_with()
        _add_members.assert_called_once_with(event)


def test_add_members_does_not_override_blocked_pitr_status(harness):
    """The reconfigure loop must not run while a failed PITR restore blocks the unit.

    Port of the trunk charm's guard (canonical/postgresql-operator#2044): the
    deferred reconfigure cannot succeed while Patroni is down, and running it
    overrides the blocked status on every peer-changed tick. Trunk inspected
    the live unit status; here the durable ``pitr-failed`` peer flag is the
    pre-collect equivalent (``get_statuses`` replays the blocked from it).
    """
    harness.charm.state.application.data["restore-to-time"] = "20261015T120000Z"
    harness.charm.restore_manager.block_on_pitr_failure()
    harness.charm.unit.status = BlockedStatus(CANNOT_RESTORE_PITR)

    membership = ClusterMembershipManager(
        state=harness.charm.state,
        workload=harness.charm.workload,
        patroni_manager=Mock(),
        update_config=Mock(),
        set_unit_status=lambda status: setattr(harness.charm.unit, "status", status),
        watcher_handler=Mock(),
        async_replication_manager=Mock(),
        update_relation_endpoints=Mock(),
        update_certificate=Mock(),
        raft_manager=Mock(),
        primary_endpoint=lambda: harness.charm.primary_endpoint,
    )

    # The reconfigure path is exercised: the Mock cluster members differ from
    # the harness hosts, and the member-add body is inert (patched) so the
    # status write is the observable.
    membership.patroni_manager.cluster_members = set()
    with patch.object(membership, "add_cluster_member"):
        membership.add_members(Mock())

    assert isinstance(harness.charm.unit.status, BlockedStatus)
    assert harness.charm.unit.status.message == CANNOT_RESTORE_PITR


def test_add_cluster_member(harness, membership):
    with (
        patch.object(type(harness.charm.state), "model", new_callable=PropertyMock) as _model,
    ):
        model = Mock()
        model.unit = harness.charm.unit
        model.app = harness.charm.app
        unit = Mock()
        unit.name = "postgresql/0"
        model.get_unit.return_value = unit
        model.get_relation.return_value = harness.model.get_relation(PEER_RELATION)
        _model.return_value = model
        membership.get_unit_ip = Mock(return_value="1.1.1.1")
        membership.patroni_manager.are_all_members_ready.return_value = True
        with harness.hooks_disabled():
            harness.set_leader(True)

        membership.add_cluster_member("postgresql-0")
        membership.update_config.assert_called_once()
        assert json.loads(
            harness.get_relation_data(_rel_id(harness), harness.charm.app.name)["members_ips"]
        ) == ["1.1.1.1"]

        # update_config retrying -> maintenance status.
        membership.update_config.reset_mock()
        membership.update_config.side_effect = RetryError(last_attempt=None)
        membership.add_cluster_member("postgresql-0")
        assert isinstance(harness.charm.unit.status, MaintenanceStatus)

        # Not ready -> NotReadyError.
        membership.update_config.side_effect = None
        membership.patroni_manager.are_all_members_ready.return_value = False
        with pytest.raises(NotReadyError):
            membership.add_cluster_member("postgresql-0")


def test_update_member_ip(harness, membership):
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.unit.name, {"ip": "192.0.2.0", PEER_ADDRESS: "192.0.2.0"}
        )
    with patch.object(
        type(harness.charm.state), "unit_ip", new_callable=PropertyMock, return_value="192.0.2.0"
    ):
        assert membership.update_member_ip() is False
    relation_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
    assert "ip-to-remove" not in relation_data or relation_data.get("ip-to-remove") == ""
    membership.patroni_manager.stop_patroni.assert_not_called()
    membership.update_certificate.assert_not_called()

    with patch.object(
        type(harness.charm.state), "unit_ip", new_callable=PropertyMock, return_value="2.2.2.2"
    ):
        assert membership.update_member_ip() is True
    relation_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
    assert relation_data["ip"] == "2.2.2.2"
    assert relation_data["ip-to-remove"] == "192.0.2.0"
    membership.patroni_manager.stop_patroni.assert_called_once()
    membership.update_certificate.assert_called_once()
    membership.watcher_handler.update_unit_address.assert_called_once()


def test_stuck_raft_cluster_check(harness, membership):
    rel_id = _rel_id(harness)
    all_units = [p.unit.name for p in harness.charm.state.application_peers]

    # No raft flags: nothing selected.
    membership.stuck_cluster_check()
    assert "raft_selected_candidate" not in harness.get_relation_data(
        rel_id, harness.charm.app.name
    )

    # A stuck unit without a candidate selects nothing.
    with harness.hooks_disabled():
        for unit in all_units:
            harness.update_relation_data(rel_id, unit, {"raft_stuck": "True"})
    membership.stuck_cluster_check()
    assert "raft_selected_candidate" not in harness.get_relation_data(
        rel_id, harness.charm.app.name
    )

    # A stuck unit with a candidate selects it (all units stuck; the candidate
    # flag only on this unit so the selection is deterministic).
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, harness.charm.unit.name, {"raft_candidate": "True"})
    membership.stuck_cluster_check()
    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert app_data["raft_selected_candidate"] == harness.charm.unit.name

    # An existing candidate is not overridden.
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"raft_selected_candidate": "something_else"}
        )
    membership.stuck_cluster_check()
    assert (
        harness.get_relation_data(rel_id, harness.charm.app.name)["raft_selected_candidate"]
        == "something_else"
    )


def test_stuck_raft_cluster_cleanup(harness, membership):
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            harness.charm.app.name,
            {
                "raft_rejoin": "True",
                "raft_reset_primary": "True",
                "raft_selected_candidate": "unit_name",
            },
        )
    membership.stuck_cluster_cleanup()
    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert "raft_rejoin" not in app_data
    assert "raft_reset_primary" not in app_data
    assert "raft_selected_candidate" not in app_data

    # Unit raft flags block the cleanup.
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, harness.charm.unit.name, {"raft_primary": "True"})
    membership.stuck_cluster_cleanup()
    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    # The unit-level raft flag blocks the second cleanup; the app flags stay clean.
    assert "raft_rejoin" not in app_data
    assert "raft_reset_primary" not in app_data
    assert "raft_selected_candidate" not in app_data
    unit_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
    assert "raft_primary" in unit_data


def test_stuck_raft_cluster_rejoin(harness, membership):
    rel_id = _rel_id(harness)

    # No data: nothing happens.
    membership.stuck_cluster_rejoin()
    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert "raft_reset_primary" not in app_data
    assert "raft_rejoin" not in app_data

    # A primary unit resets the members and notifies the rejoin.
    with harness.hooks_disabled():
        harness.set_leader(True)
        harness.update_relation_data(
            rel_id, harness.charm.unit.name, {"raft_primary": "True", PEER_ADDRESS: "192.0.2.0"}
        )
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"raft_followers_stopped": "True"}
        )
    with patch.object(
        type(harness.charm.state), "unit_ip", new_callable=PropertyMock, return_value="192.0.2.0"
    ):
        membership.stuck_cluster_rejoin()
    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert "raft_reset_primary" in app_data
    assert "raft_rejoin" in app_data
    assert json.loads(app_data["members_ips"]) == ["192.0.2.0"]
    membership.update_relation_endpoints.assert_called_once()


def test_raft_reinitialisation(harness, membership):
    rel_id = _rel_id(harness)
    membership.raft_manager = Mock()
    with (
        patch.object(type(harness.charm.state), "has_raft_keys", Mock(return_value=False)),
    ):
        # No data: nothing happens.
        membership.raft_reinitialisation()

        # A selected candidate on a stuck unit: raft data removed, no reinit.
        with harness.hooks_disabled():
            harness.set_leader(True)
            harness.update_relation_data(
                rel_id, harness.charm.unit.name, {"raft_stuck": "True", "raft_candidate": "True"}
            )
            harness.update_relation_data(
                rel_id, harness.charm.app.name, {"raft_selected_candidate": "test_candidate"}
            )
        membership.raft_reinitialisation()
        membership.raft_manager.remove_raft_data.assert_called_once()
        membership.raft_manager.reinitialise_raft_data.assert_not_called()

        # The selected candidate reinitialises when the followers stopped.
        with harness.hooks_disabled():
            harness.update_relation_data(
                rel_id,
                harness.charm.unit.name,
                {"raft_stuck": "", "raft_candidate": "True", "raft_stopped": "True"},
            )
            harness.update_relation_data(
                rel_id,
                harness.charm.app.name,
                {
                    "raft_selected_candidate": harness.charm.unit.name,
                    "raft_followers_stopped": "True",
                },
            )
        membership.raft_reinitialisation()
        membership.raft_manager.reinitialise_raft_data.assert_called_once()


def test_on_peer_relation_departed_protocol(harness, membership):
    # Self-departing: skip.
    event = Mock()
    event.departing_unit = harness.charm.unit
    assert membership.departed_early_exit(event) is True

    # No departing unit: skip.
    event.departing_unit = None
    assert membership.departed_early_exit(event) is True

    # Raft recovery in progress: skip.
    other_unit = Mock()
    other_unit.name = f"{harness.charm.app.name}/1"
    event.departing_unit = other_unit
    with patch.object(type(harness.charm.state), "has_raft_keys", Mock(return_value=True)):
        assert membership.departed_early_exit(event) is True

    # Otherwise: proceed.
    with patch.object(type(harness.charm.state), "has_raft_keys", Mock(return_value=False)):
        assert membership.departed_early_exit(event) is False


def test_remove_departing_raft_member(harness, membership):
    other_unit = Mock()
    other_unit.name = f"{harness.charm.app.name}/1"
    event = Mock()
    event.departing_unit = other_unit

    membership.patroni_manager.get_member_ip.return_value = "1.1.1.1"
    with patch.object(membership.raft_manager, "remove_raft_member") as _remove:
        assert membership.remove_departing_raft_member(event) == COMPLETED
        _remove.assert_called_once_with("1.1.1.1:2222")

    with patch.object(
        membership.raft_manager,
        "remove_raft_member",
        side_effect=RemoveRaftMemberFailedError,
    ) as _remove:
        assert membership.remove_departing_raft_member(event) == DEFER


def test_remove_departed_members_defers_without_dropping_members(harness, membership):
    """Review Focus 1: a deferred re-add must not drop or duplicate members_ips."""
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.set_leader(True)
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"members_ips": json.dumps(["1.1.1.1", "2.2.2.2"])}
        )
        harness.update_relation_data(rel_id, harness.charm.unit.name, {PEER_ADDRESS: "1.1.1.1"})
    with patch.object(
        type(harness.charm.state), "unit_ip", new_callable=PropertyMock, return_value="1.1.1.1"
    ):
        membership.patroni_manager.are_all_members_ready.return_value = False
        assert membership.remove_departed_members() == "defer"
        # The members list is untouched by the deferred attempt.
        assert json.loads(
            harness.get_relation_data(rel_id, harness.charm.app.name)["members_ips"]
        ) == ["1.1.1.1", "2.2.2.2"]

        # Ready path removes the stale IP and keeps the current one.
        membership.patroni_manager.are_all_members_ready.return_value = True
        membership.update_relation_endpoints.return_value = "primary"
        with (
            patch(
                "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
                new_callable=PropertyMock,
                return_value=True,
            ),
            patch.object(
                type(harness.charm),
                "primary_endpoint",
                new_callable=PropertyMock,
                return_value="10.0.0.1",
            ),
        ):
            assert membership.remove_departed_members() == COMPLETED
            assert json.loads(
                harness.get_relation_data(rel_id, harness.charm.app.name)["members_ips"]
            ) == ["1.1.1.1"]


# -- raft_reconnect bounded retry / DEFER (leg 116790413638) -----------------------

RETRY_PATCHES = (
    patch(
        "single_kernel_postgresql.managers.cluster_membership.stop_after_delay",
        lambda _: stop_after_attempt(2),
    ),
    patch(
        "single_kernel_postgresql.managers.cluster_membership.wait_fixed",
        lambda _: wait_fixed(0),
    ),
)


def _stuck_raft_state(harness, membership, raft_status):
    """Wire the harness state so raft_reconnect passes its early-exit guards."""
    rel_id = _rel_id(harness)
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id, harness.charm.app.name, {"members_ips": json.dumps(["1.1.1.1", "2.2.2.2"])}
        )
    membership.raft_manager.get_raft_status.return_value = raft_status
    return (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(harness.charm.state), "has_raft_keys", Mock(return_value=False)),
        patch.object(
            type(harness.charm.state),
            "unit_ip",
            new_callable=PropertyMock,
            return_value="1.1.1.1",
        ),
    )


def test_raft_reconnect_retries_transient_remove_failure(harness, membership):
    """A transient remove failure is retried in-hook; the add still runs."""
    stuck_status = {"partner_node_status_server_2.2.2.2:2222": 1}
    patches = _stuck_raft_state(harness, membership, stuck_status)
    membership.raft_manager.remove_raft_member.side_effect = [
        RemoveRaftMemberFailedError,
        None,
    ]
    with ExitStack() as stack:
        for ctx in (*patches, *RETRY_PATCHES):
            stack.enter_context(ctx)
        assert membership.raft_reconnect(MagicMock()) == COMPLETED
    assert membership.raft_manager.remove_raft_member.call_count == 2
    membership.raft_manager.add_raft_member.assert_called_once()


def test_raft_reconnect_defers_after_exhausted_retries(harness, membership):
    """When the retry budget is exhausted the flow returns DEFER, never adds."""
    stuck_status = {"partner_node_status_server_2.2.2.2:2222": 1}
    patches = _stuck_raft_state(harness, membership, stuck_status)
    membership.raft_manager.remove_raft_member.side_effect = RemoveRaftMemberFailedError
    with ExitStack() as stack:
        for ctx in (*patches, *RETRY_PATCHES):
            stack.enter_context(ctx)
        assert membership.raft_reconnect(MagicMock()) == DEFER
    membership.raft_manager.add_raft_member.assert_not_called()


def test_raft_reconnect_no_retry_when_cluster_recovered(harness, membership):
    """A dispatch after a failed one sees all partners active and does not retry."""
    stuck_status = {"partner_node_status_server_2.2.2.2:2222": 1}
    patches = _stuck_raft_state(harness, membership, stuck_status)
    membership.raft_manager.remove_raft_member.side_effect = RemoveRaftMemberFailedError
    with ExitStack() as stack:
        for ctx in (*patches, *RETRY_PATCHES):
            stack.enter_context(ctx)
        assert membership.raft_reconnect(MagicMock()) == DEFER
    attempts_after_failure = membership.raft_manager.remove_raft_member.call_count
    membership.raft_manager.get_raft_status.return_value = {
        "partner_node_status_server_2.2.2.2:2222": 2
    }
    with ExitStack() as stack:
        for ctx in (*patches, *RETRY_PATCHES):
            stack.enter_context(ctx)
        assert membership.raft_reconnect(MagicMock()) == COMPLETED
    assert membership.raft_manager.remove_raft_member.call_count == attempts_after_failure


# -- leader-side update-status membership reconcile (leg 116790413638) -------------


def _reconcile_context(charm, running_members, planned_units):
    """Patches to drive _on_update_status up to the membership reconcile point."""
    charm.state.application.data["cluster_initialised"] = "True"
    charm.refresh_manager.refresh = Mock(in_progress=False)
    charm.state.application.data.pop("pitr-failed", None)
    return (
        patch.object(
            type(charm.patroni_manager),
            "member_started",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(
            type(charm.patroni_manager),
            "is_member_isolated",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            charm.patroni_manager,
            "get_running_cluster_members",
            Mock(return_value=running_members),
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.planned_units",
            new_callable=PropertyMock,
            return_value=planned_units,
        ),
        patch.object(
            type(charm), "primary_endpoint", new_callable=PropertyMock, return_value="10.0.0.1"
        ),
        patch.object(charm.database_manager, "oversee_users"),
        patch.object(charm.membership_manager, "update_relation_endpoints"),
        patch.object(charm.async_replication_manager, "update_async_replication_data"),
        patch.object(charm.async_replication_manager, "clear_stale_promotion"),
        patch.object(charm.backup_manager, "coordinate_stanza_fields"),
        patch.object(charm, "set_primary_status_message"),
        patch.object(charm.observer_manager, "start_observer"),
        patch.object(charm.watcher_handler, "update_unit_address"),
        patch.object(charm.postgresql_events_handler, "postgresql"),
    )


def test_reconcile_running_members_below_planned_adds_missing_member(charm, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    patches = _reconcile_context(charm, ["postgresql-0", "postgresql-1"], 3)
    recorded = Mock(return_value=True)
    with ExitStack() as stack:
        for ctx in (
            *patches,
            patch.object(charm.membership_manager, "reconfigure_cluster", new=recorded),
        ):
            stack.enter_context(ctx)
        charm.postgresql_events_handler._on_update_status(Mock())
    recorded.assert_called_once()


def test_reconcile_skipped_when_running_members_match_planned(charm, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    patches = _reconcile_context(charm, ["postgresql-0", "postgresql-1", "postgresql-2"], 3)
    recorded = Mock(return_value=True)
    with ExitStack() as stack:
        for ctx in (*patches,):
            stack.enter_context(ctx)
        stack.enter_context(
            patch.object(charm.membership_manager, "reconfigure_cluster", new=recorded)
        )
        charm.postgresql_events_handler._on_update_status(Mock())
    recorded.assert_not_called()
    # Manager-level steady-state no-op: equal members and IPs short-circuits.
    membership = charm.membership_manager
    with (
        patch.object(
            type(membership.patroni_manager),
            "cluster_members",
            new_callable=PropertyMock,
            return_value={"postgresql-0", "postgresql-1"},
        ),
        patch.object(membership, "hosts", return_value={"postgresql-0", "postgresql-1"}),
        patch.object(
            type(charm.state),
            "units_ips",
            new_callable=PropertyMock,
            return_value={"1.1.1.1"},
        ),
        patch.object(
            type(charm.state.application),
            "members_ips",
            new_callable=PropertyMock,
            return_value={"1.1.1.1"},
        ),
        patch.object(membership, "set_unit_status") as set_status,
        patch.object(membership, "add_cluster_member") as add_member,
    ):
        membership.add_members(Mock())
    set_status.assert_not_called()
    add_member.assert_not_called()


def test_reconcile_leader_gated(charm, harness):
    with harness.hooks_disabled():
        harness.set_leader(False)
    patches = _reconcile_context(charm, ["postgresql-0", "postgresql-1"], 3)
    recorded = Mock(return_value=True)
    with ExitStack() as stack:
        for ctx in (
            *patches,
            patch.object(charm.membership_manager, "reconfigure_cluster", new=recorded),
        ):
            stack.enter_context(ctx)
        charm.postgresql_events_handler._on_update_status(Mock())
    recorded.assert_not_called()


def test_reconcile_skipped_when_leader_isolated(charm, harness):
    with harness.hooks_disabled():
        harness.set_leader(True)
    patches = _reconcile_context(charm, [], 3)
    patches = list(patches)
    # Force the isolation early-return: member not started + zero members.
    patches[0] = patch.object(
        type(charm.patroni_manager),
        "member_started",
        new_callable=PropertyMock,
        return_value=False,
    )
    patches[1] = patch.object(
        type(charm.patroni_manager),
        "is_member_isolated",
        new_callable=PropertyMock,
        return_value=True,
    )
    recorded = Mock(return_value=True)
    with ExitStack() as stack:
        for ctx in (
            *patches,
            patch.object(charm.membership_manager, "reconfigure_cluster", new=recorded),
        ):
            stack.enter_context(ctx)
        restarted = stack.enter_context(patch.object(charm.patroni_manager, "restart_patroni"))
        charm.postgresql_events_handler._on_update_status(Mock())
    recorded.assert_not_called()
    restarted.assert_called_once()
