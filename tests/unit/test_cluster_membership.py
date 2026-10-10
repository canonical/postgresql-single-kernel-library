# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the VM cluster-membership flows and the RAFT recovery state machine.

Ports of the VM charm's tests/unit/test_charm.py cases for _reconfigure_cluster,
_update_member_ip, add_cluster_member, the stuck-raft family, _raft_reinitialisation
and _on_peer_relation_departed, against the lib managers.
"""

import json
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from ops import MaintenanceStatus
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
from single_kernel_postgresql.utils import new_password  # noqa: F401  (parity with charm utils)
from tenacity import RetryError

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
    )


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
    with patch.object(harness.charm.state.application, "data", {}, create=True):
        pass
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
        patch("single_kernel_postgresql.utils.new_password", return_value="pw"),
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
                type(harness.charm.state),
                "primary_endpoint",
                new_callable=PropertyMock,
                return_value="10.0.0.1",
            ),
        ):
            assert membership.remove_departed_members() == COMPLETED
            assert json.loads(
                harness.get_relation_data(rel_id, harness.charm.app.name)["members_ips"]
            ) == ["1.1.1.1"]
