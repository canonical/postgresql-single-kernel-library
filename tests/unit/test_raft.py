#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the RAFT manager (ported from postgresql-operator test_cluster.py)."""

from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch, sentinel

import pytest
from data_platform_helpers.advanced_statuses import StatusObject
from ops import BlockedStatus
from pysyncobj.utility import UtilityException
from single_kernel_postgresql.config.exceptions import (
    AddRaftMemberFailedError,
    RaftRecoveryTimeoutError,
    RemoveRaftMemberFailedError,
)
from single_kernel_postgresql.config.literals import (
    PEER_RELATION,
    RAFT_MAJORITY_LOSS_MESSAGE,
    RAFT_PARTNER_PREFIX,
    RAFT_PORT,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.managers.raft import RaftManager
from tenacity import stop_after_attempt, wait_fixed


@pytest.fixture()
def raft_manager(substrate, harness):
    """A RaftManager built on the VM test charm's real state and workload."""
    if substrate != "vm":
        pytest.skip("Raft manager is VM-substrate only")
    with patch(
        "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.raft_password",
        new_callable=PropertyMock,
        return_value="fake-raft-password",
    ):
        manager = RaftManager(
            harness.charm.state,
            harness.charm.workload,
            patroni_manager=Mock(),
            watcher_handler=Mock(),
            update_config=Mock(),
            set_unit_status=Mock(),
            peer_relation_changed=Mock(),
            remove_from_members_ips=Mock(),
        )
        manager.watcher_handler.watcher_raft_address = f"3.3.3.3:{RAFT_PORT}"
        yield manager


def test_cleanup_raft_cluster(raft_manager, harness):
    raft_manager.watcher_handler.watcher_raft_address = None
    with (
        patch("single_kernel_postgresql.managers.raft.TcpUtility") as _tcp_utility,
        patch.object(RaftManager, "remove_raft_member", return_value=True) as _remove_raft_member,
        patch(
            "single_kernel_postgresql.core.state.CharmState.units_ips",
            new_callable=PropertyMock,
            return_value={"1.1.1.1"},
        ) as _units_ips,
        patch.object(
            type(harness.charm.workload), "is_patroni_running", return_value=True
        ) as _is_patroni_running,
    ):
        # Error connecting to raft
        _tcp_utility.side_effect = Exception

        assert not raft_manager.cleanup_raft_cluster()

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        _tcp_utility.reset_mock()

        # No status
        _tcp_utility.side_effect = None
        _tcp_utility.return_value.executeCommand.return_value = {}

        assert not raft_manager.cleanup_raft_cluster()

        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.reset_mock()

        # All members active
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.1.1.1:{RAFT_PORT}": 2
        }

        assert raft_manager.cleanup_raft_cluster()

        assert not _remove_raft_member.called

        # Filter by unit ips
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.1.1.1:{RAFT_PORT}": 0,
            f"{RAFT_PARTNER_PREFIX}2.2.2.2:{RAFT_PORT}": 0,
        }

        assert raft_manager.cleanup_raft_cluster()

        _remove_raft_member.assert_called_once_with(f"2.2.2.2:{RAFT_PORT}")
        raft_manager.remove_from_members_ips.assert_called_once_with("2.2.2.2")
        # Coverage for the patched property read.
        assert _units_ips.call_count >= 1
        assert _is_patroni_running.call_count >= 1


def test_add_raft_member(raft_manager):
    with patch("single_kernel_postgresql.managers.raft.TcpUtility") as _tcp_utility:
        # Member already removed
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
            "has_quorum": True,
            "leader": sentinel.raft_leader,
        }

        raft_manager.add_raft_member(f"1.2.3.4:{RAFT_PORT}")

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.reset_mock()

        # Add member
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            "SUCCESS",
        ]

        raft_manager.add_raft_member(f"1.2.3.4:{RAFT_PORT}")

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        assert _tcp_utility.return_value.executeCommand.call_count == 2
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["add", f"1.2.3.4:{RAFT_PORT}"]
        )
        _tcp_utility.reset_mock()

        # Raises on failed status
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            "FAIL",
        ]

        with pytest.raises(AddRaftMemberFailedError):
            raft_manager.add_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False

        # Raises on add error
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            UtilityException,
        ]

        with pytest.raises(AddRaftMemberFailedError):
            raft_manager.add_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False

        # Raises on status error
        _tcp_utility.return_value.executeCommand.side_effect = [UtilityException]

        with pytest.raises(AddRaftMemberFailedError):
            raft_manager.add_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False


def test_remove_raft_member(raft_manager):
    with patch("single_kernel_postgresql.managers.raft.TcpUtility") as _tcp_utility:
        # Member already removed
        _tcp_utility.return_value.executeCommand.return_value = "Response message"

        raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.reset_mock()

        # Removing member
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            "SUCCESS",
        ]

        raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        assert _tcp_utility.return_value.executeCommand.call_count == 2
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["remove", f"1.2.3.4:{RAFT_PORT}"]
        )
        _tcp_utility.reset_mock()

        # Raises on failed status
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            "FAIL",
        ]

        with pytest.raises(RemoveRaftMemberFailedError):
            raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False

        # Raises on remove error
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            UtilityException,
        ]

        with pytest.raises(RemoveRaftMemberFailedError):
            raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False

        # Raises on status error
        _tcp_utility.return_value.executeCommand.side_effect = [UtilityException]

        with pytest.raises(RemoveRaftMemberFailedError):
            raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")
            assert False


def test_remove_raft_member_no_quorum(raft_manager):
    with patch("single_kernel_postgresql.managers.raft.TcpUtility") as _tcp_utility:
        # No leader
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
            "has_quorum": False,
            "leader": None,
        }

        raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")
        assert raft_manager.state.peer.data == {"raft_stuck": "True"}

        # No health
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
            "has_quorum": False,
            "leader": None,
        }

        raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")

        assert raft_manager.state.peer.data == {"raft_stuck": "True"}

        # Sync replica
        leader_mock = Mock()
        leader_mock.address = f"1.2.3.4:{RAFT_PORT}"
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.2.3.4:{RAFT_PORT}": 0,
            "has_quorum": False,
            "leader": leader_mock,
        }

        raft_manager.remove_raft_member(f"1.2.3.4:{RAFT_PORT}")

        assert raft_manager.state.peer.data == {"raft_stuck": "True"}
        # The first no-quorum path blocks the unit with the trunk-parity message;
        # the stuck flag then short-circuits the further removals.
        assert raft_manager.set_unit_status.call_count == 1
        raft_manager.set_unit_status.assert_any_call(BlockedStatus(RAFT_MAJORITY_LOSS_MESSAGE))
        # The old charm-funct emit is gone; the leader re-emits through the charm events.
        assert not raft_manager.peer_relation_changed.emit.called


def test_remove_raft_data(raft_manager):
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch("single_kernel_postgresql.managers.raft.wait_fixed", return_value=wait_fixed(0)),
        patch("shutil.rmtree") as _rmtree,
        patch("pathlib.Path.is_dir", return_value=True) as _is_dir,
        patch("pathlib.Path.exists", return_value=True) as _exists,
    ):
        mock_proc_pg = Mock()
        mock_proc_not_pg = Mock()
        mock_proc_pg.name.return_value = "postgres"
        mock_proc_not_pg.name.return_value = "something_else"
        _psutil.process_iter.side_effect = [[mock_proc_not_pg, mock_proc_pg], [mock_proc_not_pg]]

        raft_manager.remove_raft_data()

        raft_manager.patroni_manager.stop_patroni.assert_called_once_with()
        assert _psutil.process_iter.call_count == 2
        _psutil.process_iter.assert_any_call(["name"])
        _rmtree.assert_called_once_with(
            str(Path(raft_manager.workload.paths.patroni_conf) / "raft")
        )
        assert _is_dir.called
        assert _exists.called


def test_reinitialise_raft_data(raft_manager):
    patroni_manager = raft_manager.patroni_manager
    patroni_manager.get_patroni_health.side_effect = [
        {"role": "replica", "state": "streaming"},
        {"role": "leader", "state": "running"},
    ]
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch("single_kernel_postgresql.managers.raft.wait_fixed", return_value=wait_fixed(0)),
    ):
        mock_proc_pg = Mock()
        mock_proc_not_pg = Mock()
        mock_proc_pg.name.return_value = "postgres"
        mock_proc_not_pg.name.return_value = "something_else"
        _psutil.process_iter.side_effect = [[mock_proc_not_pg], [mock_proc_not_pg, mock_proc_pg]]

        raft_manager.reinitialise_raft_data()

        raft_manager.update_config.assert_called_once_with(no_peers=True)
        patroni_manager.start_patroni.assert_called_once_with()
        patroni_manager.restart_patroni.assert_called_once_with()
        assert _psutil.process_iter.call_count == 2
        _psutil.process_iter.assert_any_call(["name"])


def test_set_stuck_raft_flag_reemits_peer_relation_changed_on_the_leader(raft_manager, harness):
    """The leader re-emits the peer-relation-changed event through the charm events."""
    relation = harness.charm.model.get_relation(PEER_RELATION)
    unit = harness.charm.state.peer.unit
    events = Mock()
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.is_app_leader",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(harness.charm.state, "charm", events),
    ):
        raft_manager._set_stuck_raft_flag()

    assert harness.charm.state.peer.data["raft_stuck"] == "True"
    raft_manager.set_unit_status.assert_called_once_with(BlockedStatus(RAFT_MAJORITY_LOSS_MESSAGE))
    relation_changed = getattr(events.on, f"{PEER_RELATION.replace('-', '_')}_relation_changed")
    relation_changed.emit.assert_called_once_with(relation, app=unit.app, unit=unit)


def test_get_statuses(raft_manager):
    """The manager reports the stuck-raft block only at unit scope."""
    active_idle = [GeneralStatuses.ACTIVE_IDLE.value]
    assert raft_manager.get_statuses("unit") == active_idle
    assert raft_manager.get_statuses("app") == active_idle

    raft_manager.state.peer.data.update({"raft_stuck": "True"})

    assert raft_manager.get_statuses("unit") == [
        StatusObject(status="blocked", message=RAFT_MAJORITY_LOSS_MESSAGE)
    ]
    assert raft_manager.get_statuses("app") == active_idle


def test_remove_raft_data_times_out(raft_manager):
    """A wedged postgres stop defers the hook instead of hanging it."""
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch(
            "single_kernel_postgresql.managers.raft.stop_after_delay",
            return_value=stop_after_attempt(1),
        ),
        patch("shutil.rmtree") as _rmtree,
    ):
        proc = Mock()
        proc.name.return_value = "postgres"
        _psutil.process_iter.return_value = [proc]

        with pytest.raises(RaftRecoveryTimeoutError):
            raft_manager.remove_raft_data()

        raft_manager.patroni_manager.stop_patroni.assert_called_once_with()
        assert not _rmtree.called


def test_reinitialise_raft_data_times_out_waiting_for_promotion(raft_manager):
    """A promotion that never completes defers the hook instead of hanging it."""
    patroni_manager = raft_manager.patroni_manager
    patroni_manager.get_patroni_health.return_value = {"role": "replica", "state": "streaming"}
    with (
        patch(
            "single_kernel_postgresql.managers.raft.stop_after_delay",
            return_value=stop_after_attempt(1),
        ),
        pytest.raises(RaftRecoveryTimeoutError),
    ):
        raft_manager.reinitialise_raft_data()

    raft_manager.update_config.assert_called_once_with(no_peers=True)
    patroni_manager.start_patroni.assert_called_once_with()
    patroni_manager.restart_patroni.assert_not_called()


def test_reinitialise_raft_data_times_out_waiting_for_postgresql(raft_manager):
    """A postgres that never comes back defers the hook instead of hanging it."""
    patroni_manager = raft_manager.patroni_manager
    patroni_manager.get_patroni_health.return_value = {"role": "leader", "state": "running"}
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch(
            "single_kernel_postgresql.managers.raft.stop_after_delay",
            return_value=stop_after_attempt(2),
        ),
        patch("single_kernel_postgresql.managers.raft.wait_fixed", return_value=wait_fixed(0)),
    ):
        proc = Mock()
        proc.name.return_value = "not-postgres"
        _psutil.process_iter.return_value = [proc]

        with pytest.raises(RaftRecoveryTimeoutError):
            raft_manager.reinitialise_raft_data()

    patroni_manager.restart_patroni.assert_called_once_with()
