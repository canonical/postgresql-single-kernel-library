#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the RAFT manager (ported from postgresql-operator test_cluster.py)."""

from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch, sentinel

import pytest
from pysyncobj.utility import UtilityException
from single_kernel_postgresql.config.exceptions import (
    AddRaftMemberFailedError,
    RemoveRaftMemberFailedError,
)
from single_kernel_postgresql.config.literals import RAFT_PARTNER_PREFIX, RAFT_PORT
from single_kernel_postgresql.managers.raft import RaftManager
from tenacity import wait_fixed


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
