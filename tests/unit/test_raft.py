# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import PATRONI_CONF_PATH, RAFT_PARTNER_PREFIX
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.raft import RaftManager
from single_kernel_postgresql.workload.vm import VMWorkload
from tenacity import wait_fixed


@pytest.fixture(autouse=True)
def raft(substrate):
    if substrate == Substrates.K8S:
        pytest.skip("VM only test")
    mock_charm = Mock()
    mock_patroni_manager = Mock()
    mock_watcher_handler = Mock()
    mock_update_config = Mock()
    mock_set_unit_status = Mock()
    mock_peer_relation_changed = Mock()
    mock_remove_from_members_ips = Mock()

    workload = VMWorkload(".")
    raft = RaftManager(
        state=CharmState(charm=mock_charm, substrate=substrate, s3_requirer=Mock()),
        workload=workload,
        patroni_manager=mock_patroni_manager,
        watcher_handler=mock_watcher_handler,
        update_config=mock_update_config,
        set_unit_status=mock_set_unit_status,
        peer_relation_changed=mock_peer_relation_changed,
        remove_from_members_ips=mock_remove_from_members_ips,
    )
    yield raft


def test_cleanup_raft_cluster(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.TcpUtility") as _tcp_utility,
        patch(
            "single_kernel_postgresql.managers.raft.RaftManager.remove_raft_member",
            return_value=True,
        ) as _remove_raft_member,
        patch(
            "single_kernel_postgresql.core.state.CharmState.units_ips",
            new_callable=PropertyMock,
            return_value={"1.1.1.1"},
        ),
        patch(
            "single_kernel_postgresql.workload.vm.VMWorkload.is_patroni_running", return_value=True
        ),
    ):
        # Error connecting to raft
        _tcp_utility.side_effect = Exception

        assert not raft.cleanup_raft_cluster()

        _tcp_utility.assert_called_once_with(password="fake-raft-password", timeout=3)
        _tcp_utility.reset_mock()

        # No status
        _tcp_utility.side_effect = None
        _tcp_utility.return_value.executeCommand.return_value = {}

        assert not raft.cleanup_raft_cluster()

        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            "127.0.0.1:2222", ["status"]
        )

        # All members active
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.1.1.1:2222": 2
        }

        assert raft.cleanup_raft_cluster()

        assert not _remove_raft_member.called

        # Filter by unit ips
        _tcp_utility.return_value.executeCommand.return_value = {
            f"{RAFT_PARTNER_PREFIX}1.1.1.1:2222": 0,
            f"{RAFT_PARTNER_PREFIX}2.2.2.2:2222": 0,
        }

        assert raft.cleanup_raft_cluster()

        _remove_raft_member.assert_called_once_with("2.2.2.2:2222")
        raft.remove_from_members_ips.assert_called_once_with("2.2.2.2")


def test_remove_raft_data(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch("single_kernel_postgresql.managers.raft.wait_fixed", return_value=wait_fixed(0)),
        patch("shutil.rmtree") as _rmtree,
        patch("pathlib.Path.is_dir") as _is_dir,
        patch("pathlib.Path.exists") as _exists,
    ):
        mock_proc_pg = Mock()
        mock_proc_not_pg = Mock()
        mock_proc_pg.name.return_value = "postgres"
        mock_proc_not_pg.name.return_value = "something_else"
        _psutil.process_iter.side_effect = [[mock_proc_not_pg, mock_proc_pg], [mock_proc_not_pg]]

        raft.remove_raft_data()

        raft.patroni_manager.stop_patroni.assert_called_once_with()
        assert _psutil.process_iter.call_count == 2
        _psutil.process_iter.assert_any_call(["name"])
        _rmtree.assert_called_once_with(Path(f"{PATRONI_CONF_PATH}/raft"))


def test_reinitialise_raft_data(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.psutil") as _psutil,
        patch("single_kernel_postgresql.managers.raft.wait_fixed", return_value=wait_fixed(0)),
    ):
        mock_proc_pg = Mock()
        mock_proc_not_pg = Mock()
        mock_proc_pg.name.return_value = "postgres"
        mock_proc_not_pg.name.return_value = "something_else"
        _psutil.process_iter.side_effect = [[mock_proc_not_pg], [mock_proc_not_pg, mock_proc_pg]]
        raft.patroni_manager.get_patroni_health.side_effect = [
            {"role": "replica", "state": "streaming"},
            {"role": "leader", "state": "running"},
        ]

        raft.reinitialise_raft_data()

        raft.update_config.assert_called_once_with(no_peers=True)
        raft.patroni_manager.start_patroni.assert_called_once_with()
        raft.patroni_manager.restart_patroni.assert_called_once_with()
        assert _psutil.process_iter.call_count == 2
        _psutil.process_iter.assert_any_call(["name"])
