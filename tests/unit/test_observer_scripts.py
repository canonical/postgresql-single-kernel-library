# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
import sys
from unittest.mock import Mock, mock_open, patch, sentinel

import pytest
from pysyncobj.utility import UtilityException
from single_kernel_postgresql.scripts.authorisation_rules_observer import (
    UnreachableUnitsError as AuthorisationRulesUnreachableUnitsError,
)
from single_kernel_postgresql.scripts.authorisation_rules_observer import (
    check_for_database_changes as authorisation_rules_check_for_database_changes,
)
from single_kernel_postgresql.scripts.authorisation_rules_observer import (
    main as authorisation_rules_main,
)
from single_kernel_postgresql.scripts.cluster_topology_observer import (
    UnreachableUnitsError,
    check_for_database_changes,
    dispatch,
    main,
)
from single_kernel_postgresql.scripts.raft_observer import check_raft_connection


def test_dispatch():
    with patch("subprocess.run") as _run:
        command = "test-command"
        charm_dir = "/path"
        dispatch(command, "postgresql-single-kernel/0", charm_dir, "cluster_topology_change")
        _run.assert_called_once_with([
            command,
            "-u",
            "postgresql-single-kernel/0",
            f"JUJU_DISPATCH_PATH=hooks/cluster_topology_change {charm_dir}/dispatch",
        ])


async def test_main():
    with (
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.check_for_database_changes"
        ),
        patch.object(
            sys,
            "argv",
            ["cmd", "http://server1:8008,http://server2:8008", "run_cmd", "unit/0", "charm_dir"],
        ),
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.sleep", return_value=None
        ),
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.AsyncClient"
        ) as _async_client,
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.subprocess"
        ) as _subprocess,
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.create_default_context"
        ) as _context,
    ):
        mock1 = Mock()
        mock1.json.return_value = {
            "members": [
                {"name": "unit-2", "api_url": "http://server3:8008/patroni", "role": "standby"},
                {"name": "unit-0", "api_url": "http://server1:8008/patroni", "role": "leader"},
            ]
        }
        mock2 = Mock()
        mock2.json.return_value = {
            "members": [
                {"name": "unit-2", "api_url": "https://server3:8008/patroni", "role": "leader"},
            ]
        }
        async with _async_client() as cli:
            _get = cli.get
            _get.side_effect = [
                mock1,
                Exception,
                mock2,
            ]
        with pytest.raises(UnreachableUnitsError):
            await main()
        _async_client.assert_any_call(timeout=5, verify=_context.return_value)
        _get.assert_any_call("http://server1:8008/cluster")
        _get.assert_any_call("http://server3:8008/cluster")

        _subprocess.run.assert_called_once_with([
            "run_cmd",
            "-u",
            "unit/0",
            "JUJU_DISPATCH_PATH=hooks/cluster_topology_change charm_dir/dispatch",
        ])


def test_check_for_database_changes():
    with (
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.subprocess"
        ) as _subprocess,
        patch("single_kernel_postgresql.scripts.cluster_topology_observer.psycopg2") as _psycopg2,
    ):
        run_cmd = "run_cmd"
        unit = "unit/0"
        charm_dir = "charm_dir"
        mock = mock_open(
            read_data="""postgresql:
  listen: test:5432
  authentication:
    superuser:
      username: test_user
      password: test_password"""
        )
        with patch("builtins.open", mock, create=True):
            _cursor = _psycopg2.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            _cursor.fetchall.side_effect = [[sentinel.databases], sentinel.relation_users]

            # Test the first time this function is called.
            result = check_for_database_changes(run_cmd, unit, charm_dir, None)
            assert result == [sentinel.databases, sentinel.relation_users]
            _subprocess.run.assert_not_called()
            _psycopg2.connect.assert_called_once_with(
                "dbname='postgres' user='operator' host='/tmp/snap-private-tmp/snap.charmed-postgresql/tmp/' "
                "password='test_password' connect_timeout=1"
            )
            assert _cursor.execute.call_count == 2
            _cursor.execute.assert_any_call("SELECT datname, datacl FROM pg_database;")
            _cursor.execute.assert_any_call(
                "SELECT oid, rolname FROM pg_roles WHERE pg_has_role(oid, 'relation_access', 'member');"
            )

            # Test when the databases changed.
            _cursor.fetchall.side_effect = [[sentinel.databases_changed], sentinel.relation_users]
            result = check_for_database_changes(run_cmd, unit, charm_dir, result)
            assert result == [sentinel.databases_changed, sentinel.relation_users]

            _subprocess.run.assert_called_once_with([
                run_cmd,
                "-u",
                unit,
                f"JUJU_DISPATCH_PATH=hooks/databases_change {charm_dir}/dispatch",
            ])

            # Test when the databases haven't changed.
            _subprocess.reset_mock()
            _cursor.fetchall.side_effect = [[sentinel.databases_changed], sentinel.relation_users]
            check_for_database_changes(run_cmd, unit, charm_dir, result)
            assert result == [sentinel.databases_changed, sentinel.relation_users]
            _subprocess.run.assert_not_called()


def test_check_raft_connection():
    with (
        patch("single_kernel_postgresql.scripts.raft_observer.TcpUtility") as _tcp_utility,
        patch("single_kernel_postgresql.scripts.raft_observer.dispatch") as _dispatch,
    ):
        # No status
        _tcp_utility.return_value.executeCommand.return_value = None
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            "127.0.0.1:2222", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # No leader
        _tcp_utility.return_value.executeCommand.return_value = {
            "has_quorum": False,
            "leader": None,
        }

        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            "127.0.0.1:2222", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Status exception
        _tcp_utility.return_value.executeCommand.side_effect = UtilityException
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            "127.0.0.1:2222", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Disconnected partner
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                "partner_node_status_server_1.1.1.1:2222": 2,
                "partner_node_status_server_2.2.2.2:2222": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            UtilityException,
        ]
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        assert _tcp_utility.return_value.executeCommand.call_count == 2
        _tcp_utility.return_value.executeCommand.assert_any_call("127.0.0.1:2222", ["status"])
        _tcp_utility.return_value.executeCommand.assert_any_call("2.2.2.2:2222", ["status"])
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Stuck partner
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                "partner_node_status_server_1.1.1.1:2222": 2,
                "partner_node_status_server_2.2.2.2:2222": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            {
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
        ]
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        assert _tcp_utility.return_value.executeCommand.call_count == 2
        _tcp_utility.return_value.executeCommand.assert_any_call("127.0.0.1:2222", ["status"])
        _tcp_utility.return_value.executeCommand.assert_any_call("2.2.2.2:2222", ["status"])
        _dispatch.assert_called_once_with("raft_reconnect")
        _tcp_utility.reset_mock()


async def test_authorisation_rules_main():
    with (
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.check_for_database_changes"
        ),
        patch.object(
            sys,
            "argv",
            ["cmd", "http://server1:8008,http://server2:8008", "run_cmd", "unit/0", "charm_dir"],
        ),
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.sleep",
            return_value=None,
        ),
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.AsyncClient"
        ) as _async_client,
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.create_default_context"
        ) as _context,
    ):
        mock1 = Mock()
        mock1.json.return_value = {
            "members": [
                {"name": "unit-2", "api_url": "http://server3:8008/patroni", "role": "standby"},
                {"name": "unit-0", "api_url": "http://server1:8008/patroni", "role": "leader"},
            ]
        }
        mock2 = Mock()
        mock2.json.return_value = {
            "members": [
                {"name": "unit-2", "api_url": "https://server3:8008/patroni", "role": "leader"},
            ]
        }
        async with _async_client() as cli:
            _get = cli.get
            _get.side_effect = [
                mock1,
                Exception,
                mock2,
            ]
        with pytest.raises(AuthorisationRulesUnreachableUnitsError):
            await authorisation_rules_main()
        _async_client.assert_any_call(timeout=5, verify=_context.return_value)
        _get.assert_any_call("http://server1:8008/cluster")
        _get.assert_any_call("http://server3:8008/cluster")


def test_authorisation_rules_check_for_database_changes():
    with (
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.subprocess"
        ) as _subprocess,
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.psycopg2"
        ) as _psycopg2,
    ):
        run_cmd = "run_cmd"
        unit = "unit/0"
        charm_dir = "charm_dir"
        mock = mock_open(
            read_data="""postgresql:
  authentication:
    superuser:
      username: test_user
      password: test_password"""
        )
        with patch("builtins.open", mock, create=True):
            _cursor = _psycopg2.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            _cursor.fetchall.side_effect = [[sentinel.databases], sentinel.relation_users]

            # Test the first time this function is called.
            result = authorisation_rules_check_for_database_changes(run_cmd, unit, charm_dir, None)
            assert result == [sentinel.databases, sentinel.relation_users]
            _subprocess.run.assert_not_called()
            _psycopg2.connect.assert_called_once_with(
                "dbname='postgres' user='operator' host='localhost' password='test_password' connect_timeout=1"
            )
            assert _cursor.execute.call_count == 2
            _cursor.execute.assert_any_call("SELECT datname, datacl FROM pg_database;")
            _cursor.execute.assert_any_call(
                "SELECT oid, rolname FROM pg_roles WHERE pg_has_role(oid, 'relation_access', 'member');"
            )

            # Test when the databases changed.
            _cursor.fetchall.side_effect = [[sentinel.databases_changed], sentinel.relation_users]
            result = authorisation_rules_check_for_database_changes(
                run_cmd, unit, charm_dir, result
            )
            assert result == [sentinel.databases_changed, sentinel.relation_users]

            _subprocess.run.assert_called_once_with([
                run_cmd,
                "-u",
                unit,
                f"JUJU_DISPATCH_PATH=hooks/databases_change {charm_dir}/dispatch",
            ])

            # Test when the databases haven't changed.
            _subprocess.reset_mock()
            _cursor.fetchall.side_effect = [[sentinel.databases_changed], sentinel.relation_users]
            authorisation_rules_check_for_database_changes(run_cmd, unit, charm_dir, result)
            assert result == [sentinel.databases_changed, sentinel.relation_users]
            _subprocess.run.assert_not_called()
