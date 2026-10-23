#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the observer manager, scripts and event handler.

Ported from postgresql-operator's test_cluster_topology_observer.py, covering
managers/observer.py, scripts/cluster_topology_observer.py and
scripts/raft_observer.py.
"""

import asyncio
import signal
import sys
from unittest.mock import Mock, PropertyMock, mock_open, patch, sentinel

import pytest
from jinja2 import Template
from ops import ActiveStatus, WaitingStatus
from pysyncobj.utility import UtilityException
from single_kernel_postgresql.config.literals import (
    JUJU_RUN_CMD,
    PEER_RELATION,
    RAFT_PARTNER_PREFIX,
    RAFT_PORT,
)
from single_kernel_postgresql.managers.observer import ObserverManager
from single_kernel_postgresql.scripts.authorisation_rules_observer import (
    UnreachableUnitsError as AuthorisationRulesUnreachableUnitsError,
)
from single_kernel_postgresql.scripts.authorisation_rules_observer import (
    check_for_database_changes as check_for_authorisation_rules_changes,
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
from yaml import safe_dump


@pytest.fixture()
def observer_manager(harness):
    """An ObserverManager built on the charm's real state and workload."""
    return ObserverManager(harness.charm.state, harness.charm.workload)


def test_start_observer(observer_manager, substrate, harness, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/tmp/lib/python/site-packages")
    monkeypatch.setenv("JUJU_CONTEXT_ID", "test-context")
    with (
        patch("builtins.open") as _open,
        patch("single_kernel_postgresql.managers.observer.subprocess.Popen") as _popen,
        patch("os.kill") as _kill,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.observer_pid",
            new_callable=PropertyMock,
        ) as _observer_pid,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.authorisation_rules_observer_pid",
            new_callable=PropertyMock,
        ) as _auth_rules_observer_pid,
        patch(
            "single_kernel_postgresql.core.state.CharmState.unit_ip",
            new_callable=PropertyMock,
            return_value="1.1.1.1",
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.patroni_url",
            new_callable=PropertyMock,
            return_value="https://1.1.1.1:8008",
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.peer_members_ips",
            new_callable=PropertyMock,
            return_value={"2.2.2.2"},
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ) as _cluster_initialised,
        patch(
            "single_kernel_postgresql.workload.k8s.K8sWorkload.container_ready",
            return_value=True,
        ) as _container_ready,
        patch(
            "single_kernel_postgresql.core.state.CharmState.peer_relation",
            new_callable=PropertyMock,
            return_value=harness.charm.model.get_relation(PEER_RELATION),
        ) as _peers,
    ):
        harness.charm.unit.status = ActiveStatus()

        # Test that nothing is done if there is already a running process.
        _observer_pid.return_value = 1
        _auth_rules_observer_pid.return_value = 1
        observer_manager.start_observer()
        _popen.assert_not_called()
        _kill.assert_called_once_with(1, 0)
        _kill.reset_mock()

        # Test that nothing is done if the charm is not in an active status.
        harness.charm.unit.status = WaitingStatus()
        _observer_pid.return_value = None
        _auth_rules_observer_pid.return_value = None
        observer_manager.start_observer()
        _popen.assert_not_called()

        # Test that nothing is done if peer relation is not available yet.
        harness.charm.unit.status = ActiveStatus()
        _peers.return_value = None
        observer_manager.start_observer()
        _popen.assert_not_called()

        # Test that nothing is done before the cluster is initialised
        # (the AuthorisationRulesObserver trunk-parity gate).
        _peers.return_value = harness.charm.model.get_relation(PEER_RELATION)
        _cluster_initialised.return_value = False
        observer_manager.start_observer()
        _popen.assert_not_called()
        _cluster_initialised.return_value = True

        # Test that nothing is done on K8s while the workload container
        # isn't connected yet (the AuthorisationRulesObserver trunk-parity gate).
        if substrate == "k8s":
            _container_ready.return_value = False
            observer_manager.start_observer()
            _popen.assert_not_called()
            _container_ready.return_value = True

        # Test that the observer process is started.
        _popen.return_value = Mock(pid=123)
        observer_manager.start_observer()
        _popen.assert_called_once()
        _open.assert_called_once()
        observer_script = _popen.call_args.args[0][2]
        urls = _popen.call_args.args[0][3]
        assert observer_script.startswith("single_kernel_postgresql.scripts.")
        assert urls == "https://1.1.1.1:8008,https://2.2.2.2:8008"
        assert _popen.call_args.args[0][4] == JUJU_RUN_CMD


def test_start_observer_already_running(observer_manager, harness, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/tmp/lib/python/site-packages")
    with (
        patch("builtins.open"),
        patch("single_kernel_postgresql.managers.observer.subprocess.Popen") as _popen,
        patch("os.kill") as _kill,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.observer_pid",
            new_callable=PropertyMock,
            return_value=1234,
        ) as _observer_pid,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.authorisation_rules_observer_pid",
            new_callable=PropertyMock,
            return_value=1234,
        ) as _auth_rules_observer_pid,
        patch(
            "single_kernel_postgresql.core.state.CharmState.unit_ip",
            new_callable=PropertyMock,
            return_value="1.1.1.1",
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.patroni_url",
            new_callable=PropertyMock,
            return_value="https://1.1.1.1:8008",
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.peer_members_ips",
            new_callable=PropertyMock,
            return_value={"2.2.2.2"},
        ),
        # The merge gates the observer on an initialised cluster and, on K8s,
        # a connected container.
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch(
            "single_kernel_postgresql.workload.k8s.K8sWorkload.container_ready",
            return_value=True,
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.peer_relation",
            new_callable=PropertyMock,
            return_value=harness.charm.model.get_relation(PEER_RELATION),
        ),
    ):
        harness.charm.unit.status = ActiveStatus()
        observer_manager.start_observer()
        _kill.assert_called_once_with(1234, 0)
        assert not _popen.called
        _kill.reset_mock()

        # If process is already dead, it should restart
        _kill.side_effect = OSError
        observer_manager.start_observer()
        _kill.assert_called_once_with(1234, 0)
        _popen.assert_called_once()
        _kill.reset_mock()


def test_stop_observer(observer_manager, harness):
    with (
        patch("os.kill") as _kill,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.observer_pid",
            new_callable=PropertyMock,
            return_value=1,
        ) as _observer_pid,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.authorisation_rules_observer_pid",
            new_callable=PropertyMock,
            return_value=1,
        ) as _auth_rules_observer_pid,
        patch(
            "single_kernel_postgresql.core.state.CharmState.peer_relation",
            new_callable=PropertyMock,
            return_value=harness.charm.model.get_relation(PEER_RELATION),
        ) as _peers,
    ):
        peer_relation = harness.charm.model.get_relation(PEER_RELATION)

        # Test that nothing is done if there is no process running.
        _peers.return_value = None
        observer_manager.stop_observer()
        _kill.assert_not_called()

        _peers.return_value = peer_relation
        _observer_pid.return_value = None
        _auth_rules_observer_pid.return_value = None
        observer_manager.stop_observer()
        _kill.assert_not_called()

        # Test that the process is killed.
        _observer_pid.return_value = 1
        _auth_rules_observer_pid.return_value = 1
        observer_manager.stop_observer()
        _kill.assert_called_once_with(1, signal.SIGINT)
        _kill.reset_mock()

        # Dead process doesn't break the script
        _kill.side_effect = OSError
        observer_manager.stop_observer()
        _kill.assert_called_once_with(1, signal.SIGINT)
        _kill.reset_mock()


def test_cluster_topology_dispatch(harness):
    with patch(
        "single_kernel_postgresql.scripts.cluster_topology_observer.subprocess.run"
    ) as _run:
        command = "test-command"
        charm_dir = "/path"
        dispatch(command, harness.charm.unit.name, charm_dir, "cluster_topology_change")
        _run.assert_called_once_with([
            command,
            "-u",
            harness.charm.unit.name,
            f"JUJU_DISPATCH_PATH=hooks/cluster_topology_change {charm_dir}/dispatch",
        ])


def test_cluster_topology_main():
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
                {"name": "unit-0", "api_url": "http://server1:8008/patroni", "role": "leader"},
            ]
        }
        mock2 = Mock()
        mock2.json.return_value = {
            "members": [
                {"name": "unit-0", "api_url": "https://server3:8008/patroni", "role": "standby"},
            ]
        }
        _get = _async_client.return_value.__aenter__.return_value.get
        # Iterations consume from this list until the first success; the
        # repeated mock1 makes the first iteration succeed no matter the task
        # completion order, mock2 triggers the topology-change dispatch and
        # the trailing errors exhaust the list with UnreachableUnitsError.
        _get.side_effect = [mock1, mock1, mock2, Exception, Exception]

        with pytest.raises(UnreachableUnitsError):
            asyncio.run(main())

        _async_client.assert_any_call(timeout=5, verify=_context.return_value, trust_env=False)
        _get.assert_any_call("http://server1:8008/cluster")
        _get.assert_any_call("http://server2:8008/cluster")

        _subprocess.run.assert_called_once_with([
            "run_cmd",
            "-u",
            "unit/0",
            "JUJU_DISPATCH_PATH=hooks/cluster_topology_change charm_dir/dispatch",
        ])


def test_cluster_topology_check_for_database_changes():
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
            read_data=safe_dump({
                "postgresql": {
                    "listen": "test:5432",
                    "authentication": {
                        "superuser": {
                            "username": "test_user",
                            "password": "test_password",
                        },
                    },
                }
            })
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


def test_start_raft_observer(observer_manager, substrate):
    with (
        patch("single_kernel_postgresql.managers.observer.daemon_reload") as _daemon_reload,
        patch("single_kernel_postgresql.managers.observer.service_enable") as _service_enable,
        patch("single_kernel_postgresql.managers.observer.render_file") as _render_file,
        patch(
            "single_kernel_postgresql.managers.observer.copy_environment",
            return_value={"ENV": "var"},
        ) as _copy_environment,
    ):
        observer_manager.start_raft_observer()

        if substrate != "vm":
            # The merge restricts the raft-observer timer to the VM substrate:
            # nothing is rendered or enabled on K8s.
            _daemon_reload.assert_not_called()
            _service_enable.assert_not_called()
            _render_file.assert_not_called()
            _copy_environment.assert_not_called()
            return

        # Get the expected content from a file.
        with open("single_kernel_postgresql/templates/vm/raft-observer.service.j2") as file:
            contents = file.read()
            template = Template(contents)
        expected_service = template.render(
            envvars={"ENV": "var"}, script="-m single_kernel_postgresql.scripts.raft_observer"
        )
        with open("single_kernel_postgresql/templates/vm/raft-observer.timer.j2") as file:
            contents = file.read()
            template = Template(contents)
        expected_timer = template.render()

        _daemon_reload.assert_called_once_with()
        _service_enable.assert_called_once_with("/etc/systemd/system/raft-observer.timer", "--now")
        assert _render_file.call_count == 2
        _render_file.assert_any_call(
            "vm",
            "/etc/systemd/system/raft-observer.service",
            expected_service,
            0o644,
            change_owner=False,
        )
        _render_file.assert_any_call(
            "vm",
            "/etc/systemd/system/raft-observer.timer",
            expected_timer,
            0o644,
            change_owner=False,
        )
        assert _copy_environment.called


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
            f"127.0.0.1:{RAFT_PORT}", ["status"]
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
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Status exception
        _tcp_utility.return_value.executeCommand.side_effect = UtilityException
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        _tcp_utility.return_value.executeCommand.assert_called_once_with(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Disconnected partner
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                f"{RAFT_PARTNER_PREFIX}1.1.1.1:{RAFT_PORT}": 2,
                f"{RAFT_PARTNER_PREFIX}2.2.2.2:{RAFT_PORT}": 0,
                "has_quorum": True,
                "leader": sentinel.raft_leader,
            },
            UtilityException,
        ]
        check_raft_connection("testpass")

        _tcp_utility.assert_called_once_with(password="testpass", timeout=3)
        assert _tcp_utility.return_value.executeCommand.call_count == 2
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"2.2.2.2:{RAFT_PORT}", ["status"]
        )
        assert not _dispatch.called
        _tcp_utility.reset_mock()

        # Stuck partner
        _tcp_utility.return_value.executeCommand.side_effect = [
            {
                f"{RAFT_PARTNER_PREFIX}1.1.1.1:{RAFT_PORT}": 2,
                f"{RAFT_PARTNER_PREFIX}2.2.2.2:{RAFT_PORT}": 0,
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
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"127.0.0.1:{RAFT_PORT}", ["status"]
        )
        _tcp_utility.return_value.executeCommand.assert_any_call(
            f"2.2.2.2:{RAFT_PORT}", ["status"]
        )
        _dispatch.assert_called_once_with("raft_reconnect")
        _tcp_utility.reset_mock()


def test_authorisation_rules_check_for_database_changes(substrate):
    if substrate != "k8s":
        pytest.skip("Authorisation rules observer is k8s-substrate only")
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
            read_data=safe_dump({
                "postgresql": {
                    "authentication": {
                        "superuser": {
                            "username": "test_user",
                            "password": "test_password",
                        },
                    },
                }
            })
        )
        with patch("builtins.open", mock, create=True):
            _cursor = _psycopg2.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            _cursor.fetchall.side_effect = [[sentinel.databases], sentinel.relation_users]

            # Test the first time this function is called.
            result = check_for_authorisation_rules_changes(run_cmd, unit, charm_dir, None)
            assert result == [sentinel.databases, sentinel.relation_users]
            _subprocess.run.assert_not_called()
            _psycopg2.connect.assert_called_once_with(
                "dbname='postgres' user='operator' host='localhost' "
                "password='test_password' connect_timeout=1"
            )
            assert _cursor.execute.call_count == 2
            _cursor.execute.assert_any_call("SELECT datname, datacl FROM pg_database;")
            _cursor.execute.assert_any_call(
                "SELECT oid, rolname FROM pg_roles WHERE pg_has_role(oid, 'relation_access', 'member');"
            )

            # Test when the databases changed.
            _cursor.fetchall.side_effect = [[sentinel.databases_changed], sentinel.relation_users]
            result = check_for_authorisation_rules_changes(run_cmd, unit, charm_dir, result)
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
            check_for_authorisation_rules_changes(run_cmd, unit, charm_dir, result)
            assert result == [sentinel.databases_changed, sentinel.relation_users]
            _subprocess.run.assert_not_called()


def test_authorisation_rules_main(substrate):
    if substrate != "k8s":
        pytest.skip("Authorisation rules observer is k8s-substrate only")
    with (
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.check_for_database_changes"
        ) as _check_for_database_changes,
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
        _get = _async_client.return_value.__aenter__.return_value.get
        # Iterations consume from this list until the first success; the
        # repeated mock1 makes the first iteration succeed no matter the task
        # completion order, mock2 keeps the loop running and the trailing
        # errors exhaust the list with UnreachableUnitsError.
        _get.side_effect = [mock1, mock1, mock2, Exception, Exception]

        with pytest.raises(AuthorisationRulesUnreachableUnitsError):
            asyncio.run(authorisation_rules_main())

        _async_client.assert_any_call(timeout=5, verify=_context.return_value, trust_env=False)
        _get.assert_any_call("http://server1:8008/cluster")
        _get.assert_any_call("http://server2:8008/cluster")
        assert _check_for_database_changes.called
