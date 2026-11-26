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
            "single_kernel_postgresql.core.state.CharmState.peer_members_ips",
            new_callable=PropertyMock,
            return_value={"2.2.2.2"},
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.endpoints",
            new_callable=PropertyMock,
            # On K8s the endpoints are service hostnames.
            return_value=[
                "postgresql-0.postgresql-endpoints.test-model.svc.cluster.local",
                "postgresql-1.postgresql-endpoints.test-model.svc.cluster.local",
            ],
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
        endpoints = _popen.call_args.args[0][3]
        assert observer_script.startswith("single_kernel_postgresql.scripts.")
        if substrate == "vm":
            # On VM the endpoints are the unit IP followed by its peers' IPs.
            assert endpoints == "1.1.1.1,2.2.2.2"
        else:
            # On K8s they come from the application endpoints (mocked above).
            assert endpoints == (
                "postgresql-0.postgresql-endpoints.test-model.svc.cluster.local,"
                "postgresql-1.postgresql-endpoints.test-model.svc.cluster.local"
            )
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
            "single_kernel_postgresql.core.state.CharmState.peer_members_ips",
            new_callable=PropertyMock,
            return_value={"2.2.2.2"},
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.endpoints",
            new_callable=PropertyMock,
            # On K8s the endpoints are service hostnames.
            return_value=[
                "postgresql-0.postgresql-endpoints.test-model.svc.cluster.local",
                "postgresql-1.postgresql-endpoints.test-model.svc.cluster.local",
            ],
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
        ) as _check_for_database_changes,
        patch.object(
            sys,
            "argv",
            ["cmd", "10.0.0.1,10.0.0.2", "run_cmd", "unit/0", "charm_dir"],
        ),
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.sleep", return_value=None
        ),
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.parallel_patroni_get_request"
        ) as _parallel_patroni_get_request,
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.subprocess"
        ) as _subprocess,
        patch(
            "single_kernel_postgresql.scripts.cluster_topology_observer.create_default_context"
        ) as _context,
    ):
        # The first iteration captures the initial topology and, this unit
        # being the leader, checks the databases. The second iteration
        # detects the topology change and dispatches the event. The final
        # failure to reach any member raises UnreachableUnitsError.
        _parallel_patroni_get_request.side_effect = [
            {
                "members": [
                    {"name": "unit-0", "host": "10.0.0.1", "role": "leader"},
                    {"name": "unit-1", "host": "10.0.0.3", "role": "standby"},
                ]
            },
            {
                "members": [
                    {"name": "unit-1", "host": "10.0.0.3", "role": "leader"},
                    {"name": "unit-0", "host": "10.0.0.1", "role": "standby"},
                ]
            },
            None,
        ]

        with pytest.raises(UnreachableUnitsError):
            asyncio.run(main())

        # The first call uses the endpoints passed by the manager; the next
        # calls use the members' hosts rebuilt from the cluster status, with
        # the current unit called first.
        cafile = "/var/snap/charmed-postgresql/current/etc/patroni/peer_ca_bundle.pem"
        _parallel_patroni_get_request.assert_any_call(
            "/cluster", ["10.0.0.1", "10.0.0.2"], cafile, None, True
        )
        _parallel_patroni_get_request.assert_any_call(
            "/cluster", ["10.0.0.1", "10.0.0.3"], cafile, None, True
        )
        assert _check_for_database_changes.call_count == 1

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
            ["cmd", "10.0.0.1,10.0.0.2", "run_cmd", "unit/0", "charm_dir"],
        ),
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.sleep",
            return_value=None,
        ),
        patch(
            "single_kernel_postgresql.scripts.authorisation_rules_observer.parallel_patroni_get_request"
        ) as _parallel_patroni_get_request,
    ):
        # The first iteration finds this unit as the leader, so the databases
        # are checked. The second iteration finds it demoted, so only the
        # endpoints are rebuilt. The final failure to reach any member raises
        # UnreachableUnitsError.
        _parallel_patroni_get_request.side_effect = [
            {
                "members": [
                    {"name": "unit-2", "host": "10.0.0.3", "role": "standby"},
                    {"name": "unit-0", "host": "10.0.0.1", "role": "leader"},
                ]
            },
            {
                "members": [
                    {"name": "unit-2", "host": "10.0.0.3", "role": "leader"},
                    {"name": "unit-0", "host": "10.0.0.1", "role": "standby"},
                ]
            },
            None,
        ]

        with pytest.raises(AuthorisationRulesUnreachableUnitsError):
            asyncio.run(authorisation_rules_main())

        # The first call uses the endpoints passed by the manager; the next
        # calls use the members' hosts rebuilt from the cluster status, with
        # the current unit called first.
        _parallel_patroni_get_request.assert_any_call(
            "/cluster", ["10.0.0.1", "10.0.0.2"], "/tmp/peer_ca_bundle.pem", None, True
        )
        _parallel_patroni_get_request.assert_any_call(
            "/cluster", ["10.0.0.1", "10.0.0.3"], "/tmp/peer_ca_bundle.pem", None, True
        )
        assert _check_for_database_changes.call_count == 1


@pytest.mark.parametrize(
    "states,check", [({"patroni": True}, True), ({"patroni": False}, False), ({}, False)]
)
def test_raft_observer_gates_on_patroni_state(states, check):
    from single_kernel_postgresql.scripts import raft_observer

    with (
        patch.object(raft_observer, "service_states", return_value=states),
        patch.object(raft_observer, "get_raft_password", return_value="secret") as password,
        patch.object(raft_observer, "check_raft_connection") as connection,
    ):
        raft_observer.main()
    assert password.called is check
    if check:
        connection.assert_called_once_with("secret")
    else:
        connection.assert_not_called()


def test_raft_observer_query_failure_does_not_check_connection():
    from charmlibs import snap
    from single_kernel_postgresql.scripts import raft_observer

    with (
        patch.object(
            raft_observer, "service_states", side_effect=snap.ConnectionError("unavailable")
        ),
        patch.object(raft_observer, "check_raft_connection") as connection,
        pytest.raises(snap.ConnectionError),
    ):
        raft_observer.main()
    connection.assert_not_called()
