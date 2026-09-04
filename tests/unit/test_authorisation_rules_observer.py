# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the K8s authorisation rules observer handler.

The spawned-script suite lives in test_observer_scripts.py; these cover the
start/stop process lifecycle the K8s charm drives on its event handlers.
"""

import signal
from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops.model import ActiveStatus, WaitingStatus
from single_kernel_postgresql.config.enums import Substrates


def test_observer_is_substrate_gated(harness, substrate):
    charm = harness.charm
    if substrate == Substrates.K8S:
        assert hasattr(charm, "authorisation_rules_observer")
        assert not hasattr(charm, "cluster_topology_observer")
    else:
        assert hasattr(charm, "cluster_topology_observer")
        assert not hasattr(charm, "authorisation_rules_observer")


def test_start_authorisation_rules_observer(harness, substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the authorisation rules observer is K8s-only")
    charm = harness.charm
    charm.unit.status = ActiveStatus()
    container = Mock()
    container.can_connect.return_value = True
    with (
        patch("builtins.open"),
        patch("subprocess.Popen") as _popen,
        patch("os.kill") as _kill,
        patch.object(charm.unit, "get_container", return_value=container),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.data",
            new_callable=PropertyMock,
        ) as _peer_data,
        patch(
            "single_kernel_postgresql.core.state.CharmState.endpoints",
            new_callable=PropertyMock,
            return_value=set(),
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.application",
            new_callable=PropertyMock,
        ) as _application,
    ):
        _application.return_value = Mock(is_cluster_initialised=True)
        observer = charm.authorisation_rules_observer

        # Test that the process is started on a connected, initialised cluster.
        _peer_data.return_value = {}
        _popen.return_value = Mock(pid=42)
        observer.start_authorisation_rules_observer()
        _popen.assert_called_once()
        container.can_connect.assert_called_once()
        _kill.assert_not_called()
        assert _peer_data.return_value["authorisation-rules-observer-pid"] == "42"

        # Test that nothing is done if the charm is not in an active status.
        _popen.reset_mock()
        charm.unit.status = WaitingStatus()
        observer.start_authorisation_rules_observer()
        _popen.assert_not_called()
        charm.unit.status = ActiveStatus()

        # Test that nothing is done if there is already a running process.
        _kill.side_effect = None
        observer.start_authorisation_rules_observer()
        _popen.assert_not_called()
        _kill.assert_called_once_with(42, 0)
        _kill.reset_mock()

        # If the stored process is already dead, it should restart.
        _kill.side_effect = OSError
        observer.start_authorisation_rules_observer()
        _kill.assert_called_once_with(42, 0)
        _popen.assert_called_once()


def test_start_authorisation_rules_observer_on_uninitialised_cluster(harness, substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the authorisation rules observer is K8s-only")
    charm = harness.charm
    charm.unit.status = ActiveStatus()
    container = Mock()
    container.can_connect.return_value = True
    with (
        patch("subprocess.Popen") as _popen,
        patch.object(charm.unit, "get_container", return_value=container),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.data",
            new_callable=PropertyMock,
        ) as _peer_data,
        patch(
            "single_kernel_postgresql.core.state.CharmState.application",
            new_callable=PropertyMock,
        ) as _application,
    ):
        _application.return_value = Mock(is_cluster_initialised=False)
        charm.authorisation_rules_observer.start_authorisation_rules_observer()
        _popen.assert_not_called()


def test_stop_authorisation_rules_observer(harness, substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the authorisation rules observer is K8s-only")
    with (
        patch("os.kill") as _kill,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.data",
            new_callable=PropertyMock,
        ) as _peer_data,
    ):
        observer = harness.charm.authorisation_rules_observer

        # Test that nothing is done if there is no process running.
        observer.stop_authorisation_rules_observer()
        _kill.assert_not_called()

        _peer_data.return_value = {}
        observer.stop_authorisation_rules_observer()
        _kill.assert_not_called()

        # Test that the process is killed and its pid dropped.
        _peer_data.return_value = {"authorisation-rules-observer-pid": "1"}
        observer.stop_authorisation_rules_observer()
        _kill.assert_called_once_with(1, signal.SIGTERM)
        assert "authorisation-rules-observer-pid" not in _peer_data.return_value
        _kill.reset_mock()

        # Dead process doesn't break the stop.
        _peer_data.return_value = {"authorisation-rules-observer-pid": "1"}
        _kill.side_effect = OSError
        observer.stop_authorisation_rules_observer()
        _kill.assert_called_once_with(1, signal.SIGTERM)
        _kill.reset_mock()
