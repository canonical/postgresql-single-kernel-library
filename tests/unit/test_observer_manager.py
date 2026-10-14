# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the ObserverManager raft-observer systemd unit rendering (VM).

Port of the VM charm's tests/unit/test_cluster_topology_observer.py
raft-observer cases (post-#2029 residual) against the lib manager.
"""

import importlib.resources
from unittest.mock import MagicMock, patch

from jinja2 import Template
from ops import ActiveStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.managers.observer import ObserverManager


def _expected(service: bool, envvars: dict[str, str] | None = None) -> str:
    name = "raft-observer.service.j2" if service else "raft-observer.timer.j2"
    template = Template(
        importlib.resources
        .files("single_kernel_postgresql.templates")
        .joinpath("vm", name)
        .read_text()
    )
    if service:
        return template.render(
            envvars=envvars,
            script="-m single_kernel_postgresql.scripts.raft_observer",
        )
    return template.render()


def test_start_raft_observer_renders_and_enables_the_timer():
    state = MagicMock()
    state.substrate = Substrates.VM
    manager = ObserverManager(state, MagicMock())
    with (
        patch("single_kernel_postgresql.managers.observer.daemon_reload") as _daemon_reload,
        patch("single_kernel_postgresql.managers.observer.service_enable") as _service_enable,
        patch("single_kernel_postgresql.managers.observer.render_file") as _render_file,
        patch(
            "single_kernel_postgresql.managers.observer.copy_environment",
            return_value={"ENV": "var"},
        ),
    ):
        manager.start_raft_observer()

    _daemon_reload.assert_called_once_with()
    _service_enable.assert_called_once_with("/etc/systemd/system/raft-observer.timer", "--now")
    assert _render_file.call_count == 2
    _render_file.assert_any_call(
        Substrates.VM,
        "/etc/systemd/system/raft-observer.service",
        _expected(True, envvars={"ENV": "var"}),
        0o644,
        change_owner=False,
    )
    _render_file.assert_any_call(
        Substrates.VM,
        "/etc/systemd/system/raft-observer.timer",
        _expected(False),
        0o644,
        change_owner=False,
    )


def test_start_raft_observer_noop_on_k8s():
    state = MagicMock()
    state.substrate = Substrates.K8S
    manager = ObserverManager(state, MagicMock())
    with patch("single_kernel_postgresql.managers.observer.render_file") as _render_file:
        manager.start_raft_observer()
    _render_file.assert_not_called()


def _gate_state(substrate: Substrates) -> MagicMock:
    state = MagicMock()
    state.substrate = substrate
    state.peer.unit.status = ActiveStatus()
    state.application.is_cluster_initialised = True
    state.peer.observer_pid = None
    state.peer.authorisation_rules_observer_pid = None
    state.peer_relation = MagicMock()
    state.unit_ip = "10.0.0.5"
    state.patroni_url = "http://10.0.0.5:8008"
    state.peer_members_ips = ["10.0.0.6"]
    state.model.unit.name = "postgresql-k8s/0"
    state.framework.charm_dir = "/tmp/charm"
    return state


@patch("builtins.open", MagicMock())
@patch("single_kernel_postgresql.managers.observer.subprocess.Popen")
def test_start_observer_starts_when_trunk_gates_pass(_popen):
    state = _gate_state(Substrates.VM)
    state.patroni_url = "http://10.0.0.5:8008"
    _popen.return_value.pid = 42
    manager = ObserverManager(state, MagicMock())
    with patch(
        "single_kernel_postgresql.managers.observer.copy_environment",
        return_value={"PYTHONPATH": "/charm/lib/python3.12/site-packages"},
    ):
        manager.start_observer()
    assert state.peer.observer_pid == 42


@patch("builtins.open", MagicMock())
@patch("single_kernel_postgresql.managers.observer.subprocess.Popen")
def test_start_observer_skips_when_cluster_not_initialised(_popen):
    state = _gate_state(Substrates.VM)
    state.application.is_cluster_initialised = False
    manager = ObserverManager(state, MagicMock())
    manager.start_observer()
    _popen.assert_not_called()


@patch("builtins.open", MagicMock())
@patch("single_kernel_postgresql.managers.observer.subprocess.Popen")
def test_start_observer_skips_when_container_not_connected(_popen):
    state = _gate_state(Substrates.K8S)
    workload = MagicMock()
    workload.container.can_connect.return_value = False
    manager = ObserverManager(state, workload)
    manager.start_observer()
    _popen.assert_not_called()
