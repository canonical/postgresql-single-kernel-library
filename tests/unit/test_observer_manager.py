# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the ObserverManager raft-observer systemd unit rendering (VM).

Port of the VM charm's tests/unit/test_cluster_topology_observer.py
raft-observer cases (post-#2029 residual) against the lib manager.
"""

import importlib.resources
from unittest.mock import MagicMock, patch

from jinja2 import Template
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
