# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the generic workload service primitives and pgBackRest paths.

pgBackRest runs as a snap service on VM and as a Pebble service on K8s, so the
backup domain drives them through one name-taking interface. Every service test
runs against two distinct names per substrate: with a single name, an
implementation that hardcoded its own constant would be indistinguishable from
one that honours the argument.
"""

import signal
from unittest.mock import MagicMock, patch

import pytest
from charmlibs import snap
from ops.pebble import ServiceStatus
from single_kernel_postgresql.config.literals import (
    K8S_PGBACK_REST_SERVER_SERVICE_NAME,
    K8S_PGBACKREST_METRICS_SERVER_SERVICE_NAME,
    VM_PGBACKREST_EXECUTABLE,
    VM_PGBACKREST_EXPORTER_SERVICE_NAME,
    VM_PGBACKREST_SERVICE_NAME,
)

VM_SERVICE_NAMES = [VM_PGBACKREST_SERVICE_NAME, VM_PGBACKREST_EXPORTER_SERVICE_NAME]
K8S_SERVICE_NAMES = [
    K8S_PGBACK_REST_SERVER_SERVICE_NAME,
    K8S_PGBACKREST_METRICS_SERVER_SERVICE_NAME,
]


@pytest.fixture
def workload(harness):
    return harness.charm.workload


@pytest.fixture
def snap_package():
    """Patch charmlibs.snap.SnapCache and yield the charmed-postgresql package mock."""
    package = MagicMock()
    with patch(
        "single_kernel_postgresql.workload.vm.snap.SnapCache",
        return_value={"charmed-postgresql": package},
    ):
        yield package


def _pebble_service(status):
    service = MagicMock()
    service.current = status
    return service


# -- literals ----------------------------------------------------------------


def test_pgbackrest_service_literals():
    assert VM_PGBACKREST_SERVICE_NAME == "pgbackrest-service"
    assert VM_PGBACKREST_EXPORTER_SERVICE_NAME == "pgbackrest-exporter"
    assert K8S_PGBACK_REST_SERVER_SERVICE_NAME == "pgbackrest server"
    assert VM_PGBACKREST_EXECUTABLE == "charmed-postgresql.pgbackrest"


# -- pgbackrest executable ---------------------------------------------------


def test_pgbackrest_executable_is_substrate_specific(substrate, workload):
    if substrate == "vm":
        assert workload.pgbackrest_executable == "charmed-postgresql.pgbackrest"
    else:
        assert workload.pgbackrest_executable == "pgbackrest"


# -- pgbackrest log directory ------------------------------------------------


def test_pgbackrest_logs_path(substrate, workload):
    if substrate == "vm":
        assert (
            str(workload.paths.pgbackrest_logs)
            == "/var/snap/charmed-postgresql/common/var/log/pgbackrest"
        )
    else:
        assert str(workload.paths.pgbackrest_logs) == "/var/lib/pg/logs/16/main/pgbackrest_logs"


# -- VM: snap-backed service actions -----------------------------------------


@pytest.mark.parametrize("service_name", VM_SERVICE_NAMES)
@pytest.mark.parametrize(
    ("method", "snap_action"),
    [("start_service", "start"), ("stop_service", "stop"), ("restart_service", "restart")],
)
def test_vm_service_actions_drive_the_snap(
    substrate, workload, snap_package, method, snap_action, service_name
):
    if substrate != "vm":
        pytest.skip("VM only")

    getattr(workload, method)(service_name)

    getattr(snap_package, snap_action).assert_called_once_with(services=[service_name])


@pytest.mark.parametrize("service_name", VM_SERVICE_NAMES)
def test_vm_reload_service_restarts_the_snap_service(
    substrate, workload, snap_package, service_name
):
    """The snap layer exposes no signal channel, so reload degrades to a restart."""
    if substrate != "vm":
        pytest.skip("VM only")

    workload.reload_service(service_name)

    snap_package.restart.assert_called_once_with(services=[service_name])


@pytest.mark.parametrize("service_name", VM_SERVICE_NAMES)
@pytest.mark.parametrize("active", [True, False])
def test_vm_is_service_running_reads_the_snap_service_state(
    substrate, workload, snap_package, active, service_name
):
    if substrate != "vm":
        pytest.skip("VM only")
    # The other service carries the opposite state, so a lookup that ignores the
    # requested name answers for the wrong service.
    snap_package.services = {
        name: {"active": active if name == service_name else not active}
        for name in VM_SERVICE_NAMES
    }

    assert workload.is_service_running(service_name) is active


def test_vm_is_service_running_is_false_for_an_unknown_service(substrate, workload, snap_package):
    """A snap revision predating a service omits it, which must not raise KeyError."""
    if substrate != "vm":
        pytest.skip("VM only")
    snap_package.services = {VM_PGBACKREST_SERVICE_NAME: {"active": True}}

    assert workload.is_service_running(VM_PGBACKREST_EXPORTER_SERVICE_NAME) is False


def test_vm_is_service_running_is_false_when_the_snap_errors(substrate, workload):
    if substrate != "vm":
        pytest.skip("VM only")

    with patch(
        "single_kernel_postgresql.workload.vm.snap.SnapCache", side_effect=snap.SnapError("boom")
    ):
        assert workload.is_service_running(VM_PGBACKREST_SERVICE_NAME) is False


# -- K8s: pebble-backed service actions --------------------------------------


@pytest.mark.parametrize("service_name", K8S_SERVICE_NAMES)
@pytest.mark.parametrize(
    ("method", "container_action"),
    [("start_service", "start"), ("stop_service", "stop"), ("restart_service", "restart")],
)
def test_k8s_service_actions_drive_the_container(
    substrate, workload, method, container_action, service_name
):
    if substrate != "k8s":
        pytest.skip("K8s only")

    with patch.object(workload, "container") as container:
        getattr(workload, method)(service_name)

    getattr(container, container_action).assert_called_once_with(service_name)


@pytest.mark.parametrize("service_name", K8S_SERVICE_NAMES)
def test_k8s_reload_service_sends_sighup(substrate, workload, service_name):
    if substrate != "k8s":
        pytest.skip("K8s only")

    with patch.object(workload, "container") as container:
        workload.reload_service(service_name)

    container.pebble.send_signal.assert_called_once_with(signal.SIGHUP, services=[service_name])


@pytest.mark.parametrize("service_name", K8S_SERVICE_NAMES)
@pytest.mark.parametrize(
    ("status", "expected"),
    [(ServiceStatus.ACTIVE, True), (ServiceStatus.INACTIVE, False)],
)
def test_k8s_is_service_running_reads_the_pebble_status(
    substrate, workload, status, expected, service_name
):
    if substrate != "k8s":
        pytest.skip("K8s only")

    with patch.object(workload, "container") as container:
        container.pebble.get_services.return_value = [_pebble_service(status)]
        running = workload.is_service_running(service_name)

    container.pebble.get_services.assert_called_once_with(names=[service_name])
    assert running is expected


def test_k8s_is_service_running_is_false_for_an_unknown_service(substrate, workload):
    if substrate != "k8s":
        pytest.skip("K8s only")

    with patch.object(workload, "container") as container:
        container.pebble.get_services.return_value = []

        assert workload.is_service_running(K8S_PGBACK_REST_SERVER_SERVICE_NAME) is False


def test_k8s_is_service_running_is_false_when_the_container_is_unreachable(substrate, workload):
    """Pebble is not answerable before the container is up, as for is_patroni_running."""
    if substrate != "k8s":
        pytest.skip("K8s only")

    with patch.object(workload, "container") as container:
        container.can_connect.return_value = False

        assert workload.is_service_running(K8S_PGBACK_REST_SERVER_SERVICE_NAME) is False

    container.pebble.get_services.assert_not_called()
