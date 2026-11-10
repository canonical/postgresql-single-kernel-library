# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Juju helpers that don't depend on the charm under test."""

from collections.abc import Callable, Generator
from contextlib import contextmanager

import jubilant
from single_kernel_postgresql.config.literals import PEER_RELATION
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_fixed,
)

type StatusFn = Callable[[jubilant.Status], bool]

MINUTE_SECS = 60


def retry_if_cli_error[T](fn: Callable[[], T], *, max_attempts: int = 10) -> T:
    """Call fn, retrying it while it raises jubilant.CLIError.

    Juju CLI calls can fail intermittently under CPU pressure (e.g. "connection is shut
    down"), mostly seen on K8s.
    """
    for attempt in Retrying(
        retry=retry_if_exception_type(jubilant.CLIError),
        stop=stop_after_attempt(max_attempts),
        wait=wait_fixed(10),
        reraise=True,
    ):
        with attempt:
            return fn()
    raise AssertionError("unreachable")


def split_model(model: str) -> tuple[str | None, str]:
    """Split a "[controller:]model" name into the controller (or None) and the bare model."""
    controller, _, name = model.rpartition(":")
    return controller or None, name


def get_app_leader(juju: jubilant.Juju, app_name: str) -> str:
    """Return the name of the leader unit of an application."""
    for name, unit in juju.status().apps[app_name].units.items():
        if unit.leader:
            return name
    raise AssertionError(f"No leader unit found for {app_name}")


def get_user_password(juju: jubilant.Juju, app_name: str, user: str) -> str | None:
    """Get a system user's password."""
    for secret in juju.secrets():
        if secret.label == f"{PEER_RELATION}.{app_name}.app":
            revealed_secret = juju.show_secret(secret.uri, reveal=True)
            return revealed_secret.content.get(f"{user}-password")


def get_app_units(
    juju: jubilant.Juju, app_name: str
) -> dict[str, jubilant.statustypes.UnitStatus]:
    """Return the units of an application."""
    return juju.status().get_units(app_name)


def get_unit_address(juju: jubilant.Juju, unit_name: str) -> str:
    """Return the IP address of a unit.

    Machine units only have a public address, K8s units only have the pod address.
    """
    status = juju.status()
    unit = status.get_units(unit_name.split("/")[0])[unit_name]
    return unit.address if status.model.type == "caas" else unit.public_address


def relations(juju: jubilant.Juju, provider_app: str, requirer_app: str) -> list:
    """Return the relations of provider_app that involve requirer_app."""
    return [
        relation
        for relation in juju.status().apps[provider_app].relations.values()
        if any(instance.related_app == requirer_app for instance in relation)
    ]


def get_credentials(juju: jubilant.Juju, unit_name: str) -> dict:
    """Return the credentials of a data-integrator unit."""
    return juju.run(unit_name, "get-credentials").results


def get_secret_by_label(juju: jubilant.Juju, label: str) -> dict[str, str]:
    """Return the content of the secret with the given label in the model."""
    for secret in juju.secrets():
        revealed = juju.show_secret(secret.uri, reveal=True)
        if revealed.label == label:
            return revealed.content
    raise AssertionError(f"Secret with label {label} not found")


@contextmanager
def fast_forward(juju: jubilant.Juju, interval: str = "10s") -> Generator[None]:
    """Run update-status hooks every `interval` inside the context (back to 5m on exit)."""
    juju.model_config({"update-status-hook-interval": interval})
    try:
        yield
    finally:
        juju.model_config({"update-status-hook-interval": "5m"})


def remove_application(juju: jubilant.Juju, app_name: str, timeout: float = 1000) -> None:
    """Remove an application and wait until it is gone from the model."""
    juju.remove_application(app_name)
    juju.wait(lambda status: app_name not in status.apps, timeout=timeout)


def wait_for_apps_status(status_fn: Callable[..., bool], *apps: str) -> StatusFn:
    """Return a wait condition: agents idle and apps in the status checked by status_fn.

    Args:
        status_fn: jubilant status function, e.g. jubilant.all_active.
        apps: the applications to check.
    """
    return lambda status: jubilant.all_agents_idle(status, *apps) and status_fn(status, *apps)


def wait_resilient(juju: jubilant.Juju, **kwargs) -> None:
    """Run ``juju.wait`` but retry through transient controller CLIErrors.

    Force-stopping a whole datacenter's machines can momentarily stress a single
    LXD-hosted juju controller — ``juju status`` can transiently fail mid-teardown
    with "no controller API addresses; is bootstrap still in progress?". Retry
    across those blips; a real readiness timeout (``WaitError``) still propagates.
    """
    for attempt in Retrying(
        stop=stop_after_delay(8 * MINUTE_SECS),
        wait=wait_fixed(15),
        retry=retry_if_exception_type(jubilant.CLIError),
        reraise=True,
    ):
        with attempt:
            juju.wait(**kwargs)
