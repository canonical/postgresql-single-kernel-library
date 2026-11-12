#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Service

from tests.integration.charm.pytest_operator_helpers import CHARM_BASE
from tests.integration.helpers.juju import split_model

logger = logging.getLogger(__name__)

TIMEOUT = 20 * 60

pytestmark = pytest.mark.k8s_only


def database_active_idle(status: jubilant.Status, app_name: str) -> bool:
    return jubilant.all_active(status, app_name) and jubilant.all_agents_idle(status, app_name)


def any_database_unit_error(status: jubilant.Status, app_name: str) -> bool:
    app = status.apps.get(app_name)
    if app is None:
        return False

    return any(unit.workload_status.current == "error" for unit in app.units.values())


@pytest.mark.abort_on_fail
def test_deploy(juju: jubilant.Juju, charm, app_name: str, charm_resources) -> None:
    """Deploy the charm and wait for active/idle."""
    if app_name not in juju.status().apps:
        juju.deploy(
            charm,
            app_name,
            base=CHARM_BASE,
            config={"profile": "testing"},
            num_units=3,
            resources=charm_resources,
            trust=True,
        )

    juju.wait(lambda status: database_active_idle(status, app_name), timeout=TIMEOUT)


def test_headless_service_error_and_recovery(juju: jubilant.Juju, app_name: str) -> None:
    """Delete headless service, verify error state, recreate, and recover."""
    _, model_name = split_model(juju.model)
    svc_name = f"{app_name}-endpoints"

    # Verify the headless service exists.
    client = Client(namespace=model_name)
    svc = client.get(Service, name=svc_name)
    assert svc.spec.clusterIP == "None"

    # Delete the headless service.
    logger.info("Deleting headless service %s", svc_name)
    client.delete(Service, name=svc_name)

    # Verify it's gone.
    with pytest.raises(ApiError, match="not found"):
        client.get(Service, name=svc_name)

    # Speed up update-status so the charm detects the missing service.
    juju.model_config(values={"update-status-hook-interval": "1m"})
    try:
        # Wait for at least one unit to go into error state.
        juju.wait(lambda status: any_database_unit_error(status, app_name), timeout=TIMEOUT)
        logger.info("Unit(s) entered error state as expected")
    finally:
        juju.model_config(reset=("update-status-hook-interval",))

    # Recreate the headless service (simulating the operator's manual fix).
    logger.info("Recreating headless service %s", svc_name)
    client.apply(
        Service(
            metadata=ObjectMeta(
                name=svc_name,
                namespace=model_name,
                labels={
                    "app.kubernetes.io/name": app_name,
                    "app.kubernetes.io/managed-by": "juju",
                },
            ),
            spec=ServiceSpec(
                clusterIP="None",
                publishNotReadyAddresses=True,
                selector={"app.kubernetes.io/name": app_name},
            ),
        ),
        field_manager="integration-test",
        force=True,
    )

    # Verify it's back.
    svc = client.get(Service, name=svc_name)
    assert svc.spec.clusterIP == "None"
    logger.info("Headless service recreated")

    # Resolve all units in error state. A unit may self-recover between the
    # status snapshot and the resolve call (the recreated service unblocks the
    # queued hook, which then succeeds) — juju's `resolve --no-retry` fails
    # with "not in an error state" for such units, which is the desired
    # outcome for that unit, not a test failure (round-6 a1+a2: the race hit
    # a different unit each attempt).
    for unit_name, unit_status in juju.status().apps[app_name].units.items():
        if unit_status.workload_status.current == "error":
            logger.info("Resolving %s", unit_name)
            try:
                juju.cli("resolve", "--no-retry", unit_name)
            except jubilant.CLIError as cli_error:
                if "is not in an error state" not in str(cli_error):
                    raise
                logger.info("%s already self-recovered before the resolve", unit_name)

    # Wait for units to settle back to active/idle.
    juju.wait(lambda status: database_active_idle(status, app_name), timeout=TIMEOUT)
