#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest
import requests
from kubernetes import config, dynamic
from kubernetes.client import api_client

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    check_writes,
    get_primary_from_app,
    remove_charm_code,
    start_ha_continuous_writes,
)
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_idle
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)


CLUSTER_SIZE = 3

pytestmark = pytest.mark.k8s_only


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Build and deploy three unit of PostgreSQL."""
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in juju.status().apps:
        with fast_forward(juju):
            juju.deploy(
                charm,
                app=app_name,
                num_units=CLUSTER_SIZE,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
    # Deploy the continuous writes application charm if it wasn't already deployed.
    if APPLICATION_NAME not in juju.status().apps:
        with fast_forward(juju):
            juju.deploy(
                APPLICATION_NAME,
                app=APPLICATION_NAME,
                base=CHARM_BASE,
                channel="latest/edge",
            )

    juju.integrate(app_name, f"{APPLICATION_NAME}:database")
    with fast_forward(juju):
        wait_for_idle(
            juju,
            apps=[app_name],
            status="active",
            timeout=1000,
            raise_on_error=False,
        )


@pytest.mark.abort_on_fail
def test_restart(juju: jubilant.Juju, juju_continuous_writes, app_name: str) -> None:
    """Test restart of all the units simultaneously."""
    logger.info("starting continuous writes to the database")
    start_ha_continuous_writes(juju, app_name)

    logger.info("checking whether writes are increasing")
    are_writes_increasing(juju)

    logger.info(
        "removing charm code from one non-primary unit to simulate a crash and prevent firing the update-charm hook"
    )
    primary = get_primary_from_app(juju, app_name)
    status = juju.status()
    for unit in status.apps[app_name].units:
        if unit != primary:
            non_primary = unit
            break
    remove_charm_code(juju, non_primary)
    logger.info(f"removed charm code from {non_primary}")

    logger.info("restarting all the units by deleting their pods")
    client = dynamic.DynamicClient(api_client.ApiClient(configuration=config.load_kube_config()))
    api = client.resources.get(api_version="v1", kind="Pod")
    api.delete(
        namespace=juju.status().model.name,
        label_selector=f"app.kubernetes.io/name={app_name}",
    )
    juju.wait(
        lambda status: all(
            unit.workload_status.current == "error"
            for app in status.apps.values()
            for unit_name, unit in app.units.items()
            if unit_name == non_primary
        ),
        timeout=float("inf"),
        delay=0.5,
        successes=1,
    )

    # Resolve the error on the non-primary unit.
    for app in juju.status().apps.values():
        for unit_name, unit in app.units.items():
            if unit_name == non_primary and unit.workload_status.current == "error":
                logger.info(f"resolving {non_primary} error")
                juju.cli("resolved", "--no-retry", unit_name)
                break

    with fast_forward(juju):
        wait_for_idle(juju, apps=[app_name], status="active", raise_on_error=False, timeout=300)

    # Check that all replication slots are present in the primary
    # (by checking the list of cluster members).
    logger.info(
        "checking that all the replication slots are present in the primary by checking the list of cluster members"
    )
    primary = get_primary_from_app(juju, app_name)
    address = get_unit_address(juju, primary)
    members = requests.get(f"https://{address}:8008/cluster", verify=False).json()["members"]

    assert [member["name"] for member in members] == [
        unit_name.replace("/", "-") for unit_name in juju.status().apps[app_name].units
    ]

    logger.info("ensure continuous_writes after the crashed unit")
    are_writes_increasing(juju)

    # Verify that no writes to the database were missed after stopping the writes
    # (check that all the units have all the writes).
    logger.info("checking whether no writes were lost")
    check_writes(juju)
