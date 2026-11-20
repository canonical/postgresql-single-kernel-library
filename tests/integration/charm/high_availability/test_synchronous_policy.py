#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates
from tenacity import Retrying, stop_after_attempt, wait_fixed

from tests.integration.charm.helpers import CHARM_BASE, get_cluster_roles
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju, charm: str, app_name: str, charm_resources, substrate: Substrates
) -> None:
    """Build and deploy three units of PostgreSQL."""
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            num_units=3,
            base=CHARM_BASE,
            config={"profile": "testing"},
            resources=charm_resources,
            trust=substrate == Substrates.K8S,
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=1500)


def test_default_all(juju: jubilant.Juju, app_name: str) -> None:
    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0


def test_majority(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, {"synchronous-node-count": "majority"})

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 1
            assert len(roles["replicas"]) == 1


def test_constant(juju: jubilant.Juju, app_name: str) -> None:
    juju.config(app_name, {"synchronous-node-count": "2"})

    with fast_forward(juju):
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
        with attempt:
            roles = get_cluster_roles(juju, next(iter(juju.status().get_units(app_name))))
            assert len(roles["primaries"]) == 1
            assert len(roles["sync_standbys"]) == 2
            assert len(roles["replicas"]) == 0
