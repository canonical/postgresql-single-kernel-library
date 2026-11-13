#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os

import jubilant
import pytest
from jubilant import Juju

from tests.integration.charm.helpers import (
    scale_application,
)
from tests.integration.helpers.juju import wait_for_apps_status
from tests.integration.types import Substrate

DATABASE_APP_NAME = "pg"
LS_CLIENT = "landscape-client"
UBUNTU_PRO_APP_NAME = "ubuntu-advantage"

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.vm_only


@pytest.fixture(scope="module")
def check_subordinate_env_vars(juju: Juju) -> None:
    if (
        not os.environ.get("UBUNTU_PRO_TOKEN", "").strip()
        or not os.environ.get("LANDSCAPE_ACCOUNT_NAME", "").strip()
        or not os.environ.get("LANDSCAPE_REGISTRATION_KEY", "").strip()
    ):
        pytest.skip("Subordinate configs not set")


@pytest.mark.abort_on_fail
def test_deploy(juju: Juju, charm: str, check_subordinate_env_vars):
    juju.deploy(
        charm,
        DATABASE_APP_NAME,
        num_units=3,
        base="ubuntu@24.04",
    )
    juju.deploy(
        UBUNTU_PRO_APP_NAME,
        config={"token": os.environ["UBUNTU_PRO_TOKEN"]},
        channel="latest/edge",
        base="ubuntu@24.04",
    )
    juju.deploy(
        LS_CLIENT,
        config={
            "account-name": os.environ["LANDSCAPE_ACCOUNT_NAME"],
            "registration-key": os.environ["LANDSCAPE_REGISTRATION_KEY"],
            "ppa": "ppa:landscape/self-hosted-beta",
        },
        channel="latest/edge",
    )

    juju.wait(
        wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME),
        error=jubilant.any_error,
        timeout=2000,
    )
    juju.integrate(f"{DATABASE_APP_NAME}:juju-info", f"{LS_CLIENT}:container")
    juju.integrate(f"{DATABASE_APP_NAME}:juju-info", f"{UBUNTU_PRO_APP_NAME}:juju-info")
    juju.wait(
        wait_for_apps_status(
            jubilant.all_active, LS_CLIENT, UBUNTU_PRO_APP_NAME, DATABASE_APP_NAME
        ),
        error=jubilant.any_error,
    )


def test_scale_up(juju: Juju, substrate: Substrate, check_subordinate_env_vars):
    scale_application(juju, DATABASE_APP_NAME, 4, substrate)

    juju.wait(
        wait_for_apps_status(
            jubilant.all_active, LS_CLIENT, UBUNTU_PRO_APP_NAME, DATABASE_APP_NAME
        ),
        error=jubilant.any_error,
        timeout=1500,
    )


def test_scale_down(juju: Juju, substrate: Substrate, check_subordinate_env_vars):
    scale_application(juju, DATABASE_APP_NAME, 3, substrate)

    juju.wait(
        wait_for_apps_status(
            jubilant.all_active, LS_CLIENT, UBUNTU_PRO_APP_NAME, DATABASE_APP_NAME
        ),
        error=jubilant.any_error,
        timeout=1500,
    )
