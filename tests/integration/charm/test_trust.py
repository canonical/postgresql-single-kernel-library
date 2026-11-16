#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest

from tests.integration.charm.helpers import CHARM_BASE
from tests.integration.helpers.juju import wait_for_apps_status

logger = logging.getLogger(__name__)

APP_NAME = "untrusted-postgresql-k8s"
UNTRUST_ERROR_MESSAGE = (
    f"Run `juju trust {APP_NAME} --scope=cluster`. Needed for in-place refreshes"
)

pytestmark = pytest.mark.k8s_only


@pytest.mark.abort_on_fail
def test_deploy_without_trust(juju: jubilant.Juju, charm, charm_resources):
    """Build and deploy the charm with trust set to false.

    Assert on the unit status being blocked due to lack of trust.
    """
    juju.deploy(
        charm,
        resources=charm_resources,
        app=APP_NAME,
        num_units=3,
        trust=False,
        base=CHARM_BASE,
        config={"profile": "testing"},
    )

    logger.info("Waiting for charm to become blocked due to missing --trust")
    juju.wait(
        lambda status: (
            APP_NAME in status.apps
            and status.apps[APP_NAME].app_status.current == "blocked"
            and status.apps[APP_NAME].app_status.message == UNTRUST_ERROR_MESSAGE
        ),
        timeout=1000,
    )


def test_trust_blocked_deployment(juju: jubilant.Juju):
    """Trust existing blocked deployment.

    Assert on the application status recovering to active.
    """
    juju.cli("trust", APP_NAME, "--scope=cluster")
    juju.wait(wait_for_apps_status(jubilant.all_active, APP_NAME), timeout=3000)
