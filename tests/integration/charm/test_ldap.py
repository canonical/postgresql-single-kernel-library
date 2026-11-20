#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates

from tests.integration.charm.helpers import CHARM_BASE, get_password
from tests.integration.helpers.juju import fast_forward, get_unit_address, wait_for_apps_status
from tests.integration.helpers.postgresql import execute_queries_on_unit

logger = logging.getLogger(__name__)

GLAUTH_PSQL_APP_NAME = "postgresql-k8s"
GLAUTH_CERT_APP_NAME = "self-signed-certificates"
GLAUTH_APP_NAME = "glauth-k8s"

pytestmark = pytest.mark.k8s_only


@pytest.fixture(scope="module", autouse=True)
def require_juju3(juju: jubilant.Juju) -> None:
    """Check the actual Juju CLI version rather than the removed libjuju dependency."""
    if juju.version().major != 3:
        pytest.skip("Requires Juju 3")


@pytest.mark.abort_on_fail
def test_build_and_deploy(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrates
) -> None:
    """Build and deploy one unit of PostgreSQL."""
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in juju.status().apps:
        with fast_forward(juju):
            juju.deploy(
                charm,
                app=app_name,
                num_units=1,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrates.K8S,
            )
            juju.wait(
                lambda status: (
                    wait_for_apps_status(jubilant.all_active, app_name)(status)
                    and len(status.get_units(app_name)) == 1
                ),
                error=lambda status: jubilant.any_blocked(status, app_name),
                timeout=1000,
            )


@pytest.mark.abort_on_fail
def test_glauth_integration(juju: jubilant.Juju, app_name: str):
    glauth_psql_app_name = f"glauth-{GLAUTH_PSQL_APP_NAME}"
    glauth_cert_app_name = f"glauth-{GLAUTH_CERT_APP_NAME}"

    # Deploy GLAuth charm.
    juju.deploy(
        GLAUTH_PSQL_APP_NAME,
        app=glauth_psql_app_name,
        channel="14/stable",
        trust=True,
    )
    juju.deploy(
        GLAUTH_CERT_APP_NAME,
        app=glauth_cert_app_name,
        channel="1/stable",
        trust=False,
    )
    juju.deploy(
        GLAUTH_APP_NAME,
        app=GLAUTH_APP_NAME,
        channel="latest/edge",
        trust=True,
    )

    with fast_forward(juju):
        juju.wait(
            lambda status: (
                wait_for_apps_status(
                    jubilant.all_active, glauth_psql_app_name, glauth_cert_app_name
                )(status)
                and wait_for_apps_status(jubilant.all_blocked, GLAUTH_APP_NAME)(status)
            ),
            timeout=600,
        )

        # Add both relations to GLAuth (PostgreSQL and self-signed-certificates).
        logger.info("Adding relations to GLAuth")
        juju.integrate(f"{GLAUTH_APP_NAME}:pg-database", f"{glauth_psql_app_name}:database")
        juju.integrate(GLAUTH_APP_NAME, glauth_cert_app_name)
        juju.wait(
            wait_for_apps_status(
                jubilant.all_active, glauth_psql_app_name, glauth_cert_app_name, GLAUTH_APP_NAME
            ),
            timeout=600,
        )

        # Add relation to PostgreSQL.
        logger.info("Adding relation to PostgreSQL")
        juju.integrate(f"{GLAUTH_APP_NAME}:ldap", f"{app_name}:ldap")
        juju.integrate(f"{GLAUTH_APP_NAME}:send-ca-cert", f"{app_name}:receive-ca-cert")
        juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)

        unit_name = next(iter(juju.status().get_units(app_name)))
        address = get_unit_address(juju, unit_name)
        password = get_password(juju, app_name)

        # Validate the 'operator' user can still access the instance.
        execute_queries_on_unit(
            unit_address=address,
            username="operator",
            password=password,
            queries=["SELECT VERSION();"],
            database="postgres",
        )
