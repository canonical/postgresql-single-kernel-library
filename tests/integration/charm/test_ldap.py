#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import importlib.metadata
import logging

import pytest
from pytest_operator.plugin import OpsTest

from tests.integration.charm.pytest_operator_helpers import (
    CHARM_BASE,
    get_password,
    get_unit_address,
)
from tests.integration.helpers.postgresql import execute_queries_on_unit
from tests.integration.types import Substrate

logger = logging.getLogger(__name__)

GLAUTH_PSQL_APP_NAME = "postgresql-k8s"
GLAUTH_CERT_APP_NAME = "self-signed-certificates"
GLAUTH_APP_NAME = "glauth-k8s"

pytestmark = [
    pytest.mark.k8s_only,
    pytest.mark.skipif(
        int(importlib.metadata.version("juju").split(".")[0]) != 3,
        reason="Requires Juju 3",
    ),
]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(
    ops_test: OpsTest, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Build and deploy three unit of PostgreSQL."""
    # It is possible for users to provide their own cluster for HA testing. Hence, check if there
    # is a pre-existing cluster.
    if app_name not in ops_test.model.applications:
        async with ops_test.fast_forward():
            await ops_test.model.deploy(
                charm,
                application_name=app_name,
                num_units=1,
                base=CHARM_BASE,
                config={"profile": "testing"},
                resources=charm_resources,
                trust=substrate == Substrate.k8s,
            )
            await ops_test.model.wait_for_idle(
                apps=[app_name],
                status="active",
                raise_on_blocked=True,
                timeout=1000,
                wait_for_exact_units=1,
            )


@pytest.mark.abort_on_fail
async def test_glauth_integration(ops_test: OpsTest, app_name: str):
    glauth_psql_app_name = f"glauth-{GLAUTH_PSQL_APP_NAME}"
    glauth_cert_app_name = f"glauth-{GLAUTH_CERT_APP_NAME}"

    # Deploy GLAuth charm
    await asyncio.gather(
        ops_test.model.deploy(
            GLAUTH_PSQL_APP_NAME,
            application_name=glauth_psql_app_name,
            channel="14/stable",
            trust=True,
        ),
        ops_test.model.deploy(
            GLAUTH_CERT_APP_NAME,
            application_name=glauth_cert_app_name,
            channel="1/stable",
            trust=False,
        ),
        ops_test.model.deploy(
            GLAUTH_APP_NAME,
            application_name=GLAUTH_APP_NAME,
            channel="latest/edge",
            trust=True,
        ),
    )

    async with ops_test.fast_forward():
        await asyncio.gather(
            ops_test.model.wait_for_idle(apps=[glauth_psql_app_name], status="active"),
            ops_test.model.wait_for_idle(apps=[glauth_cert_app_name], status="active"),
            ops_test.model.wait_for_idle(apps=[GLAUTH_APP_NAME], status="blocked"),
        )

        # Add both relations to GLAuth (PostgreSQL and self-signed-certificates)
        logger.info("Adding relations to GLAuth")
        await asyncio.gather(
            ops_test.model.add_relation(
                f"{GLAUTH_APP_NAME}:pg-database", f"{glauth_psql_app_name}:database"
            ),
            ops_test.model.add_relation(GLAUTH_APP_NAME, glauth_cert_app_name),
        )
        await asyncio.gather(
            ops_test.model.wait_for_idle(apps=[glauth_psql_app_name], status="active"),
            ops_test.model.wait_for_idle(apps=[glauth_cert_app_name], status="active"),
            ops_test.model.wait_for_idle(apps=[GLAUTH_APP_NAME], status="active"),
        )

        # Add relation to PostgreSQL
        logger.info("Adding relation to PostgreSQL")
        await ops_test.model.add_relation(
            f"{GLAUTH_APP_NAME}:ldap",
            f"{app_name}:ldap",
        )
        await ops_test.model.add_relation(
            f"{GLAUTH_APP_NAME}:send-ca-cert",
            f"{app_name}:receive-ca-cert",
        )

        await ops_test.model.wait_for_idle(apps=[app_name], status="active")

        database_units = ops_test.model.applications[app_name].units
        address = await get_unit_address(ops_test, database_units[0].name)
        password = await get_password(ops_test, database_app_name=app_name)

        # Validate the 'operator' user can still access the instance
        execute_queries_on_unit(
            unit_address=address,
            username="operator",
            password=password,
            queries=["SELECT VERSION();"],
            database="postgres",
        )
