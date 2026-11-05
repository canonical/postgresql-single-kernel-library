# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
from asyncio import gather

import pytest
from pytest_operator.plugin import OpsTest

from tests.integration.charm.pytest_operator_helpers import CHARM_BASE

logger = logging.getLogger(__name__)

DATABASE_APP_NAME = "database"
DISCOURSE_APP_NAME = "discourse-k8s"
REDIS_APP_NAME = "redis-k8s"


@pytest.mark.vm_only
@pytest.mark.skip(reason="Unstable")
async def test_nextcloud_db_blocked(ops_test: OpsTest, charm: str, architecture: str) -> None:
    if architecture != "amd64":
        pytest.skip("nextcloud charm not available for arm64")
    # Deploy Database Charm and Nextcloud
    await asyncio.gather(
        ops_test.model.deploy(
            charm,
            application_name=DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            config={"profile": "testing"},
        ),
        ops_test.model.deploy(
            "nextcloud",
            channel="edge",
            application_name="nextcloud",
            num_units=1,
            base=CHARM_BASE,
        ),
    )
    await asyncio.gather(
        ops_test.model.wait_for_idle(apps=[DATABASE_APP_NAME], status="active", timeout=2000),
        ops_test.model.wait_for_idle(
            apps=["nextcloud"],
            status="blocked",
            raise_on_blocked=False,
            timeout=2000,
        ),
    )

    await ops_test.model.relate("nextcloud:database", f"{DATABASE_APP_NAME}:database")

    await ops_test.model.wait_for_idle(
        apps=[DATABASE_APP_NAME, "nextcloud"],
        status="active",
        raise_on_blocked=False,
        timeout=1000,
    )


@pytest.mark.k8s_only
@pytest.mark.abort_on_fail
async def test_database_deploy_clientapps(ops_test: OpsTest, charm, charm_resources):
    """Test basic functionality of database relation interface."""
    # Deploy both charms (multiple units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    async with ops_test.fast_forward():
        await asyncio.gather(
            ops_test.model.deploy(
                charm,
                resources=charm_resources,
                application_name=DATABASE_APP_NAME,
                num_units=3,
                base=CHARM_BASE,
                trust=True,
                config={"profile": "testing"},
            ),
        )
        await ops_test.model.wait_for_idle(
            apps=[DATABASE_APP_NAME],
            status="active",
            raise_on_blocked=True,
            raise_on_error=False,
            timeout=1000,
        )


@pytest.mark.k8s_only
async def test_discourse(ops_test: OpsTest, architecture: str):
    if architecture != "amd64":
        pytest.skip("discourse-k8s charm not available for arm64")
    # Deploy Discourse and Redis.
    await gather(
        ops_test.model.deploy(DISCOURSE_APP_NAME, application_name=DISCOURSE_APP_NAME),
        ops_test.model.deploy(
            REDIS_APP_NAME, application_name=REDIS_APP_NAME, channel="latest/edge", series="jammy"
        ),
    )

    async with ops_test.fast_forward():
        # Enable the plugins/extensions required by Discourse.
        logger.info("Enabling the plugins/extensions required by Discourse")
        config = {"plugin-hstore-enable": "True", "plugin-pg-trgm-enable": "True"}
        await ops_test.model.applications[DATABASE_APP_NAME].set_config(config)
        await gather(
            ops_test.model.wait_for_idle(apps=[DISCOURSE_APP_NAME], status="waiting"),
            ops_test.model.wait_for_idle(
                apps=[DATABASE_APP_NAME, REDIS_APP_NAME], status="active"
            ),
        )
        # Add both relations to Discourse (PostgreSQL and Redis)
        # and wait for it to be ready.
        logger.info("Adding relations")
        await gather(
            ops_test.model.add_relation(DATABASE_APP_NAME, DISCOURSE_APP_NAME),
            ops_test.model.add_relation(REDIS_APP_NAME, DISCOURSE_APP_NAME),
        )
        await gather(
            ops_test.model.wait_for_idle(apps=[DISCOURSE_APP_NAME], timeout=2000),
            ops_test.model.wait_for_idle(
                apps=[DATABASE_APP_NAME, REDIS_APP_NAME], status="active"
            ),
        )
        logger.info("Configuring Discourse")
        config = {
            "developer_emails": "noreply@canonical.com",
            "external_hostname": "discourse-k8s",
            "smtp_address": "test.local",
            "smtp_domain": "test.local",
            "s3_install_cors_rule": "false",
        }
        await ops_test.model.applications[DISCOURSE_APP_NAME].set_config(config)
        await ops_test.model.wait_for_idle(apps=[DISCOURSE_APP_NAME], status="active")

        # Deploy a new discourse application (https://github.com/canonical/data-platform-libs/issues/118
        # prevents from re-relating the same Discourse application; Discourse uses the old secret and fails).
        await ops_test.model.applications[DISCOURSE_APP_NAME].remove()
        other_discourse_app_name = f"other-{DISCOURSE_APP_NAME}"
        await ops_test.model.deploy(DISCOURSE_APP_NAME, application_name=other_discourse_app_name)

        # Add both relations to Discourse (PostgreSQL and Redis)
        # and wait for it to be ready.
        logger.info("Adding relations")
        await gather(
            ops_test.model.add_relation(DATABASE_APP_NAME, other_discourse_app_name),
            ops_test.model.add_relation(REDIS_APP_NAME, other_discourse_app_name),
        )
        await gather(
            ops_test.model.wait_for_idle(apps=[other_discourse_app_name], timeout=2000),
            ops_test.model.wait_for_idle(
                apps=[DATABASE_APP_NAME, REDIS_APP_NAME], status="active"
            ),
        )
        logger.info("Configuring Discourse")
        config = {
            "developer_emails": "noreply@canonical.com",
            "external_hostname": "discourse-k8s",
            "smtp_address": "test.local",
            "smtp_domain": "test.local",
            "s3_install_cors_rule": "false",
        }
        await ops_test.model.applications[other_discourse_app_name].set_config(config)
        await ops_test.model.wait_for_idle(apps=[other_discourse_app_name], status="active")


@pytest.mark.k8s_only
async def test_indico_datatabase(ops_test: OpsTest, architecture: str) -> None:
    """Tests deploying and relating to the Indico charm."""
    if architecture != "amd64":
        pytest.skip("indico charm not available for arm64")
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.deploy(
            "indico",
            channel="latest/edge",
            application_name="indico",
            num_units=1,
            series="focal",
        )
        await ops_test.model.deploy(
            REDIS_APP_NAME, application_name="redis-broker", channel="latest/edge", series="jammy"
        )
        await ops_test.model.deploy(
            REDIS_APP_NAME, application_name="redis-cache", channel="latest/edge", series="jammy"
        )
        await asyncio.gather(
            ops_test.model.relate("redis-broker", "indico:redis-broker"),
            ops_test.model.relate("redis-cache", "indico:redis-cache"),
        )

        # Wait for model to stabilise
        await ops_test.model.wait_for_idle(
            apps=["indico"],
            status="waiting",
            timeout=1000,
        )

        # Verify that the charm doesn't block when the extensions are enabled.
        logger.info("Verifying that the charm doesn't block when the extensions are enabled")
        config = {"plugin-pg-trgm-enable": "True", "plugin-unaccent-enable": "True"}
        await ops_test.model.applications[DATABASE_APP_NAME].set_config(config)
        await ops_test.model.wait_for_idle(apps=[DATABASE_APP_NAME], status="active")
        await ops_test.model.relate(DATABASE_APP_NAME, "indico")
        await ops_test.model.wait_for_idle(
            apps=[DATABASE_APP_NAME, "indico"],
            status="active",
            timeout=2000,
        )
