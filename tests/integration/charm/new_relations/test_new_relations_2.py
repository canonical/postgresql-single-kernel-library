# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest

from tests.integration.charm.helpers import CHARM_BASE
from tests.integration.helpers.juju import (
    fast_forward,
    remove_application,
    wait_for_apps_status,
)

logger = logging.getLogger(__name__)

DATABASE_APP_NAME = "database"
DISCOURSE_APP_NAME = "discourse-k8s"
REDIS_APP_NAME = "redis-k8s"


@pytest.mark.vm_only
@pytest.mark.skip(reason="Unstable")
def test_nextcloud_db_blocked(juju: jubilant.Juju, charm: str, architecture: str) -> None:
    if architecture != "amd64":
        pytest.skip("nextcloud charm not available for arm64")
    # Deploy Database Charm and Nextcloud.
    juju.deploy(
        charm,
        app=DATABASE_APP_NAME,
        num_units=1,
        base=CHARM_BASE,
        config={"profile": "testing"},
    )
    juju.deploy(
        "nextcloud",
        channel="edge",
        app="nextcloud",
        num_units=1,
        base=CHARM_BASE,
    )
    juju.wait(
        lambda status: (
            wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME)(status)
            and wait_for_apps_status(jubilant.all_blocked, "nextcloud")(status)
        ),
        timeout=2000,
    )

    juju.integrate("nextcloud:database", f"{DATABASE_APP_NAME}:database")
    juju.wait(
        wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME, "nextcloud"), timeout=1000
    )


@pytest.mark.k8s_only
@pytest.mark.abort_on_fail
def test_database_deploy_clientapps(juju: jubilant.Juju, charm, charm_resources):
    """Test basic functionality of database relation interface."""
    # Deploy multiple units to test that only the leader sets the relation application databag.
    with fast_forward(juju):
        juju.deploy(
            charm,
            resources=charm_resources,
            app=DATABASE_APP_NAME,
            num_units=3,
            base=CHARM_BASE,
            trust=True,
            config={"profile": "testing"},
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME),
            error=lambda status: jubilant.any_blocked(status, DATABASE_APP_NAME),
            timeout=1000,
        )


@pytest.mark.k8s_only
def test_discourse(juju: jubilant.Juju, architecture: str):
    if architecture != "amd64":
        pytest.skip("discourse-k8s charm not available for arm64")
    # Deploy Discourse and Redis.
    juju.deploy(DISCOURSE_APP_NAME, app=DISCOURSE_APP_NAME)
    juju.deploy(REDIS_APP_NAME, app=REDIS_APP_NAME, channel="latest/edge", base="ubuntu@22.04")

    with fast_forward(juju):
        # Enable the plugins/extensions required by Discourse.
        logger.info("Enabling the plugins/extensions required by Discourse")
        config = {
            "plugin-hstore-enable": "True",
            "plugin-pg-trgm-enable": "True",
            "plugin-vector-enable": "True",
        }
        juju.config(DATABASE_APP_NAME, config)
        juju.wait(
            lambda status: (
                wait_for_apps_status(jubilant.all_waiting, DISCOURSE_APP_NAME)(status)
                and wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME, REDIS_APP_NAME)(
                    status
                )
            ),
            timeout=600,
        )
        # Add both relations to Discourse (PostgreSQL and Redis) and wait for it to be ready.
        logger.info("Adding relations")
        juju.integrate(DATABASE_APP_NAME, DISCOURSE_APP_NAME)
        juju.integrate(REDIS_APP_NAME, DISCOURSE_APP_NAME)
        juju.wait(
            lambda status: jubilant.all_agents_idle(status, DISCOURSE_APP_NAME), timeout=2000
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME, REDIS_APP_NAME),
            timeout=600,
        )

        logger.info("Configuring Discourse")
        config = {
            "developer_emails": "noreply@canonical.com",
            "external_hostname": "discourse-k8s",
            "smtp_address": "test.local",
            "smtp_domain": "test.local",
            "s3_install_cors_rule": "false",
        }
        juju.config(DISCOURSE_APP_NAME, config)
        juju.wait(wait_for_apps_status(jubilant.all_active, DISCOURSE_APP_NAME), timeout=600)

        # Use a new application: Discourse retains the old secret when re-related.
        # https://github.com/canonical/data-platform-libs/issues/118
        remove_application(juju, DISCOURSE_APP_NAME)
        other_discourse_app_name = f"other-{DISCOURSE_APP_NAME}"
        juju.deploy(DISCOURSE_APP_NAME, app=other_discourse_app_name)

        logger.info("Adding relations")
        juju.integrate(DATABASE_APP_NAME, other_discourse_app_name)
        juju.integrate(REDIS_APP_NAME, other_discourse_app_name)
        juju.wait(
            lambda status: jubilant.all_agents_idle(status, other_discourse_app_name), timeout=2000
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME, REDIS_APP_NAME),
            timeout=600,
        )
        logger.info("Configuring Discourse")
        juju.config(other_discourse_app_name, config)
        juju.wait(wait_for_apps_status(jubilant.all_active, other_discourse_app_name), timeout=600)


@pytest.mark.k8s_only
def test_indico_datatabase(juju: jubilant.Juju, architecture: str) -> None:
    """Tests deploying and relating to the Indico charm."""
    if architecture != "amd64":
        pytest.skip("indico charm not available for arm64")
    with fast_forward(juju, interval="30s"):
        juju.deploy(
            "indico", channel="latest/edge", app="indico", num_units=1, base="ubuntu@20.04"
        )
        juju.deploy(REDIS_APP_NAME, app="redis-broker", channel="latest/edge", base="ubuntu@22.04")
        juju.deploy(REDIS_APP_NAME, app="redis-cache", channel="latest/edge", base="ubuntu@22.04")
        juju.integrate("redis-broker", "indico:redis-broker")
        juju.integrate("redis-cache", "indico:redis-cache")

        # Wait for model to stabilise.
        juju.wait(wait_for_apps_status(jubilant.all_waiting, "indico"), timeout=1000)

        # Verify that the charm doesn't block when the extensions are enabled.
        logger.info("Verifying that the charm doesn't block when the extensions are enabled")
        config = {"plugin-pg-trgm-enable": "True", "plugin-unaccent-enable": "True"}
        juju.config(DATABASE_APP_NAME, config)
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=600)
        juju.integrate(DATABASE_APP_NAME, "indico")
        juju.wait(
            wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME, "indico"), timeout=2000
        )
