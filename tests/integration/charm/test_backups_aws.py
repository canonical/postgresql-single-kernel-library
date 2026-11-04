#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest
from jubilant import Juju
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tests.integration.charm.backup_helpers import AWS, backup_operations
from tests.integration.charm.helpers import (
    get_password,
    get_primary,
    scale_application,
    switchover,
)
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_unit_address,
    remove_application,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

S3_INTEGRATOR_APP_NAME = "s3-integrator"
tls_certificates_app_name = "self-signed-certificates"
tls_channel = "1/stable"

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
def test_backup_aws(
    juju: Juju,
    charm: str,
    app_name: str,
    charm_resources,
    substrate: Substrate,
    aws_cloud_configs: tuple[dict, dict],
) -> None:
    """Build and deploy two units of PostgreSQL in AWS, test backup and restore actions."""
    config = aws_cloud_configs[0]
    credentials = aws_cloud_configs[1]

    backup_operations(
        juju,
        charm,
        app_name,
        charm_resources,
        substrate,
        S3_INTEGRATOR_APP_NAME,
        tls_certificates_app_name,
        tls_channel,
        credentials,
        AWS,
        config,
    )
    database_app_name = f"{app_name}-aws"

    with fast_forward(juju):
        logger.info("removing the TLS relation")
        juju.remove_relation(
            f"{database_app_name}:client-certificates", f"{tls_certificates_app_name}:certificates"
        )
        juju.remove_relation(
            f"{database_app_name}:peer-certificates", f"{tls_certificates_app_name}:certificates"
        )

        # Scale up to be able to test primary and leader being different.
        units_before = set(get_app_units(juju, database_app_name))
        scale_application(juju, database_app_name, 2, substrate)
        new_unit_name = (set(get_app_units(juju, database_app_name)) - units_before).pop()

        logger.info("ensuring that the replication is working correctly")
        address = get_unit_address(juju, new_unit_name)
        password = get_password(juju, database_app_name)
        patroni_password = get_password(juju, database_app_name, "patroni")
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                with (
                    db_connect(host=address, password=password) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables"
                        " WHERE table_schema = 'public' AND table_name = 'backup_table_1');"
                    )
                    assert cursor.fetchone()[0], (
                        f"replication isn't working correctly: table 'backup_table_1' doesn't exist in {new_unit_name}"
                    )
                    cursor.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables"
                        " WHERE table_schema = 'public' AND table_name = 'backup_table_2');"
                    )
                    assert not cursor.fetchone()[0], (
                        f"replication isn't working correctly: table 'backup_table_2' exists in {new_unit_name}"
                    )
                connection.close()

        old_primary = get_primary(juju, new_unit_name)
        logger.info(f"performing a switchover from {old_primary} to {new_unit_name}")
        switchover(juju, old_primary, patroni_password, new_unit_name)

        logger.info("checking that the primary unit has changed")
        primary = get_primary(juju, new_unit_name)
        for attempt in Retrying(
            stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                assert primary == new_unit_name

        # Ensure stanza is working correctly.
        logger.info("listing the available backups to ensure that the stanza is working correctly")
        backups = juju.run(new_unit_name, "list-backups").results.get("backups")
        assert backups, "backups not outputted"

        juju.wait(
            wait_for_apps_status(jubilant.all_active), error=jubilant.any_error, timeout=1000
        )

    # Remove the database app.
    remove_application(juju, database_app_name)

    # Remove the TLS operator.
    remove_application(juju, tls_certificates_app_name)
