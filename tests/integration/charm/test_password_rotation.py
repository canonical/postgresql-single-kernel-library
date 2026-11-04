#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import re
import time

import jubilant
import psycopg2
import pytest
from jubilant import Juju
from single_kernel_postgresql.config import literals
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tests.integration.charm.helpers import (
    check_patroni,
    get_password,
    get_primary,
    restart_patroni,
    run_command_on_unit,
    set_password,
)
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.helpers.postgresql import db_connect
from tests.integration.types import Substrate

# Directory of the PostgreSQL logs on each substrate.
VM_POSTGRESQL_LOGS_DIR = f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_LOGS_PATH}"
K8S_POSTGRESQL_LOGS_DIR = f"/{literals.K8S_LOGS_STORAGE_PATH}/{literals.K8S_PG_LOGS_PATH}"


def _wait_for_active(juju: Juju, app_name: str, timeout: float = 1000) -> None:
    juju.wait(
        wait_for_apps_status(jubilant.all_active, app_name),
        error=jubilant.any_error,
        timeout=timeout,
    )


@pytest.mark.abort_on_fail
def test_deploy(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy three units of PostgreSQL."""
    with fast_forward(juju):
        juju.deploy(
            charm,
            app=app_name,
            base="ubuntu@24.04",
            config={"profile": "testing"},
            num_units=3,
            resources=charm_resources,
            trust=substrate == Substrate.k8s,
        )
        _wait_for_active(juju, app_name, timeout=1500)


def test_password_rotation(juju: Juju, app_name: str) -> None:
    """Test password rotation action."""
    # Get the initial passwords set for the system users.
    superuser_password = get_password(juju, app_name)
    passwords = {
        user: get_password(juju, app_name, user)
        for user in ("replication", "monitoring", "backup", "rewind")
    }

    # Change both passwords.
    set_password(juju, app_name, password="test-password")
    _wait_for_active(juju, app_name)
    for attempt in Retrying(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
    ):
        with attempt:
            assert superuser_password != get_password(juju, app_name)

    # For the other users, generate a specific password and pass it to the action.
    new_password = "test-password"
    for user, old_password in passwords.items():
        set_password(juju, app_name, username=user, password=new_password)
        _wait_for_active(juju, app_name)
        for attempt in Retrying(
            stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30)
        ):
            with attempt:
                assert new_password == get_password(juju, app_name, user)
                assert old_password != new_password

    patroni_password = get_password(juju, app_name, "patroni")

    # Restart Patroni on any non-leader unit and check that
    # Patroni and PostgreSQL continue to work.
    restart_time = time.time()
    for unit_name, unit in get_app_units(juju, app_name).items():
        if not unit.leader:
            restart_patroni(juju, unit_name, patroni_password)
            assert check_patroni(juju, unit_name, restart_time)


def test_db_connection_with_empty_password(juju: Juju, app_name: str) -> None:
    """Test that user can't connect with empty password."""
    primary = get_primary(juju, f"{app_name}/0")
    address = get_unit_address(juju, primary)
    with pytest.raises(psycopg2.Error), db_connect(address, "") as connection:
        connection.close()


def test_no_password_change_on_invalid_password(juju: Juju, app_name: str) -> None:
    """Test that in general, there is no change when password validation fails."""
    password1 = get_password(juju, app_name, "replication")
    # The password has to be minimum 3 characters
    set_password(juju, app_name, username="replication", password="1")
    password2 = get_password(juju, app_name, "replication")
    # The password didn't change
    assert password1 == password2


def test_no_password_exposed_on_logs(juju: Juju, app_name: str, substrate: Substrate) -> None:
    """Test that passwords don't get exposed on postgresql logs."""
    logs_dir = VM_POSTGRESQL_LOGS_DIR if substrate == Substrate.vm else K8S_POSTGRESQL_LOGS_DIR
    for unit_name in get_app_units(juju, app_name):
        try:
            logs = run_command_on_unit(
                juju, unit_name, f"grep PASSWORD {logs_dir}/postgresql-*.log", substrate
            )
        except Exception:
            continue
        regex = re.compile("(PASSWORD )(?!<REDACTED>)")
        logs_without_false_positives = regex.findall(logs)
        assert len(logs_without_false_positives) == 0, (
            f"Sensitive information detected on {unit_name} logs"
        )
