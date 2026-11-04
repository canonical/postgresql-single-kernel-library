#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import psycopg2
import pytest

from tests.integration.charm.helpers import DATA_INTEGRATOR_APP_NAME
from tests.integration.helpers.juju import (
    get_app_leader,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.types import Substrate

MINUTE_SECS = 60


def uris_host(juju: jubilant.Juju, app_name: str, substrate: Substrate) -> str:
    """Host the charm publishes in the relation URIs.

    The leader unit IP on VM, the primary service on K8s.
    """
    if substrate == Substrate.vm:
        return get_unit_address(juju, get_app_leader(juju, app_name))
    # The K8s namespace is the model name, without the "controller:" prefix.
    namespace = juju.model.split(":")[-1]
    return f"{app_name}-primary.{namespace}.svc.cluster.local"


@pytest.mark.abort_on_fail
def test_deploy(
    juju: jubilant.Juju, charm, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy the charms."""
    logging.info("Deploying database charm")
    juju.deploy(
        charm,
        app=app_name,
        config={"profile": "testing"},
        num_units=1,
        resources=charm_resources,
        trust=substrate == Substrate.k8s,
    )

    # Deploy the data integrator if not already deployed.
    logging.info("Deploying data integrator charm")
    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app="di1",
        channel="latest/edge",
        config={"database-name": "postgre1"},
    )
    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app="di2",
        channel="latest/edge",
        config={"database-name": "postgre2"},
    )

    creds_secret = juju.add_secret("creds-secret", {"tester": "password"})

    juju.deploy(
        DATA_INTEGRATOR_APP_NAME,
        app="di3",
        channel="latest/edge",
        config={"requested-entities-secret": creds_secret},
    )
    juju.grant_secret(creds_secret, "di3")

    logging.info("Relating data integrator")
    juju.integrate(f"{app_name}:database", "di1")
    juju.integrate(f"{app_name}:database", "di2")

    logging.info("Waiting for the applications to settle")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name, "di1", "di2"),
        timeout=20 * MINUTE_SECS,
    )


def test_prefix_too_short(juju: jubilant.Juju, app_name: str) -> None:
    logging.info("Setting short prefix")
    juju.config("di3", {"database-name": "po*"})

    logging.info("Integrating with database")
    juju.integrate(f"{app_name}:database", "di3")

    logging.info("Waiting for the applications to settle")
    db_leader = get_app_leader(juju, app_name)
    juju.wait(
        ready=lambda status: (
            status.apps[app_name].units[db_leader].workload_status.message == "Prefix too short"
            and status.apps[app_name].units[db_leader].is_blocked
        ),
        timeout=5 * MINUTE_SECS,
    )

    juju.remove_relation(app_name, "di3")
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)


def test_get_prefix(juju: jubilant.Juju, app_name: str, substrate: Substrate) -> None:
    db_ip = get_unit_address(juju, get_app_leader(juju, app_name))

    logging.info("Setting prefix")
    juju.config("di3", {"database-name": "postgre*"})

    logging.info("Integrating with database")
    juju.integrate(f"{app_name}:database", "di3")

    logging.info("Waiting for the applications to settle")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name, "di1", "di2", "di3"),
        timeout=5 * MINUTE_SECS,
    )

    integrator_leader = get_app_leader(juju, "di3")
    result = juju.run(unit=integrator_leader, action="get-credentials")
    result.raise_on_failure()

    # Assert credentials and prefix
    pg_data = result.results["postgresql"]
    assert "postgres" not in pg_data["prefix-databases"]
    assert pg_data["prefix-databases"] == "postgre1,postgre2"
    assert pg_data["username"] == "tester"
    assert pg_data["password"] == "password"
    host = uris_host(juju, app_name, substrate)
    assert pg_data["uris"] == f"postgresql://tester:password@{host}:5432/postgre1"

    # Connect to database
    psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre1")
    psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre2")


def test_remove_db_from_prefix(juju: jubilant.Juju, app_name: str, substrate: Substrate) -> None:
    db_ip = get_unit_address(juju, get_app_leader(juju, app_name))

    juju.remove_relation(app_name, "di1")
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    integrator_leader = get_app_leader(juju, "di3")
    result = juju.run(unit=integrator_leader, action="get-credentials")
    result.raise_on_failure()

    pg_data = result.results["postgresql"]
    assert "postgres" not in pg_data["prefix-databases"]
    assert pg_data["prefix-databases"] == "postgre2"
    host = uris_host(juju, app_name, substrate)
    assert pg_data["uris"] == f"postgresql://tester:password@{host}:5432/postgre2"

    # Connect to database
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre1")
    psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre2")


def test_readd_db_from_prefix(juju: jubilant.Juju, app_name: str, substrate: Substrate) -> None:
    db_ip = get_unit_address(juju, get_app_leader(juju, app_name))

    juju.integrate(app_name, "di1")
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    integrator_leader = get_app_leader(juju, "di3")
    result = juju.run(unit=integrator_leader, action="get-credentials")
    result.raise_on_failure()

    pg_data = result.results["postgresql"]
    assert "postgres" not in pg_data["prefix-databases"]
    assert pg_data["prefix-databases"] == "postgre1,postgre2"
    host = uris_host(juju, app_name, substrate)
    assert pg_data["uris"] == f"postgresql://tester:password@{host}:5432/postgre1"

    # Connect to database
    psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre1")
    psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre2")


def test_no_prefix_dbs(juju: jubilant.Juju, app_name: str) -> None:
    db_ip = get_unit_address(juju, get_app_leader(juju, app_name))

    # Remove the relations one at a time, waiting for idle after each: removing both and
    # waiting once is racy, as all_agents_idle can be true in the gap between the hooks.
    juju.remove_relation(app_name, "di1")
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)
    juju.remove_relation(app_name, "di2")
    juju.wait(jubilant.all_agents_idle, timeout=5 * MINUTE_SECS)

    integrator_leader = get_app_leader(juju, "di3")
    result = juju.run(unit=integrator_leader, action="get-credentials")
    result.raise_on_failure()

    pg_data = result.results["postgresql"]
    assert "prefix-databases" not in pg_data
    assert "uris" not in pg_data

    # Connect to database
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre1")
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(f"postgresql://tester:password@{db_ip}:5432/postgre2")
