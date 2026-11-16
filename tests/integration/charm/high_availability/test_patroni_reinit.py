# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Integration test: replica reinit after pg_control corruption via patronictl reinit."""

import logging
import subprocess
import time

import jubilant
import pytest
import requests
from jubilant import Juju
from single_kernel_postgresql.config import literals
from tenacity import Retrying, retry_if_exception, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    check_db_units_writes_increment,
    get_db_primary_unit,
    get_member_state,
)
from tests.integration.helpers.juju import (
    get_app_units,
    get_unit_address,
    wait_for_apps_status,
)
from tests.integration.types import Substrate

logging.getLogger("jubilant.wait").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DB_TEST_APP_NAME = "postgresql-test-app"

MINUTE_SECS = 60

# VM: same path layout the library's VMPaths composes.
VM_PATRONI_CONF_PATH = (
    f"{literals.BASE_SNAP_DIR}/{literals.SNAP_DATA}/{literals.PATRONI_CONF_PATH}"
)
VM_PATRONI_LOGS_PATH = (
    f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.PATRONI_LOGS_PATH}"
)
VM_POSTGRESQL_DATA_DIR = (
    f"{literals.BASE_SNAP_DIR}/{literals.SNAP_COMMON}/{literals.VM_DATA_PATH}/16/main"
)
VM_PG_CONTROL_PATH = f"{VM_POSTGRESQL_DATA_DIR}/global/pg_control"

# K8s: paths inside the workload container.
K8S_STORAGE_PATH = f"/{literals.K8S_DATA_PATH}"
K8S_PG_CONTROL_PATH = f"{K8S_STORAGE_PATH}/16/main/global/pg_control"


def _reinit_transiently_failed(exc: BaseException) -> bool:
    """Return True for transient reinit failures that clear once Patroni settles."""
    text = str(exc)  # jubilant's CLIError.__str__ already includes stdout and stderr
    # Member not yet re-registered in the cluster after the restart.
    if "No replica among provided members" in text:
        return True
    # The target's REST API was briefly not listening right after the restart.
    return "Connection refused" in text and "/reinitialize" in text


def _grep_patroni_logs(juju: Juju, unit: str, pattern: str) -> str:
    """Return matching Patroni log lines on the unit, or "" when grep finds none (exit 1)."""
    command = f"sudo grep -iEh '{pattern}' {VM_PATRONI_LOGS_PATH}/patroni.log*"
    try:
        return juju.ssh(unit, command).strip()
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 1:  # grep exit 1 means no lines matched
            return ""
        raise


def test_deploy(
    juju: Juju, charm: str, app_name: str, charm_resources, substrate: Substrate
) -> None:
    """Deploy a 3-unit PostgreSQL cluster and the continuous-writes test application."""
    logger.info("Deploying PostgreSQL cluster (%s, 3 units)", app_name)
    juju.deploy(
        charm=charm,
        app=app_name,
        base="ubuntu@24.04",
        config={"profile": "testing"},
        resources=charm_resources,
        num_units=3,
        trust=substrate == Substrate.k8s,
    )

    logger.info("Deploying test application (%s)", DB_TEST_APP_NAME)
    juju.deploy(
        charm=DB_TEST_APP_NAME,
        app=DB_TEST_APP_NAME,
        base="ubuntu@24.04",
        channel="latest/edge",
        num_units=1,
    )

    juju.integrate(f"{app_name}:database", f"{DB_TEST_APP_NAME}:database")

    logger.info("Waiting for all applications to become active")
    juju.wait(
        ready=wait_for_apps_status(jubilant.all_active, app_name, DB_TEST_APP_NAME),
        timeout=20 * MINUTE_SECS,
    )


@pytest.mark.vm_only
def test_patroni_reinit_after_pg_control_deletion_vm(
    juju: Juju, app_name: str, continuous_writes
) -> None:
    """Verify a replica reinitialises correctly after pg_control deletion and patronictl reinit.

    This validates that the versioned storage layout (data under 16/main subdirectories)
    works correctly with Patroni's reinit operation, which was previously broken when
    data lived at the mount point roots.

    Steps:
    1. Confirm the cluster is healthy and continuous writes are flowing.
    2. Stop Patroni on a replica (waiting until it is fully stopped), delete pg_control,
       then restart Patroni so that PostgreSQL fails to start due to the missing control file.
    3. Verify Patroni logs record a pg_control startup failure and that the replica is no
       longer streaming, so the test cannot falsely pass on a still-healthy replica.
    4. Run ``patronictl reinit`` to restore the replica from the primary.
    5. Confirm the replica returns to the streaming state, that reinit did not log
       data-directory permission errors, that pg_control is present, and that data
       integrity is maintained across all units.
    """
    logger.info("Identifying primary and replica units")
    primary_unit = get_db_primary_unit(juju, app_name)
    all_units = get_app_units(juju, app_name)
    replica_unit = next(unit for unit in all_units if unit != primary_unit)

    logger.info("Primary: %s | Replica under test: %s", primary_unit, replica_unit)

    replica_member_name = replica_unit.replace("/", "-")

    logger.info("Verifying continuous writes are flowing")
    check_db_units_writes_increment(juju, app_name)

    logger.info("Stopping Patroni on %s", replica_unit)
    juju.ssh(replica_unit, "sudo systemctl stop snap.charmed-postgresql.patroni")

    logger.info("Waiting for Patroni to stop on %s before deleting pg_control", replica_unit)
    for attempt in Retrying(stop=stop_after_delay(MINUTE_SECS), wait=wait_fixed(3), reraise=True):
        with attempt:
            # `systemctl is-active` prints the state on stdout and exits non-zero once the
            # service is no longer active; read that state whatever the exit code.
            try:
                active_state = juju.ssh(
                    replica_unit, "systemctl is-active snap.charmed-postgresql.patroni"
                ).strip()
            except subprocess.CalledProcessError as exc:
                active_state = (exc.stdout or "").strip()
            assert active_state and active_state != "active", (
                f"Patroni still active on {replica_unit} ({active_state!r}); refusing to delete "
                "pg_control while PostgreSQL may still be running"
            )

    logger.info("Deleting pg_control at %s on %s", VM_PG_CONTROL_PATH, replica_unit)
    juju.ssh(replica_unit, f"sudo rm {VM_PG_CONTROL_PATH}")

    logger.info("Starting Patroni on %s (pg_control is now missing)", replica_unit)
    juju.ssh(replica_unit, "sudo systemctl start snap.charmed-postgresql.patroni")

    logger.info("Confirming pg_control deletion broke replica %s (not streaming)", replica_unit)
    broken_state: str | None = None
    for attempt in Retrying(
        stop=stop_after_delay(3 * MINUTE_SECS), wait=wait_fixed(5), reraise=True
    ):
        with attempt:
            broken_state = get_member_state(juju, app_name, replica_member_name)
            # Reject None: right after restart the member is briefly absent from the cluster
            # (its DCS key TTL-expires while PostgreSQL fails to start), and reinit cannot
            # target an absent member ("No replica among provided members").
            assert broken_state is not None and broken_state not in ("running", "streaming"), (
                f"Replica {replica_unit} not yet present-and-broken ({broken_state!r}); expected it "
                "registered but not running/streaming before reinit"
            )
            assert _grep_patroni_logs(
                juju, replica_unit, "error when calling pg_controldata|system ID is invalid"
            ), (
                f"Patroni on {replica_unit} has not logged a pg_control startup failure; the "
                "pg_control deletion may not have broken PostgreSQL startup"
            )
    logger.info("Replica %s broken (state=%r) before reinit", replica_unit, broken_state)

    logger.info("Running patronictl reinit on %s", replica_unit)
    patronictl_cmd = (
        f"sudo charmed-postgresql.patronictl -c {VM_PATRONI_CONF_PATH}/patroni.yaml "
        f"reinit {app_name} {replica_member_name} --force"
    )
    # Retry transient reinit failures (member not yet re-registered, or its REST API not yet
    # listening after the restart) until Patroni accepts the request.
    for attempt in Retrying(
        stop=stop_after_delay(3 * MINUTE_SECS),
        wait=wait_fixed(10),
        retry=retry_if_exception(_reinit_transiently_failed),
        reraise=True,
    ):
        with attempt:
            juju.ssh(replica_unit, patronictl_cmd)

    logger.info("Waiting for %s to reach the streaming state after reinit", replica_unit)
    for attempt in Retrying(
        stop=stop_after_delay(5 * MINUTE_SECS), wait=wait_fixed(10), reraise=True
    ):
        with attempt:
            replica_state = get_member_state(juju, app_name, replica_member_name)
            assert replica_state == "streaming", (
                f"Replica {replica_unit} not streaming after reinit: {replica_state!r}"
            )
    logger.info("Replica %s is streaming after reinit", replica_unit)

    logger.info("Checking reinit logged no data-directory permission errors on %s", replica_unit)
    permission_errors = _grep_patroni_logs(
        juju,
        replica_unit,
        "could not remove data directory|could not rename data directory|permissionerror",
    )
    assert not permission_errors, (
        f"patronictl reinit on {replica_unit} logged data-directory permission failures; the "
        "versioned-storage parent directory is not writable by the PostgreSQL user. "
        f"Matching Patroni log lines:\n{permission_errors}"
    )
    logger.info("No data-directory permission errors logged during reinit on %s", replica_unit)

    logger.info("Verifying pg_control exists in data directory of %s after reinit", replica_unit)
    result = juju.ssh(
        replica_unit,
        f"sudo test -f {VM_PG_CONTROL_PATH} && echo exists || echo missing",
    )
    assert "exists" in result, (
        f"pg_control is missing from {replica_unit} after reinit "
        f"(expected at {VM_PG_CONTROL_PATH})"
    )
    logger.info("pg_control is present at %s on %s", VM_PG_CONTROL_PATH, replica_unit)

    logger.info("Verifying data integrity on all units after reinit")
    check_db_units_writes_increment(juju, app_name)
    logger.info(
        "Data integrity verified: all units (including %s) have consistent data", replica_unit
    )


@pytest.mark.k8s_only
def test_patroni_reinit_after_pg_control_deletion_k8s(
    juju: Juju, app_name: str, continuous_writes
) -> None:
    """Verify a replica reinitialises correctly after pg_control deletion and patronictl reinit.

    Steps:
    1. Confirm the cluster is healthy and continuous writes are flowing.
    2. Stop Patroni on a replica, delete pg_control, then restart Patroni so that
       PostgreSQL fails to start due to the missing control file.
    3. Verify pebble logs on the replica record a pg_control startup failure.
    4. Run ``patronictl reinit`` inside the replica container to restore it from the primary.
    5. Confirm the replica recovers, that pg_control is present in the data directory,
       and that data integrity is maintained across all units.
    """
    logger.info("Identifying primary and replica units")
    primary_unit = get_db_primary_unit(juju, app_name)
    all_units = get_app_units(juju, app_name)
    replica_unit = next(unit for unit in all_units if unit != primary_unit)

    logger.info("Primary: %s | Replica under test: %s", primary_unit, replica_unit)

    primary_ip = get_unit_address(juju, primary_unit)
    replica_ip = get_unit_address(juju, replica_unit)

    # Patroni member names match Kubernetes pod names (e.g. "postgresql-k8s/1" -> "postgresql-k8s-1").
    replica_member_name = replica_unit.replace("/", "-")
    cluster_name = f"patroni-{app_name}"

    # Kubernetes namespace equals the Juju model name.
    namespace = (juju.model or "").split(":")[-1]

    logger.info("Verifying initial health of replica %s", replica_unit)
    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(5), reraise=True):
        with attempt:
            resp = requests.get(f"https://{replica_ip}:8008/health", verify=False)
            assert resp.status_code == 200, (
                f"Replica {replica_unit} unhealthy before test: HTTP {resp.status_code}"
            )

    logger.info("Verifying continuous writes are flowing")
    check_db_units_writes_increment(juju, app_name)

    logger.info("Stopping Patroni on %s", replica_unit)
    juju.ssh(replica_unit, "/charm/bin/pebble stop postgresql", container="postgresql")

    logger.info("Deleting pg_control at %s on %s", K8S_PG_CONTROL_PATH, replica_unit)
    juju.ssh(replica_unit, f"rm {K8S_PG_CONTROL_PATH}", container="postgresql")

    logger.info("Starting Patroni on %s (pg_control is now missing)", replica_unit)
    juju.ssh(replica_unit, "/charm/bin/pebble start postgresql", container="postgresql")

    # Give pebble a moment to start the service and capture the first startup attempt in its logs.
    time.sleep(10)

    logger.info("Verifying pebble logs show a pg_control startup failure on %s", replica_unit)
    for attempt in Retrying(
        stop=stop_after_delay(2 * MINUTE_SECS), wait=wait_fixed(5), reraise=True
    ):
        with attempt:
            logs = juju.ssh(
                replica_unit,
                "/charm/bin/pebble logs postgresql -n all 2>&1 || true",
                container="postgresql",
            )
            assert "pg_control" in logs.lower(), (
                f"Expected pebble logs on {replica_unit} to contain a pg_control startup error; "
                f"last 1000 chars of logs: {logs[-1000:]!r}"
            )

    logger.info("Confirmed pg_control startup failure recorded in pebble logs on %s", replica_unit)

    # patronictl needs the same PATRONI_* environment variables that the Patroni pebble service
    # uses, but those are not available in a plain SSH session. We export them inline before the
    # command.
    logger.info("Running patronictl reinit on %s", replica_unit)
    k8s_labels = "{application: patroni, cluster-name: " + cluster_name + "}"
    patronictl_cmd = (
        f"PATRONI_SCOPE={cluster_name} "
        f"PATRONI_KUBERNETES_NAMESPACE={namespace} "
        f"PATRONI_KUBERNETES_USE_ENDPOINTS=true "
        f"PATRONI_KUBERNETES_LEADER_LABEL_VALUE=primary "
        f"PATRONI_KUBERNETES_LABELS='{k8s_labels}' "
        f"patronictl -c {K8S_STORAGE_PATH}/patroni.yaml "
        f"reinit {cluster_name} {replica_member_name} --force"
    )
    juju.ssh(replica_unit, patronictl_cmd, container="postgresql")

    logger.info("Waiting for %s to recover after patronictl reinit", replica_unit)
    for attempt in Retrying(
        stop=stop_after_delay(5 * MINUTE_SECS), wait=wait_fixed(10), reraise=True
    ):
        with attempt:
            resp = requests.get(f"https://{replica_ip}:8008/health", verify=False)
            assert resp.status_code == 200, (
                f"Replica {replica_unit} still unhealthy after reinit: HTTP {resp.status_code}"
            )

    for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(5), reraise=True):
        with attempt:
            cluster_resp = requests.get(f"https://{primary_ip}:8008/cluster", verify=False)
            members = cluster_resp.json()["members"]
            replica_state = next(
                (m["state"] for m in members if m["name"] == replica_member_name), None
            )
            assert replica_state in ("running", "streaming"), (
                f"Replica {replica_unit} not streaming after reinit: {replica_state!r}"
            )

    logger.info("Replica %s is healthy and streaming after reinit", replica_unit)

    logger.info("Verifying pg_control exists in data directory of %s after reinit", replica_unit)
    result = juju.ssh(
        replica_unit,
        f"test -f {K8S_PG_CONTROL_PATH} && echo exists || echo missing",
        container="postgresql",
    )
    assert "exists" in result, (
        f"pg_control is missing from {replica_unit} after reinit "
        f"(expected at {K8S_PG_CONTROL_PATH})"
    )
    logger.info("pg_control is present at %s on %s", K8S_PG_CONTROL_PATH, replica_unit)

    logger.info("Verifying data integrity on all units after reinit")
    check_db_units_writes_increment(juju, app_name)
    logger.info(
        "Data integrity verified: all units (including %s) have consistent data", replica_unit
    )
