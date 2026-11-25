#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for PostgreSQL stereo mode with watcher.

Tests the deployment and failover scenarios for 2-node PostgreSQL clusters
with a watcher/witness node for quorum.

Test scenarios from acceptance criteria:
1. Replica shutdown: clients rerouted to primary, no significant outage
2. Primary shutdown: replica promoted, old primary becomes replica when healthy
3. Watcher shutdown: no service outage
4. Network isolation variants of above
"""

import logging
from time import sleep

import jubilant
import pytest
from pysyncobj.utility import TcpUtility
from single_kernel_postgresql.config.literals import RAFT_PARTNER_PREFIX
from tenacity import Retrying, stop_after_delay, wait_fixed
from yaml import safe_load

from tests.integration.charm.helpers import (
    APPLICATION_NAME,
    CHARM_BASE,
    are_writes_increasing,
    check_writes,
    cut_network_from_unit,
    cut_network_from_unit_without_ip_change,
    get_ha_cluster_roles,
    get_machine_from_unit,
    get_primary_from_app,
    restore_network_for_unit,
    restore_network_for_unit_without_ip_change,
    stop_machine,
)
from tests.integration.helpers.juju import fast_forward, wait_for_apps_status

WATCHER_APP_NAME = "postgresql-watcher"
SECOND_PG_APP_NAME = "postgresql-b"

pytestmark = pytest.mark.vm_only

# Jubilant's remove_unit API does not expose libjuju's max_wait=1500/300.
# Keep the original force/storage policy and subsequent waits without inventing
# a replacement for that server-side removal limit.


def start_writes(juju: jubilant.Juju) -> None:
    """Start continuous writes to PostgreSQL (assumes relation already exists)."""
    for attempt in Retrying(stop=stop_after_delay(60 * 5), wait=wait_fixed(3), reraise=True):
        with attempt:
            unit = next(iter(juju.status().apps[APPLICATION_NAME].units))
            action = juju.run(unit, "start-continuous-writes")
            assert action.results["result"] == "True", "Unable to create continuous_writes table"


logger = logging.getLogger(__name__)


def verify_raft_cluster_health(
    juju: jubilant.Juju,
    db_app_name: str,
    watcher_app_name: str,
    expected_members: int = 3,
    check_watcher_ip: bool = True,
) -> None:
    """Verify that the Raft cluster has the expected number of members and quorum.

    This function checks that all PostgreSQL units see the expected number of
    Raft members (including the watcher) and have quorum. This is critical
    after watcher re-deployment to ensure the cluster is properly formed.

    Args:
        juju: The Jubilant instance.
        db_app_name: The PostgreSQL application name.
        watcher_app_name: The watcher application name.
        expected_members: Expected number of Raft members (default 3 for stereo mode).
        check_watcher_ip: Whether to verify the watcher IP in Raft status (default True).
            Set to False after network isolation tests where watcher may have been
            redeployed with a new IP that isn't yet in the Raft configuration.

    Raises:
        AssertionError: If the Raft cluster is not healthy.
    """
    logger.info(f"Verifying Raft cluster health with {expected_members} expected members")

    # Get watcher address for verification using juju exec to avoid cached IPs
    watcher_unit = next(iter(juju.status().apps[watcher_app_name].units))
    watcher_ip = juju.cli(
        "exec", "--unit", watcher_unit, "--", "unit-get", "private-address"
    ).strip()

    for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(5), reraise=True):
        with attempt:
            for unit in juju.status().apps[db_app_name].units:
                # Get the Raft password from Patroni config using juju exec directly
                # We need to avoid shell interpretation issues with run_command_on_unit
                complete_command = [
                    "exec",
                    "--unit",
                    unit,
                    "--",
                    "cat",
                    "/var/snap/charmed-postgresql/current/etc/patroni/patroni.yaml",
                ]
                stdout = juju.cli(*complete_command)

                conf = safe_load(stdout)
                password = conf.get("raft", {}).get("password")
                self_addr = conf.get("raft", {}).get("self_addr")
                assert password, f"Could not find Raft password in patroni.yaml on {unit}"

                # Check Raft status using the password
                syncobj_util = TcpUtility(password=password, timeout=3)
                status = syncobj_util.executeCommand(self_addr, ["status"])
                logger.info(f"Raft status on {unit}: {status}")

                # Verify quorum
                assert status["has_quorum"] is True, f"Unit {unit} does not have Raft quorum"

                assert status["partner_nodes_count"] + 1 == expected_members

                # Verify watcher is in the cluster (if requested)
                # After network isolation tests, the watcher may have been redeployed
                # with a new IP that isn't yet updated in the Raft configuration
                if check_watcher_ip:
                    assert watcher_ip in [
                        key.split(":")[0].split(RAFT_PARTNER_PREFIX)[-1]
                        for key in status
                        if key.startswith(RAFT_PARTNER_PREFIX)
                    ], f"Watcher {watcher_ip} not found in Raft cluster on {unit}"

    logger.info("Raft cluster health verified successfully")


@pytest.mark.abort_on_fail
def test_build_and_deploy_stereo_mode(juju: jubilant.Juju, charm, app_name: str) -> None:
    """Build and deploy PostgreSQL in stereo mode with watcher.

    Deploys 2 PostgreSQL units and a watcher (same charm, role=watcher),
    then relates them to form a 3-node Raft cluster for quorum.
    """
    # Check if PostgreSQL is already deployed (e.g., from a previous test run)
    # If so, verify it's in the expected state or skip deployment
    if app_name in juju.status().apps:
        logger.info("PostgreSQL already deployed, checking state...")
        pg_units = len(juju.status().apps[app_name].units)
        watcher_deployed = WATCHER_APP_NAME in juju.status().apps
        test_app_deployed = APPLICATION_NAME in juju.status().apps

        if pg_units == 2 and watcher_deployed and test_app_deployed:
            logger.info("Stereo mode already deployed with expected state, verifying...")
            juju.wait(wait_for_apps_status(jubilant.all_active), timeout=300)
            return

        # If state is incorrect, we need to clean up and redeploy
        logger.info(f"Unexpected state (pg_units={pg_units}), cleaning up...")
        for app in [WATCHER_APP_NAME, WATCHER_APP_NAME, APPLICATION_NAME]:
            if app in juju.status().apps:
                juju.remove_application(app)
                juju.wait(
                    lambda status, app=app: app not in status.apps,
                    delay=0.5,
                    successes=1,
                    timeout=1000,
                )

    with fast_forward(juju):
        # Deploy PostgreSQL with 2 units from the start
        logger.info("Deploying PostgreSQL charm with 2 units...")
        juju.deploy(
            charm,
            app=app_name,
            num_units=2,
            base=CHARM_BASE,
            config={"profile": "testing", "synchronous-mode-strict": False},
        )
        # Deploy watcher using the same charm with role=watcher
        logger.info("Deploying watcher (same charm, role=watcher)...")
        juju.deploy(
            WATCHER_APP_NAME,
            app=WATCHER_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            channel="16/edge",
            config={"profile": "testing"},
        )
        logger.info("Deploying test application...")
        juju.deploy(
            APPLICATION_NAME,
            app=APPLICATION_NAME,
            base=CHARM_BASE,
            channel="edge",
        )

        logger.info("Deploying second PostgreSQL cluster for multi-cluster watcher test")
        juju.deploy(
            charm,
            app=SECOND_PG_APP_NAME,
            num_units=2,
            base=CHARM_BASE,
            config={"profile": "testing", "synchronous-mode-strict": False},
        )

        # Relate PostgreSQL (watcher-offer) to watcher (watcher)
        # The relation may already exist if deploying into a model with prior state
        logger.info("Relating PostgreSQL to watcher")
        try:
            juju.integrate(f"{app_name}:watcher-offer", f"{WATCHER_APP_NAME}:watcher")
        except Exception as e:
            if "already exists" in str(e) or "relation" in str(e).lower():
                logger.info(f"Watcher relation already exists: {e}")
            else:
                raise

        # Relate PostgreSQL to test app
        try:
            juju.integrate(app_name, f"{APPLICATION_NAME}:database")
        except Exception as e:
            if "already exists" in str(e) or "relation" in str(e).lower():
                logger.info(f"Database relation already exists: {e}")
            else:
                raise

        juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1800)

    # Verify deployment
    assert len(juju.status().apps[app_name].units) == 2
    assert len(juju.status().apps[WATCHER_APP_NAME].units) == 1


@pytest.mark.abort_on_fail
def test_replica_shutdown_with_watcher(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test replica shutdown with watcher providing quorum.

    Expected behavior:
    - All connected clients to the primary should not be interrupted
    - Clients connected to replica should be re-routed to primary
    - No significant outage (less than a minute)
    """
    start_writes(juju)

    # Get current cluster roles
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit)
    primary = original_roles["primaries"][0]

    # Get the replica unit
    replica = None
    for unit in juju.status().apps[app_name].units:
        if unit != primary:
            replica = unit
            break

    assert replica is not None, "Could not find replica unit"
    logger.info(f"Shutting down replica: {replica}")

    # Shutdown the replica
    stop_machine(get_machine_from_unit(juju, replica))
    juju.remove_unit(replica, force=True, destroy_storage=False)

    # Wait for the cluster to stabilize after unit removal
    # The primary needs time to reconfigure the cluster and update secrets
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300, successes=31)

    # Verify writes continue (primary should still be available)
    # With watcher, we should maintain quorum
    are_writes_increasing(juju, down_unit=replica)

    # Wait for cluster to stabilize
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600, successes=31)

    # Scale back up
    logger.info("Scaling back up after replica shutdown")
    juju.add_unit(app_name, num_units=1)
    juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1500, successes=16)

    # Wait for the new replica to become a sync_standby
    # This ensures the cluster is fully ready for the next test
    for attempt in Retrying(stop=stop_after_delay(180), wait=wait_fixed(10), reraise=True):
        with attempt:
            new_roles = get_ha_cluster_roles(juju, next(iter(juju.status().apps[app_name].units)))
            logger.info(f"Cluster roles: {new_roles}")
            assert len(new_roles["primaries"]) == 1, "Should have exactly one primary"
            assert new_roles["primaries"][0] == primary, "Primary should not have changed"
            assert len(new_roles["sync_standbys"]) == 1, "New replica should become sync_standby"

    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)
    check_writes(juju)


@pytest.mark.abort_on_fail
def test_primary_shutdown_with_watcher(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test primary shutdown with watcher providing quorum.

    Expected behavior:
    - Old primary should be network-isolated (Patroni handles this)
    - Replica should be promoted to primary
    - Clients re-routed to new primary
    - When old primary is healthy, it should become a replica
    """
    start_writes(juju)

    # Get current cluster roles
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit)
    original_primary = original_roles["primaries"][0]

    # Get the replica - prefer sync_standby if available, otherwise any replica
    # After a previous test scales up, the new unit may not yet be a sync_standby
    if original_roles["sync_standbys"]:
        original_replica = original_roles["sync_standbys"][0]
    elif original_roles["replicas"]:
        original_replica = original_roles["replicas"][0]
    else:
        # Fall back to finding the other unit manually
        original_replica = None
        for unit in juju.status().apps[app_name].units:
            if unit != original_primary:
                original_replica = unit
                break
        assert original_replica is not None, "Could not find replica unit"

    logger.info(f"Shutting down primary: {original_primary}")

    # Shutdown the primary
    stop_machine(get_machine_from_unit(juju, original_primary))
    juju.remove_unit(original_primary, force=True, destroy_storage=False)

    # With watcher providing quorum, failover should happen automatically
    # Wait for the model to stabilize first
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)

    # Wait for the replica to be promoted to primary
    # Patroni needs time to detect leader failure and elect new leader (30-90s)
    remaining_unit = next(iter(juju.status().apps[app_name].units))
    for attempt in Retrying(stop=stop_after_delay(180), wait=wait_fixed(10), reraise=True):
        with attempt:
            new_roles = get_ha_cluster_roles(juju, remaining_unit)
            logger.info(f"Waiting for failover - current roles: {new_roles}")
            assert len(new_roles["primaries"]) == 1, "Should have exactly one primary"
            assert new_roles["primaries"][0] == original_replica, (
                f"Replica {original_replica} should have been promoted, "
                f"but primary is {new_roles['primaries'][0]}"
            )

    # Wait for the charm to reconfigure after failover
    # This ensures the relation endpoints are updated for the test app to reconnect
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    # Scale back up FIRST - with synchronous_mode_strict=true, the primary cannot
    # accept writes when there's no sync_standby available. We need 2 units before
    # we can verify writes are working.
    logger.info("Scaling back up after primary shutdown")
    juju.add_unit(app_name, num_units=1)
    # Wait longer for the new unit to fully join the cluster
    # The new unit needs to: start PostgreSQL, join Raft cluster, become sync_standby
    juju.wait(wait_for_apps_status(jubilant.all_active), timeout=1800)

    # Wait for the new replica to become a sync_standby
    # This can take a while as the new unit needs to fully sync and be recognized
    for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(10), reraise=True):
        with attempt:
            final_roles = get_ha_cluster_roles(
                juju, next(iter(juju.status().apps[app_name].units))
            )
            logger.info(f"Final cluster roles: {final_roles}")
            assert len(final_roles["primaries"]) == 1, "Should have exactly one primary"
            assert len(final_roles["sync_standbys"]) == 1, "New replica should become sync_standby"

    # Now that we have a sync_standby, restart continuous writes and verify
    # The continuous writes app caches the connection string, so we need to clear
    # and restart it after failover to pick up the new primary's address.
    # First clear the old writes state
    unit = next(iter(juju.status().apps[APPLICATION_NAME].units))
    juju.run(unit, "clear-continuous-writes")

    # Then start fresh writes
    start_writes(juju)

    # Verify writes continue on the new primary
    are_writes_increasing(juju, down_unit=original_primary)
    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)
    check_writes(juju)


@pytest.mark.abort_on_fail
def test_watcher_shutdown_no_outage(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test watcher shutdown - should not cause service outage.

    Expected behavior:
    - No outage experienced by either primary or replica
    - Cluster continues to function (but loses quorum guarantee)
    """
    start_writes(juju)

    # Get current cluster state
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit)

    logger.info("Removing watcher unit")

    # Remove the watcher
    watcher_unit = next(iter(juju.status().apps[WATCHER_APP_NAME].units))
    stop_machine(get_machine_from_unit(juju, watcher_unit))
    juju.remove_unit(watcher_unit, force=True)

    # Verify writes continue without interruption
    are_writes_increasing(juju)

    # PostgreSQL cluster should remain active
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=300)

    # Verify cluster roles unchanged
    new_roles = get_ha_cluster_roles(juju, any_unit)
    assert new_roles["primaries"] == original_roles["primaries"]

    # Re-deploy watcher
    logger.info("Re-deploying watcher")
    juju.add_unit(WATCHER_APP_NAME, num_units=1)
    # Wait for the replacement, so Raft verification cannot select the removed watcher.
    juju.wait(
        lambda status: (
            watcher_unit not in status.apps[WATCHER_APP_NAME].units
            and len(status.apps[WATCHER_APP_NAME].units) == 1
            and wait_for_apps_status(jubilant.all_active, WATCHER_APP_NAME)(status)
        ),
        timeout=600,
        successes=16,
    )

    # Verify the Raft cluster is properly formed with the new watcher
    # This is critical - without this verification, subsequent tests might fail
    # because the watcher is not actually participating in the Raft cluster
    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)

    check_writes(juju)


@pytest.mark.abort_on_fail
def test_primary_network_isolation_with_watcher(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test network isolation of primary with watcher.

    Expected behavior:
    - Isolated primary's connections terminated
    - Replica promoted to primary
    - When network restored, old primary becomes replica
    """
    start_writes(juju)

    # Get current cluster state
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit)
    primary = original_roles["primaries"][0]
    replica = original_roles["sync_standbys"][0]

    # Get primary machine name for network manipulation
    model_status = juju.status()
    primary_unit = None
    for unit_name, unit in model_status.apps[app_name].units.items():
        if unit_name == primary:
            primary_unit = unit
            break

    assert primary_unit is not None
    primary_machine = model_status.machines[primary_unit.machine].hostname

    logger.info(f"Isolating primary network: {primary} on {primary_machine}")

    try:
        # Cut network from primary (this removes the eth0 interface entirely)
        cut_network_from_unit(primary_machine)

        # Wait for failover to happen - Patroni needs time to detect leader failure
        # and elect a new leader. This can take 30-90 seconds depending on TTL settings.
        # Use an explicit retry loop instead of just waiting for idle/active agents.
        new_primary = None
        for attempt in Retrying(stop=stop_after_delay(180), wait=wait_fixed(10), reraise=True):
            with attempt:
                new_primary = get_primary_from_app(juju, app_name, down_unit=primary)
                logger.info(f"Current primary: {new_primary}, expected: {replica}")
                assert new_primary == replica, (
                    f"Waiting for failover: replica {replica} should be promoted, "
                    f"but primary is still {new_primary}"
                )
                are_writes_increasing(juju, down_unit=primary)
    finally:
        # Restore network
        logger.info(f"Restoring network for {primary_machine}")
        restore_network_for_unit(primary_machine)

    # Wait for cluster to stabilize with restored network
    # The old primary may take time to rejoin after getting a new IP address,
    # so wait only for idle agents with a longer timeout.
    with fast_forward(juju, "60s"):
        juju.wait(lambda status: jubilant.all_agents_idle(status, app_name), timeout=900)

    # Wait for the old primary to rejoin as replica
    # This can take a while as it needs to recover with a new IP
    for attempt in Retrying(stop=stop_after_delay(360), wait=wait_fixed(10), reraise=True):
        with attempt:
            final_roles = get_ha_cluster_roles(juju, replica)
            logger.info(f"Final cluster roles: {final_roles}")
            assert replica in final_roles["primaries"], (
                "Replica should remain primary after network restore"
            )
            # Old primary should not be primary anymore
            assert (
                primary not in final_roles["primaries"] and primary in final_roles["sync_standbys"]
            ), "Old primary should now be a sync standby"

    # Use use_ip_from_inside=True because the old primary got a new IP after network restore
    # and Juju's cached IP may be stale
    check_writes(juju, use_ip_from_inside=True)
    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)


@pytest.mark.abort_on_fail
def test_replica_network_isolation_with_watcher(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test network isolation of replica with watcher.

    Expected behavior:
    - Primary remains primary (doesn't failover) - Raft quorum maintained with watcher
    - With synchronous_mode_strict=true, writes pause (no sync_standby available)
    - After network restore, writes resume
    - No data loss

    Note: This test uses iptables-based network isolation to preserve the replica's IP,
    avoiding the complexity of IP changes when using eth0 device removal.
    """
    start_writes(juju)

    # Get current cluster state - use use_ip_from_inside=True because the previous test
    # may have left units with stale IPs in Juju's cache after network restore
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit, use_ip_from_inside=True)
    primary = original_roles["primaries"][0]
    replica = original_roles["sync_standbys"][0]

    # Get replica machine for network manipulation
    model_status = juju.status()
    replica_unit = None
    for unit_name, unit in model_status.apps[app_name].units.items():
        if unit_name == replica:
            replica_unit = unit
            break

    assert replica_unit is not None
    replica_machine = model_status.machines[replica_unit.machine].hostname

    logger.info(f"Isolating replica network: {replica} on {replica_machine}")

    try:
        # Cut network from replica using iptables (preserves IP)
        cut_network_from_unit_without_ip_change(replica_machine)

        # Give Patroni time to detect the network isolation.
        sleep(30)

        # Primary should remain primary (no failover should happen)
        # Raft quorum is maintained with primary + watcher (2 out of 3)
        current_primary = get_primary_from_app(juju, app_name, down_unit=replica)
        assert current_primary == primary, "Primary should not change during replica isolation"
        are_writes_increasing(juju, down_unit=replica)
    finally:
        # Restore network
        logger.info(f"Restoring network for {replica_machine}")
        restore_network_for_unit_without_ip_change(replica_machine)

    # Wait for cluster to stabilize - replica should rejoin
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name), timeout=600)

    # Verify cluster has a primary after restore (may or may not be the same one,
    # since Patroni can switchover during network restore/rejoin)
    final_roles = get_ha_cluster_roles(juju, any_unit, use_ip_from_inside=True)
    assert len(final_roles["primaries"]) == 1, (
        "Cluster should have exactly one primary after restore"
    )

    # Verify writes continue after network restore
    # Use use_ip_from_inside=True because previous tests may have caused IP changes
    are_writes_increasing(juju, use_ip_from_inside=True)
    check_writes(juju, use_ip_from_inside=True)
    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)


@pytest.mark.abort_on_fail
def test_watcher_network_isolation(
    juju: jubilant.Juju, juju_continuous_writes, app_name: str
) -> None:
    """Test network isolation of watcher.

    Expected behavior:
    - No service outage for PostgreSQL cluster
    - Cluster loses quorum guarantee but continues operating
    """
    start_writes(juju)

    # Get watcher machine
    model_status = juju.status()
    watcher_unit = next(iter(model_status.apps[WATCHER_APP_NAME].units.values()))
    watcher_machine = model_status.machines[watcher_unit.machine].hostname

    # Get current cluster state - use use_ip_from_inside=True because previous tests
    # may have left units with stale IPs in Juju's cache after network manipulation
    any_unit = next(iter(juju.status().apps[app_name].units))
    original_roles = get_ha_cluster_roles(juju, any_unit, use_ip_from_inside=True)

    logger.info(f"Isolating watcher network: {watcher_machine}")

    try:
        # Cut network from watcher
        cut_network_from_unit(watcher_machine)

        # Verify writes continue without interruption
        are_writes_increasing(juju, use_ip_from_inside=True)

        # Cluster roles should remain unchanged
        current_roles = get_ha_cluster_roles(juju, any_unit, use_ip_from_inside=True)
        assert current_roles["primaries"] == original_roles["primaries"]

    finally:
        # Restore network
        logger.info(f"Restoring watcher network: {watcher_machine}")
        restore_network_for_unit(watcher_machine)

    # Wait for full recovery
    juju.wait(wait_for_apps_status(jubilant.all_active), timeout=600)
    verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME)

    # Use use_ip_from_inside=True because the watcher got a new IP after network restore
    check_writes(juju, use_ip_from_inside=True)


@pytest.mark.abort_on_fail
def test_multi_cluster_watcher(juju: jubilant.Juju, charm, app_name: str) -> None:
    """Verify that a single watcher can monitor multiple PostgreSQL clusters.

    The watcher relation no longer has limit: 1, so the watcher can relate
    to multiple PostgreSQL clusters simultaneously. Each relation gets its own
    Raft instance with a dedicated port and data directory.
    """
    try:
        # Deploy a second PostgreSQL cluster
        juju.wait(wait_for_apps_status(jubilant.all_active, SECOND_PG_APP_NAME), timeout=330)

        # Relate the watcher to the second cluster
        logger.info("Relating watcher to second PostgreSQL cluster")
        juju.integrate(f"{SECOND_PG_APP_NAME}:watcher-offer", f"{WATCHER_APP_NAME}:watcher")

        # Use fast_forward to trigger update_status quickly, which runs
        # ensure_watcher_in_raft to add the watcher to the second cluster's Raft
        with fast_forward(juju):
            # Wait for the watcher to connect to both clusters
            juju.wait(
                wait_for_apps_status(
                    jubilant.all_active, app_name, SECOND_PG_APP_NAME, WATCHER_APP_NAME
                ),
                timeout=600,
            )

            # Verify both Raft clusters have the watcher as a member
            # Check first cluster
            verify_raft_cluster_health(juju, app_name, WATCHER_APP_NAME, expected_members=3)
            # Check second cluster
            verify_raft_cluster_health(
                juju, SECOND_PG_APP_NAME, WATCHER_APP_NAME, expected_members=3
            )

    finally:
        # Clean up the second cluster relation and app
        if SECOND_PG_APP_NAME in juju.status().apps:
            juju.remove_application(SECOND_PG_APP_NAME, force=True)
            juju.wait(
                lambda status: SECOND_PG_APP_NAME not in status.apps,
                delay=0.5,
                successes=1,
                timeout=1000,
            )

    # Verify original watcher is still healthy after removing the second cluster
    juju.wait(wait_for_apps_status(jubilant.all_active, app_name, WATCHER_APP_NAME), timeout=300)


@pytest.mark.abort_on_fail
def test_watcher_production_profile_az_blocked(juju: jubilant.Juju, charm, app_name: str) -> None:
    """Test watcher with profile=production blocks on AZ co-location.

    When all units are in the same availability zone (common on single-host
    LXD deployments), a watcher with profile=production should enter
    BlockedStatus because it shares an AZ with the PostgreSQL units.
    This validates the AZ enforcement behavior.

    If JUJU_AVAILABILITY_ZONE is not set (some CI environments), the watcher
    should reach active status since no AZ co-location can be detected.

    Since watcher-offer has limit: 1, we must remove the existing testing watcher
    before deploying the production one, then restore it afterward.
    """
    production_watcher = "pg-watcher-prod"

    # Remove existing watcher to free the watcher-offer relation slot
    logger.info("Removing existing testing watcher to free relation slot")
    if WATCHER_APP_NAME in juju.status().apps:
        juju.remove_application(WATCHER_APP_NAME, force=True)
        juju.wait(
            lambda status: WATCHER_APP_NAME not in status.apps,
            delay=0.5,
            successes=1,
            timeout=1000,
        )

    try:
        # Deploy a production-profile watcher
        logger.info("Deploying watcher with profile=production")
        juju.deploy(
            WATCHER_APP_NAME,
            app=production_watcher,
            num_units=1,
            base=CHARM_BASE,
            channel="16/edge",
            config={"profile": "production"},
        )

        # Wait for initial install
        juju.wait(lambda status: jubilant.all_agents_idle(status, production_watcher), timeout=600)

        # Relate to the existing PostgreSQL cluster
        juju.integrate(f"{app_name}:watcher-offer", f"{production_watcher}:watcher")

        # Wait for the watcher to settle (it may block or go active depending on AZ)
        with fast_forward(juju):
            juju.wait(
                lambda status: jubilant.all_agents_idle(status, production_watcher), timeout=600
            )

        # Check the watcher's status
        watcher_unit = next(iter(juju.status().apps[production_watcher].units.values()))
        status = watcher_unit.workload_status.current
        status_msg = watcher_unit.workload_status.message

        if status == "blocked":
            # AZ is set and co-located — expected on single-host deployments
            assert "AZ co-location" in status_msg, (
                f"Blocked status should mention AZ co-location, got: {status_msg}"
            )
            logger.info(f"Production watcher correctly blocked: {status_msg}")
        elif status == "active":
            # AZ is not set — no co-location detected, watcher is active
            logger.info("JUJU_AVAILABILITY_ZONE not set, watcher is active (no AZ enforcement)")
        else:
            pytest.fail(
                f"Unexpected watcher status: {status} - {status_msg}. "
                "Expected 'blocked' (AZ co-location) or 'active' (no AZ)."
            )

    finally:
        # Clean up production watcher
        if production_watcher in juju.status().apps:
            juju.remove_application(production_watcher, force=True)
            juju.wait(
                lambda status: production_watcher not in status.apps,
                delay=0.5,
                successes=1,
                timeout=1000,
            )

        # Restore the original testing watcher
        logger.info("Restoring original testing watcher")
        juju.deploy(
            WATCHER_APP_NAME,
            app=WATCHER_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            channel="16/edge",
            config={"profile": "testing"},
        )
        juju.wait(lambda status: jubilant.all_agents_idle(status, WATCHER_APP_NAME), timeout=600)
        juju.integrate(f"{app_name}:watcher-offer", f"{WATCHER_APP_NAME}:watcher")
        juju.wait(
            wait_for_apps_status(jubilant.all_active, app_name, WATCHER_APP_NAME), timeout=600
        )
