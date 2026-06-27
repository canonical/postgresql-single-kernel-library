#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Patroni Manager.

This manager is responsible for handling operations related to Patroni,
such as starting the service and checking its status.
"""

# import shutil
# from pathlib import Path
import glob
import logging
import os
import pathlib
import re
import subprocess
from contextlib import suppress
from functools import cached_property
from typing import Any, Literal, TypedDict

# Platform specific imports
with suppress(ImportError):
    # import psutil
    from charmlibs import snap
    # from pysyncobj.utility import TcpUtility, UtilityException
# from ops import BlockedStatus
import psycopg2
import psycopg2.extras
import requests
import tomli
from data_platform_helpers.advanced_statuses import StatusObject
from data_platform_helpers.advanced_statuses.types import Scope as AdvancedStatusesScope
from httpx import BasicAuth
from requests.auth import HTTPBasicAuth
from tenacity import (
    Future,
    RetryError,
    Retrying,
    retry,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    API_REQUEST_TIMEOUT,
    PATRONI_CLUSTER_STATUS_ENDPOINT,
    # PEER_RELATION,
    POSTGRESQL_STORAGE_PERMISSIONS,
    # RAFT_PARTNER_PREFIX,
    # RAFT_PORT,
    RUNNING_STATES,
    STARTED_STATES,
    TLS_CA_BUNDLE_FILE,
    VM_PATRONI_SERVICE_DEFAULT_PATH,
)
from single_kernel_postgresql.config.statuses import GeneralStatuses
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.base import BaseManager
from single_kernel_postgresql.utils import _change_owner, label2name, parallel_patroni_get_request
from single_kernel_postgresql.workload.base import BaseWorkload
from single_kernel_postgresql.workload.vm import VMWorkload

logger = logging.getLogger(__name__)


class RaftPostgresqlNotUpError(Exception):
    """Postgresql not yet started."""


class RaftPostgresqlStillUpError(Exception):
    """Postgresql not yet down."""


class RaftNotPromotedError(Exception):
    """Leader not yet set when reinitialising raft."""


class ClusterNotPromotedError(Exception):
    """Raised when a cluster is not promoted."""


class NotReadyError(Exception):
    """Raised when not all cluster members healthy or finished initial sync."""


class EndpointNotReadyError(Exception):
    """Raised when an endpoint is not ready."""


class StandbyClusterAlreadyPromotedError(Exception):
    """Raised when a standby cluster is already promoted."""


class RemoveRaftMemberFailedError(Exception):
    """Raised when a remove raft member failed for some reason."""


class SwitchoverFailedError(Exception):
    """Raised when a switchover failed for some reason."""


class SwitchoverNotSyncError(SwitchoverFailedError):
    """Raised when a switchover failed because node is not sync."""


class UpdateSyncNodeCountError(Exception):
    """Raised when updating synchronous_node_count failed for some reason."""


class ClusterMember(TypedDict):
    """Type for cluster member."""

    name: str
    role: str
    state: str
    api_url: str
    host: str
    port: int
    timeline: int
    lag: int


class PatroniManager(BaseManager):
    """PostgreSQL Patroni Manager.

    This manager is responsible for handling operations related to Patroni.
    """

    def __init__(
        self,
        state: CharmState,
        workload: BaseWorkload,
    ):
        super().__init__(state, workload, "patroni_manager")
        # Variable mapping to requests library verify parameter.
        # The CA bundle file is used to validate the server certificate when
        # TLS is enabled, otherwise True is set because it's the default value.
        if self.state.substrate == Substrates.VM:
            # if any([primary_endpoint]):
            #     raise Exception("K8s attributes set with VM substrate")
            self.verify = f"{self.workload.paths.patroni_conf}/{TLS_CA_BUNDLE_FILE}"
        else:
            # if any([raft_password]):
            #     raise Exception("VM attributes set with K8s substrate")
            # CA bundle is not secret
            self.verify = f"/tmp/{TLS_CA_BUNDLE_FILE}"  # noqa: S108

    def start_patroni(self) -> bool:
        """Start Patroni."""
        if self.state.substrate == Substrates.VM and isinstance(self.workload, VMWorkload):
            return self.workload.start_patroni()
        else:
            # TODO: Implement for other substrates
            return False

    @property
    def member_started(self) -> bool:
        """Has the member started Patroni and PostgreSQL.

        Returns:
            True if services is ready False otherwise. Retries over a period of 60 seconds times to
            allow server time to start up.
        """
        if self.state.substrate == Substrates.VM and isinstance(self.workload, VMWorkload):
            if not self.workload.is_patroni_running():
                return False
            try:
                response = self.cached_patroni_health
            except RetryError:
                return False

            return response["state"] in RUNNING_STATES
        else:
            # TODO: Implement for other substrates
            return False

    @cached_property
    def cached_patroni_health(self) -> dict[str, str]:
        """Cached local unit health."""
        return self.get_patroni_health()

    def get_patroni_health(self) -> dict[str, str]:
        """Gets, retires and parses the Patroni health endpoint."""
        # TODO: Revert stop after delay to 60 and wait fixed to 7 after testing
        for attempt in Retrying(stop=stop_after_delay(1), wait=wait_fixed(1)):
            with attempt:
                r = requests.get(
                    f"{self.state.patroni_url}/health",
                    verify=self.verify,
                    timeout=API_REQUEST_TIMEOUT,
                    auth=self._patroni_auth,
                )
                logger.debug("API get_patroni_health: %s (%s)", r, r.elapsed.total_seconds())

        return r.json()

    @cached_property
    def _patroni_auth(self) -> HTTPBasicAuth | None:
        if self.state.application.patroni_password:
            return HTTPBasicAuth("patroni", self.state.application.patroni_password)

    @cached_property
    def _patroni_async_auth(self) -> BasicAuth | None:
        if self.state.application.patroni_password:
            return BasicAuth("patroni", password=self.state.application.patroni_password)

    def bootstrap_cluster(self) -> bool:
        """Bootstrap a PostgreSQL cluster using Patroni."""
        # Render the configuration files and start the cluster.
        self.configure_patroni_on_unit()
        return self.start_patroni()

    def configure_patroni_on_unit(self):
        """Configure Patroni (configuration files and service) on the unit."""
        patroni_conf_file = str(self.workload.paths.patroni_conf / "patroni.yaml")
        patroni_data_path = str(self.workload.paths.data)
        os.makedirs(patroni_data_path, exist_ok=True)
        # Parent must be _daemon_-owned so Patroni can rename/remove the data dir on reinit.
        _change_owner(Substrates.VM, os.path.dirname(patroni_data_path))
        _change_owner(Substrates.VM, patroni_data_path)

        # Create empty base config
        open(patroni_conf_file, "a").close()

        # Expected permission
        # Replicas refuse to start with the default permissions
        os.chmod(patroni_data_path, POSTGRESQL_STORAGE_PERMISSIONS)

    @cached_property
    def cluster_members(self) -> set:
        """Get the current cluster members."""
        # Request info from cluster endpoint (which returns all members of the cluster).
        # Request info from cluster endpoint (which returns all members of the cluster).
        try:
            return {member["name"] for member in self.cluster_status()}
        except Exception:
            return set()

    def get_postgresql_version(self) -> str:
        """Return the PostgreSQL version from the system."""
        with pathlib.Path("refresh_versions.toml").open("rb") as file:
            return tomli.load(file)["workload"]

    @cached_property
    def cached_cluster_status(self):
        """Cached cluster status."""
        return self.cluster_status()

    def cluster_status(self, alternative_endpoints: list | None = None) -> list[ClusterMember]:
        """Query the cluster status."""
        if not self._patroni_async_auth:
            raise RetryError(
                last_attempt=Future.construct(1, Exception("Unable to reach any units"), True)
            )

        # TODO we don't know the other cluster's ca
        verify = not bool(alternative_endpoints)
        if alternative_endpoints:
            endpoints = alternative_endpoints
        else:
            endpoints = []
            if self.state.endpoint:
                endpoints.append(self.state.endpoint)
                for peer in self.state.endpoints:
                    endpoints.append(peer)
        # Request info from cluster endpoint (which returns all members of the cluster).
        if response := parallel_patroni_get_request(
            f"/{PATRONI_CLUSTER_STATUS_ENDPOINT}",
            endpoints,
            self.verify,
            self._patroni_async_auth,
            verify,
        ):
            logger.debug("API cluster_status: %s", response["members"])
            return response["members"]
        raise RetryError(
            last_attempt=Future.construct(1, Exception("Unable to reach any units"), True)
        )

    def get_member_ip(self, member_name: str) -> str | None:
        """Get cluster member IP address.

        Args:
            member_name: cluster member name.

        Returns:
            IP address of the cluster member.
        """
        try:
            cluster_status = self.cluster_status()

            for member in cluster_status:
                if member["name"] == member_name:
                    return member["host"]
        except RetryError:
            logger.debug("Unable to get IP. Cluster status unreachable")

    def get_member_status(self, member_name: str) -> str:
        """Get cluster member status.

        Args:
            member_name: cluster member name.

        Returns:
            status of the cluster member or an empty string if the status
                couldn't be retrieved yet.
        """
        # Request info from cluster endpoint (which returns all members of the cluster).
        cluster_status = self.cluster_status()
        if cluster_status:
            for member in cluster_status:
                if member["name"] == member_name:
                    return member["state"]
        return ""

    def get_primary(
        self, unit_name_pattern=False, alternative_endpoints: list[str] | None = None
    ) -> str | None:
        """Get primary instance.

        Args:
            unit_name_pattern: whether to convert pod name to unit name
            alternative_endpoints: list of alternative endpoints to check for the primary.

        Returns:
            primary pod or unit name.
        """
        # Request info from cluster endpoint (which returns all members of the cluster).
        try:
            cluster_status = self.cluster_status(alternative_endpoints)
            for member in cluster_status:
                if member["role"] == "leader":
                    primary = member["name"]
                    if unit_name_pattern:
                        # Change the last dash to / in order to match unit name pattern.
                        primary = label2name(primary)
                    return primary
        except RetryError:
            logger.debug("Unable to get primary. Cluster status unreachable")

    def get_standby_leader(
        self, unit_name_pattern=False, check_whether_is_running: bool = False
    ) -> str | None:
        """Get standby leader instance.

        Args:
            unit_name_pattern: whether to convert pod name to unit name
            check_whether_is_running: whether to check if the standby leader is running

        Returns:
            standby leader pod or unit name.
        """
        # Request info from cluster endpoint (which returns all members of the cluster).
        cluster_status = self.cluster_status()
        if cluster_status:
            for member in cluster_status:
                if member["role"] == "standby_leader":
                    if check_whether_is_running and member["state"] not in STARTED_STATES:
                        logger.warning(f"standby leader {member['name']} is not running")
                        continue
                    standby_leader = member["name"]
                    if unit_name_pattern:
                        # Change the last dash to / in order to match unit name pattern.
                        standby_leader = label2name(standby_leader)
                    return standby_leader

    def get_sync_standby_names(self) -> list[str]:
        """Get the list of sync standby unit names."""
        sync_standbys = []
        # Request info from cluster endpoint (which returns all members of the cluster).
        cluster_status = self.cluster_status()
        if cluster_status:
            for member in cluster_status:
                if member["role"] == "sync_standby":
                    sync_standbys.append(label2name(member["name"]))
        return sync_standbys

    def are_all_members_ready(self) -> bool:
        """Check if all members are correctly running Patroni and PostgreSQL.

        Returns:
            True if all members are ready False otherwise. Retries over a period of 10 seconds
            3 times to allow server time to start up.
        """
        if self.state.substrate == Substrates.VM:
            # Request info from cluster endpoint
            # (which returns all members of the cluster and their states).
            try:
                members = self.cluster_status()
            except RetryError:
                return False

            # Check if all members are running and one of them is a leader (primary) or
            # a standby leader, because sometimes there may exist (for some period of time)
            # only replicas after a failed switchover.
            return all(member["state"] in STARTED_STATES for member in members) and any(
                member["role"] in ["leader", "standby_leader"] for member in members
            )
        else:
            # Request info from cluster endpoint
            # (which returns all members of the cluster and their states).
            return len(self.get_running_cluster_members()) == len(self.state.endpoints)

    @property
    def is_creating_backup(self) -> bool:
        """Returns whether a backup is being created."""
        # Request info from cluster endpoint (which returns the list of tags from each
        # cluster member; the "is_creating_backup" tag means that the member is creating
        # a backup).
        try:
            members = self.cached_cluster_status
        except RetryError:
            return False

        return any(
            "tags" in member and member["tags"].get("is_creating_backup") for member in members
        )

    def is_replication_healthy(self) -> bool:
        """Return whether the replication is healthy."""
        if not self.state.endpoint:
            logger.debug("Failed replication check no endpoint set")
            return False
        try:
            for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
                with attempt:
                    primary = self.get_primary() or self.get_standby_leader()
                    if not primary:
                        logger.debug("Failed replication check no primary reported")
                        raise Exception
                    primary_addr = self.get_member_ip(primary)
                    members_addrs = {self.state.endpoint}
                    members_addrs.update(self.state.endpoints)
                    for members_addr in members_addrs:
                        endpoint = (
                            "leader" if members_addr == primary_addr else "replica?lag=100MB"
                        )
                        url = self.state.patroni_url.replace(self.state.endpoint, members_addr)
                        r = requests.get(
                            f"{url}/{endpoint}",
                            verify=self.verify,
                            auth=self._patroni_auth,
                            timeout=API_REQUEST_TIMEOUT,
                        )
                        logger.debug(
                            "API is_replication_healthy: %s (%s)",
                            r,
                            r.elapsed.total_seconds(),
                        )
                        if r.status_code != 200:
                            logger.debug(
                                f"Failed replication check for {members_addr} with code {r.status_code}"
                            )
                            raise Exception
        except RetryError:
            logger.exception("replication is not healthy")
            return False

        logger.debug("replication is healthy")
        return True

    @property
    def member_inactive(self) -> bool:
        """Are Patroni and PostgreSQL in inactive state.

        Returns:
            True if services is not running, starting or restarting. Retries over a period of 60
            seconds times to allow server time to start up.
        """
        try:
            response = self.cached_patroni_health
        except RetryError:
            return True

        return response["state"] not in [
            *RUNNING_STATES,
            "creating replica",
            "starting",
            "restarting",
        ]

    @property
    def is_member_isolated(self) -> bool:
        """Returns whether the unit is isolated from the cluster."""
        try:
            for attempt in Retrying(stop=stop_after_delay(10), wait=wait_fixed(3)):
                with attempt:
                    r = requests.get(
                        f"{self.state.patroni_url}/{PATRONI_CLUSTER_STATUS_ENDPOINT}",
                        verify=self.verify,
                        timeout=API_REQUEST_TIMEOUT,
                        auth=self._patroni_auth,
                    )
                    logger.debug(
                        "API is_member_isolated: %s (%s)",
                        r.json()["members"],
                        r.elapsed.total_seconds(),
                    )
        except RetryError:
            # Return False if it was not possible to get the cluster info. Try again later.
            return False

        return len(r.json()["members"]) == 0

    def is_member_registered_in_cluster(self) -> bool:
        """Check if this member is registered in the Raft DCS cluster.

        In Raft mode, a new member may be running and replicating but not yet
        registered in the DCS if it hasn't been added to the Raft cluster.

        Returns:
            True if this member appears in the /cluster endpoint, False otherwise.
        """
        try:
            cluster_status = self.cluster_status()
        except RetryError:
            logger.debug("Could not get cluster status to check member registration")
            return False

        if not cluster_status:
            return False

        # Check if this member's name appears in the cluster members list
        member_name = self.state.peer.member_name
        return any(member.get("name") == member_name for member in cluster_status)

    def online_cluster_members(self) -> list[ClusterMember]:
        """Return list of online cluster members."""
        try:
            cluster_status = self.cluster_status()
        except RetryError:
            logger.exception("Unable to get the state of the cluster")
            return []
        if not cluster_status:
            return []

        return [member for member in cluster_status if member["state"] in STARTED_STATES]

    def are_replicas_up(self) -> dict[str, bool] | None:
        """Check if cluster members are running or streaming."""
        try:
            members = self.cluster_status()
            return {member["host"]: member["state"] in STARTED_STATES for member in members}
        except Exception:
            logger.exception("Unable to get the state of the cluster")
            return

    def promote_standby_cluster(self) -> None:
        """Promote a standby cluster to be a regular cluster."""
        config_response = requests.get(
            f"{self.state.patroni_url}/config",
            verify=self.verify,
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug(
            "API promote_standby_cluster: %s (%s)",
            config_response,
            config_response.elapsed.total_seconds(),
        )
        if "standby_cluster" not in config_response.json():
            raise StandbyClusterAlreadyPromotedError("standby cluster is already promoted")
        r = requests.patch(
            f"{self.state.patroni_url}/config",
            verify=self.verify,
            json={"standby_cluster": None},
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug("API promote_standby_cluster patch: %s (%s)", r, r.elapsed.total_seconds())
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                if self.get_primary() is None:
                    raise ClusterNotPromotedError("cluster not promoted")

    def set_max_timelines_history(self) -> None:
        """Patch the DCS with max_timelines_history limit."""
        requests.patch(
            f"{self.state.patroni_url}/config",
            verify=self.verify,
            json={"max_timelines_history": 50},
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )

    def patroni_logs(self, num_lines: int | Literal["all"] = 10) -> str:
        """Get Patroni snap service logs. Executes only on current unit.

        Args:
            num_lines: number of log last lines being returned.

        Returns:
            Multi-line logs string.
        """
        try:
            logger.debug("Getting Patroni logs...")
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            # Lib definition of num_lines only allows int
            return selected_snap.logs(services=["patroni"], num_lines=num_lines)  # pyright: ignore
        except snap.SnapError as e:
            error_message = "Failed to get logs from patroni snap service"
            logger.exception(error_message, exc_info=e)
            return ""

    def last_postgresql_logs(self) -> str:
        """Get last log file content of Postgresql service.

        If there is no available log files, empty line will be returned.

        Returns:
            Content of last log file of Postgresql service.
        """
        log_files = glob.glob(f"{self.workload.paths.logs}/*.log")
        if len(log_files) == 0:
            return ""
        log_files.sort(reverse=True)
        try:
            with open(log_files[0]) as last_log_file:
                return last_log_file.read()
        except OSError as e:
            error_message = "Failed to read last postgresql log file"
            logger.exception(error_message, exc_info=e)
            return ""

    def stop_patroni(self) -> bool:
        """Stop Patroni service using systemd.

        Returns:
            Whether the service stopped successfully.
        """
        try:
            logger.debug("Stopping Patroni...")
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            selected_snap.stop(services=["patroni"])
            return not selected_snap.services["patroni"]["active"]
        except snap.SnapError as e:
            error_message = "Failed to stop patroni snap service"
            logger.exception(error_message, exc_info=e)
            return False

    def switchover(
        self, candidate: str | None = None, async_cluster: bool = False, wait: bool = True
    ) -> None:
        """Trigger a switchover."""
        # Try to trigger the switchover.
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                current_primary = (
                    self.get_primary() if not async_cluster else self.get_standby_leader()
                )
                if current_primary == candidate:
                    logger.info("Candidate and leader are the same")
                    return

                body = {"leader": current_primary}
                if candidate:
                    body["candidate"] = candidate
                r = requests.post(
                    f"{self.state.patroni_url}/switchover",
                    json=body,
                    verify=self.verify,
                    auth=self._patroni_auth,
                    timeout=API_REQUEST_TIMEOUT,
                )
                logger.debug("API switchover: %s (%s)", r, r.elapsed.total_seconds())

        # Check whether the switchover was unsuccessful.
        if r.status_code != 200:
            if (
                r.status_code == 412
                and r.text == "candidate name does not match with sync_standby"
            ):
                logger.debug("Unit is not sync standby")
                raise SwitchoverNotSyncError()
            logger.warning(f"Switchover call failed with code {r.status_code} {r.text}")
            raise SwitchoverFailedError(f"received {r.status_code}")

        if not wait:
            return

        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
            with attempt:
                new_primary = self.get_primary()
                if (
                    candidate is not None and new_primary != candidate
                ) or new_primary == current_primary:
                    raise SwitchoverFailedError("primary was not switched correctly")

    @retry(
        retry=retry_if_result(lambda x: not x),
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def primary_changed(self, old_primary: str) -> bool:
        """Checks whether the primary unit has changed."""
        primary = self.get_primary()
        return primary != old_primary

    # def has_raft_quorum(self) -> bool:
    #     """Check if raft cluster has quorum."""
    #     # Get the status of the raft cluster.
    #     syncobj_util = TcpUtility(password=self.state.application.raft_password, timeout=3)

    #     raft_host = "127.0.0.1:2222"
    #     try:
    #         raft_status = syncobj_util.executeCommand(raft_host, ["status"])
    #     except UtilityException:
    #         logger.warning("Has raft quorum: Cannot connect to raft cluster")
    #         return False
    #     if not raft_status:
    #         logger.warning("Has raft quorum: No status reported")
    #         return False
    #     return raft_status["has_quorum"]

    # def remove_raft_data(self) -> None:
    #     """Stops Patroni and removes the raft journals."""
    #     logger.info("Stopping patroni")
    #     self.stop_patroni()

    #     logger.info("Wait for postgresql to stop")
    #     for attempt in Retrying(wait=wait_fixed(5)):
    #         with attempt:
    #             for proc in psutil.process_iter(["name"]):
    #                 if proc.name() == "postgres":
    #                     raise RaftPostgresqlStillUpError()

    #     logger.info("Removing raft data")
    #     try:
    #         path = Path(f"{self.workload.paths.patroni_conf}/raft")
    #         if path.exists() and path.is_dir():
    #             shutil.rmtree(path)
    #     except OSError as e:
    #         raise Exception(
    #             f"Failed to remove previous cluster information with error: {e!s}"
    #         ) from e
    #     logger.info("Raft ready to reinitialise")

    # def reinitialise_raft_data(self, update_config, update_config_kwargs) -> None:
    #     """Reinitialise the raft journals and promoting the unit to leader. Should only be run on sync replicas."""
    #     logger.info("Rerendering patroni config without peers")
    #     # TODO figure out cross manager update config calls
    #     update_config(no_peers=True, **update_config_kwargs)
    #     logger.info("Starting patroni")
    #     self.start_patroni()

    #     logger.info("Waiting for new raft cluster to initialise")
    #     for attempt in Retrying(wait=wait_fixed(5)):
    #         with attempt:
    #             health_status = self.get_patroni_health()
    #             if (
    #                 health_status["role"] not in ["leader", "master"]
    #                 or health_status["state"] != "running"
    #             ):
    #                 raise RaftNotPromotedError()

    #     logger.info("Restarting patroni")
    #     self.restart_patroni()
    #     for attempt in Retrying(wait=wait_fixed(5)):
    #         with attempt:
    #             found_postgres = False
    #             for proc in psutil.process_iter(["name"]):
    #                 if proc.name() == "postgres":
    #                     found_postgres = True
    #                     break
    #             if not found_postgres:
    #                 raise RaftPostgresqlNotUpError()
    #     logger.info("Raft should be unstuck")

    def get_running_cluster_members(self) -> list[str]:
        """List running patroni members."""
        try:
            members = self.cluster_status()
            return [member["name"] for member in members if member["state"] in STARTED_STATES]
        except Exception:
            return []

    # def cleanup_raft_cluster(self, watcher_addr) -> bool:
    #     """Cleanup RAFT members not belonging to the current cluster or not a related watcher."""
    #     # Get Raft cluster status to find all members
    #     try:
    #         if not self.workload.is_patroni_running():
    #             logger.warning("Raft cleanup: Patroni service not running.")
    #             return True
    #         syncobj_util = TcpUtility(password=self.state.application.raft_password, timeout=3)
    #         if raft_status := syncobj_util.executeCommand(f"127.0.0.1:{RAFT_PORT}", ["status"]):
    #             # Find all partner nodes in the Raft cluster
    #             # Keys look like: partner_node_status_server_10.131.50.142:2222
    #             for key in raft_status:
    #                 if key.startswith(RAFT_PARTNER_PREFIX) and raft_status[key] != 2:
    #                     member_addr = key.replace(RAFT_PARTNER_PREFIX, "")
    #                     member_ip = member_addr.split(":")[0]

    #                     # Check if this is a stale watcher (not a PostgreSQL node and not current watcher)
    #                     if member_ip not in self.charm._units_ips and member_addr != watcher_addr:
    #                         logger.info(f"Removing stale Raft member: {member_addr}")
    #                         self.remove_raft_member(member_addr)
    #                         self.state.application.remove_from_members_ips(member_ip)
    #             return True
    #         return False
    #     except Exception as e:
    #         logger.debug(f"Error during Raft cleanup: {e}")
    #         return False

    # def _set_stuck_raft_flag(self) -> None:
    #     self.charm.set_unit_status(BlockedStatus("Raft majority loss, run: promote-to-primary"))
    #     logger.warning("Remove raft member: Stuck raft cluster detected")
    #     data_flags = {"raft_stuck": "True"}
    #     self.state.peer.data.update(data_flags)

    # Leader doesn't always trigger when changing it's own peer data.
    # if self.state.peer.unit.is_leader():
    #     self.charm.on[PEER_RELATION].relation_changed.emit(
    #         unit=self.state.peer.unit,
    #         app=self.state.application.app,
    #         relation=self.state.application.relation,
    #     )

    # def remove_raft_member(self, member_address: str | None) -> None:
    #     """Remove a member from the raft cluster.

    #     The raft cluster is a different cluster from the Patroni cluster.
    #     It is responsible for defining which Patroni member can update
    #     the primary member in the DCS.

    #     Raises:
    #         RaftMemberNotFoundError: if the member to be removed
    #             is not part of the raft cluster.
    #     """
    #     if not member_address:
    #         logger.debug("Remove raft member: No address provided")
    #         return

    #     if self.state.has_raft_keys():
    #         logger.debug("Remove raft member: Raft already in recovery")
    #         return

    #     # Get the status of the raft cluster.
    #     syncobj_util = TcpUtility(password=self.state.application.raft_password, timeout=3)

    #     raft_host = "127.0.0.1:2222"
    #     try:
    #         raft_status = syncobj_util.executeCommand(raft_host, ["status"])
    #     except UtilityException as e:
    #         logger.warning("Remove raft member: Cannot connect to raft cluster")
    #         raise RemoveRaftMemberFailedError() from e
    #     if not raft_status:
    #         logger.warning("Remove raft member: No raft status")
    #         raise RemoveRaftMemberFailedError() from None

    #     # Check whether the member is still part of the raft cluster.
    #     if f"{RAFT_PARTNER_PREFIX}{member_address}" not in raft_status:
    #         return

    #     # If there's no quorum and the leader left raft cluster is stuck
    #     if raft_status["has_quorum"] and not raft_status["leader"]:
    #         logger.warning("Remove raft member: No raft leader")
    #         raise RemoveRaftMemberFailedError() from None
    #     if not raft_status["has_quorum"] and (
    #         not raft_status["leader"] or raft_status["leader"].address == member_address
    #     ):
    #         self._set_stuck_raft_flag()
    #         return

    #     # Remove the member from the raft cluster.
    #     try:
    #         result = syncobj_util.executeCommand(raft_host, ["remove", member_address])
    #     except UtilityException as e:
    #         logger.debug("Remove raft member: Remove call failed")
    #         raise RemoveRaftMemberFailedError() from e

    #     if not result or not result.startswith("SUCCESS"):
    #         logger.debug(f"Remove raft member: Remove call not successful with {result}")
    #         raise RemoveRaftMemberFailedError() from None

    @retry(stop=stop_after_attempt(20), wait=wait_exponential(multiplier=1, min=2, max=10))
    def reload_patroni_configuration(self):
        """Reload Patroni configuration after it was changed."""
        logger.debug("Reloading Patroni configuration...")
        r = requests.post(
            f"{self.state.patroni_url}/reload",
            verify=self.verify,
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug("API reload_patroni_configuration: %s (%s)", r, r.elapsed.total_seconds())

    def is_patroni_running(self) -> bool:
        """Check if the Patroni service is running."""
        try:
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            return selected_snap.services["patroni"]["active"]
        except snap.SnapError as e:
            logger.debug(f"Failed to check Patroni service: {e}")
            return False

    def restart_patroni(self) -> bool:
        """Restart Patroni.

        Returns:
            Whether the service restarted successfully.
        """
        try:
            logger.debug("Restarting Patroni...")
            cache = snap.SnapCache()
            selected_snap = cache["charmed-postgresql"]
            selected_snap.restart(services=["patroni"])
            return selected_snap.services["patroni"]["active"]
        except snap.SnapError as e:
            error_message = "Failed to start patroni snap service"
            logger.exception(error_message, exc_info=e)
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def restart_postgresql(self) -> None:
        """Restart PostgreSQL."""
        logger.debug("Restarting PostgreSQL...")
        r = requests.post(
            f"{self.state.patroni_url}/restart",
            verify=self.verify,
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug("API restart_postgresql: %s (%s)", r, r.elapsed.total_seconds())

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def reinitialize_postgresql(self) -> None:
        """Reinitialize PostgreSQL."""
        logger.debug("Reinitializing PostgreSQL...")
        r = requests.post(
            f"{self.state.patroni_url}/reinitialize",
            verify=self.verify,
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug("API reinitialize_postgresql: %s (%s)", r, r.elapsed.total_seconds())

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def bulk_update_parameters_controller_by_patroni(
        self, parameters: dict[str, Any], base_parameters: dict[str, Any] | None
    ) -> None:
        """Update the value of a parameter controller by Patroni.

        For more information, check https://patroni.readthedocs.io/en/latest/patroni_configuration.html#postgresql-parameters-controlled-by-patroni.
        """
        if not base_parameters:
            base_parameters = {}
        r = requests.patch(
            f"{self.state.patroni_url}/config",
            verify=self.verify,
            json={
                "postgresql": {
                    "remove_data_directory_on_rewind_failure": False,
                    "remove_data_directory_on_diverged_timelines": False,
                    "parameters": parameters,
                },
                **base_parameters,
            },
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug(
            "API bulk_update_parameters_controller_by_patroni: %s (%s)",
            r,
            r.elapsed.total_seconds(),
        )
        r.raise_for_status()

    def ensure_slots_controller_by_patroni(self, slots: dict[str, str]) -> None:
        """Synchronises slots controlled by Patroni with the provided state by removing unneeded slots and creating new ones.

        Args:
            slots: dictionary of slots in the {slot: database} format.
        """
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3), reraise=True):
            with attempt:
                current_config = requests.get(
                    f"{self.state.patroni_url}/config",
                    verify=self.verify,
                    timeout=API_REQUEST_TIMEOUT,
                    auth=self._patroni_auth,
                )
                logger.debug(
                    "API ensure_slots_controller_by_patroni: %s (%s)",
                    current_config,
                    current_config.elapsed.total_seconds(),
                )
                if current_config.status_code != 200:
                    raise Exception(
                        f"Failed to get current Patroni config: {current_config.status_code} {current_config.text}"
                    )
        slots_patch: dict[str, dict[str, str] | None] = dict.fromkeys(
            current_config.json().get("slots") or {}
        )
        for slot, database in slots.items():
            slots_patch[slot] = {
                "database": database,
                "plugin": "pgoutput",
                "type": "logical",
            }
        r = requests.patch(
            f"{self.state.patroni_url}/config",
            verify=self.verify,
            json={"slots": slots_patch},
            auth=self._patroni_auth,
            timeout=API_REQUEST_TIMEOUT,
        )
        logger.debug(
            "API ensure_slots_controller_by_patroni: %s (%s)",
            r,
            r.elapsed.total_seconds(),
        )

    def update_synchronous_node_count(self) -> None:
        """Update synchronous_node_count to the minority of the planned cluster."""
        for attempt in Retrying(stop=stop_after_delay(60), wait=wait_fixed(3)):
            with attempt:
                r = requests.patch(
                    f"{self.state.patroni_url}/config",
                    json=self.state.synchronous_configuration,
                    verify=self.verify,
                    auth=self._patroni_auth,
                    timeout=API_REQUEST_TIMEOUT,
                )
                logger.debug(
                    "API update_synchronous_node_count: %s (%s)", r, r.elapsed.total_seconds()
                )

                # Check whether the update was unsuccessful.
                if r.status_code != 200:
                    raise UpdateSyncNodeCountError(f"received {r.status_code}")

    def get_patroni_restart_condition(self) -> str:
        """Get current restart condition for Patroni systemd service. Executes only on current unit.

        Returns:
            Patroni systemd service restart condition.
        """
        with open(VM_PATRONI_SERVICE_DEFAULT_PATH) as patroni_service_file:
            patroni_service = patroni_service_file.read()
            found_restart = re.findall(r"Restart=(\w+)", patroni_service)
            if len(found_restart) == 1:
                return str(found_restart[0])
        raise RuntimeError("failed to find patroni service restart condition")

    def update_patroni_restart_condition(self, new_condition: str) -> None:
        """Override restart condition for Patroni systemd service by rewriting service file and doing daemon-reload.

        Executes only on current unit.

        Args:
            new_condition: new Patroni systemd service restart condition.
        """
        logger.info(f"setting restart-condition to {new_condition} for patroni service")
        with open(VM_PATRONI_SERVICE_DEFAULT_PATH) as patroni_service_file:
            patroni_service = patroni_service_file.read()
        logger.debug(f"patroni service file: {patroni_service}")
        new_patroni_service = re.sub(r"Restart=\w+", f"Restart={new_condition}", patroni_service)
        logger.debug(f"new patroni service file: {new_patroni_service}")
        with open(VM_PATRONI_SERVICE_DEFAULT_PATH, "w") as patroni_service_file:
            patroni_service_file.write(new_patroni_service)
        subprocess.run(["/bin/systemctl", "daemon-reload"])

    @property
    def primary_endpoint_ready(self) -> bool:
        """Is the primary endpoint redirecting connections to the primary pod.

        Returns:
            Return whether the primary endpoint is redirecting connections to the primary pod.
        """
        if not self.state.primary_endpoint:
            return False

        try:
            for attempt in Retrying(stop=stop_after_delay(10), wait=wait_fixed(1)):
                with attempt:
                    r = requests.get(
                        f"https://{self.state.primary_endpoint}:8008/health",
                        verify=self.verify,
                        auth=self._patroni_auth,
                        timeout=API_REQUEST_TIMEOUT,
                    )
                    if r.json()["state"] not in RUNNING_STATES:
                        raise EndpointNotReadyError
        except RetryError:
            return False

        return True

    def is_replication_hba_ready(self, endpoint: str | None = None) -> bool:
        """Check whether the pg_hba allows replication from this unit.

        Attempts a physical replication connection. If it succeeds, the pg_hba
        has been reloaded with this unit's endpoint.

        Args:
            endpoint: The host to connect to. If None, uses self._primary_endpoint.

        Returns True if the connection is accepted, False otherwise.
        """
        host = endpoint if endpoint is not None else self.state.primary_endpoint
        try:
            conn = psycopg2.connect(
                host=host,
                port=5432,
                user="replication",
                password=self.state.application.replication_password,
                dbname="replication",
                connect_timeout=1,
                connection_factory=psycopg2.extras.PhysicalReplicationConnection,
            )
            conn.close()
            logger.debug("Replication HBA check passed: %s accepts replication connection", host)
            return True
        except psycopg2.OperationalError as e:
            logger.debug("Replication HBA check failed: %s", e)
            return False

    def get_statuses(
        self, scope: AdvancedStatusesScope, recompute: bool = False
    ) -> list[StatusObject]:
        """Compute the manager's statuses."""
        # if self.workload.workload_present and self.state.substrate == Substrates.VM and not self.member_started:
        #    return [PatroniStatuses.WAITING_MEMBER_START.value]
        return [GeneralStatuses.ACTIVE_IDLE.value]
