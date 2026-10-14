# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the ClusterMembershipManager departed flow (VM)."""

from unittest.mock import Mock, patch

from ops.model import WaitingStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import PRIMARY_NOT_REACHABLE_MESSAGE
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.cluster_membership import (
    COMPLETED,
    WAITING,
    ClusterMembershipManager,
)
from single_kernel_postgresql.workload.vm import VMWorkload


def _manager():
    state = CharmState(charm=Mock(), substrate=Substrates.VM, s3_requirer=Mock())
    manager = ClusterMembershipManager(
        state=state,
        workload=VMWorkload("."),
        patroni_manager=Mock(),
        update_config=Mock(),
        set_unit_status=Mock(),
        watcher_handler=Mock(),
        async_replication_manager=Mock(),
        update_relation_endpoints=Mock(),
        raft_manager=Mock(),
        primary_endpoint=lambda: None,
    )
    return manager


def test_remove_departed_members_waits_without_a_patroni_primary():
    """The Patroni-resolved endpoint (None) gates the waiting branch, not the k8s service name."""
    manager = _manager()
    manager.state.model.unit.is_leader.return_value = True
    with (
        patch.object(
            type(manager.state.application),
            "is_cluster_initialised",
            new_callable=lambda: property(lambda _: True),
        ),
        patch.object(manager, "updated_synchronous_node_count", return_value=1),
        patch.object(manager, "get_ips_to_remove", return_value=["10.0.0.2"]),
        patch.object(manager, "remove_member_ip"),
        # The state's k8s-service primary endpoint is never falsy on VM; it
        # must not satisfy the gate on its own.
        patch.object(
            type(manager.state),
            "primary_endpoint",
            new_callable=lambda: property(
                lambda _: "postgresql-primary.test-model.svc.cluster.local"
            ),
        ),
    ):
        manager.patroni_manager.are_all_members_ready.return_value = True
        result = manager.remove_departed_members()
    assert result == WAITING
    manager.set_unit_status.assert_called_once_with(WaitingStatus(PRIMARY_NOT_REACHABLE_MESSAGE))
    manager.update_relation_endpoints.assert_not_called()
    assert COMPLETED  # guard against accidental constant edits


def test_remove_departed_members_updates_endpoints_with_a_primary():
    manager = _manager()
    manager.primary_endpoint = lambda: "10.0.0.1"
    manager.state.model.unit.is_leader.return_value = True
    with (
        patch.object(
            type(manager.state.application),
            "is_cluster_initialised",
            new_callable=lambda: property(lambda _: True),
        ),
        patch.object(manager, "updated_synchronous_node_count", return_value=1),
        patch.object(manager, "get_ips_to_remove", return_value=["10.0.0.2"]),
        patch.object(manager, "remove_member_ip"),
    ):
        manager.patroni_manager.are_all_members_ready.return_value = True
        result = manager.remove_departed_members()
    assert result == COMPLETED
    manager.update_relation_endpoints.assert_called_once()
    manager.set_unit_status.assert_not_called()
