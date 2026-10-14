# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the K8s cluster-membership reconfiguration (add-members) flow.

The K8s add-members flow defers on NotReadyError/RetryError and adds members
through the K8s variant (endpoint + pod labels), never through the VM variant.
"""

from unittest.mock import Mock, patch

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import NotReadyError
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.managers.cluster_membership import ClusterMembershipManager
from tenacity import RetryError


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != Substrates.K8S:
        pytest.skip("the K8s membership flows are K8s-only")


@pytest.fixture
def membership(harness):
    manager = ClusterMembershipManager(
        state=harness.charm.state,
        workload=harness.charm.workload,
        patroni_manager=Mock(),
        update_config=Mock(),
        set_unit_status=Mock(),
        watcher_handler=Mock(),
        async_replication_manager=Mock(),
        update_relation_endpoints=Mock(),
        raft_manager=Mock(),
        primary_endpoint=lambda: "10.0.0.1",
        k8s_manager=Mock(),
    )
    manager.patroni_manager.are_all_members_ready.return_value = True
    return manager


def _seed_leader_on_initialised_cluster(harness, membership):
    rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, harness.charm.app.name, {"cluster_initialised": "T"})
        harness.set_leader(True)
    membership.patroni_manager.cluster_members = set()
    membership.hosts = Mock(return_value={"host-0"})
    membership.get_hostname_from_member = Mock(side_effect=lambda member: member)


def test_add_members_k8s_uses_the_k8s_member_variant(harness, membership):
    _seed_leader_on_initialised_cluster(harness, membership)
    with (
        patch.object(membership, "add_cluster_member_k8s") as k8s_add,
        patch.object(membership, "add_cluster_member") as vm_add,
    ):
        membership.add_members_k8s(Mock())
    k8s_add.assert_called_once_with("host-0")
    vm_add.assert_not_called()
    membership.patroni_manager.update_synchronous_node_count.assert_called_once()


def test_add_members_k8s_defers_when_a_member_is_not_ready(harness, membership):
    _seed_leader_on_initialised_cluster(harness, membership)
    event = Mock()
    with patch.object(
        membership,
        "add_cluster_member_k8s",
        side_effect=NotReadyError("not all members are ready"),
    ):
        membership.add_members_k8s(event)
    event.defer.assert_called_once()


def test_add_members_k8s_defers_when_patroni_members_are_unavailable(harness, membership):
    _seed_leader_on_initialised_cluster(harness, membership)

    class _RaisingPatroni(Mock):
        @property
        def cluster_members(self):
            raise RetryError(Mock())

    membership.patroni_manager = _RaisingPatroni()
    membership.patroni_manager.are_all_members_ready.return_value = True
    event = Mock()
    membership.add_members_k8s(event)
    event.defer.assert_called_once()
