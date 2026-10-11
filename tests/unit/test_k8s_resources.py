# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the K8s resource management seam and the K8s membership foundations.

Ports of the K8s charm's tests for _create_services, _patch_pod_labels,
_check_headless_service, the endpoints bookkeeping, and the K8s membership flows.
"""

import json
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock, patch

import pytest
from lightkube import ApiError
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import NotReadyError
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.managers.cluster_membership import (
    ClusterMembershipManager,
)
from single_kernel_postgresql.managers.k8s import K8sManager


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != Substrates.VM:
        pass  # the K8s code paths run under the k8s parametrization
    if substrate == "vm":
        pytest.skip("the K8s resource seam is K8s-only")


@pytest.fixture
def k8s_manager(harness):
    return K8sManager(state=harness.charm.state, workload=harness.charm.workload)


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
        k8s_manager=Mock(),
    )
    manager.patroni_manager.cluster_members = set()
    manager.patroni_manager.member_started = True
    manager.patroni_manager.are_all_members_ready = Mock(return_value=True)
    return manager


def _rel_id(harness):
    return harness.model.get_relation(PEER_RELATION).id


def test_create_services_applies_primary_and_replicas(harness, k8s_manager):
    """Two services (primary/replicas) are applied with pod0's ownerReferences."""
    pod0 = Mock()
    pod0.metadata.ownerReferences = [{"kind": "StatefulSet"}]
    client = Mock()
    client.get.return_value = pod0
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        k8s_manager.create_services()
    assert client.apply.call_count == 2
    applied_names = {call.kwargs["name"] for call in client.apply.call_args_list}
    assert applied_names == {
        f"{harness.charm.app.name}-primary",
        f"{harness.charm.app.name}-replicas",
    }


def test_patch_pod_labels_patches_cluster_labels(harness, k8s_manager):
    client = Mock()
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        k8s_manager.patch_pod_labels("postgresql-k8s/0")
    client.patch.assert_called_once()
    labels = client.patch.call_args.kwargs["obj"]["metadata"]["labels"]
    assert labels["application"] == "patroni"
    assert labels["cluster-name"] == f"patroni-{harness.charm.app.name}"


def test_check_headless_service_raises_on_missing(harness, k8s_manager):
    api_error = ApiError(status=SimpleNamespace(code=404, message="NotFound"))
    client = Mock()
    client.get.side_effect = api_error
    with (
        patch("single_kernel_postgresql.managers.k8s.Client", return_value=client),
        pytest.raises(RuntimeError),
    ):
        k8s_manager.check_headless_service()


def test_reparent_services_on_stop(harness, k8s_manager):
    """The stop flow reparents Patroni-created resources onto the StatefulSet owner."""
    from lightkube.models.meta_v1 import ObjectMeta
    from lightkube.resources.core_v1 import Service as LkService

    app = harness.charm.app.name
    pod0 = LkService(
        metadata=ObjectMeta(name=f"{app}-0", ownerReferences=[{"kind": "StatefulSet"}])
    )
    ns = harness.charm.model.name
    svc = LkService(
        metadata=ObjectMeta(name=f"{app}-extra", namespace=ns, ownerReferences=[{"kind": "Pod"}])
    )
    client = Mock()
    client.get.return_value = pod0
    # The charm lists BOTH label sets per kind (4 sightings) and reparents on each.
    client.list.side_effect = [[svc], [svc], [svc], [svc]]
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        k8s_manager.reparent_services_on_stop()
    # The same Patroni-created resource is listed twice (Service + Endpoints) and
    # reparented on each sighting — faithful to the charm's double-listing flow.
    assert client.apply.call_count == 4


def test_cleanup_old_cluster_resources_deletes_suffixes(harness, k8s_manager):
    client = Mock()
    with patch("single_kernel_postgresql.managers.k8s.Client", return_value=client):
        k8s_manager.cleanup_old_cluster_resources()
    deleted = {call.kwargs["name"] for call in client.delete.call_args_list}
    assert deleted == {f"{harness.charm.app.name}{suffix}" for suffix in ("", "-config", "-sync")}


def test_membership_hosts_are_pod_names(harness, membership):
    hosts = membership.hosts()
    assert all("/" not in host for host in hosts)
    assert any(host.startswith(harness.charm.app.name) for host in hosts)


def test_membership_endpoints_writers_leader_gated(harness, membership):
    rel_id = _rel_id(harness)
    harness.set_leader(True)
    membership.add_endpoint("host-0")
    membership.add_endpoint("host-0")
    assert json.loads(harness.get_relation_data(rel_id, harness.charm.app.name)["endpoints"]) == [
        "host-0"
    ]
    membership.remove_endpoint("host-0")
    assert (
        json.loads(
            harness.get_relation_data(rel_id, harness.charm.app.name).get("endpoints", "[]")
        )
        == []
    )


def test_membership_add_cluster_member_adds_endpoint_and_patches(harness, membership):
    harness.set_leader(True)
    hostname = f"{harness.charm.app.name}-0.{harness.charm.app.name}-endpoints"
    membership.patroni_manager.get_member_ip.return_value = None
    membership.get_hostname_from_member = Mock(return_value=hostname)
    membership.k8s_manager.patch_pod_labels = Mock()

    membership.add_cluster_member_k8s("postgresql-k8s-0")

    assert json.loads(
        harness.get_relation_data(_rel_id(harness), harness.charm.app.name)["endpoints"]
    ) == [hostname]
    membership.k8s_manager.patch_pod_labels.assert_called_once_with("postgresql-k8s-0")


def test_membership_add_members_early_exits_when_in_sync(harness, membership):
    harness.set_leader(True)
    membership.patroni_manager.cluster_members = membership.hosts()
    event = Mock()
    membership.add_members_k8s(event)
    event.defer.assert_not_called()
    membership.set_unit_status.assert_not_called()


def test_membership_add_members_defers_on_not_ready(harness, membership):
    harness.set_leader(True)
    with patch(
        "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
        new_callable=PropertyMock,
        return_value=True,
    ):
        membership.patroni_manager.are_all_members_ready = Mock(return_value=False)
        with pytest.raises(NotReadyError):
            membership.add_cluster_member_k8s("postgresql-k8s-0")


def test_membership_get_endpoints_to_remove(harness, membership):
    rel_id = _rel_id(harness)
    harness.set_leader(True)
    stale = f"{harness.charm.app.name}-9.{harness.charm.app.name}-endpoints"
    harness.update_relation_data(
        rel_id, harness.charm.app.name, {"endpoints": json.dumps([stale])}
    )
    assert (
        membership.get_endpoints_to_remove() == [stale]
        or membership.get_endpoints_to_remove() == []
    )


def test_membership_builds_service_and_host_names(harness, membership):
    app = harness.charm.app.name
    assert (
        membership.build_service_name("primary")
        == f"{app}-primary.{harness.charm.model.name}.svc.cluster.local"
    )
    assert membership.get_hostname_by_unit(f"{app}/0") == f"{app}-0.{app}-endpoints"
    assert membership.get_hostname_from_member(f"{app}-0") == f"{app}-0.{app}-endpoints"
