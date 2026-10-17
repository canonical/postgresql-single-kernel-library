# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the ClusterMembershipManager departed flow (VM)."""

from unittest.mock import Mock, patch

from ops.model import WaitingStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import RemoveRaftMemberFailedError
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


# -- Stale-raft-recovery unwedge (CI 2026-10-17 stereo_mode ghost + wedge) ---------


def _mock_state():
    """A full mock state: the unwedge/reconfigure tests drive raw dicts."""
    return Mock(name="state")


def test_unwedge_stale_recovery_clears_keys_when_quorate():
    """A quorate cluster must not stay wedged: clear the recovery keys."""
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    manager.raft_manager.has_raft_quorum.return_value = True
    manager.state.has_raft_keys.return_value = True
    manager.state.model.unit.is_leader.return_value = True
    manager.state.application_peers = []
    manager.state.peer.data = {"raft_stuck": "True", "ip": "10.0.0.5"}
    # No machine markers (raft_rejoin/raft_selected_candidate/...): a bare
    # stuck flag with quorum is the false-alarm stereo_mode wedge.
    manager.state.application.data = {"members_ips": "x"}

    assert manager.unwedge_stale_recovery() is True
    assert manager.state.peer.data == {"ip": "10.0.0.5"}
    assert manager.state.application.data == {"members_ips": "x"}


def test_unwedge_stale_recovery_keeps_keys_without_quorum():
    """A genuine majority loss keeps the recovery state and the gates shut."""
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    manager.raft_manager.has_raft_quorum.return_value = False
    manager.state.has_raft_keys.return_value = True
    manager.state.peer.data = {"raft_stuck": "True"}

    assert manager.unwedge_stale_recovery() is False
    assert manager.state.peer.data == {"raft_stuck": "True"}


def test_unwedge_stale_recovery_never_touches_a_committed_recovery():
    """A forced promote-to-primary machine must survive a quorate tick.

    The machine's durable markers (raft_stopped on the wiped peers,
    raft_selected_candidate/raft_followers_stopped/raft_rejoin on the app
    data) mean recovery is genuine and in flight; quorum can read True on the
    candidate's fresh 1-member cluster mid-machine.
    """
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    manager.raft_manager.has_raft_quorum.return_value = True
    manager.state.has_raft_keys.return_value = True
    peer = Mock(name="wiped-peer")
    peer.data = {"raft_stopped": "True", "raft_stopped_flag": None}
    manager.state.application_peers = [peer]
    manager.state.application.data = {"raft_selected_candidate": "postgresql-2"}
    manager.state.peer.data = {"raft_stuck": "True"}

    assert manager.unwedge_stale_recovery() is False
    assert manager.state.peer.data == {"raft_stuck": "True"}


def test_unwedge_stale_recovery_noops_without_keys():
    """No recovery keys: nothing to unwedge, no raft probe."""
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    manager.state.has_raft_keys.return_value = False

    assert manager.unwedge_stale_recovery() is False
    manager.raft_manager.has_raft_quorum.assert_not_called()


def test_reconfigure_cluster_drops_residue_member_ips():
    """The leader reconfigure explicitly removes leftover partner addresses.

    A re-IP'd or departed unit can leave an entry in members_ips after the
    departed flow already ran; the stale raft partner would otherwise survive
    as a status-0 ghost with a diluted quorum (stereo_mode CI 2026-10-17,
    postgresql-2's pre-isolation IP).
    """
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    with (
        patch.object(manager, "unwedge_stale_recovery", return_value=False),
        patch.object(manager.raft_manager, "cleanup_raft_cluster", return_value=True),
        patch.object(manager, "add_members"),
        patch.object(manager, "get_ips_to_remove", return_value=["10.0.0.9"]),
        patch.object(manager, "remove_member_ip") as remove_ip,
    ):
        assert manager.reconfigure_cluster(Mock(name="event")) is True
    manager.raft_manager.remove_raft_member.assert_called_once_with("10.0.0.9:2222")
    remove_ip.assert_called_once_with("10.0.0.9")


def test_reconfigure_cluster_defers_when_ghost_removal_fails():
    """A failed ghost removal defers the reconfigure (departed-flow symmetry)."""
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    with (
        patch.object(manager, "unwedge_stale_recovery", return_value=False),
        patch.object(manager.raft_manager, "cleanup_raft_cluster", return_value=True),
        patch.object(manager, "add_members"),
        patch.object(manager, "get_ips_to_remove", return_value=["10.0.0.9"]),
        patch.object(manager, "remove_member_ip") as remove_ip,
    ):
        manager.raft_manager.remove_raft_member.side_effect = RemoveRaftMemberFailedError()
        assert manager.reconfigure_cluster(Mock(name="event")) is False
        remove_ip.assert_not_called()


def test_reconnect_prefers_the_current_raft_leader_as_remote():
    """The reconnect remove/add dance must target the live raft leader.

    The previous fallback selected the first other member from members_ips,
    which can be the stale ghost itself (300s burn then defer). The local
    unit here is a replica whose IP just changed; the leader is another live
    member (the stereo_mode CI 2026-10-17 shape).
    """
    manager = _manager()
    manager.state = _mock_state()
    manager.raft_manager = Mock()
    leader = Mock(name="raft-leader", address="10.0.0.1:2222")
    manager.raft_manager.get_raft_status.return_value = {
        "has_quorum": True,
        "leader": leader,
        "partner_node_status_server_10.0.0.9:2222": 0,
        "partner_node_status_server_10.0.0.2:2222": 2,
    }
    manager.state.has_raft_keys.return_value = False
    manager.state.unit_ip = "10.0.0.5"
    manager.state.application.is_cluster_initialised = True
    manager.state.application.members_ips = {"10.0.0.5", "10.0.0.9", "10.0.0.2"}
    manager.watcher_handler.watcher_raft_address = None
    manager.watcher_handler.is_active = False

    assert manager.raft_reconnect(Mock(name="event")) == COMPLETED
    manager.raft_manager.remove_raft_member.assert_called_once_with(
        "10.0.0.5:2222", remote_address="10.0.0.1:2222", set_raft_flags=False
    )
    manager.raft_manager.add_raft_member.assert_called_once_with(
        "10.0.0.5:2222", remote_address="10.0.0.1:2222"
    )
