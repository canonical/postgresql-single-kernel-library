# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Bounded raft-recovery loops (F13): timeouts raise and defer instead of hanging the hook."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.exceptions import RaftRecoveryTimeoutError
from tenacity import RetryError


def _immediate_retry_error():
    """A Retrying stand-in that exhausts immediately."""

    def _fake_retrying(*args, **kwargs):
        class _R:
            def __iter__(self):
                raise RetryError(last_attempt=Mock())
                yield  # pragma: no cover

        return _R()

    return _fake_retrying


@pytest.fixture
def raft(substrate):
    if substrate == Substrates.K8S:
        pytest.skip("VM only test")
    from single_kernel_postgresql.config.literals import RAFT_PARTNER_PREFIX  # noqa: F401
    from single_kernel_postgresql.core.state import CharmState
    from single_kernel_postgresql.managers.raft import RaftManager
    from single_kernel_postgresql.workload.vm import VMWorkload

    workload = VMWorkload(".")
    raft = RaftManager(
        state=CharmState(charm=Mock(), substrate=substrate, s3_requirer=Mock()),
        workload=workload,
        patroni_manager=Mock(),
        watcher_handler=Mock(),
        update_config=Mock(),
        set_unit_status=Mock(),
        peer_relation_changed=Mock(),
        remove_from_members_ips=Mock(),
    )
    return raft


@pytest.fixture
def handler(harness):
    return harness.charm.postgresql_events_handler


def test_remove_raft_data_times_out(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.Retrying", _immediate_retry_error()),
        patch("single_kernel_postgresql.managers.raft.psutil") as psutil,
    ):
        psutil.process_iter.return_value = [Mock(name="postgres")]
        with pytest.raises(RaftRecoveryTimeoutError):
            raft.remove_raft_data()


def test_reinitialise_raft_data_promotion_times_out(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.Retrying", _immediate_retry_error()),
        patch.object(
            raft.patroni_manager,
            "get_patroni_health",
            return_value={"role": "replica", "state": "running"},
        ),
        pytest.raises(RaftRecoveryTimeoutError),
    ):
        raft.reinitialise_raft_data()


def test_reinitialise_raft_data_postgres_up_wait_times_out(raft):
    with (
        patch("single_kernel_postgresql.managers.raft.Retrying", _immediate_retry_error()),
        patch.object(
            raft.patroni_manager,
            "get_patroni_health",
            return_value={"role": "leader", "state": "running"},
        ),
        patch("single_kernel_postgresql.managers.raft.psutil") as psutil,
    ):
        psutil.process_iter.return_value = []
        with pytest.raises(RaftRecoveryTimeoutError):
            raft.reinitialise_raft_data()


def test_reinitialise_raft_data_proceeds_when_conditions_satisfy(raft):
    postgres_proc = Mock()
    postgres_proc.name.return_value = "postgres"
    with (
        patch.object(
            raft.patroni_manager,
            "get_patroni_health",
            return_value={"role": "leader", "state": "running"},
        ),
        patch("single_kernel_postgresql.managers.raft.psutil") as psutil,
    ):
        psutil.process_iter.return_value = [postgres_proc]
        raft.reinitialise_raft_data()


def _unused_stub(harness, handler):
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(handler.membership_manager.state), "has_raft_keys", lambda self: True),
    ):
        yield handler


def test_peer_relation_changed_defers_on_raft_recovery_timeout(harness, handler):
    with (
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_cluster_initialised",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch.object(type(handler.membership_manager.state), "has_raft_keys", lambda self: True),
        patch.object(
            handler.membership_manager,
            "raft_reinitialisation",
            side_effect=RaftRecoveryTimeoutError("timed out"),
        ),
    ):
        assert handler._peer_relation_changed_checks(Mock()) is False
