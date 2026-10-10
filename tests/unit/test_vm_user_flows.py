# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the VM user flows (setup_instance_users, rotate_system_user_passwords).

Ports of the VM charm's tests/unit/test_charm.py _setup_users cases plus new
pins for the password-rotation port (the charm shipped that flow untested).
"""

from unittest.mock import Mock, patch

import psycopg2
import pytest
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.cluster import ClusterManager
from single_kernel_postgresql.utils.postgresql import PostgreSQLUpdateUserPasswordError


@pytest.fixture
def cluster(harness):
    """A ClusterManager wired to the harness state with mocked collaborators."""
    return ClusterManager(
        state=harness.charm.state,
        workload=harness.charm.workload,
        patroni_manager=Mock(),
        async_replication_manager=Mock(),
        update_config=Mock(),
        set_unit_status=Mock(),
    )


def test_setup_instance_users_skips_on_standby(cluster):
    """A standby cluster is read-only; setup must not issue write DDL (DPE-10284)."""
    client = Mock()
    client.create_predefined_instance_roles.side_effect = psycopg2.errors.ReadOnlySqlTransaction
    cluster.is_standby_cluster = lambda: True

    cluster.setup_instance_users(client)

    client.create_predefined_instance_roles.assert_not_called()
    client.create_user.assert_not_called()
    client.set_up_database.assert_not_called()


def test_setup_instance_users_provisions_on_primary(cluster):
    """On a primary cluster the predefined roles and system users are provisioned."""
    client = Mock()
    client.list_users.return_value = []
    client.list_access_groups.return_value = set()
    cluster.is_standby_cluster = lambda: False

    with patch.object(CharmState, "get_secret", return_value="monitoring-password"):
        cluster.setup_instance_users(client)

    client.create_predefined_instance_roles.assert_called_once()
    assert client.create_user.call_count == 2  # backup user + monitoring user
    client.grant_database_privileges_to_user.assert_called_once_with(
        "backup", "postgres", ["connect"]
    )
    client.set_up_database.assert_called_once()
    client.create_access_groups.assert_called_once()
    client.grant_internal_access_group_memberships.assert_called_once()


def test_rotate_system_user_passwords_raises_when_members_not_ready(cluster):
    """Not all members ready -> the rotation raises instead of half-applying."""
    client = Mock()
    cluster.patroni_manager.are_all_members_ready.return_value = False

    with pytest.raises(PostgreSQLUpdateUserPasswordError):
        cluster.rotate_system_user_passwords(client, "secret-id")


def test_rotate_system_user_passwords_updates_changed_users(cluster):
    """Only changed system users are updated and re-stored; unchanged ones skipped."""
    client = Mock()
    cluster.patroni_manager.are_all_members_ready.return_value = True
    cluster.async_replication_manager.is_primary_cluster.return_value = True

    with (
        patch.object(
            CharmState,
            "get_secret_from_id",
            return_value={"operator": "new", "backup": "changed", "attacker": "nope"},
        ),
        patch.object(
            CharmState,
            "get_secret",
            side_effect=lambda scope, key: "new" if key == "operator-password" else "old",
        ),
        patch.object(CharmState, "set_secret") as _set_secret,
        patch.object(CharmState, "model", new_callable=Mock) as _model,
    ):
        _model.get_relation.return_value = None
        cluster.rotate_system_user_passwords(client, "secret-id")

    client.update_user_password.assert_called_once_with("backup", "changed", database_host=None)
    _set_secret.assert_called_once()
    cluster.update_config.assert_called_once()
