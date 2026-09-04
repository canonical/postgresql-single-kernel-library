# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the refresh module's charm-specific classes."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from charm_refresh import CharmVersion, PrecheckFailed
from single_kernel_postgresql.config.exceptions import SwitchoverFailedError
from single_kernel_postgresql.managers.refresh import PostgreSQLRefreshK8s

CHARM_VERSION = "16/1.0.0"


@pytest.fixture
def refresh_versions_toml(tmp_path, monkeypatch):
    """Provide a minimal refresh_versions.toml in the working directory.

    charm_refresh reads the pinned charm and workload versions from the working
    directory when a charm-specific class is instantiated; the repository keeps the
    file only inside the test charms.
    """
    monkeypatch.chdir(tmp_path)
    Path("refresh_versions.toml").write_text(f'charm = "{CHARM_VERSION}"\nworkload = "16.14"\n')
    return tmp_path


@pytest.fixture
def charm():
    """A mock charm with the surfaces the K8s pre-refresh checks touch."""
    return MagicMock(name="charm")


@pytest.fixture
def refresh_k8s(charm, refresh_versions_toml) -> PostgreSQLRefreshK8s:
    """The K8s charm-specific refresh class wired to the mock charm."""
    return PostgreSQLRefreshK8s(
        workload_name="PostgreSQL",
        charm_name="postgresql-k8s",
        oci_resource_name="postgresql-image",
        _charm=charm,
    )


@pytest.mark.parametrize(
    "old_charm,new_charm,expected",
    [
        # Released, same track, upgrade: compatible.
        ("16/1.0.0", "16/1.1.0", True),
        # Charm code downgrade: incompatible.
        ("16/1.1.0", "16/1.0.0", False),
        # Track change: incompatible.
        ("16/1.0.0", "14/1.1.0", False),
        # Unreleased charm versions: incompatible.
        ("16/1.0.0.dev0+abc", "16/1.1.0", False),
        ("16/1.0.0", "16/1.1.0.post1.dev0+abc", False),
    ],
)
def test_is_compatible_charm_version(old_charm, new_charm, expected, refresh_k8s):
    assert (
        PostgreSQLRefreshK8s.is_compatible(
            old_charm_version=CharmVersion(old_charm),
            new_charm_version=CharmVersion(new_charm),
            old_workload_version="16.9",
            new_workload_version="16.14",
        )
        is expected
    )


@pytest.mark.parametrize(
    "old_workload,new_workload,expected",
    [
        # Same major, minor upgrade: compatible.
        ("16.9", "16.14", True),
        # Same major, same version: compatible.
        ("16.14", "16.14", True),
        # Same major, minor downgrade: incompatible.
        ("16.14", "16.9", False),
        # Major upgrade: incompatible (dump/restore required instead).
        ("16.14", "17.0", False),
    ],
)
def test_is_compatible_workload_version(old_workload, new_workload, expected, refresh_k8s):
    assert (
        PostgreSQLRefreshK8s.is_compatible(
            old_charm_version=CharmVersion("16/1.0.0"),
            new_charm_version=CharmVersion("16/1.1.0"),
            old_workload_version=old_workload,
            new_workload_version=new_workload,
        )
        is expected
    )


def test_pre_refresh_check_after_1_unit_refreshed_backup_in_progress(refresh_k8s, charm):
    charm.patroni_manager.is_creating_backup = True
    with pytest.raises(PrecheckFailed, match=r"Backup in progress"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_after_1_unit_refreshed_member_not_running(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-2",
    ]
    with pytest.raises(PrecheckFailed, match="PostgreSQL is not running on unit 1"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_after_1_unit_refreshed_switches_primary(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/2"
    charm.get_async_primary_cluster_endpoint.return_value = None

    refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()

    charm.patroni_manager.switchover.assert_called_once_with(
        candidate="postgresql-k8s/0", async_cluster=False
    )


def test_pre_refresh_check_after_1_unit_refreshed_already_primary(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/0"

    refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()

    charm.patroni_manager.switchover.assert_not_called()


def test_pre_refresh_check_after_1_unit_refreshed_switchover_failed(refresh_k8s, charm):
    charm.app.planned_units.return_value = 3
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = [
        "postgresql-k8s-0",
        "postgresql-k8s-1",
        "postgresql-k8s-2",
    ]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/2"
    charm.patroni_manager.switchover.side_effect = SwitchoverFailedError

    with pytest.raises(PrecheckFailed, match="Unable to switch primary"):
        refresh_k8s.run_pre_refresh_checks_after_1_unit_refreshed()


def test_pre_refresh_check_before_any_units_refreshed_members_not_ready(refresh_k8s, charm):
    charm.patroni_manager.are_all_members_ready.return_value = False
    with pytest.raises(PrecheckFailed, match=r"PostgreSQL is not running on 1\+ units"):
        refresh_k8s.run_pre_refresh_checks_before_any_units_refreshed()


def test_pre_refresh_check_before_any_units_refreshed_delegates(refresh_k8s, charm):
    charm.patroni_manager.are_all_members_ready.return_value = True
    charm.app.planned_units.return_value = 1
    charm.app.name = "postgresql-k8s"
    charm.patroni_manager.is_creating_backup = False
    charm.patroni_manager.get_running_cluster_members.return_value = ["postgresql-k8s-0"]
    charm.patroni_manager.get_primary.return_value = "postgresql-k8s/0"

    with patch.object(
        PostgreSQLRefreshK8s, "run_pre_refresh_checks_after_1_unit_refreshed"
    ) as delegate:
        refresh_k8s.run_pre_refresh_checks_before_any_units_refreshed()

    delegate.assert_called_once()
