# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the VM workload storage layout seam.

Port of the VM charm's tests/unit/test_charm.py test_ensure_storage_layout
and test_ensure_storage_layout_recreates_temp_dir_on_reboot.
"""

from unittest.mock import MagicMock, PropertyMock, patch

from single_kernel_postgresql.workload.vm import VMWorkload


def test_ensure_storage_layout(tmp_path):
    """The versioned temp dir is created and the versioned parents chowned."""
    workload = VMWorkload(".")
    temp_root = tmp_path / "temp" / "16" / "main"
    data_root = tmp_path / "data" / "16" / "main"
    # The snap's migrate-data.sh creates the data parent (as root); the
    # workload only fixes its ownership, it does not create the data dir.
    data_root.parent.mkdir(parents=True)
    paths = MagicMock()
    paths.temp = temp_root
    paths.data = data_root
    with (
        patch("single_kernel_postgresql.workload.vm.shutil") as mock_shutil,
        patch.object(VMWorkload, "paths", new_callable=PropertyMock, return_value=paths),
    ):
        workload.ensure_storage_layout()

    assert temp_root.is_dir()
    # The workload does not create the data dir leaf — only the snap does.
    assert not data_root.exists()
    chowned = {str(call.args[0]) for call in mock_shutil.chown.call_args_list}
    assert str(temp_root) in chowned
    assert str(temp_root.parent) in chowned
    assert str(data_root.parent) in chowned
    # Only the temp and data trees are touched.
    assert not (tmp_path / "archive").exists()
    assert not (tmp_path / "logs").exists()


def test_ensure_storage_layout_recreates_temp_dir_on_reboot(tmp_path):
    """The versioned temp dir is recreated after a tmpfs wipe on reboot."""
    workload = VMWorkload(".")
    temp_root = tmp_path / "temp" / "16" / "main"
    paths = MagicMock()
    paths.temp = temp_root
    paths.data = tmp_path / "data" / "16" / "main"
    with (
        patch("single_kernel_postgresql.workload.vm.shutil"),
        patch.object(VMWorkload, "paths", new_callable=PropertyMock, return_value=paths),
    ):
        workload.ensure_storage_layout()

    assert temp_root.is_dir()
