#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the data-safety seam (constants, manifest, workload primitives, manager)."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from single_kernel_postgresql.config.exceptions import (
    DataSafetyError,
    DataSafetyPostflightError,
    DataSafetyPreflightError,
)
from single_kernel_postgresql.config.literals import (
    BACKUP_SAFETY_DIR_NAME,
    BACKUP_SAFETY_MANIFEST_NAME,
    SAFETY_OPERATIONS,
)
from single_kernel_postgresql.managers.data_safety import DataSafetyManager
from single_kernel_postgresql.workload.base import (
    SafetyCopyInfo,
    build_safety_manifest,
    read_safety_manifest,
    safety_copy_id,
    write_safety_manifest,
)
from single_kernel_postgresql.workload.vm import VMWorkload


def test_safety_copy_id_format():
    now = datetime(2026, 10, 22, 10, 15, 0)
    assert safety_copy_id("restore", now) == "restore-20261022T1015Z"


def test_safety_operations_are_pinned():
    assert SAFETY_OPERATIONS == ("restore", "async-conversion")


def test_manifest_round_trip(tmp_path):
    copy_dir = tmp_path / "restore-20261022T1015Z"
    copy_dir.mkdir(parents=True)
    manifest = build_safety_manifest(
        schema_version=1,
        operation="restore",
        created="2026-10-22T10:15:00Z",
        cluster_name="prod-db",
        system_identifier="7389",
        model="mymodel",
        unit="postgresql/0",
        versions={"charm": "", "lib": "16.3.21", "postgresql": "16.9"},
    )
    write_safety_manifest(copy_dir, manifest)
    assert read_safety_manifest(copy_dir) == manifest
    assert (
        json.loads(
            (tmp_path / "restore-20261022T1015Z" / BACKUP_SAFETY_MANIFEST_NAME).read_text()
        )["schema_version"]
        == 1
    )


def test_read_safety_manifest_returns_none_when_absent(tmp_path):
    copy_dir = tmp_path / "restore-junk"
    copy_dir.mkdir(parents=True)
    assert read_safety_manifest(copy_dir) is None


def test_read_safety_manifest_returns_none_when_corrupt(tmp_path):
    copy_dir = tmp_path / "restore-junk"
    copy_dir.mkdir(parents=True)
    (tmp_path / "restore-junk" / BACKUP_SAFETY_MANIFEST_NAME).write_text("{not json")
    assert read_safety_manifest(copy_dir) is None


def test_safety_copy_info_total_size_bytes():
    info = SafetyCopyInfo(
        copy_id="restore-20261022T1015Z",
        operation="restore",
        created="2026-10-22T10:15:00Z",
        path="/tmp/x",
        manifest={"contents": [{"size_bytes": 10}, {"size_bytes": 32}]},
    )
    assert info.total_size_bytes == 42


def test_exceptions_hierarchy():
    assert issubclass(DataSafetyPreflightError, DataSafetyError)
    assert issubclass(DataSafetyPostflightError, DataSafetyError)


def test_backupsafety_dir_name_is_not_a_wipe_target():
    """The safety root must not collide with any wiped directory (spec §4)."""
    from single_kernel_postgresql.config.literals import (
        VM_ARCHIVE_PATH,
        VM_DATA_PATH,
        VM_TEMP_PATH,
    )

    assert BACKUP_SAFETY_DIR_NAME not in (VM_DATA_PATH, VM_ARCHIVE_PATH, VM_TEMP_PATH)


# -- VM workload primitives ---------------------------------------------------------


@pytest.fixture
def vm_workload(tmp_path):
    """A VM workload whose paths live on a throwaway tree.

    The paths property is patched with the test_vm_storage_layout pattern: the
    snap paths are absolute (base_snap_dir joins /var/snap/...), so patching
    `root` cannot redirect them.
    """
    workload = VMWorkload(".")
    paths = MagicMock()
    paths.data = tmp_path / "data" / "16" / "main"
    paths.wal = tmp_path / "wal" / "16" / "main"
    paths.archive = tmp_path / "archive" / "16" / "main"
    paths.snap_common = tmp_path / "snap-common"
    paths_patch = patch.object(VMWorkload, "paths", new_callable=PropertyMock, return_value=paths)
    paths_patch.start()
    yield workload, paths
    paths_patch.stop()


def _make_tree(paths):
    """Populate data/wal/archive trees with marker files (and the snap-common root)."""
    for attr in ("data", "wal", "archive"):
        target = Path(str(getattr(paths, attr)))
        target.mkdir(parents=True, exist_ok=True)
        (target / "PG_VERSION").write_text("16")
        (target / "marker.bin").write_bytes(b"x" * 1024)
    Path(str(paths.snap_common)).mkdir(parents=True, exist_ok=True)


def _manifest_stub():
    return {
        "schema_version": 1,
        "operation": "restore",
        "created": "2026-10-22T10:15:00Z",
        "cluster_name": "prod-db",
        "system_identifier": "7389",
        "model": "mymodel",
        "unit": "postgresql/0",
        "versions": {"charm": "", "lib": "16.3.21", "postgresql": "16.9"},
    }


def test_preserve_creates_verified_tars_and_manifest(vm_workload):
    workload, paths = vm_workload
    _make_tree(paths)
    info = workload.preserve_data_directories(
        "restore",
        [
            ("pgdata", workload.paths.data),
            ("wal", workload.paths.wal),
            ("archive", workload.paths.archive),
        ],
        _manifest_stub(),
    )
    copy_dir = Path(str(workload.paths.snap_common)) / BACKUP_SAFETY_DIR_NAME / info.copy_id
    assert copy_dir.is_dir()
    assert (copy_dir / "manifest.json").is_file()
    for name in ("pgdata.tar.zst", "wal.tar.zst", "archive.tar.zst"):
        assert (copy_dir / name).stat().st_size > 0
    entry = next(e for e in info.manifest["contents"] if e["name"] == "pgdata.tar.zst")
    assert entry["source_path"] == str(workload.paths.data)
    assert len(entry["sha256"]) == 64
    assert info.total_size_bytes > 0


def test_preserve_removes_partial_dir_on_verify_failure(vm_workload, monkeypatch):
    workload, paths = vm_workload
    _make_tree(paths)
    monkeypatch.setattr(
        VMWorkload, "_verify_tarball", lambda self, tarball: (_ for _ in ()).throw(OSError("boom"))
    )
    with pytest.raises(DataSafetyError):
        workload.preserve_data_directories(
            "restore", [("pgdata", workload.paths.data)], _manifest_stub()
        )
    safety_root = Path(str(workload.paths.snap_common)) / BACKUP_SAFETY_DIR_NAME
    assert not safety_root.exists() or not list(safety_root.iterdir())


def test_preserve_wraps_subprocess_failure_as_data_safety_error(vm_workload, monkeypatch):
    workload, paths = vm_workload
    _make_tree(paths)

    def _explode(*args, **kwargs):
        raise FileNotFoundError("tar not found")

    monkeypatch.setattr("single_kernel_postgresql.workload.vm.subprocess.check_call", _explode)
    with pytest.raises(DataSafetyError):
        workload.preserve_data_directories(
            "restore", [("pgdata", workload.paths.data)], _manifest_stub()
        )


def test_list_returns_only_complete_copies(vm_workload):
    workload, paths = vm_workload
    _make_tree(paths)
    info = workload.preserve_data_directories(
        "restore", [("pgdata", workload.paths.data)], _manifest_stub()
    )
    junk = Path(str(workload.paths.snap_common)) / BACKUP_SAFETY_DIR_NAME / "restore-junk"
    junk.mkdir(parents=True)
    listed = {c.copy_id for c in workload.list_data_safety_copies()}
    assert listed == {info.copy_id}


def test_evict_removes_junk_first_then_oldest_complete(vm_workload):
    workload, paths = vm_workload
    _make_tree(paths)
    info_a = workload.preserve_data_directories(
        "restore", [("pgdata", workload.paths.data)], _manifest_stub()
    )
    info_b = workload.preserve_data_directories(
        "restore", [("pgdata", workload.paths.data)], _manifest_stub()
    )
    junk = Path(str(workload.paths.snap_common)) / BACKUP_SAFETY_DIR_NAME / "restore-junk"
    junk.mkdir(parents=True)
    evicted = workload.evict_data_safety_copies(0)
    assert "restore-junk" in evicted
    assert {c.copy_id for c in workload.list_data_safety_copies()} == {
        info_a.copy_id,
        info_b.copy_id,
    }
    # Now evict oldest complete copies until enough bytes are freed.
    evicted = workload.evict_data_safety_copies(info_a.total_size_bytes + 1)
    assert info_a.copy_id in evicted
    remaining = {c.copy_id for c in workload.list_data_safety_copies()}
    assert info_a.copy_id not in remaining


def test_read_data_safety_copy_round_trip(vm_workload):
    workload, paths = vm_workload
    _make_tree(paths)
    info = workload.preserve_data_directories(
        "restore", [("pgdata", workload.paths.data)], _manifest_stub()
    )
    again = workload.read_data_safety_copy(info.copy_id)
    assert again.manifest == info.manifest


def test_read_data_safety_copy_rejects_traversal(vm_workload):
    workload, _paths = vm_workload
    with pytest.raises(DataSafetyError):
        workload.read_data_safety_copy("../evil")


def test_dir_size_and_free_space(vm_workload):
    workload, paths = vm_workload
    _make_tree(paths)
    assert workload.dir_size(workload.paths.data) >= 1024 + len("16")
    assert workload.free_space(workload.paths.snap_common) > 0


def test_k8s_stubs_raise():
    """The k8s workload raises NotImplementedError for the seam until the follow-up PR."""
    from single_kernel_postgresql.workload.k8s import K8sWorkload

    workload = K8sWorkload.__new__(K8sWorkload)  # skip __init__ (needs a Container)
    method_args = {
        "preserve_data_directories": ("restore", [], {}),
        "list_data_safety_copies": (),
        "evict_data_safety_copies": (0,),
        "read_data_safety_copy": ("some-id",),
        "data_safety_root": (),
        "dir_size": (Path("/tmp/whatever"),),
        "free_space": (Path("/tmp/whatever"),),
    }
    for method, args in method_args.items():
        with pytest.raises(NotImplementedError):
            getattr(workload, method)(*args)


# -- DataSafetyManager ----------------------------------------------------------------


@pytest.fixture
def data_safety_manager(vm_workload, monkeypatch):
    """A DataSafetyManager over a redirected VM workload with canned manifest inputs."""
    from single_kernel_postgresql.managers.data_safety import DataSafetyManager

    workload, paths = vm_workload
    _make_tree(paths)
    # The manifest builder reads these from the workload; patch to canned values
    # so the tests do not depend on a live snap.
    monkeypatch.setattr(type(workload), "get_system_identifier", lambda self: ("7389", None))
    monkeypatch.setattr(type(workload), "get_workload_version", lambda self: "16.9")
    state = MagicMock()
    state.cluster_name = "prod-db"
    state.model.name = "mymodel"
    state.model.unit.name = "postgresql/0"
    return DataSafetyManager(state, workload)


def test_preserve_success_writes_copy_and_returns_info(data_safety_manager):
    info = data_safety_manager.preserve(
        "restore",
        [
            ("pgdata", data_safety_manager.workload.paths.data),
            ("wal", data_safety_manager.workload.paths.wal),
        ],
    )
    assert info.operation == "restore"
    assert info.manifest["cluster_name"] == "prod-db"
    assert info.manifest["unit"] == "postgresql/0"
    assert info.manifest["contents"]


def test_preflight_refuses_when_space_is_short_even_with_no_copies(
    data_safety_manager, monkeypatch
):
    monkeypatch.setattr(type(data_safety_manager.workload), "free_space", lambda self, path: 1)
    with pytest.raises(DataSafetyPreflightError) as exc:
        data_safety_manager.preflight(
            "restore", [("pgdata", data_safety_manager.workload.paths.data)]
        )
    assert "Not enough free space" in str(exc.value)
    assert "safety=none" in str(exc.value)


def test_preflight_gcs_incomplete_copies_first(data_safety_manager):
    junk = (
        Path(str(data_safety_manager.workload.paths.snap_common))
        / BACKUP_SAFETY_DIR_NAME
        / "restore-junk"
    )
    junk.mkdir(parents=True)
    (junk / "pgdata.tar.zst").write_bytes(b"x" * 1024 * 5)  # big junk dir
    data_safety_manager.preflight("restore", [("pgdata", data_safety_manager.workload.paths.data)])
    assert not junk.exists()


def test_preflight_evicts_the_oldest_complete_copy_to_fit(data_safety_manager, monkeypatch):
    info_a = data_safety_manager.preserve(
        "restore", [("pgdata", data_safety_manager.workload.paths.data)]
    )
    real_free_space = type(data_safety_manager.workload).free_space
    first_call = {"done": False}

    def short_then_real(self, path):
        """Short only on the preflight's first reading; real afterwards (the eviction freed bytes)."""
        if not first_call["done"]:
            first_call["done"] = True
            return 1
        return real_free_space(self, path)

    monkeypatch.setattr(type(data_safety_manager.workload), "free_space", short_then_real)
    info_b = data_safety_manager.preserve(
        "restore", [("pgdata", data_safety_manager.workload.paths.data)]
    )
    remaining = {c.copy_id for c in data_safety_manager.workload.list_data_safety_copies()}
    assert info_a.copy_id not in remaining  # the oldest complete copy was evicted
    assert info_b.copy_id in remaining


def test_postcopy_gate_raises_when_space_is_short(data_safety_manager, monkeypatch):
    real_free_space = type(data_safety_manager.workload).free_space
    calls = {"n": 0}

    def short_after_tar(self, path):
        """Real during preflight, short for the post-copy re-check."""
        calls["n"] += 1
        return 1 if calls["n"] > 2 else real_free_space(self, path)

    monkeypatch.setattr(type(data_safety_manager.workload), "free_space", short_after_tar)
    with pytest.raises(DataSafetyPostflightError):
        data_safety_manager.preserve(
            "restore", [("pgdata", data_safety_manager.workload.paths.data)]
        )


def test_preserve_wraps_notimplementederror_for_k8s(monkeypatch):
    workload = MagicMock()
    workload.evict_data_safety_copies.side_effect = NotImplementedError("k8s stub")
    manager = DataSafetyManager(MagicMock(), workload)
    with pytest.raises(DataSafetyError) as exc:
        manager.preserve("restore", [("pgdata", MagicMock())])
    assert "not supported on this substrate" in str(exc.value)
