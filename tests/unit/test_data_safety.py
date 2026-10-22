#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the data-safety seam (constants, manifest, workload primitives, manager)."""

import json
from datetime import datetime

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
from single_kernel_postgresql.workload.base import (
    SafetyCopyInfo,
    build_safety_manifest,
    read_safety_manifest,
    safety_copy_id,
    write_safety_manifest,
)


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
    assert json.loads((tmp_path / "restore-20261022T1015Z" / BACKUP_SAFETY_MANIFEST_NAME).read_text())["schema_version"] == 1


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
    from single_kernel_postgresql.config.literals import VM_ARCHIVE_PATH, VM_DATA_PATH, VM_TEMP_PATH

    assert BACKUP_SAFETY_DIR_NAME not in (VM_DATA_PATH, VM_ARCHIVE_PATH, VM_TEMP_PATH)
