#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the pure backup helpers in single_kernel_postgresql.utils.backup.

These are behavior tables ported alongside the helpers: pgBackRest label and
timestamp parsing, backup-list formatting, error extraction, and the
fake-id / nearest-timeline logic shared by both substrates.
"""

import re
from datetime import datetime, timedelta

import pytest
from single_kernel_postgresql.utils.backup import (
    extract_error_message,
    fetch_backup_from_id,
    format_backup_list,
    generate_fake_backup_id,
    get_nearest_timeline,
    is_psql_timestamp,
    parse_backup_id,
    parse_psql_timestamp,
)

# -- extract_error_message -----------------------------------------------------


def test_extract_error_message_empty_stderr_reports_logs_path():
    assert (
        extract_error_message("", "/var/log/pgbackrest")
        == "Unknown error occurred. Please check the logs at /var/log/pgbackrest"
    )


def test_extract_error_message_prioritizes_error_and_warn_lines():
    stderr = (
        "P00   INFO: check command begin\n"
        "P00  ERROR: [055]: unable to find stanza\n"
        "P00   WARN: some warning\n"
        "P00   INFO: check command end\n"
    )
    assert extract_error_message(stderr, "logs") == (
        "ERROR: [055]: unable to find stanza; WARN: some warning"
    )


def test_extract_error_message_falls_back_to_last_line():
    assert extract_error_message("some plain failure\n", "logs") == "some plain failure"


# -- psql timestamps ------------------------------------------------------------


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        ("2024-01-01 10:00:00", True),
        ("2024-01-01 10:00:00.123456", True),
        ("2024-01-01 10:00:00+01", True),
        ("2024-01-01 10:00:00+0100", True),
        ("2024-01-01 10:00:00+01:00", True),
        ("not-a-timestamp", False),
        ("2024-13-01 10:00:00", False),
        ("", False),
    ],
)
def test_is_psql_timestamp(timestamp, expected):
    assert is_psql_timestamp(timestamp) is expected


@pytest.mark.parametrize(
    "timestamp,offset_seconds",
    [
        ("2024-01-01 10:00:00+01", 3600),
        ("2024-01-01 10:00:00-0230", -9000),
    ],
)
def test_parse_psql_timestamp_converts_to_naive_utc(timestamp, offset_seconds):
    parsed = parse_psql_timestamp(timestamp)
    assert parsed == datetime(2024, 1, 1, 10, 0, 0) - timedelta(seconds=offset_seconds)
    assert parsed.tzinfo is None


def test_parse_psql_timestamp_strips_utc_offset():
    assert parse_psql_timestamp("2024-01-01 10:00:00+00:00") == datetime(2024, 1, 1, 10, 0, 0)


# -- parse_backup_id ------------------------------------------------------------


@pytest.mark.parametrize(
    "label,expected",
    [
        ("20240101-101010F", ("2024-01-01T10:10:10Z", "full")),
        ("20240101-101010F_20240102-111111D", ("2024-01-02T11:11:11Z", "differential")),
        ("20240101-101010F_20240102-111111I", ("2024-01-02T11:11:11Z", "incremental")),
    ],
)
def test_parse_backup_id(label, expected):
    assert parse_backup_id(label) == expected


def test_parse_backup_id_rejects_unknown_label():
    with pytest.raises(ValueError):
        parse_backup_id("20240101-101010Z")


# -- format_backup_list ---------------------------------------------------------


def test_format_backup_list_renders_table():
    s3_parameters = {"bucket": "my-bucket", "path": "/backups"}
    backup_list = [
        (
            "2024-01-01T10:10:10Z",
            "full backup",
            "finished",
            "None",
            "0/1000000 / 0/2000000",
            "2024-01-01T10:10:10Z",
            "2024-01-01T10:20:10Z",
            "1",
            "/test-model.app/20240101-101010F",
        )
    ]
    output = format_backup_list(backup_list, s3_parameters)
    assert "Storage bucket name: my-bucket" in output
    assert "Backups base path: /backups/backup/" in output
    assert "2024-01-01T10:10:10Z" in output
    assert "full backup" in output


# -- generate_fake_backup_id ----------------------------------------------------


def test_generate_fake_backup_id_full():
    assert re.match(r"\d{8}-\d{6}F$", generate_fake_backup_id("full", []))


def test_generate_fake_backup_id_differential_references_last_full():
    fake_id = generate_fake_backup_id(
        "differential", ["20240101-101010F", "20240101-101010F_20240102-111111D"]
    )
    assert re.match(r"20240101-101010F_\d{8}-\d{6}D$", fake_id)


def test_generate_fake_backup_id_differential_without_full_backup_raises():
    with pytest.raises(TypeError):
        generate_fake_backup_id("differential", ["20240101-101010F_20240102-111111I"])


def test_generate_fake_backup_id_incremental_references_latest():
    fake_id = generate_fake_backup_id("incremental", ["20240101-101010F_20240102-111111I"])
    assert re.match(r"20240101-101010F_20240102-111111I_\d{8}-\d{6}I$", fake_id)


def test_generate_fake_backup_id_incremental_without_backups():
    with pytest.raises(TypeError):
        generate_fake_backup_id("incremental", [])


def test_generate_fake_backup_id_invalid_type():
    with pytest.raises(Exception, match="Invalid backup type"):
        generate_fake_backup_id("bogus", [])


# -- fetch_backup_from_id -------------------------------------------------------


def test_fetch_backup_from_id_finds_matching_label():
    labels = ["20240101-101010F", "20240101-101010F_20240102-111111D"]
    assert fetch_backup_from_id("2024-01-02T11:11:11Z", labels) == labels[1]


def test_fetch_backup_from_id_returns_none_without_match():
    assert fetch_backup_from_id("2024-01-02T11:11:11Z", ["20240101-101010F"]) is None


# -- get_nearest_timeline -------------------------------------------------------


def test_get_nearest_timeline_latest():
    timelines = {
        "2024-01-01T10:10:10Z": ("stanza", "1"),
        "2024-02-01T10:10:10Z": ("stanza", "2"),
    }
    assert get_nearest_timeline("latest", timelines) == ("stanza", "2")


def test_get_nearest_timeline_latest_empty():
    assert get_nearest_timeline("latest", {}) is None


def test_get_nearest_timeline_before_timestamp():
    timelines = {
        "2024-01-01T10:10:10Z": ("stanza", "1"),
        "2024-02-01T10:10:10Z": ("stanza", "2"),
    }
    assert get_nearest_timeline("2024-01-15 10:00:00", timelines) == ("stanza", "1")


def test_get_nearest_timeline_no_match():
    timelines = {"2024-02-01T10:10:10Z": ("stanza", "2")}
    assert get_nearest_timeline("2024-01-15 10:00:00", timelines) is None


def test_get_nearest_timeline_rejects_bad_timestamp():
    timelines = {"2024-02-01T10:10:10Z": ("stanza", "2")}
    with pytest.raises(ValueError):
        get_nearest_timeline("not-a-timestamp", timelines)
