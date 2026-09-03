# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Pure helpers for the pgBackRest backup implementation.

Ported from the 16/edge charm backup modules (``src/backups.py`` on the VM and
K8s charms); function behavior is byte-equivalent where the charms agree.

The S3 block / standby-cluster message constants live here rather than in
``config/literals.py`` because this slice does not own that module; the events
layer (backups-9) imports them from here to map manager results onto unit
statuses and action failures.
"""

import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime

from single_kernel_postgresql.config.literals import (
    BACKUP_ID_FORMAT,
    PGBACKREST_BACKUP_ID_FORMAT,
)

logger = logging.getLogger(__name__)

# S3 initialization block messages, verbatim from the charms. The events layer
# matches the stored block message against these to tell an S3-caused blocked
# state apart from any other blocking condition.
ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE = "the S3 repository has backups from another cluster"
FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE = (
    "failed to access/create the bucket, check your S3 settings"
)
FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE = "failed to initialize stanza, check your S3 settings"
CANNOT_RESTORE_PITR = "cannot restore PITR, juju debug-log for details"

S3_BLOCK_MESSAGES = [
    ANOTHER_CLUSTER_REPOSITORY_ERROR_MESSAGE,
    FAILED_TO_ACCESS_CREATE_BUCKET_ERROR_MESSAGE,
    FAILED_TO_INITIALIZE_STANZA_ERROR_MESSAGE,
]

# Standby (async-replication) cluster guard messages. ``is_standby_cluster`` is
# a bridge injected into BackupManager (a VM-only concept), so the messages live
# next to the helpers that produce them instead of the charms.
STANDBY_CLUSTER_CREATE_BACKUP_ERROR_MESSAGE = (
    "Backups are not supported on a standby cluster. "
    "Run create-backup on the primary cluster instead."
)
STANDBY_CLUSTER_LIST_BACKUPS_ERROR_MESSAGE = (
    "Backups are not supported on a standby cluster. "
    "Run list-backups on the primary cluster instead."
)
STANDBY_CLUSTER_RESTORE_ERROR_MESSAGE = (
    "Restoring backups is not supported on a standby cluster. "
    "Run restore on the primary cluster instead."
)

# Backup id recovered from a failed backup's stdout ("new backup label = ").
BACKUP_LABEL_STDOUT_PATTERN = r"(new backup label = )([0-9]{8}[-][0-9]{6}[F])$"


def extract_error_message(stderr: str, logs_path: str) -> str:
    """Extract key error message from pgBackRest stderr output.

    Since we standardize all pgBackRest commands to use --log-level-stderr=warn,
    all errors and warnings are consistently written to stderr. This makes error
    extraction predictable and avoids potential log duplication issues.

    Args:
        stderr: Standard error from pgBackRest command containing errors/warnings.
        logs_path: Path to the pgBackRest logs, reported when no message can be
            extracted.

    Returns:
        Extracted error message from stderr, prioritizing ERROR/WARN lines.
    """
    if not stderr.strip():
        return f"Unknown error occurred. Please check the logs at {logs_path}"

    # Extract lines with ERROR or WARN markers from pgBackRest stderr output
    error_lines = []
    for line in stderr.splitlines():
        if "ERROR:" in line or "WARN:" in line:
            # Clean up the line by removing debug prefixes like "P00  ERROR:"
            cleaned = re.sub(r"^.*?(ERROR:|WARN:)", r"\1", line).strip()
            error_lines.append(cleaned)

    # If we found error/warning lines, return them joined
    if error_lines:
        return "; ".join(error_lines)

    # Otherwise return the last non-empty line from stderr
    return stderr.strip().splitlines()[-1]


def is_psql_timestamp(timestamp: str) -> bool:
    """Return whether the provided timestamp is a valid PostgreSQL timestamp."""
    if not re.match(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?([-+](?:\d{2}|\d{4}|\d{2}:\d{2}))?$",
        timestamp,
    ):
        return False
    try:
        parse_psql_timestamp(timestamp)
        return True
    except ValueError:
        return False


def parse_psql_timestamp(timestamp: str) -> datetime:
    """Intended to use with data only after is_psql_timestamp check."""
    # With the python >= 3.11 only the datetime.fromisoformat will be sufficient without any regexes. Therefore,
    # it will not be required for the is_psql_timestamp check that ensures intended regex execution.
    t = re.sub(r"([-+]\d{2})$", r"\1:00", timestamp)
    t = re.sub(r"([-+]\d{2})(\d{2})$", r"\1:\2", t)
    t = re.sub(r"\.(\d+)", lambda x: f".{x[1]:06}", t)
    dt = datetime.fromisoformat(t)
    # Convert to the timezone-naive
    if dt.tzinfo is not None and dt.tzinfo is not UTC:
        dt = dt.astimezone(tz=UTC)
    return dt.replace(tzinfo=None)


def parse_backup_id(label: str) -> tuple[str, str]:
    """Parse backup ID as a timestamp and its type."""
    if label[-1] == "F":
        timestamp = label
        backup_type = "full"
    elif label[-1] == "D":
        timestamp = label.split("_")[1]
        backup_type = "differential"
    elif label[-1] == "I":
        timestamp = label.split("_")[1]
        backup_type = "incremental"
    else:
        raise ValueError("Unknown label format for backup ID: %s", label)

    return (
        datetime.strftime(
            datetime.strptime(timestamp[:-1], PGBACKREST_BACKUP_ID_FORMAT),
            BACKUP_ID_FORMAT,
        ),
        backup_type,
    )


def format_backup_list(backup_list, s3_parameters: dict) -> str:
    """Formats provided list of backups as a table."""
    backups = [
        "Storage bucket name: {:s}".format(s3_parameters["bucket"]),
        "Backups base path: {:s}/backup/\n".format(s3_parameters["path"]),
        "{:<20s} | {:<19s} | {:<8s} | {:<20s} | {:<23s} | {:<20s} | {:<20s} | {:<8s} | {:s}".format(
            "backup-id",
            "action",
            "status",
            "reference-backup-id",
            "LSN start/stop",
            "start-time",
            "finish-time",
            "timeline",
            "backup-path",
        ),
    ]
    backups.append("-" * len(backups[2]))
    for (
        backup_id,
        backup_action,
        backup_status,
        reference,
        lsn_start_stop,
        start,
        stop,
        backup_timeline,
        path,
    ) in backup_list:
        backups.append(
            f"{backup_id:<20s} | {backup_action:<19s} | {backup_status:<8s} | {reference:<20s} | {lsn_start_stop:<23s} | {start:<20s} | {stop:<20s} | {backup_timeline:<8s} | {path:s}"
        )
    return "\n".join(backups)


def generate_fake_backup_id(backup_type: str, backup_labels: Iterable[str]) -> str:
    """Creates a backup id for failed backup operations (to store log file).

    Args:
        backup_type: one of "full", "differential", or "incremental".
        backup_labels: the un-parsed pgBackRest labels of the successful
            backups currently in the repository.

    Raises:
        TypeError: a differential/incremental backup has no base backup to
            reference.
        Exception: the backup type is invalid.
    """
    if backup_type == "full":
        return datetime.strftime(datetime.now(), "%Y%m%d-%H%M%SF")
    if backup_type == "differential":
        backups = list(backup_labels)
        last_full_backup = None
        for label in backups[::-1]:
            if label.endswith("F"):
                last_full_backup = label
                break

        if last_full_backup is None:
            raise TypeError("Differential backup requested but no previous full backup")
        return f"{last_full_backup}_{datetime.strftime(datetime.now(), '%Y%m%d-%H%M%SD')}"
    if backup_type == "incremental":
        backups = list(backup_labels)
        if not backups:
            raise TypeError("Incremental backup requested but no previous successful backup")
        return f"{backups[-1]}_{datetime.strftime(datetime.now(), '%Y%m%d-%H%M%SI')}"
    else:
        raise Exception("Invalid backup type")


def fetch_backup_from_id(backup_id: str, backup_labels: Iterable[str]) -> str | None:
    """Fetches backup's pgbackrest label from backup id."""
    timestamp = (
        f"{datetime.strftime(datetime.strptime(backup_id, '%Y-%m-%dT%H:%M:%SZ'), '%Y%m%d-%H%M%S')}"
    )
    for label in backup_labels:
        if timestamp in label:
            return label

    return None


def get_nearest_timeline(
    timestamp: str, timelines: dict[str, tuple[str, str]]
) -> tuple[str, str] | None:
    """Finds the nearest timeline or backup prior to the specified timeline.

    Args:
        timestamp: the restore-to-time target, or "latest".
        timelines: merged mapping of backup ids and timeline keys to
            (stanza, timeline) pairs.

    Returns:
        (stanza, timeline) of the nearest timeline or backup. None, if there are no matches.
    """
    if timestamp == "latest":
        return max(timelines.items())[1] if len(timelines) > 0 else None
    filtered_timelines = [
        (timeline_key, timeline_object)
        for timeline_key, timeline_object in timelines.items()
        if datetime.strptime(timeline_key, BACKUP_ID_FORMAT) <= parse_psql_timestamp(timestamp)
    ]
    return max(filtered_timelines)[1] if len(filtered_timelines) > 0 else None
