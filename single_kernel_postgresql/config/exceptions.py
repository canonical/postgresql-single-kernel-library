#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm-specific exceptions."""

from single_kernel_postgresql.compat.postgresql import PostgreSQLBaseError


class PostgreSQLFileOperationError(PostgreSQLBaseError):
    """Exception thrown when file operations related to PostgreSQL fail."""


class StorageUnavailableError(Exception):
    """Cannot find storage mountpoint."""


class SettingSystemPasswordError(PostgreSQLBaseError):
    """Exception thrown when setting the system password fails."""


class PostgreSQLCannotConnectError(Exception):
    """Cannot run smoke check on connected Database."""


class TlsError(Exception):
    """TLS implementation internal exception."""


class RaftPostgresqlNotUpError(Exception):
    """Postgresql not yet started."""


class RaftPostgresqlStillUpError(Exception):
    """Postgresql not yet down."""


class RaftNotPromotedError(Exception):
    """Leader not yet set when reinitialising raft."""


class ClusterNotPromotedError(Exception):
    """Raised when a cluster is not promoted."""


class NotReadyError(Exception):
    """Raised when not all cluster members healthy or finished initial sync."""


class EndpointNotReadyError(Exception):
    """Raised when an endpoint is not ready."""


class StandbyClusterAlreadyPromotedError(Exception):
    """Raised when a standby cluster is already promoted."""


class RemoveRaftMemberFailedError(Exception):
    """Raised when a remove raft member failed for some reason."""


class SwitchoverFailedError(Exception):
    """Raised when a switchover failed for some reason."""


class SwitchoverNotSyncError(SwitchoverFailedError):
    """Raised when a switchover failed because node is not sync."""


class UpdateSyncNodeCountError(Exception):
    """Raised when updating synchronous_node_count failed for some reason."""
