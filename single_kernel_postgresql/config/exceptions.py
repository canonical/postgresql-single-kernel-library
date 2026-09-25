#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm-specific exceptions."""

from single_kernel_postgresql.compat.postgresql import PostgreSQLBaseError


class PostgreSQLFileOperationError(PostgreSQLBaseError):
    """Exception thrown when file operations related to PostgreSQL fail."""


class StorageUnavailableError(PostgreSQLBaseError):
    """Cannot find storage mountpoint."""


class SettingSystemPasswordError(PostgreSQLBaseError):
    """Exception thrown when setting the system password fails."""


class PostgreSQLCannotConnectError(PostgreSQLBaseError):
    """Cannot run smoke check on connected Database."""


class TlsError(PostgreSQLBaseError):
    """TLS implementation internal exception."""


class RaftPostgresqlNotUpError(PostgreSQLBaseError):
    """Postgresql not yet started."""


class RaftPostgresqlStillUpError(PostgreSQLBaseError):
    """Postgresql not yet down."""


class RaftNotPromotedError(PostgreSQLBaseError):
    """Leader not yet set when reinitialising raft."""


class ClusterNotPromotedError(PostgreSQLBaseError):
    """Raised when a cluster is not promoted."""


class NotReadyError(PostgreSQLBaseError):
    """Raised when not all cluster members healthy or finished initial sync."""


class EndpointNotReadyError(PostgreSQLBaseError):
    """Raised when an endpoint is not ready."""


class StandbyClusterAlreadyPromotedError(PostgreSQLBaseError):
    """Raised when a standby cluster is already promoted."""


class RemoveRaftMemberFailedError(PostgreSQLBaseError):
    """Raised when a remove raft member failed for some reason."""


class AddRaftMemberFailedError(PostgreSQLBaseError):
    """Raised when adding raft member failed for some reason."""


class SwitchoverFailedError(PostgreSQLBaseError):
    """Raised when a switchover failed for some reason."""


class SwitchoverNotSyncError(SwitchoverFailedError):
    """Raised when a switchover failed because node is not sync."""


class UpdateSyncNodeCountError(PostgreSQLBaseError):
    """Raised when updating synchronous_node_count failed for some reason."""


class DeployedWithoutTrustError(PostgreSQLBaseError):
    """Raised when the K8s API denies access because the app wasn't deployed with --trust."""
