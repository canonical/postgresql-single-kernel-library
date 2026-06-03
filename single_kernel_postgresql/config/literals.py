# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Literal string for the different charms.

This module should contain the literals used in the charms (paths, relation names, etc).
"""

from typing import Literal

# Permissions.
POSTGRESQL_STORAGE_PERMISSIONS = 0o700

# Relations
PEER_RELATION = "database-peers"
STATUS_PEERS_RELATION = "status-peers"

# Users.
BACKUP_USER = "backup"
MONITORING_USER = "monitoring"
REPLICATION_USER = "replication"
REWIND_USER = "rewind"
SNAP_USER = "_daemon_"
USER = "operator"
SYSTEM_USERS = [MONITORING_USER, REPLICATION_USER, REWIND_USER, USER]

# Paths
## VM Paths
BASE_SNAP_DIR = "/var/snap/postgresql"
SNAP_DATA = "current"
SNAP_COMMON = "common"
SNAP = "/snap/postgresql/current"

## Shared Paths
DATA_PATH = "/var/lib/postgresql"
POSTGRESQL_CONF_PATH = "/etc/postgresql"
POSTGRESQL_CONF_FILE = "postgresql.conf"

## Patroni Paths
PATRONI_CONF_PATH = "/etc/patroni"

## TLS Paths
TLS_CA_BUNDLE_FILE = "peer_ca_bundle.pem"

# Scopes
SCOPES = Literal["app", "unit"]
APP_SCOPE = "app"
UNIT_SCOPE = "unit"

# Patroni states
STARTED_STATES = ["running", "streaming"]
RUNNING_STATES = [*STARTED_STATES, "starting"]


# Secrets
SECRET_KEY_OVERRIDES = {"ca": "cauth"}


# Password keys
REPLICATION_PASSWORD_KEY = "replication-password"  # noqa: S105
REWIND_PASSWORD_KEY = "rewind-password"  # noqa: S105
USER_PASSWORD_KEY = "operator-password"  # noqa: S105
MONITORING_PASSWORD_KEY = "monitoring-password"  # noqa: S105
RAFT_PASSWORD_KEY = "raft-password"  # noqa: S105
PATRONI_PASSWORD_KEY = "patroni-password"  # noqa: S105
SECRET_INTERNAL_LABEL = "internal-secret"  # noqa: S105
SECRET_DELETED_LABEL = "None"  # noqa: S105
SYSTEM_USERS_PASSWORD_CONFIG = "system-users"  # noqa: S105


# File permissions as octal
# standard directory permissions
DIR_PERMISSIONS_READONLY = 0o750


# Container name for K8s deployments
CONTAINER_NAME = "postgresql"

API_REQUEST_TIMEOUT = 5
