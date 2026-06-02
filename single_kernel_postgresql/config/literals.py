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

# Scopes
SCOPES = Literal["app", "unit"]
APP_SCOPE = "app"
UNIT_SCOPE = "unit"


# Secrets
SECRET_KEY_OVERRIDES = {"ca": "cauth"}



# File permissions as octal
# standard directory permissions
DIR_PERMISSIONS_READONLY = 0o750


# Container name for K8s deployments
CONTAINER_NAME = "postgresql"

API_REQUEST_TIMEOUT = 5