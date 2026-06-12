# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Literal string for the different charms.

This module should contain the literals used in the charms (paths, relation names, etc).
"""

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

# File permissions as octal
# standard directory permissions
DIR_PERMISSIONS_READONLY = 0o750


# Container name for K8s deployments
CONTAINER_NAME = "postgresql"

# Patroni constants
API_REQUEST_TIMEOUT = 5
PATRONI_CLUSTER_STATUS_ENDPOINT = "cluster"
PATRONI_SERVICE_NAME = "snap.charmed-postgresql.patroni.service"
PATRONI_SERVICE_DEFAULT_PATH = f"/etc/systemd/system/{PATRONI_SERVICE_NAME}"

# TLS constants
TLS_CA_BUNDLE_FILE = "peer_ca_bundle.pem"

# Raft constants
RAFT_PORT = 2222
RAFT_PARTNER_PREFIX = "partner_node_status_server_"

# Snap constants
SNAP_COMMON_PATH = "/var/snap/charmed-postgresql/common"
SNAP_CURRENT_PATH = "/var/snap/charmed-postgresql/current"
DATA_DIR_SUBFOLDER = "16/main"

SNAP_CONF_PATH = f"{SNAP_CURRENT_PATH}/etc"
SNAP_DATA_PATH = f"{SNAP_COMMON_PATH}/var/lib"
SNAP_LOGS_PATH = f"{SNAP_COMMON_PATH}/var/log"
ARCHIVE_STORAGE_PATH = f"{SNAP_COMMON_PATH}/data/archive"
LOGS_STORAGE_PATH = f"{SNAP_COMMON_PATH}/data/logs"
TEMP_STORAGE_PATH = f"{SNAP_COMMON_PATH}/data/temp"

PATRONI_CONF_PATH = f"{SNAP_CONF_PATH}/patroni"
PATRONI_LOGS_PATH = f"{SNAP_LOGS_PATH}/patroni"

PGBACKREST_CONF_PATH = f"{SNAP_CONF_PATH}/pgbackrest"
PGBACKREST_LOGS_PATH = f"{SNAP_LOGS_PATH}/pgbackrest"

POSTGRESQL_CONF_PATH = f"{SNAP_CONF_PATH}/postgresql"
POSTGRESQL_DATA_PATH = f"{SNAP_DATA_PATH}/postgresql"
POSTGRESQL_DATA_DIR = f"{POSTGRESQL_DATA_PATH}/{DATA_DIR_SUBFOLDER}"
ARCHIVE_DATA_DIR = f"{ARCHIVE_STORAGE_PATH}/{DATA_DIR_SUBFOLDER}"
LOGS_DATA_DIR = f"{LOGS_STORAGE_PATH}/{DATA_DIR_SUBFOLDER}"
TEMP_DATA_DIR = f"{TEMP_STORAGE_PATH}/{DATA_DIR_SUBFOLDER}"
POSTGRESQL_LOGS_PATH = f"{SNAP_LOGS_PATH}/postgresql"

# K8s storage paths
STORAGE_PATH = "/var/lib/pg"
ARCHIVE_PATH = f"{STORAGE_PATH}/archive"
DATA_STORAGE_PATH = f"{STORAGE_PATH}/data"
LOGS_STORAGE_PATH = f"{STORAGE_PATH}/logs"
TEMP_STORAGE_PATH = f"{STORAGE_PATH}/temp"
POSTGRESQL_LOGS_PATH = f"{LOGS_STORAGE_PATH}/16/main/pg_logs"
PATRONI_LOGS_PATH = f"{LOGS_STORAGE_PATH}/16/main/patroni_logs"
PGBACKREST_LOGS_PATH = f"{LOGS_STORAGE_PATH}/16/main/pgbackrest_logs"
POSTGRESQL_LOGS_PATTERN = "postgresql*.log"
POSTGRES_LOG_FILES = [
    f"{PGBACKREST_LOGS_PATH}/*",
    f"{PATRONI_LOGS_PATH}/patroni.log",
    f"{POSTGRESQL_LOGS_PATH}/postgresql*.log",
]

# Pgbackrest constants
PGBACKREST_CONFIGURATION_FILE = f"--config={PGBACKREST_CONF_PATH}/pgbackrest.conf"
