# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Literal string for the different charms.

This module should contain the literals used in the charms (paths, enums, etc).
"""

# Relations.
PEER = "database-peers"

# Users.
BACKUP_USER = "backup"
MONITORING_USER = "monitoring"
REPLICATION_USER = "replication"
REWIND_USER = "rewind"
USER = "operator"
SYSTEM_USERS = [MONITORING_USER, REPLICATION_USER, REWIND_USER, USER]
