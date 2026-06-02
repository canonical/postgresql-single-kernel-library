# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Statuses for the PostgreSQL Charm.

This module defines various status enums that represent the state of the charm.
"""
from enum import Enum
from data_platform_helpers.advanced_statuses import StatusObject

class GeneralStatuses(Enum):
    """Collection of common charm statuses."""

    ACTIVE_IDLE = StatusObject(status="active", message="")
    WAITING_DIRECTORY_NOT_ATTACHED = StatusObject(status="waiting", message="Data directory not attached.")
    MAINTAINENANCE_INSTALLING = StatusObject(status="maintenance", message="installing PostgreSQL")
    WAITING_POSTGRESQL_START = StatusObject(status="waiting", message="waiting to start PostgreSQL")


# Manager specific statuses
# TODO: populate with actual statuses as we implement the managers.
class TlsStatuses(Enum):
    """Collection of charm statuses related to tls manager."""

    TLS_RELATION_MISSING = StatusObject(
        status="blocked", message="Missing TLS relation with this cluster."
    )
