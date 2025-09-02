# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Skeleton for the abstract charm."""

from ops.charm import CharmBase

from .config.literals import SYSTEM_USERS, USER
from .utils.postgresql import PostgreSQL


class AbstractPostgreSQLCharm(CharmBase):
    """An abstract PostgreSQL charm."""

    def __init__(self, *args):
        super().__init__(*args)

        self.postgresql = PostgreSQL(
            primary_host="localhost",
            current_host="localhost",
            user=USER,
            # The password is hardcoded because this is an abstract charm and
            # it meant to be used only in unit tests.
            password="test-password",  # noqa S106
            database="test-database",
            system_users=SYSTEM_USERS,
        )
