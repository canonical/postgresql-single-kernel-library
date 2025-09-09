# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Filesystem utilities."""

import os
import pwd


def change_owner(path: str) -> None:
    """Change the ownership of a file or a directory to the _daemon_ user.

    Args:
        path: path to a file or directory.
    """
    # Get the uid/gid for the _daemon_ user.
    user_database = pwd.getpwnam("_daemon_")
    # Set the correct ownership for the file or directory.
    os.chown(path, uid=user_database.pw_uid, gid=user_database.pw_gid)
