# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Filesystem utilities."""

import os
import pwd
from typing import Optional

from ops import Container


def change_owner(path: str, user: str, container: Optional[Container] = None) -> None:
    """Change the ownership of a file or a directory to the specifier user.

    Args:
        path: path to a file or directory.
        user: the owner username to set.
        container: optional container where to execute the command.
    """
    if container is not None:
        # For now, assume the group has the same name as the user.
        container.exec(["chown", f"{user}:{user}", path]).wait()
        return
    # Get the uid/gid for the user.
    user_database = pwd.getpwnam(user)
    # Set the correct ownership for the file or directory.
    os.chown(path, uid=user_database.pw_uid, gid=user_database.pw_gid)
