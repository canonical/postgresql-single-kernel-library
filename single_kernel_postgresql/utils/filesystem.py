# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Filesystem utilities."""

import logging
import os
import pwd
from typing import Optional

from ops import Container

logger = logging.getLogger(__name__)


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


def has_correct_ownership_and_permissions(
    path: str, user: str, mode: int, container: Optional[Container] = None
) -> bool:
    """Check if a file or directory has the correct ownership and permissions.

    Args:
        path: path to a file or directory.
        user: the expected owner username.
        mode: the expected file mode (permissions) in octal format (e.g., 0o755).
        container: optional container where to check the file or directory.

    Returns:
        True if the file or directory has the correct ownership and permissions, False otherwise.
    """
    if container is not None:
        owner = container.exec(["stat", "-c", "'%U'", path]).wait_output()[0].strip().strip("'")
        logger.debug(f"Owner of {path} in container is '{owner}'")
        # Check ownership.
        if owner != user:
            return False
        # Check permissions (convert to the octal base).
        permissions_str = (
            container.exec(["stat", "-c", "'%a'", path]).wait_output()[0].strip().strip("'")
        )
        logger.debug(f"Permissions of {path} in container are '{permissions_str}'")
        return int(permissions_str, 8) == mode

    # Get the file or directory status.
    stat_info = os.stat(path)
    # Get the uid/gid for the expected user.
    user_database = pwd.getpwnam(user)
    logger.debug(f"Owner of {path} in container is '{user_database.pw_name}'")
    # Check ownership.
    if stat_info.st_uid != user_database.pw_uid or stat_info.st_gid != user_database.pw_gid:
        return False
    # Check permissions (mask with 0o777 to get only permission bits).
    permissions_str = stat_info.st_mode & 0o777
    logger.debug(f"Permissions of {path} in container are '{permissions_str}'")
    return int(permissions_str) == mode
