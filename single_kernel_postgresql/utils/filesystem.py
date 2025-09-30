# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Filesystem utilities."""

import logging
import os
import pwd

logger = logging.getLogger(__name__)


def change_owner(path: str, user: str) -> None:
    """Change the ownership of a file or a directory to the specifier user.

    Args:
        path: path to a file or directory.
        user: the owner username to set.
    """
    # Get the uid/gid for the user.
    user_database = pwd.getpwnam(user)
    # Set the correct ownership for the file or directory.
    os.chown(path, uid=user_database.pw_uid, gid=user_database.pw_gid)


def has_correct_ownership_and_permissions(path: str, user: str, mode: int) -> bool:
    """Check if a file or directory has the correct ownership and permissions.

    Args:
        path: path to a file or directory.
        user: the expected owner username.
        mode: the expected file mode (permissions) in octal format (e.g., 0o755).

    Returns:
        True if the file or directory has the correct ownership and permissions, False otherwise.
    """
    # Get the file or directory status.
    stat_info = os.stat(path)
    # Get the uid/gid for the expected user.
    user_database = pwd.getpwnam(user)
    logger.debug(f"Owner of {path} is '{user_database.pw_name}'")
    # Check ownership.
    if stat_info.st_uid != user_database.pw_uid or stat_info.st_gid != user_database.pw_gid:
        return False
    # Check permissions (mask with 0o777 to get only permission bits).
    permissions_str = stat_info.st_mode & 0o777
    logger.debug(f"Permissions of {path} are '{permissions_str}'")
    return int(permissions_str) == mode
