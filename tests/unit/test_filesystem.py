# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest

from single_kernel_postgresql.utils.filesystem import change_owner


def test_change_owner_calls_pwd_and_os_chown_with_daemon_user():
    with patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam") as getpwnam, patch(
        "single_kernel_postgresql.utils.filesystem.os.chown"
    ) as chown, NamedTemporaryFile(delete=True) as tmp:
        # Simulate pwd entry
        pw_entry = MagicMock()
        pw_entry.pw_uid = 1234
        pw_entry.pw_gid = 4321
        getpwnam.return_value = pw_entry

        change_owner(tmp.name)

        getpwnam.assert_called_once_with("_daemon_")
        chown.assert_called_once_with(tmp.name, uid=1234, gid=4321)


def test_change_owner_raises_when_user_missing():
    # When the _daemon_ user is not present, pwd.getpwnam raises KeyError
    with patch(
        "single_kernel_postgresql.utils.filesystem.pwd.getpwnam", side_effect=KeyError
    ), pytest.raises(KeyError), NamedTemporaryFile(delete=True) as tmp:
        change_owner(tmp.name)


def test_change_owner_bubbles_up_os_error():
    # Ensure we surface OSError coming from os.chown
    with patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam") as getpwnam, patch(
        "single_kernel_postgresql.utils.filesystem.os.chown", side_effect=OSError("denied")
    ), NamedTemporaryFile(delete=True) as tmp:
        entry = MagicMock()
        entry.pw_uid = 1
        entry.pw_gid = 1
        getpwnam.return_value = entry
        with pytest.raises(OSError):
            change_owner(tmp.name)
