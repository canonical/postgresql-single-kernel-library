# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest
from single_kernel_postgresql.utils.filesystem import change_owner


def test_change_owner_calls_pwd_and_os_chown_with_rock_user():
    with (
        patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam") as getpwnam,
        patch("single_kernel_postgresql.utils.filesystem.os.chown") as chown,
        NamedTemporaryFile(delete=True) as tmp,
    ):
        # Simulate ROCK user entry existing
        pw_entry = MagicMock()
        pw_entry.pw_uid = 1234
        pw_entry.pw_gid = 4321
        getpwnam.return_value = pw_entry

        change_owner(tmp.name)

        # The implementation should try ROCK_USER first
        getpwnam.assert_called_once_with("postgres")
        chown.assert_called_once_with(tmp.name, uid=1234, gid=4321)


def test_change_owner_calls_pwd_and_os_chown_with_daemon_user_when_rock_missing():
    with (
        patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam") as getpwnam,
        patch("single_kernel_postgresql.utils.filesystem.os.chown") as chown,
        NamedTemporaryFile(delete=True) as tmp,
    ):
        # Simulate ROCK user missing but snap daemon user present
        pw_entry = MagicMock()
        pw_entry.pw_uid = 2222
        pw_entry.pw_gid = 3333
        # First call (ROCK_USER) raises KeyError, second call (SNAP_USER) returns pw_entry
        getpwnam.side_effect = [KeyError, pw_entry]

        change_owner(tmp.name)

        # Ensure getpwnam was called twice (rock then snap) and ended up using snap user
        assert getpwnam.call_count == 2
        getpwnam.assert_called_with("_daemon_")
        chown.assert_called_once_with(tmp.name, uid=2222, gid=3333)


def test_change_owner_raises_when_user_missing():
    # When the _daemon_ user is not present, pwd.getpwnam raises KeyError
    with (
        patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam", side_effect=KeyError),
        pytest.raises(KeyError),
        NamedTemporaryFile(delete=True) as tmp,
    ):
        change_owner(tmp.name)


def test_change_owner_bubbles_up_os_error():
    # Ensure we surface OSError coming from os.chown
    with (
        patch("single_kernel_postgresql.utils.filesystem.pwd.getpwnam") as getpwnam,
        patch("single_kernel_postgresql.utils.filesystem.os.chown", side_effect=OSError("denied")),
        NamedTemporaryFile(delete=True) as tmp,
    ):
        entry = MagicMock()
        entry.pw_uid = 1
        entry.pw_gid = 1
        getpwnam.return_value = entry
        with pytest.raises(OSError):
            change_owner(tmp.name)
