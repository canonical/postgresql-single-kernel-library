# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import get_args

from single_kernel_postgresql.config.locales import K8S_LOCALES, SNAP_LOCALES


def test_snap_locales_exclude_k8s_only_entries():
    members = get_args(SNAP_LOCALES)
    assert "C" in members
    assert "C.utf8" not in members
    assert "POSIX" not in members


def test_k8s_locales_extend_snap_with_rock_only_entries():
    snap = set(get_args(SNAP_LOCALES))
    k8s = set(get_args(K8S_LOCALES))
    assert k8s - snap == {"C.utf8", "POSIX"}
    assert snap - k8s == set()
