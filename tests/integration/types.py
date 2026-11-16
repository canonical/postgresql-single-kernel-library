# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Types shared by the integration tests."""

from enum import StrEnum


class Substrate(StrEnum):
    """Substrate the integration tests run on (selected with --substrate)."""

    vm = "vm"
    k8s = "k8s"
