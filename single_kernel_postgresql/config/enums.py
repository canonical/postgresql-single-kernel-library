# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL enums."""
from enum import StrEnum

class Substrates(StrEnum):
    """Possible substrates."""

    K8S = "k8s"
    VM = "vm"

