# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Filesystem paths of the COS payload data shipped inside the package.

The grafana dashboards and the prometheus/loki alert rules moved from the
consuming charms' ``src/`` trees into the library package
(``single_kernel_postgresql/payloads/<substrate>/``). The charm-lib requirers
read them with real-filesystem ``Path.glob``/``open`` calls, so the requirer
constructors are handed these resolved paths instead of charm-root-relative
defaults.
"""

from pathlib import Path

import single_kernel_postgresql


def payload_dir(substrate: str, *parts: str) -> Path:
    """Return the package-relative payload directory for a substrate."""
    base = Path(single_kernel_postgresql.__file__).resolve().parent / "payloads" / substrate
    return base.joinpath(*parts)
