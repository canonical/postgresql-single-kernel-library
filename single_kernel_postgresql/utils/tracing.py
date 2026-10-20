#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm-tracing configuration helper (port of the VM charm's charm_tracing_config)."""

import logging

from single_kernel_postgresql.config.literals import TRACING_PROTOCOL

logger = logging.getLogger(__name__)

try:
    # The COS-agent charm lib ships the exception; the lib does not bundle it.
    from charms.grafana_agent.v0.cos_agent import ProtocolNotFoundError  # ty: ignore[unresolved-import]
except ImportError:  # pragma: no cover - the requirer lives charm-side

    class ProtocolNotFoundError(Exception):
        """Fallback when the COS-agent charm lib is absent (lib-only consumers)."""


def charm_tracing_config(endpoint_requirer) -> None:
    """Utility function to set the tracing destination (port of the charm's helper)."""
    if not endpoint_requirer.is_ready():
        return

    try:
        if not (endpoint := endpoint_requirer.get_tracing_endpoint(TRACING_PROTOCOL)):
            return
    except ProtocolNotFoundError:
        logger.warning(
            "Endpoint for tracing wasn't provided as tracing backend isn't ready yet. "
            "If grafana-agent isn't connected to a tracing backend, integrate it. "
            "Otherwise this issue should resolve itself in a few events."
        )
        return

    endpoint = f"{endpoint}/v1/traces"
    if endpoint.startswith("https://"):
        # If the endpoint is https BUT we don't have a server_cert yet: disable
        # charm tracing until we do to prevent tls errors.
        logger.warning("Cannot send traces to an https endpoint without a certificate.")
        return
    try:
        from ops_tracing import set_destination  # ty: ignore[unresolved-import]
    except ImportError:  # pragma: no cover - the tracing extra is optional
        logger.debug("ops_tracing not installed; not setting the destination")
        return
    set_destination(endpoint, None)
