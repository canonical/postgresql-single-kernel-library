# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Shared fakes for the TLS unit tests (the requirer return shape)."""

from types import SimpleNamespace


class FakePrivateKey:
    """Minimal stand-in for PrivateKey: str(key) returns the raw PEM string."""

    def __init__(self, raw: str) -> None:
        self._raw = raw

    def __str__(self) -> str:
        """Return the raw PEM string."""
        return self._raw


def fake_assigned(cert, ca, key):
    """Mimic TLSCertificatesRequiresV4.get_assigned_certificates() -> (list, PrivateKey|None)."""
    return [SimpleNamespace(certificate=cert, ca=ca)], FakePrivateKey(key)
