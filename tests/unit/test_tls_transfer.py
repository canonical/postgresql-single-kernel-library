# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
from unittest.mock import Mock

import pytest
from ops.pebble import ConnectionError as PebbleConnectionError
from single_kernel_postgresql.config.literals import PEER_RELATION
from single_kernel_postgresql.events.tls_transfer import TLSTransfer

SCOPE = "unit"


@pytest.fixture(autouse=True)
def _stub_certificate_transfer_requirer(monkeypatch):
    """The real vendored requirer needs a bound charm framework.

    These tests drive TLSTransfer's handler logic against a Mock charm, so the
    requirer class is stubbed (it is only constructed, never exercised here).
    """
    monkeypatch.setattr(
        "single_kernel_postgresql.events.tls_transfer.CertificateTransferRequires", Mock()
    )


def test_on_ca_certificate_added():
    mock_charm = Mock()
    mock_charm.model.get_relation.return_value.app.name = "testname"
    mock_event = Mock()
    tls_transfer = TLSTransfer(mock_charm, PEER_RELATION)

    # Happy scenario
    tls_transfer._on_certificate_available(mock_event)

    mock_charm.push_ca_file_into_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_not_called()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # Connection error
    mock_charm.push_ca_file_into_workload.side_effect = PebbleConnectionError

    tls_transfer._on_certificate_available(mock_event)

    mock_charm.push_ca_file_into_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_called_once_with()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # Failed to push
    mock_charm.push_ca_file_into_workload.side_effect = None
    mock_charm.push_ca_file_into_workload.return_value = False

    tls_transfer._on_certificate_available(mock_event)

    mock_charm.push_ca_file_into_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_called_once_with()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # No relation
    mock_charm.model.get_relation.return_value = None

    tls_transfer._on_certificate_available(mock_event)

    mock_charm.push_ca_file_into_workload.assert_not_called()
    mock_event.defer.assert_not_called()
    mock_charm.reset_mock()
    mock_event.reset_mock()


def test_on_ca_certificate_removed():
    mock_charm = Mock()
    mock_charm.model.get_relation.return_value.app.name = "testname"
    mock_event = Mock()
    tls_transfer = TLSTransfer(mock_charm, PEER_RELATION)

    # Happy scenario
    tls_transfer._on_certificate_removed(mock_event)

    mock_charm.clean_ca_file_from_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_not_called()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # Connection error
    mock_charm.clean_ca_file_from_workload.side_effect = PebbleConnectionError

    tls_transfer._on_certificate_removed(mock_event)

    mock_charm.clean_ca_file_from_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_called_once_with()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # Failed to push
    mock_charm.clean_ca_file_from_workload.side_effect = None
    mock_charm.clean_ca_file_from_workload.return_value = False

    tls_transfer._on_certificate_removed(mock_event)

    mock_charm.clean_ca_file_from_workload.assert_called_once_with("ca-testname")
    mock_event.defer.assert_called_once_with()
    mock_charm.reset_mock()
    mock_event.reset_mock()

    # No relation
    mock_charm.model.get_relation.return_value = None

    tls_transfer._on_certificate_removed(mock_event)

    mock_charm.clean_ca_file_from_workload.assert_not_called()
    mock_event.defer.assert_not_called()
    mock_charm.reset_mock()
    mock_event.reset_mock()


def test_ca_secrets_route_through_state():
    """The CA secrets go through the state's secret storage, not the charm.

    The migration moved secret access onto CharmState; the charm-level
    set_secret seam no longer exists, and calling it crashed every
    receive-ca-cert hook (test_ldap glauth, CI 2026-10-13).
    """
    mock_charm = Mock()
    mock_charm.model.get_relation.return_value.app.name = "testname"
    tls_transfer = TLSTransfer(mock_charm, PEER_RELATION)

    tls_transfer._on_certificate_available(Mock(ca="ca-pem"))
    mock_charm.state.set_secret.assert_called_once_with(SCOPE, "ca-testname", "ca-pem")
    mock_charm.set_secret.assert_not_called()

    tls_transfer._on_certificate_removed(Mock())
    mock_charm.state.set_secret.assert_called_with(SCOPE, "ca-testname", None)
