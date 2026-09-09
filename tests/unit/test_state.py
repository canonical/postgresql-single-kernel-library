# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import Mock

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.core.state import CharmState


def test_patroni_url_brackets_ipv6_endpoint():
    """An IPv6 unit address must be bracketed in the Patroni REST API URL."""
    mock_charm = Mock()
    binding = Mock()
    binding.network.bind_address = "fd42:a615:ea50:2a68:216:3eff:fef1:6b2"
    mock_charm.framework.model.get_binding.return_value = binding

    state = CharmState(charm=mock_charm, substrate=Substrates.VM)

    assert state.patroni_url == "https://[fd42:a615:ea50:2a68:216:3eff:fef1:6b2]:8008"


def test_patroni_url_keeps_ipv4_endpoint_unbracketed():
    mock_charm = Mock()
    binding = Mock()
    binding.network.bind_address = "192.0.2.10"
    mock_charm.framework.model.get_binding.return_value = binding

    state = CharmState(charm=mock_charm, substrate=Substrates.VM)

    assert state.patroni_url == "https://192.0.2.10:8008"
