# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import Mock, patch

from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.core.state import CharmState


def test_patroni_url_brackets_ipv6_endpoint():
    """An IPv6 unit address must be bracketed in the Patroni REST API URL."""
    mock_charm = Mock()
    binding = Mock()
    binding.network.bind_address = "fd42:a615:ea50:2a68:216:3eff:fef1:6b2"
    mock_charm.framework.model.get_binding.return_value = binding

    state = CharmState(charm=mock_charm, substrate=Substrates.VM, s3_requirer=Mock())

    assert state.patroni_url == "https://[fd42:a615:ea50:2a68:216:3eff:fef1:6b2]:8008"


def test_patroni_url_keeps_ipv4_endpoint_unbracketed():
    mock_charm = Mock()
    binding = Mock()
    binding.network.bind_address = "192.0.2.10"
    mock_charm.framework.model.get_binding.return_value = binding

    state = CharmState(charm=mock_charm, substrate=Substrates.VM, s3_requirer=Mock())

    assert state.patroni_url == "https://192.0.2.10:8008"


def test_config_normalisation():
    mock_charm = Mock()
    mock_charm.framework.model.config = {
        "experimental_max_connections": 100,
        "experimental-max-connections": 200,
        "synchronous_mode_strict": False,
        "response-lc-monetary": "en_US",
    }
    with patch("single_kernel_postgresql.core.state.CharmConfig") as _charm_config:
        state = CharmState(charm=mock_charm, substrate=Substrates.VM, s3_requirer=Mock())
        _charm_config.keys.return_value = [
            "experimental_max_connections",
            "synchronous_mode_strict",
            "response_lc_monetary",
        ]
        _ = state.config
        _charm_config.assert_called_once_with(
            experimental_max_connections=200,
            synchronous_mode_strict=False,
            response_lc_monetary="en_US",
        )
