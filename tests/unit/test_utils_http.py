# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Focused tests for the shared Patroni HTTP GET path.

Covers the two hook-cost defects from the round-5 investigation
(ci-invest/round5-test_charm.md):
- one AsyncClient (and one SSL context) per request round instead of one per
  request per endpoint, and
- local-first sequential probing with the parallel fan-out kept as fallback.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ConnectError
from single_kernel_postgresql.utils import parallel_patroni_get_request


@pytest.fixture
def fake_response():
    response = MagicMock()
    response.raise_for_status.return_value.json.return_value = {"members": []}
    return response


def _fake_client(response=None, fail_first=0):
    """An AsyncClient double sharing one instance across all requests.

    fail_first: raise ConnectError on the first N get() calls.
    """
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    outcomes = [ConnectError("boom")] * fail_first + [response] * 10
    client.get = AsyncMock(side_effect=outcomes)
    return client


def test_parallel_get_request_local_endpoint_answer_stops_the_fanout(fake_response):
    """The first (own) endpoint answering serves the request: peers are not contacted."""
    client = _fake_client(fake_response)
    with patch("single_kernel_postgresql.utils.AsyncClient", return_value=client) as factory:
        result = parallel_patroni_get_request("/cluster", ["10.0.0.1", "10.0.0.2"], "ca")
    assert result == {"members": []}
    assert factory.call_count == 1
    client.get.assert_awaited_once_with("https://10.0.0.1:8008/cluster")


def test_parallel_get_request_local_failure_falls_back_to_the_fanout(fake_response):
    """Own endpoint down: every endpoint is tried in parallel through the SAME client."""
    client = _fake_client(fake_response, fail_first=1)
    with patch("single_kernel_postgresql.utils.AsyncClient", return_value=client) as factory:
        result = parallel_patroni_get_request(
            "/cluster", ["10.0.0.1", "10.0.0.2", "10.0.0.3"], "ca"
        )
    assert result == {"members": []}
    assert factory.call_count == 1
    requested = [call.args[0] for call in client.get.await_args_list]
    assert requested[0] == "https://10.0.0.1:8008/cluster"
    assert set(requested[1:]) == {
        "https://10.0.0.1:8008/cluster",
        "https://10.0.0.2:8008/cluster",
        "https://10.0.0.3:8008/cluster",
    }


def test_parallel_get_request_builds_one_ssl_context_per_round(fake_response):
    """The SSL context is built once per request round, not once per request."""
    client = _fake_client(fake_response, fail_first=1)
    with (
        patch("single_kernel_postgresql.utils.AsyncClient", return_value=client),
        patch("single_kernel_postgresql.utils.create_default_context") as ctx_factory,
    ):
        parallel_patroni_get_request("/cluster", ["10.0.0.1", "10.0.0.2"], "ca")
    assert ctx_factory.call_count == 1


def test_parallel_get_request_all_endpoints_down_returns_none():
    """Every endpoint failing keeps the historical None result (no exception)."""
    client = _fake_client(None, fail_first=10)
    with patch("single_kernel_postgresql.utils.AsyncClient", return_value=client):
        result = parallel_patroni_get_request("/cluster", ["10.0.0.1", "10.0.0.2"], "ca")
    assert result is None


def test_parallel_get_request_empty_endpoints_returns_none():
    with patch("single_kernel_postgresql.utils.AsyncClient") as factory:
        result = parallel_patroni_get_request("/cluster", [], "ca")
    assert result is None
    factory.assert_not_called()
