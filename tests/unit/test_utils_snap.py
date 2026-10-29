# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import time
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import httpx
import pytest
from charmlibs import snap
from single_kernel_postgresql.utils import snap as snap_utils


@pytest.fixture
def snapd():
    def respond(body, status=200):
        requests = []

        def handle(request):
            requests.append(request)
            assert len(requests) == 1, "Received responses must not be retried"
            assert request.extensions["timeout"] == dict.fromkeys(
                ("connect", "read", "write", "pool"), 120
            )
            assert request.url.path == "/v2/apps"
            assert dict(request.url.params) == {"names": "charmed-postgresql", "select": "service"}
            return httpx.Response(status, json=body)

        return patch.object(
            snap_utils.httpx, "HTTPTransport", return_value=httpx.MockTransport(handle)
        )

    return respond


def test_service_states_distinguish_declared_stopped_and_missing(snapd):
    with snapd({
        "type": "sync",
        "status-code": 200,
        "result": [
            {"snap": "charmed-postgresql", "name": "patroni", "active": True},
            {"snap": "charmed-postgresql", "name": "ldap-sync", "active": False},
            {"snap": "charmed-postgresql", "name": "exporter"},
        ],
    }):
        assert snap_utils.service_states("charmed-postgresql") == {
            "patroni": True,
            "ldap-sync": False,
            "exporter": False,
        }
    with snapd({"type": "sync", "status-code": 200, "result": []}):
        assert snap_utils.service_states("charmed-postgresql") == {}


@pytest.mark.parametrize(
    "kind,status,error",
    [
        ("snap-not-found", 404, snap.NotInstalledError),
        ("", 500, snap.APIError),
    ],
)
def test_service_states_api_errors(snapd, kind, status, error):
    with (
        snapd(
            {
                "type": "error",
                "status-code": status,
                "status": "Failure",
                "result": {
                    "message": "query failed",
                    "kind": kind,
                    "value": "charmed-postgresql",
                },
            },
            status,
        ),
        pytest.raises(error, match="query failed"),
    ):
        snap_utils.service_states("charmed-postgresql")


@pytest.mark.parametrize("result", [None, [None], [{}], [{"name": "patroni", "active": "false"}]])
def test_service_states_malformed_response_is_not_absence(snapd, result):
    with (
        snapd({"type": "sync", "status-code": 200, "result": result}),
        pytest.raises(snap.BadResponseError),
    ):
        snap_utils.service_states("charmed-postgresql")


@pytest.fixture
def retry_clock(monkeypatch):
    now = 0

    def sleep(seconds):
        nonlocal now
        now += seconds

    sleeper = Mock(side_effect=sleep)
    monkeypatch.setattr(time, "monotonic", lambda: now)
    monkeypatch.setattr(time, "sleep", sleeper)
    return sleeper


def test_service_states_retries_transient_connection_failure(retry_clock):
    handle = Mock(
        side_effect=[
            httpx.ConnectError("restarting"),
            httpx.Response(
                200, json={"type": "sync", "result": [{"name": "patroni", "active": True}]}
            ),
        ]
    )
    with patch.object(snap_utils.httpx, "HTTPTransport", return_value=httpx.MockTransport(handle)):
        assert snap_utils.service_states("charmed-postgresql") == {"patroni": True}
    assert handle.call_count == 2
    retry_clock.assert_called_once_with(0.25)


def test_service_states_connection_retry_budget(retry_clock):
    error = httpx.ConnectError("unavailable")
    handle = Mock(side_effect=error)
    with (
        patch.object(snap_utils.httpx, "HTTPTransport", return_value=httpx.MockTransport(handle)),
        pytest.raises(snap.ConnectionError) as caught,
    ):
        snap_utils.service_states("charmed-postgresql")
    assert caught.value.__cause__ is error
    assert handle.call_count == 22
    assert retry_clock.call_count == 21
    assert all(call.args == (0.25,) for call in retry_clock.call_args_list)
    assert time.monotonic() == 5.25


@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ConnectTimeout])
def test_service_states_timeouts_are_not_retried(error_type, retry_clock):
    error = error_type("slow")
    handle = Mock(side_effect=error)
    with (
        patch.object(snap_utils.httpx, "HTTPTransport", return_value=httpx.MockTransport(handle)),
        pytest.raises(snap.TimeoutError) as caught,
    ):
        snap_utils.service_states("charmed-postgresql")
    assert caught.value.__cause__ is error
    handle.assert_called_once()
    retry_clock.assert_not_called()


def test_service_states_missing_socket_is_not_retried(tmp_path, retry_clock):
    # Preserve HTTPX's real cause/context chain, including its suppressed OS error.
    with (
        httpx.Client(
            transport=httpx.HTTPTransport(uds=str(tmp_path / "missing.socket"))
        ) as client,
        pytest.raises(httpx.ConnectError) as transport_error,
    ):
        client.get("http://localhost/v2/apps")
    handle = Mock(side_effect=transport_error.value)
    with (
        patch.object(snap_utils.httpx, "HTTPTransport", return_value=httpx.MockTransport(handle)),
        pytest.raises(snap.SocketNotFoundError) as caught,
    ):
        snap_utils.service_states("charmed-postgresql")
    assert caught.value.__cause__ is transport_error.value
    handle.assert_called_once()
    retry_clock.assert_not_called()


@pytest.fixture
def local_timezone(monkeypatch):
    def use(zone):
        monkeypatch.setenv("TZ", zone)
        time.tzset()

    yield use
    monkeypatch.undo()
    time.tzset()


@pytest.mark.parametrize("limit,expected", [(None, [0, 1, 2]), (2, [1, 2]), (0, [])])
def test_logs_filter_exact_sid_before_tailing(limit, expected, local_timezone):
    local_timezone("UTC")
    entries = [
        snap.LogEntry(
            datetime(2026, 10, 26, 13, 41, i, 123456, UTC),
            "charmed-postgresql.patroni",
            42,
            str(i),
        )
        for i in range(3)
    ]
    entries.insert(
        1, snap.LogEntry(datetime.now(UTC), "charmed-postgresql.patroni-other", 7, "other")
    )
    entries.append(snap.LogEntry(datetime.now(UTC), "charmed-postgresql.exporter", 7, "other"))
    with patch.object(snap, "logs", return_value=entries) as logs:
        assert snap_utils.logs_by_identifier(
            "charmed-postgresql", "charmed-postgresql.patroni", limit=limit
        ) == "".join(
            f"2026-10-26T13:41:0{i}Z charmed-postgresql.patroni[42]: {i}\n" for i in expected
        )
    logs.assert_called_once_with("charmed-postgresql", limit=None)


def test_logs_format_local_time_and_preserve_message_newlines(local_timezone):
    local_timezone("EST5")
    entry = snap.LogEntry(
        datetime(2026, 10, 26, 13, 41, 19, 340706, UTC), "service", 42, "one\ntwo"
    )
    with patch.object(snap, "logs", return_value=[entry]):
        assert (
            snap_utils.logs_by_identifier("example", "service")
            == "2026-10-26T08:41:19-05:00 service[42]: one\ntwo\n"
        )


def test_logs_empty_and_failure():
    with patch.object(snap, "logs", return_value=[]):
        assert snap_utils.logs_by_identifier("example", "service") == ""
    with (
        patch.object(snap, "logs", side_effect=snap.ConnectionError("unavailable")),
        pytest.raises(snap.ConnectionError),
    ):
        snap_utils.logs_by_identifier("example", "service")


def test_negative_log_limit_is_rejected():
    with patch.object(snap, "logs") as logs, pytest.raises(snap.Error):
        snap_utils.logs_by_identifier("example", "service", limit=-1)
    logs.assert_not_called()
