# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Snap service queries and identifier-filtered logs missing from charmlibs-snap."""

from __future__ import annotations

import httpx
from charmlibs import snap
from tenacity import Retrying, retry_if_exception, wait_fixed


def service_states(snap_name: str) -> dict[str, bool]:  # noqa: C901
    """Return declared services and their active states from snapd.

    A missing snap raises ``snap.NotInstalledError``; an installed snap without
    services returns an empty mapping. Query failures raise public ``snap.Error``
    subclasses, never an empty result that could be mistaken for absence.
    """
    with httpx.Client(
        transport=httpx.HTTPTransport(uds="/run/snapd.socket"), timeout=120
    ) as client:
        for attempt in Retrying(
            # Match charmlibs-snap 2: a failure at exactly five seconds still retries.
            stop=lambda state: (state.seconds_since_start or 0) > 5,
            wait=wait_fixed(0.25),
            retry=retry_if_exception(
                lambda error: (
                    isinstance(error, snap.ConnectionError)
                    and not isinstance(error, snap.SocketNotFoundError)
                )
            ),
            reraise=True,
        ):
            with attempt:
                try:
                    response = client.get(
                        "http://localhost/v2/apps",
                        params={"names": snap_name, "select": "service"},
                    )
                except httpx.TimeoutException as exc:
                    raise snap.TimeoutError(str(exc)) from exc
                except httpx.TransportError as exc:
                    cause: BaseException | None = exc
                    while cause is not None:
                        if isinstance(cause, FileNotFoundError):
                            raise snap.SocketNotFoundError(str(exc)) from exc
                        cause = cause.__cause__ or cause.__context__
                    raise snap.ConnectionError(str(exc)) from exc
    try:
        body = response.json()
        if body["type"] == "error":
            result = body["result"]
            error = (
                snap.NotInstalledError if result.get("kind") == "snap-not-found" else snap.APIError
            )
            raise error(
                result["message"],
                kind=result.get("kind", ""),
                value=result.get("value"),
                status_code=response.status_code,
                status=body.get("status"),
            )
        if (
            not response.is_success
            or body["type"] != "sync"
            or not isinstance(body["result"], list)
        ):
            raise ValueError("Expected a synchronous service list")
        states = {}
        for service in body["result"]:
            name, active = service["name"], service.get("active", False)
            if not isinstance(name, str) or not name or not isinstance(active, bool):
                raise ValueError("Invalid service name or active state")
            states[name] = active
        return states
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise snap.BadResponseError(
            "Invalid snapd service response", response=response.text
        ) from exc


def _format_log(entry: snap.LogEntry) -> str:
    """Match v1's local timestamp and line layout for persisted restore IDs."""
    timestamp = entry.timestamp.astimezone().isoformat(timespec="seconds")
    if timestamp.endswith("+00:00"):
        timestamp = timestamp[:-6] + "Z"
    return f"{timestamp} {entry.sid}[{entry.pid}]: {entry.message}\n"


def logs_by_identifier(snap_name: str, identifier: str, *, limit: int | None = None) -> str:
    """Filter all snap logs by exact SID, then apply the line limit.

    Entries retain chronological order and v1 formatting. Snap query errors
    propagate to the caller. Negative limits raise ``snap.Error``, like v1's CLI.
    """
    if limit is not None and limit < 0:
        raise snap.Error("Log limit must be non-negative")
    entries = [entry for entry in snap.logs(snap_name, limit=None) if entry.sid == identifier]
    if limit is not None:
        entries = entries[-limit:] if limit else []
    return "".join(_format_log(entry) for entry in entries)
