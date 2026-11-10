# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL connection helpers that don't depend on the charm under test."""

import itertools

import psycopg2
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_delay,
    wait_fixed,
)

SWEEP_CONNECT_RETRY_BUDGET = 600


def _is_connection_refused(error: BaseException) -> bool:
    """Whether the error is a transient connection refusal (e.g. unit still starting)."""
    return (
        isinstance(error, psycopg2.OperationalError) and "connection refused" in str(error).lower()
    )


def db_connect(
    host: str,
    password: str,
    user: str = "operator",
    database: str = "postgres",
    sslmode: str | None = None,
) -> psycopg2.extensions.connection:
    """Return a connection to a PostgreSQL database (as the operator user by default)."""
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=10,
        sslmode=sslmode,
    )


def execute_query(
    host: str,
    password: str,
    query: str,
    database: str = "postgres",
    sslmode: str | None = None,
) -> list:
    """Run a query as the operator user and return all the values of the rows, flattened."""
    connection = db_connect(host, password, database=database, sslmode=sslmode)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return list(itertools.chain(*cursor.fetchall()))
    finally:
        connection.close()


def execute_queries_on_unit(
    unit_address: str, username: str, password: str, queries: list[str], database: str
) -> list:
    """Execute given PostgreSQL queries on a unit.

    Args:
        unit_address: The public IP address of the unit to execute the queries on
        username: The PostgreSQL username
        password: The PostgreSQL password
        queries: A list of queries to execute
        database: Database to execute in

    Returns:
        A list of rows that were potentially queried
    """
    for attempt in Retrying(
        stop=stop_after_delay(SWEEP_CONNECT_RETRY_BUDGET),
        wait=wait_fixed(5),
        retry=retry_if_exception(_is_connection_refused),
        reraise=True,
    ):
        with attempt:
            connection = psycopg2.connect(
                f"dbname='{database}' user='{username}' host='{unit_address}' password='{password}' connect_timeout=10"
            )
    with (
        connection,
        connection.cursor() as cursor,
    ):
        for query in queries:
            cursor.execute(query)
        output = list(itertools.chain(*cursor.fetchall()))

    return output


def check_connected_user(
    cursor, session_user: str, current_user: str, primary: bool = True
) -> None:
    """Assert the session and current users of the connection."""
    cursor.execute("SELECT session_user, current_user;")
    result = cursor.fetchone()
    assert result is not None, "No result returned from the query"
    instance = "primary" if primary else "replica"
    assert result[0] == session_user, (
        f"The session user should be the {session_user} user in the {instance}"
        f" (it's currently {result[0]})"
    )
    assert result[1] == current_user, (
        f"The current user should be the {current_user} user in the {instance}"
        f" (it's currently {result[1]})"
    )
