# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL connection helpers that don't depend on the charm under test."""

import itertools

import psycopg2


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
