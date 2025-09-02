# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from unittest.mock import call, patch

import psycopg2
import pytest
from ops.testing import Harness
from psycopg2.sql import SQL, Composed, Identifier, Literal

from single_kernel_postgresql.abstract_charm import AbstractPostgreSQLCharm
from single_kernel_postgresql.config.literals import (
    PEER,
    SYSTEM_USERS,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_INTERNAL,
    ACCESS_GROUPS,
    PostgreSQLCreateDatabaseError,
    PostgreSQLGetLastArchivedWALError,
)


@pytest.fixture(autouse=True)
def harness():
    with open("single_kernel_postgresql/charmcraft.yaml") as meta_file:
        meta = meta_file.read()
    harness = Harness(AbstractPostgreSQLCharm, meta=meta)

    # Set up the initial relation and hooks.
    peer_rel_id = harness.add_relation(PEER, "postgresql-single-kernel")
    harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/0")
    harness.begin()
    yield harness
    harness.cleanup()


@pytest.mark.parametrize("users_exist", [True, False])
def test_create_access_groups(harness, users_exist):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
            True if users_exist else None
        )
        harness.charm.postgresql.create_access_groups()
        calls = [
            *(
                call(
                    Composed([
                        SQL("SELECT TRUE FROM pg_roles WHERE rolname="),
                        Literal(group),
                        SQL(";"),
                    ])
                )
                for group in ACCESS_GROUPS
            )
        ]
        if not users_exist:
            index = 1
            for group in ACCESS_GROUPS:
                calls.insert(index, call(SQL("CREATE ROLE {} NOLOGIN;").format(Identifier(group))))
                index += 2
        execute.assert_has_calls(calls)


def test_create_database(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL.enable_disable_extensions"
    ) as _enable_disable_extensions, patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        # Test a successful database creation.
        database = "test_database"
        plugins = ["test_plugin_1", "test_plugin_2"]
        with harness.hooks_disabled():
            rel_id = harness.add_relation("database", "application")
            harness.add_relation_unit(rel_id, "application/0")
            harness.update_relation_data(rel_id, "application", {"database": database})
        schemas = [("test_schema_1",), ("test_schema_2",)]
        _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = schemas
        harness.charm.postgresql.create_database(database, plugins)
        execute = _connect_to_database.return_value.cursor.return_value.execute
        execute.assert_has_calls([
            call(
                Composed([
                    SQL("SELECT datname FROM pg_database WHERE datname="),
                    Literal("test_database"),
                    SQL(";"),
                ])
            )
        ])
        _enable_disable_extensions.assert_called_once_with(
            {plugins[0]: True, plugins[1]: True}, database
        )

        # Test when two relations request the same database.
        _connect_to_database.reset_mock()
        with harness.hooks_disabled():
            other_rel_id = harness.add_relation("database", "other-application")
            harness.add_relation_unit(other_rel_id, "other-application/0")
            harness.update_relation_data(other_rel_id, "other-application", {"database": database})
        harness.charm.postgresql.create_database(database, plugins)

        # Test a failed database creation.
        _enable_disable_extensions.reset_mock()
        execute.side_effect = psycopg2.Error
        try:
            harness.charm.postgresql.create_database(database, plugins)
            assert False
        except PostgreSQLCreateDatabaseError:
            pass
        _enable_disable_extensions.assert_not_called()


def test_grant_internal_access_group_memberships(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        harness.charm.postgresql.grant_internal_access_group_memberships()

        internal_group = Identifier(ACCESS_GROUP_INTERNAL)

        execute.assert_has_calls([
            *(
                call(SQL("GRANT {} TO {};").format(internal_group, Identifier(user)))
                for user in SYSTEM_USERS
            ),
        ])


def test_grant_relation_access_group_memberships(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        harness.charm.postgresql.grant_relation_access_group_memberships()

        execute.assert_has_calls([
            call(
                "SELECT usename FROM pg_catalog.pg_user WHERE usename LIKE 'relation_id_%' OR usename LIKE 'relation-%' OR usename LIKE 'pgbouncer_auth_relation_%' OR usename LIKE '%_user_%_%' OR usename LIKE 'logical_replication_relation_%';"
            )
        ])


def test_get_last_archived_wal(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        # Test a successful call.
        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
            "000000010000000100000001",
        )
        assert harness.charm.postgresql.get_last_archived_wal() == "000000010000000100000001"
        execute.assert_called_once_with("SELECT last_archived_wal FROM pg_stat_archiver;")

        # Test a failed call.
        execute.reset_mock()
        execute.side_effect = psycopg2.Error
        try:
            harness.charm.postgresql.get_last_archived_wal()
            assert False
        except PostgreSQLGetLastArchivedWALError:
            pass
        execute.assert_called_once_with("SELECT last_archived_wal FROM pg_stat_archiver;")


def test_build_postgresql_group_map(harness):
    assert harness.charm.postgresql.build_postgresql_group_map(None) == []

    for group in ACCESS_GROUPS:
        assert harness.charm.postgresql.build_postgresql_group_map(f"ldap_group={group}") == []

    mapping_1 = "ldap_group_1=psql_group_1"
    mapping_2 = "ldap_group_2=psql_group_2"

    assert harness.charm.postgresql.build_postgresql_group_map(f"{mapping_1},{mapping_2}") == [
        ("ldap_group_1", "psql_group_1"),
        ("ldap_group_2", "psql_group_2"),
    ]
    try:
        harness.charm.postgresql.build_postgresql_group_map(f"{mapping_1} {mapping_2}")
        assert False
    except ValueError:
        assert True


def test_build_postgresql_parameters(harness):
    # Test when not limit is imposed to the available memory.
    config_options = {
        "durability_test_config_option_1": True,
        "instance_test_config_option_2": False,
        "logging_test_config_option_3": "on",
        "memory_test_config_option_4": 1024,
        "optimizer_test_config_option_5": "scheduled",
        "other_test_config_option_6": "test-value",
        "profile": "production",
        "request_date_style": "ISO, DMY",
        "request_time_zone": "UTC",
        "request_test_config_option_7": "off",
        "response_test_config_option_8": "partial",
        "vacuum_test_config_option_9": 10.5,
    }
    assert harness.charm.postgresql.build_postgresql_parameters(config_options, 1000000000) == {
        "test_config_option_1": True,
        "test_config_option_2": False,
        "test_config_option_3": "on",
        "test_config_option_4": 1024,
        "test_config_option_5": "scheduled",
        "test_config_option_7": "off",
        "DateStyle": "ISO, DMY",
        "TimeZone": "UTC",
        "test_config_option_8": "partial",
        "test_config_option_9": 10.5,
        "shared_buffers": f"{250 * 128}",
        "effective_cache_size": f"{750 * 128}",
    }

    # Test with a limited imposed to the available memory.
    parameters = harness.charm.postgresql.build_postgresql_parameters(
        config_options, 1000000000, 600000000
    )
    assert parameters["shared_buffers"] == f"{150 * 128}"
    assert parameters["effective_cache_size"] == f"{450 * 128}"

    # Test when the requested shared buffers are greater than 40% of the available memory.
    config_options["memory_shared_buffers"] = 50001
    try:
        harness.charm.postgresql.build_postgresql_parameters(config_options, 1000000000)
        assert False
    except AssertionError as e:
        raise e
    except Exception:
        pass

    # Test when the requested shared buffers are lower than 40% of the available memory
    # (also check that it's used when calculating the effective cache size value).
    config_options["memory_shared_buffers"] = 50000
    parameters = harness.charm.postgresql.build_postgresql_parameters(config_options, 1000000000)
    assert parameters["shared_buffers"] == 50000
    assert parameters["effective_cache_size"] == f"{600 * 128}"

    # Test when the profile is set to "testing".
    config_options["profile"] = "testing"
    parameters = harness.charm.postgresql.build_postgresql_parameters(config_options, 1000000000)
    assert parameters["shared_buffers"] == 50000
    assert "effective_cache_size" not in parameters

    # Test when there is no shared_buffers value set in the config option.
    del config_options["memory_shared_buffers"]
    parameters = harness.charm.postgresql.build_postgresql_parameters(config_options, 1000000000)
    assert "shared_buffers" not in parameters
    assert "effective_cache_size" not in parameters


def test_configure_pgaudit(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        # Test when pgAudit is enabled.
        execute = (
            _connect_to_database.return_value.cursor.return_value.__enter__.return_value.execute
        )
        harness.charm.postgresql._configure_pgaudit(True)
        execute.assert_has_calls([
            call("ALTER SYSTEM SET pgaudit.log = 'ROLE,DDL,MISC,MISC_SET';"),
            call("ALTER SYSTEM SET pgaudit.log_client TO off;"),
            call("ALTER SYSTEM SET pgaudit.log_parameter TO off;"),
            call("SELECT pg_reload_conf();"),
        ])

        # Test when pgAudit is disabled.
        execute.reset_mock()
        harness.charm.postgresql._configure_pgaudit(False)
        execute.assert_has_calls([
            call("ALTER SYSTEM RESET pgaudit.log;"),
            call("ALTER SYSTEM RESET pgaudit.log_client;"),
            call("ALTER SYSTEM RESET pgaudit.log_parameter;"),
            call("SELECT pg_reload_conf();"),
        ])


def test_validate_group_map(harness):
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
    ) as _connect_to_database:
        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None

        query = SQL("SELECT TRUE FROM pg_roles WHERE rolname={};")

        assert harness.charm.postgresql.validate_group_map(None) is True

        assert harness.charm.postgresql.validate_group_map("") is False
        assert harness.charm.postgresql.validate_group_map("ldap_group=") is False
        execute.assert_has_calls([
            call(query.format(Literal(""))),
        ])

        assert harness.charm.postgresql.validate_group_map("ldap_group=missing_group") is False
        execute.assert_has_calls([
            call(query.format(Literal("missing_group"))),
        ])

        _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
            True,
        )
        assert harness.charm.postgresql.validate_group_map("ldap_group=ldap_test_group") is True
        assert harness.charm.postgresql.validate_group_map("ldap_group=ldap_test_group,") is False
        assert harness.charm.postgresql.validate_group_map("ldap_group ldap_test_group") is False
