# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
# ruff: noqa: I001
from unittest.mock import call, patch, sentinel

import psycopg2
import pytest
from ops.testing import Harness
from psycopg2.sql import Composed, Identifier, Literal, SQL

from single_kernel_postgresql.abstract_charm import AbstractPostgreSQLCharm
from single_kernel_postgresql.config.literals import (
    PEER,
    POSTGRESQL_STORAGE_PERMISSIONS,
    SNAP_USER,
    SYSTEM_USERS,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUPS,
    ACCESS_GROUP_INTERNAL,
    PostgreSQL,
    PostgreSQLCreateDatabaseError,
    PostgreSQLCreateUserError,
    PostgreSQLDatabasesSetupError,
    PostgreSQLGetLastArchivedWALError,
    PostgreSQLUndefinedHostError,
    PostgreSQLUndefinedPasswordError,
    ROLE_DATABASES_OWNER,
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
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL.enable_disable_extensions"
        ) as _enable_disable_extensions,
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
    ):
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


def test_set_up_database_with_temp_tablespace_and_missing_owner_role(harness):
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_login_hook_function"),
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_predefined_catalog_roles_function"
        ),
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.create_user") as _create_user,
        patch("single_kernel_postgresql.utils.postgresql.change_owner") as _change_owner,
        patch("single_kernel_postgresql.utils.postgresql.os.chmod") as _chmod,
        patch("single_kernel_postgresql.utils.postgresql.os.stat") as _stat,
        patch("single_kernel_postgresql.utils.postgresql.pwd.getpwuid") as _getpwuid,
    ):
        # Simulate a temp location owned by wrong user/permissions to trigger fixup
        stat_result = type("stat_result", (), {"st_uid": 0, "st_mode": 0o755})
        _stat.return_value = stat_result
        _getpwuid.return_value.pw_name = "root"

        # First connection (non-context) for temp tablespace
        execute_direct = _connect_to_database.return_value.cursor.return_value.execute
        fetchone_direct = _connect_to_database.return_value.cursor.return_value.fetchone
        fetchone_direct.return_value = None

        # Second and third connections are context-managed
        execute_cm = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        fetchone_cm = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone
        fetchone_cm.return_value = None  # owner role missing

        harness.charm.postgresql.set_up_database(temp_location="/var/lib/postgresql/tmp")

        # Ensure permission fixes applied
        _change_owner.assert_called_once_with("/var/lib/postgresql/tmp")
        _chmod.assert_called_once_with("/var/lib/postgresql/tmp", 0o700)

        # Validate temp tablespace operations, including DROP when permissions are fixed
        execute_direct.assert_any_call("DROP TABLESPACE IF EXISTS temp;")
        execute_direct.assert_has_calls([
            call("SELECT TRUE FROM pg_tablespace WHERE spcname='temp';"),
            call("CREATE TABLESPACE temp LOCATION '/var/lib/postgresql/tmp';"),
            call("GRANT CREATE ON TABLESPACE temp TO public;"),
        ])

        # create_user called for missing owner role
        _create_user.assert_called_once_with(
            ROLE_DATABASES_OWNER, can_create_database=True, extra_user_roles=["charmed_dml"]
        )

        # Final revokes and grants
        system_users = harness.charm.postgresql.system_users
        expected = [
            call("REVOKE ALL PRIVILEGES ON DATABASE postgres FROM PUBLIC;"),
            call("REVOKE CREATE ON SCHEMA public FROM PUBLIC;"),
            *[
                call(SQL("GRANT ALL PRIVILEGES ON DATABASE postgres TO {};").format(Identifier(u)))
                for u in system_users
            ],
        ]
        execute_cm.assert_has_calls(expected, any_order=False)


def test_set_up_database_owner_mismatch_triggers_drop_and_fix(harness):
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_login_hook_function"),
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_predefined_catalog_roles_function"
        ),
        patch("single_kernel_postgresql.utils.postgresql.change_owner") as _change_owner,
        patch("single_kernel_postgresql.utils.postgresql.os.chmod") as _chmod,
        patch("single_kernel_postgresql.utils.postgresql.os.stat") as _stat,
        patch("single_kernel_postgresql.utils.postgresql.pwd.getpwuid") as _getpwuid,
    ):
        # Owner differs, permissions are correct
        stat_result = type(
            "stat_result", (), {"st_uid": 0, "st_mode": POSTGRESQL_STORAGE_PERMISSIONS}
        )
        _stat.return_value = stat_result
        _getpwuid.return_value.pw_name = "root"

        execute_direct = _connect_to_database.return_value.cursor.return_value.execute
        _connect_to_database.return_value.cursor.return_value.fetchone.return_value = True

        harness.charm.postgresql.set_up_database(temp_location="/var/lib/postgresql/tmp")

        _change_owner.assert_called_once_with("/var/lib/postgresql/tmp")
        _chmod.assert_called_once_with("/var/lib/postgresql/tmp", POSTGRESQL_STORAGE_PERMISSIONS)
        execute_direct.assert_any_call("DROP TABLESPACE IF EXISTS temp;")


def test_set_up_database_permissions_mismatch_triggers_drop_and_fix(harness):
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_login_hook_function"),
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_predefined_catalog_roles_function"
        ),
        patch("single_kernel_postgresql.utils.postgresql.change_owner") as _change_owner,
        patch("single_kernel_postgresql.utils.postgresql.os.chmod") as _chmod,
        patch("single_kernel_postgresql.utils.postgresql.os.stat") as _stat,
        patch("single_kernel_postgresql.utils.postgresql.pwd.getpwuid") as _getpwuid,
    ):
        # Owner matches SNAP_USER, permissions differ
        stat_result = type("stat_result", (), {"st_uid": 0, "st_mode": 0o755})
        _stat.return_value = stat_result
        _getpwuid.return_value.pw_name = SNAP_USER

        execute_direct = _connect_to_database.return_value.cursor.return_value.execute
        _connect_to_database.return_value.cursor.return_value.fetchone.return_value = True

        harness.charm.postgresql.set_up_database(temp_location="/var/lib/postgresql/tmp")

        _change_owner.assert_called_once_with("/var/lib/postgresql/tmp")
        _chmod.assert_called_once_with("/var/lib/postgresql/tmp", POSTGRESQL_STORAGE_PERMISSIONS)
        execute_direct.assert_any_call("DROP TABLESPACE IF EXISTS temp;")


def test_set_up_database_no_temp_and_existing_owner_role(harness):
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_login_hook_function"),
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL.set_up_predefined_catalog_roles_function"
        ),
        patch("single_kernel_postgresql.utils.postgresql.PostgreSQL.create_user") as _create_user,
    ):
        # owner role exists
        fetchone = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone
        fetchone.return_value = True

        harness.charm.postgresql.set_up_database()

        _create_user.assert_not_called()

        execute = _connect_to_database.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute
        system_users = harness.charm.postgresql.system_users
        execute.assert_has_calls([
            call("REVOKE ALL PRIVILEGES ON DATABASE postgres FROM PUBLIC;"),
            call("REVOKE CREATE ON SCHEMA public FROM PUBLIC;"),
            *[
                call(SQL("GRANT ALL PRIVILEGES ON DATABASE postgres TO {};").format(Identifier(u)))
                for u in system_users
            ],
        ])


def test_set_up_database_raises_wrapped_error(harness):
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database"
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.change_owner"),
        patch("single_kernel_postgresql.utils.postgresql.os.chmod"),
    ):
        execute_direct = _connect_to_database.return_value.cursor.return_value.execute
        execute_direct.side_effect = psycopg2.Error
        with pytest.raises(PostgreSQLDatabasesSetupError):
            harness.charm.postgresql.set_up_database(temp_location="/tmp")


def test_connect_to_database():
    # Error on no host
    pg = PostgreSQL(None, None, "operator", None, "postgres", None)
    with pytest.raises(PostgreSQLUndefinedHostError):
        pg._connect_to_database()

    # Error on no password
    pg = PostgreSQL("primary", "current", "operator", None, "postgres", None)
    with pytest.raises(PostgreSQLUndefinedPasswordError):
        pg._connect_to_database()

    # Returns connection
    pg = PostgreSQL("primary", "current", "operator", "password", "postgres", None)
    with patch(
        "single_kernel_postgresql.utils.postgresql.psycopg2.connect",
        return_value=sentinel.connection,
    ) as _connect:
        assert pg._connect_to_database() == sentinel.connection
        _connect.assert_called_once_with(
            "dbname='postgres' user='operator' host='primary'password='password' connect_timeout=1"
        )


def test_is_user_in_hba():
    with patch(
        "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database",
    ) as _connect_to_database:
        pg = PostgreSQL("primary", "current", "operator", "password", "postgres", None)
        _cursor = _connect_to_database().__enter__().cursor().__enter__()

        # No result
        _cursor.fetchone.return_value = None
        assert pg.is_user_in_hba("test-user") is False
        _cursor.execute.assert_called_once_with(
            Composed([
                SQL("SELECT COUNT(*) FROM pg_hba_file_rules WHERE "),
                Literal("test-user"),
                SQL(" = ANY(user_name);"),
            ])
        )

        # Exception
        _cursor.fetchone.side_effect = psycopg2.Error
        assert pg.is_user_in_hba("test-user") is False

        # Result
        _cursor.fetchone.side_effect = None
        _cursor.fetchone.return_value = (1,)
        assert pg.is_user_in_hba("test-user") is True


def test_drop_hba_triggers():
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database",
        ) as _connect_to_database,
        patch("single_kernel_postgresql.utils.postgresql.logger") as _logger,
    ):
        pg = PostgreSQL("primary", "current", "operator", "password", "postgres", None)
        _cursor = _connect_to_database().__enter__().cursor().__enter__()
        _cursor.fetchall.return_value = (("db1",), ("db2",))

        pg.drop_hba_triggers()

        assert _cursor.execute.call_count == 5
        _cursor.execute.assert_any_call(
            SQL(
                "SELECT datname FROM pg_database WHERE datname <> 'template0' AND datname <>'postgres';"
            )
        )
        _cursor.execute.assert_any_call(
            SQL("DROP EVENT TRIGGER IF EXISTS update_pg_hba_on_create_schema;")
        )
        _cursor.execute.assert_any_call(
            SQL("DROP EVENT TRIGGER IF EXISTS update_pg_hba_on_drop_schema;")
        )
        _cursor.execute.reset_mock()

        # Exception on select
        _cursor.execute.side_effect = psycopg2.Error

        pg.drop_hba_triggers()

        _cursor.execute.assert_called_once_with(
            SQL(
                "SELECT datname FROM pg_database WHERE datname <> 'template0' AND datname <>'postgres';"
            )
        )
        _logger.warning.assert_called_once_with(
            "Failed to get databases when removing hba trigger: "
        )
        _logger.warning.reset_mock()

        # Exception on drop
        _cursor.execute.side_effect = [None, psycopg2.Error, None, None]

        pg.drop_hba_triggers()

        _logger.warning.assert_called_once_with("Failed to remove hba trigger for db1: ")


def test_create_user():
    with (
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._connect_to_database",
        ) as _connect_to_database,
        patch(
            "single_kernel_postgresql.utils.postgresql.PostgreSQL._process_extra_user_roles",
        ) as _process_extra_user_roles,
    ):
        pg = PostgreSQL("primary", "current", "operator", "password", "postgres", None)
        _cursor = _connect_to_database().__enter__().cursor().__enter__()
        _process_extra_user_roles.return_value = (["role1", "role2"], ["priv1", "priv2"])

        # Create user
        _cursor.fetchone.return_value = None

        pg.create_user("username", "password")

        assert _cursor.execute.call_count == 8
        _cursor.execute.assert_any_call(
            Composed([
                SQL("SELECT TRUE FROM pg_roles WHERE rolname="),
                Literal("username"),
                SQL(";"),
            ])
        )
        _cursor.execute.assert_any_call(SQL("RESET ROLE;"))
        _cursor.execute.assert_any_call(SQL("BEGIN;"))
        _cursor.execute.assert_any_call(SQL("SET LOCAL log_statement = 'none';"))
        _cursor.execute.assert_any_call(
            Composed([
                SQL("CREATE ROLE "),
                Identifier("username"),
                SQL(" WITH LOGIN ENCRYPTED PASSWORD 'password' priv1 priv2;"),
            ])
        )
        _cursor.execute.assert_any_call(SQL("COMMIT;"))
        _cursor.execute.assert_any_call(
            Composed([
                SQL("GRANT "),
                Identifier("role1"),
                SQL(" TO "),
                Identifier("username"),
                SQL(";"),
            ])
        )
        _cursor.execute.assert_any_call(
            Composed([
                SQL("GRANT "),
                Identifier("role2"),
                SQL(" TO "),
                Identifier("username"),
                SQL(";"),
            ])
        )
        _cursor.execute.reset_mock()
        _process_extra_user_roles.reset_mock()

        # Alter user
        _cursor.fetchone.return_value = (1,)

        pg.create_user("username", "password", True, True, ["role3"], "db1", True)

        _process_extra_user_roles.assert_called_once_with("username", ["role3"])
        assert _cursor.execute.call_count == 8
        _cursor.execute.assert_any_call(
            Composed([
                SQL("SELECT TRUE FROM pg_roles WHERE rolname="),
                Literal("username"),
                SQL(";"),
            ])
        )
        _cursor.execute.assert_any_call(SQL("RESET ROLE;"))
        _cursor.execute.assert_any_call(SQL("BEGIN;"))
        _cursor.execute.assert_any_call(SQL("SET LOCAL log_statement = 'none';"))
        _cursor.execute.assert_any_call(
            Composed([
                SQL("ALTER ROLE "),
                Identifier("username"),
                SQL(
                    ' WITH LOGIN SUPERUSER REPLICATION ENCRYPTED PASSWORD \'password\' IN ROLE "charmed_db1_admin", "charmed_db1_dml" CREATEDB priv1 priv2;'
                ),
            ])
        )
        _cursor.execute.assert_any_call(SQL("COMMIT;"))
        _cursor.execute.assert_any_call(
            Composed([
                SQL("GRANT "),
                Identifier("role1"),
                SQL(" TO "),
                Identifier("username"),
                SQL(";"),
            ])
        )
        _cursor.execute.assert_any_call(
            Composed([
                SQL("GRANT "),
                Identifier("role2"),
                SQL(" TO "),
                Identifier("username"),
                SQL(";"),
            ])
        )

        # Exception
        _cursor.execute.side_effect = psycopg2.Error

        with pytest.raises(PostgreSQLCreateUserError):
            pg.create_user("username", "password")
