# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the client-relation DatabaseManager."""

import json
from unittest.mock import Mock, PropertyMock, patch

import pytest
from ops import ActiveStatus, BlockedStatus
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    DATABASE_MAPPING_LABEL,
    PEER_RELATION,
    USERNAME_MAPPING_LABEL,
)
from single_kernel_postgresql.managers.database import (
    FORBIDDEN_USER_MSG,
    NO_ACCESS_TO_SECRET_MSG,
    PREFIX_TOO_SHORT_MSG,
    DatabaseRequest,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_RELATION,
    PostgreSQLDeleteUserError,
    PostgreSQLListUsersError,
)

RELATION_NAME = "database"

CLUSTER_STATUS = [
    {"name": "postgresql-single-kernel-0", "role": "leader", "state": "running"},
    {"name": "postgresql-single-kernel-1", "role": "replica", "state": "running"},
    {"name": "postgresql-single-kernel-2", "role": "replica", "state": "running"},
]


@pytest.fixture
def manager(harness):
    """The DatabaseManager of a leader unit with three peers and a client relation."""
    with harness.hooks_disabled():
        harness.set_leader(True)
        peer_rel_id = harness.model.get_relation(PEER_RELATION).id
        harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/1")
        harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/2")
        harness.add_relation(RELATION_NAME, "application")
    return harness.charm.database_manager


@pytest.fixture
def postgresql():
    return Mock()


def test_relation_username_is_substrate_specific(substrate, manager):
    """VM and K8s keep the live role names their deployed clusters already hold."""
    expected = "relation_id_7" if substrate == "k8s" else "relation-7"
    assert manager.relation_username(7) == expected


def test_username_mapping_round_trip(manager):
    assert manager.get_username_mapping() == {}

    manager.update_username_mapping(4, "custom")
    assert manager.get_username_mapping() == {"4": "custom"}

    manager.update_username_mapping(4, None)
    assert manager.get_username_mapping() == {}


def test_update_username_mapping_ignores_the_generated_name(manager):
    """Only a custom username is cached; the derived one is recomputed on demand."""
    manager.update_username_mapping(4, manager.relation_username(4))
    assert manager.state.get_secret(APP_SCOPE, USERNAME_MAPPING_LABEL) is None


def test_set_databases_prefix_mapping_round_trip(manager):
    manager.set_databases_prefix_mapping(4, "custom", "pre", ["pre_a"])
    assert manager.get_databases_prefix_mapping() == {
        "4": {"prefix": "pre", "username": "custom", "databases": ["pre_a"]}
    }

    manager.set_databases_prefix_mapping(4, None, None, None)
    assert manager.get_databases_prefix_mapping() == {}


def test_add_database_to_prefix_mapping_appends_sorted_and_returns_users(manager):
    manager.set_databases_prefix_mapping(4, "custom", "pre", ["pre_b"])

    assert manager.add_database_to_prefix_mapping("pre_a") == ["custom"]
    assert manager.get_databases_prefix_mapping()["4"]["databases"] == ["pre_a", "pre_b"]

    # A database outside every prefix touches nothing.
    assert manager.add_database_to_prefix_mapping("other") == []
    assert manager.get_databases_prefix_mapping()["4"]["databases"] == ["pre_a", "pre_b"]


def test_remove_database_from_prefix_mapping(manager):
    manager.set_databases_prefix_mapping(4, "custom", "pre", ["pre_a", "pre_b"])

    assert manager.remove_database_from_prefix_mapping("pre_a") == ["custom"]
    assert manager.get_databases_prefix_mapping()["4"]["databases"] == ["pre_b"]

    assert manager.remove_database_from_prefix_mapping("absent") == []


def test_collect_user_relations_uses_the_custom_username_when_cached(manager, harness):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "test_db"})

    assert manager.collect_user_relations() == {manager.relation_username(rel_id): "test_db"}

    manager.update_username_mapping(rel_id, "custom")
    assert manager.collect_user_relations() == {"custom": "test_db"}


def test_collect_user_relations_skips_relations_without_a_database(manager):
    assert manager.collect_user_relations() == {}


def test_user_hash_freezes_for_the_manager_lifetime(manager, harness):
    """Both charms cached the hash per hook, so a mid-hook mapping change must not alter it."""
    frozen = manager.user_hash
    rel_id = harness.model.get_relation(RELATION_NAME).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "test_db"})

    assert manager.user_hash == frozen


def test_are_units_in_sync_compares_every_peer_unit(manager, harness):
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    expected = manager.user_hash

    with harness.hooks_disabled():
        for i in range(3):
            harness.update_relation_data(
                peer_rel_id, f"postgresql-single-kernel/{i}", {"user_hash": expected}
            )
    assert manager.are_units_in_sync() is True

    with harness.hooks_disabled():
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/2", {"user_hash": "stale"}
        )
    assert manager.are_units_in_sync() is False


def test_sanitize_extra_roles_drops_access_groups_and_lowercases(manager):
    assert manager.sanitize_extra_roles(None) == []
    assert manager.sanitize_extra_roles(f"CREATEDB,{ACCESS_GROUP_RELATION}") == ["createdb"]


def test_build_extra_user_roles_appends_the_relation_access_group(manager):
    assert manager.build_extra_user_roles("CREATEDB") == ["createdb", ACCESS_GROUP_RELATION]


def test_get_plugins_expands_overrides_and_spi(manager):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = Mock(
            plugin_keys=Mock(return_value=["plugin_audit_extension", "plugin_spi_extension"]),
            plugin_audit_extension=True,
            plugin_spi_extension=True,
        )
        plugins = manager.get_plugins()

    assert "pgaudit" in plugins
    assert "spi" not in plugins
    assert "refint" in plugins


def test_get_credentials_defaults_to_the_generated_user(manager, postgresql):
    request = DatabaseRequest(
        relation_id=4,
        database="db",
        extra_user_roles=None,
        prefix_matching=None,
        requested_entity_secret_content=None,
    )
    with patch("single_kernel_postgresql.managers.database.new_password", return_value="pw"):
        assert manager.get_credentials(postgresql, request) == (
            manager.relation_username(4),
            "pw",
        )


def test_get_credentials_blocks_on_an_existing_username(manager, postgresql, harness):
    postgresql.list_users.return_value = {"taken"}
    request = DatabaseRequest(
        relation_id=4,
        database="db",
        extra_user_roles=None,
        prefix_matching=None,
        requested_entity_secret_content={"taken": "pw"},
    )
    assert manager.get_credentials(postgresql, request) is None
    assert harness.model.unit.status == BlockedStatus(FORBIDDEN_USER_MSG)


def test_get_credentials_blocks_when_the_secret_is_not_readable(manager, postgresql, harness):
    request = DatabaseRequest(
        relation_id=4,
        database="db",
        extra_user_roles=None,
        prefix_matching=None,
        requested_entity_secret_content=Mock(items=Mock(side_effect=__import__("ops").ModelError)),
    )
    assert manager.get_credentials(postgresql, request) is None
    assert harness.model.unit.status == BlockedStatus(NO_ACCESS_TO_SECRET_MSG)


def test_collect_databases_plain_database(manager, postgresql):
    request = DatabaseRequest(
        relation_id=4,
        database="test_db",
        extra_user_roles=None,
        prefix_matching=None,
        requested_entity_secret_content=None,
    )
    assert manager.collect_databases(postgresql, "user", request) == ("test_db", ["test_db"])


def test_collect_databases_prefix_lists_and_caches(manager, postgresql):
    postgresql.list_databases.return_value = ["pre_b", "pre_a"]
    request = DatabaseRequest(
        relation_id=4,
        database="pre*",
        extra_user_roles=None,
        prefix_matching="all",
        requested_entity_secret_content=None,
    )
    assert manager.collect_databases(postgresql, "user", request) == ("pre*", ["pre_a", "pre_b"])
    postgresql.list_databases.assert_called_once_with("pre")
    assert manager.get_databases_prefix_mapping()["4"]["prefix"] == "pre"


def test_collect_databases_blocks_on_a_too_short_prefix(manager, postgresql, harness):
    request = DatabaseRequest(
        relation_id=4,
        database="a*",
        extra_user_roles=None,
        prefix_matching=None,
        requested_entity_secret_content=None,
    )
    assert manager.collect_databases(postgresql, "user", request) is None
    assert harness.model.unit.status == BlockedStatus(PREFIX_TOO_SHORT_MSG)
    postgresql.list_databases.assert_not_called()


def test_create_relation_user_and_database_plain(manager, postgresql):
    with patch.object(type(manager), "get_plugins", return_value=["pgaudit"]):
        manager.create_relation_user_and_database(
            postgresql, "user", "pw", "test_db", ["test_db"], ["createdb"]
        )
    postgresql.create_database.assert_called_once_with("test_db", plugins=["pgaudit"])
    postgresql.create_user.assert_called_once_with(
        "user", "pw", extra_user_roles=["createdb"], database="test_db"
    )


def test_create_relation_user_and_database_prefixed_skips_create_database(manager, postgresql):
    with patch.object(type(manager), "get_plugins", return_value=[]):
        manager.create_relation_user_and_database(
            postgresql, "user", "pw", "pre*", ["pre_a"], ["createdb"]
        )
    postgresql.create_database.assert_not_called()
    postgresql.create_user.assert_called_once_with("user", "pw", extra_user_roles=["createdb"])
    postgresql.add_user_to_databases.assert_called_once_with("user", ["pre_a"], ["createdb"])


def test_delete_relation_user_clears_every_mapping(manager, postgresql, harness):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    manager.update_username_mapping(rel_id, "custom")
    manager.set_databases_prefix_mapping(rel_id, "custom", "pre", ["pre_a"])
    harness.charm.app_data_setter = None
    manager.state.application.data["rel_databases"] = json.dumps({str(rel_id): "pre_a"})

    manager.delete_relation_user(postgresql, rel_id)

    postgresql.delete_user.assert_called_once_with("custom")
    assert manager.get_username_mapping() == {}
    assert manager.get_databases_prefix_mapping() == {}


def test_delete_relation_user_blocks_when_the_drop_fails(manager, postgresql, harness):
    postgresql.delete_user.side_effect = PostgreSQLDeleteUserError
    manager.delete_relation_user(postgresql, 4)
    assert isinstance(harness.model.unit.status, BlockedStatus)


def test_oversee_users_deletes_only_users_without_a_relation(manager, postgresql, harness):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    stale = manager.relation_username(rel_id + 100)
    postgresql.list_users.return_value = {
        manager.relation_username(rel_id),
        stale,
        "postgres",
    }

    manager.oversee_users(postgresql)

    postgresql.delete_user.assert_called_once_with(stale)


def test_oversee_users_honours_the_suppress_flag(manager, postgresql):
    postgresql.list_users.return_value = {manager.relation_username(999)}
    manager.state.application.data["suppress-oversee-users"] = "True"

    manager.oversee_users(postgresql)

    postgresql.delete_user.assert_not_called()


def test_oversee_users_early_exits_when_users_cannot_be_listed(manager, postgresql):
    postgresql.list_users.side_effect = PostgreSQLListUsersError
    manager.oversee_users(postgresql)
    postgresql.delete_user.assert_not_called()


def test_oversee_users_is_leader_only(manager, postgresql, harness):
    with harness.hooks_disabled():
        harness.set_leader(False)
    manager.oversee_users(postgresql)
    postgresql.list_users.assert_not_called()


def test_check_for_invalid_extra_user_roles_skips_the_current_relation(
    manager, postgresql, harness
):
    postgresql.list_valid_privileges_and_roles.return_value = ({"createrole"}, set())
    rel_id = harness.model.get_relation(RELATION_NAME).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"extra-user-roles": "bogus"})

    assert manager.check_for_invalid_extra_user_roles(postgresql, rel_id) is False
    assert manager.check_for_invalid_extra_user_roles(postgresql, rel_id + 100) is True


def test_check_for_invalid_extra_user_roles_createdb_is_vm_only(
    substrate, manager, postgresql, harness
):
    """VM accepts createdb even though PostgreSQL does not report it as a role."""
    postgresql.list_valid_privileges_and_roles.return_value = (set(), set())
    rel_id = harness.model.get_relation(RELATION_NAME).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"extra-user-roles": "createdb"})

    invalid = manager.check_for_invalid_extra_user_roles(postgresql, rel_id + 100)
    assert invalid is (substrate == "k8s")


def test_check_for_invalid_database_name(manager, harness):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "a" * 50})

    assert manager.check_for_invalid_database_name(rel_id) is False
    assert manager.check_for_invalid_database_name(rel_id + 100) is True


def test_update_endpoints_is_leader_only(manager, harness):
    with harness.hooks_disabled():
        harness.set_leader(False)
    with patch.object(manager, "database_provides") as _provides:
        manager.update_endpoints()
    _provides.fetch_relation_data.assert_not_called()


def test_update_endpoints_publishes_rw_ro_and_uris(substrate, manager, harness):
    rel_id = harness.model.get_relation(RELATION_NAME).id
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "test_db"})
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/0", {f"{RELATION_NAME}-address": "1.1.1.1"}
        )
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/1", {f"{RELATION_NAME}-address": "2.2.2.2"}
        )

    with (
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.cluster_status",
            return_value=CLUSTER_STATUS[:2],
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.is_tls_enabled",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            manager.database_provides,
            "fetch_my_relation_data",
            return_value={rel_id: {"username": "u", "password": "pw"}},
        ),
    ):
        manager.update_endpoints(rel_id)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    if substrate == "k8s":
        app = harness.charm.app.name
        assert data["endpoints"] == f"{app}-primary.test-model.svc.cluster.local:5432"
        assert data["read-only-endpoints"] == f"{app}-replicas.test-model.svc.cluster.local:5432"
    else:
        assert data["endpoints"] == "1.1.1.1:5432"
        assert data["read-only-endpoints"] == "2.2.2.2:5432"
    assert data["tls"] == "False"


def test_update_endpoints_read_only_uri_carries_one_port(substrate, manager, harness):
    """The read-only URI is built from the ro hosts, so the port is not doubled."""
    rel_id = harness.model.get_relation(RELATION_NAME).id
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(
            rel_id,
            "application",
            {"database": "test_db", "requested-secrets": json.dumps(["read-only-uris"])},
        )
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/0", {f"{RELATION_NAME}-address": "1.1.1.1"}
        )

    with (
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.cluster_status",
            return_value=CLUSTER_STATUS[:1],
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.is_tls_enabled",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            manager.database_provides,
            "fetch_my_relation_data",
            return_value={rel_id: {"username": "u", "password": "pw"}},
        ),
        patch.object(manager.database_provides, "set_read_only_uris") as _set_ro_uris,
    ):
        manager.update_endpoints(rel_id)

    _set_ro_uris.assert_called_once()
    uri = _set_ro_uris.call_args.args[1]
    assert uri.endswith(":5432/test_db")
    assert ":5432:5432" not in uri


def test_update_endpoints_falls_back_to_the_primary_without_replicas(manager, harness, substrate):
    if substrate == "k8s":
        pytest.skip("K8s reads the replicas Service, not the online Patroni members")
    rel_id = harness.model.get_relation(RELATION_NAME).id
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "test_db"})
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/0", {f"{RELATION_NAME}-address": "1.1.1.1"}
        )

    with (
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.cluster_status",
            return_value=CLUSTER_STATUS[:1],
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.is_tls_enabled",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            manager.database_provides,
            "fetch_my_relation_data",
            return_value={rel_id: {"username": "u", "password": "pw"}},
        ),
    ):
        manager.update_endpoints(rel_id)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert data["read-only-endpoints"] == "1.1.1.1:5432"


def test_update_endpoints_filters_out_of_sync_members(manager, harness, substrate):
    if substrate == "k8s":
        pytest.skip("K8s reads the replicas Service, not the online Patroni members")
    rel_id = harness.model.get_relation(RELATION_NAME).id
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    with harness.hooks_disabled():
        harness.update_relation_data(rel_id, "application", {"database": "test_db"})
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/0", {f"{RELATION_NAME}-address": "1.1.1.1"}
        )
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/1", {f"{RELATION_NAME}-address": "2.2.2.2"}
        )
        harness.update_relation_data(
            peer_rel_id, "postgresql-single-kernel/2", {f"{RELATION_NAME}-address": "3.3.3.3"}
        )

    nosync = [*CLUSTER_STATUS[:2], {**CLUSTER_STATUS[2], "tags": {"nosync": True}}]
    with (
        patch(
            "single_kernel_postgresql.managers.patroni.PatroniManager.cluster_status",
            return_value=nosync,
        ),
        patch(
            "single_kernel_postgresql.managers.database.DatabaseManager.is_tls_enabled",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch.object(
            manager.database_provides,
            "fetch_my_relation_data",
            return_value={rel_id: {"username": "u", "password": "pw"}},
        ),
    ):
        manager.update_endpoints(rel_id)

    data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert data["read-only-endpoints"] == "2.2.2.2:5432"


def test_update_unit_status_clears_a_failed_init_block(manager, postgresql, harness):
    postgresql.list_valid_privileges_and_roles.return_value = (set(), set())
    relation = harness.model.get_relation(RELATION_NAME)
    harness.model.unit.status = BlockedStatus("Failed to initialize database relation")

    manager.update_unit_status(postgresql, relation)

    assert harness.model.unit.status == ActiveStatus()


def test_update_unit_status_keeps_a_prefix_block_while_a_short_prefix_remains(
    manager, postgresql, harness
):
    postgresql.list_valid_privileges_and_roles.return_value = (set(), set())
    relation = harness.model.get_relation(RELATION_NAME)
    with harness.hooks_disabled():
        harness.update_relation_data(relation.id, "application", {"database": "a*"})
    harness.model.unit.status = BlockedStatus(PREFIX_TOO_SHORT_MSG)

    manager.update_unit_status(postgresql, relation)

    assert harness.model.unit.status == BlockedStatus(PREFIX_TOO_SHORT_MSG)


def test_is_tls_enabled_tracks_the_client_files(manager):
    with patch.object(manager.tls_manager, "get_client_tls_files", return_value=("k", "ca", "c")):
        assert manager.is_tls_enabled is True
    with patch.object(manager.tls_manager, "get_client_tls_files", return_value=("k", None, "c")):
        assert manager.is_tls_enabled is False


def test_prefix_mapping_secret_labels_are_stable(manager):
    """Deployed clusters hold these caches under these labels; renaming orphans them."""
    manager.update_username_mapping(4, "custom")
    manager.set_databases_prefix_mapping(4, "custom", "pre", [])

    assert manager.state.get_secret(APP_SCOPE, USERNAME_MAPPING_LABEL) is not None
    assert manager.state.get_secret(APP_SCOPE, DATABASE_MAPPING_LABEL) is not None


def test_request_errors_are_substrate_specific(substrate, manager):
    from single_kernel_postgresql.utils.postgresql import PostgreSQLBaseError

    if substrate == "k8s":
        assert PostgreSQLBaseError not in manager.request_errors
    else:
        assert manager.request_errors == (PostgreSQLBaseError,)


def test_manager_state_substrate_matches_the_charm(substrate, manager):
    expected = Substrates.K8S if substrate == "k8s" else Substrates.VM
    assert manager.state.substrate == expected
