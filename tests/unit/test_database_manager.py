# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the client-relation DatabaseManager."""

from unittest.mock import Mock, PropertyMock, patch

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import (
    APP_SCOPE,
    DATABASE_MAPPING_LABEL,
    PEER_RELATION,
    USERNAME_MAPPING_LABEL,
)
from single_kernel_postgresql.utils.postgresql import (
    ACCESS_GROUP_RELATION,
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
