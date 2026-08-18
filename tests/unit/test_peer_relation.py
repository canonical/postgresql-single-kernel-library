#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the config/user hash + tls plain-databag accessors on peer state.

These accessors scaffold the config-subsystem port: PostgreSQLPeer.config_hash
and .user_hash persist the last-applied config/user hashes, PostgreSQLPeer.tls
persists the last-rendered TLS flag, and PostgreSQLApplication.user_hash
persists the app-wide user hash. All are plain (non-secret) databag values,
matching the charm's unit_peer_data / app_peer_data storage.
"""


def _get_unit_db(harness, key):
    rel_id = harness.model.get_relation("database-peers").id
    return harness.get_relation_data(rel_id, harness.charm.unit.name).get(key)


def _get_app_db(harness, key):
    rel_id = harness.model.get_relation("database-peers").id
    return harness.get_relation_data(rel_id, harness.charm.app.name).get(key)


# -- PostgreSQLPeer.config_hash ----------------------------------------------


def test_config_hash_unset_is_none(harness):
    assert harness.charm.state.peer.config_hash is None


def test_config_hash_roundtrip(harness):
    peer = harness.charm.state.peer
    peer.config_hash = "abc123"
    assert peer.config_hash == "abc123"


def test_config_hash_writes_exact_databag_key(harness):
    harness.charm.state.peer.config_hash = "abc123"
    assert _get_unit_db(harness, "config_hash") == "abc123"


# -- PostgreSQLPeer.user_hash -------------------------------------------------


def test_peer_user_hash_unset_is_none(harness):
    assert harness.charm.state.peer.user_hash is None


def test_peer_user_hash_roundtrip(harness):
    peer = harness.charm.state.peer
    peer.user_hash = "def456"
    assert peer.user_hash == "def456"


def test_peer_user_hash_writes_exact_databag_key(harness):
    harness.charm.state.peer.user_hash = "def456"
    assert _get_unit_db(harness, "user_hash") == "def456"


# -- PostgreSQLPeer.tls --------------------------------------------------------


def test_tls_unset_is_false(harness):
    assert harness.charm.state.peer.tls is False


def test_tls_set_true_roundtrip(harness):
    peer = harness.charm.state.peer
    peer.tls = True
    assert peer.tls is True


def test_tls_set_true_writes_enabled(harness):
    harness.charm.state.peer.tls = True
    assert _get_unit_db(harness, "tls") == "enabled"


def test_tls_set_false_roundtrip(harness):
    peer = harness.charm.state.peer
    peer.tls = True
    peer.tls = False
    assert peer.tls is False


def test_tls_set_false_clears_enabled_key(harness):
    """tls=False writes "", which ops relation data drops on write — observed as key absence."""
    peer = harness.charm.state.peer
    peer.tls = True
    peer.tls = False
    assert _get_unit_db(harness, "tls") is None


# -- PostgreSQLApplication.user_hash ------------------------------------------


def test_app_user_hash_unset_is_none(harness):
    assert harness.charm.state.application.user_hash is None


def test_app_user_hash_roundtrip(harness):
    application = harness.charm.state.application
    application.user_hash = "ghi789"
    assert application.user_hash == "ghi789"


def test_app_user_hash_writes_exact_databag_key(harness):
    harness.charm.state.application.user_hash = "ghi789"
    assert _get_app_db(harness, "user_hash") == "ghi789"
