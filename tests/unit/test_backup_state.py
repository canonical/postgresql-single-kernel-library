#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the backup coordination state on peer relation data.

These accessors port the backup module's app_peer_data / unit_peer_data fields
verbatim (same databag labels the charms wrote): stanza initialization and
restore coordination on PostgreSQLApplication, per-unit stanza/S3 markers on
PostgreSQLPeer, and the CharmState cross-scope stanza reader that survives
clusters whose stanza was only published by the primary unit so far.
"""

import pytest
from single_kernel_postgresql.compat.postgresql import PostgreSQLBaseError
from single_kernel_postgresql.config.exceptions import ListBackupsError


def _get_unit_db(harness, key):
    rel_id = harness.model.get_relation("database-peers").id
    return harness.get_relation_data(rel_id, harness.charm.unit.name).get(key)


def _get_app_db(harness, key):
    rel_id = harness.model.get_relation("database-peers").id
    return harness.get_relation_data(rel_id, harness.charm.app.name).get(key)


def _add_remote_unit(harness, unit_name):
    rel_id = harness.model.get_relation("database-peers").id
    harness.add_relation_unit(rel_id, unit_name)
    return rel_id


# -- PostgreSQLApplication backup accessors -----------------------------------


@pytest.mark.parametrize(
    "accessor,label",
    [
        ("stanza", "stanza"),
        ("s3_initialization_start", "s3-initialization-start"),
        ("s3_initialization_block_message", "s3-initialization-block-message"),
        ("s3_initialization_done", "s3-initialization-done"),
        ("restoring_backup", "restoring-backup"),
        ("restore_stanza", "restore-stanza"),
        ("restore_timeline", "restore-timeline"),
        ("restore_to_time", "restore-to-time"),
    ],
)
def test_app_backup_accessor_writes_charm_label(harness, accessor, label):
    """Every app accessor writes the exact label the charms wrote."""
    application = harness.charm.state.application
    setattr(application, accessor, "value")
    assert _get_app_db(harness, label) == "value"


@pytest.mark.parametrize(
    "accessor",
    [
        "stanza",
        "s3_initialization_start",
        "s3_initialization_block_message",
        "s3_initialization_done",
        "restoring_backup",
        "restore_stanza",
        "restore_timeline",
        "restore_to_time",
    ],
)
def test_app_backup_accessor_unset_is_none(harness, accessor):
    assert getattr(harness.charm.state.application, accessor) is None


@pytest.mark.parametrize(
    "accessor",
    [
        "stanza",
        "s3_initialization_start",
        "s3_initialization_block_message",
        "s3_initialization_done",
        "restoring_backup",
        "restore_stanza",
        "restore_timeline",
        "restore_to_time",
    ],
)
def test_app_backup_accessor_roundtrip_and_clear(harness, accessor):
    """Set → get → clear; the charm clears by writing "", which ops drops."""
    application = harness.charm.state.application
    setattr(application, accessor, "value")
    assert getattr(application, accessor) == "value"
    setattr(application, accessor, "")
    assert getattr(application, accessor) is None


# -- PostgreSQLPeer backup accessors ------------------------------------------


@pytest.mark.parametrize(
    "accessor,label",
    [
        ("stanza", "stanza"),
        ("s3_initialization_done", "s3-initialization-done"),
        ("s3_initialization_block_message", "s3-initialization-block-message"),
    ],
)
def test_peer_backup_accessor_writes_charm_label(harness, accessor, label):
    """Every unit accessor writes the exact label the charms wrote."""
    peer = harness.charm.state.peer
    setattr(peer, accessor, "value")
    assert _get_unit_db(harness, label) == "value"


@pytest.mark.parametrize(
    "accessor",
    [
        "stanza",
        "s3_initialization_done",
        "s3_initialization_block_message",
    ],
)
def test_peer_backup_accessor_roundtrip_and_clear(harness, accessor):
    peer = harness.charm.state.peer
    setattr(peer, accessor, "value")
    assert getattr(peer, accessor) == "value"
    setattr(peer, accessor, "")
    assert getattr(peer, accessor) is None


# -- Connectivity --------------------------------------------------------------


def test_connectivity_enabled_by_default(harness):
    assert harness.charm.state.peer.is_connectivity_enabled is True
    assert _get_unit_db(harness, "connectivity") is None


def test_connectivity_setter_disables(harness):
    harness.charm.state.peer.is_connectivity_enabled = False
    assert _get_unit_db(harness, "connectivity") == "off"
    assert harness.charm.state.peer.is_connectivity_enabled is False


def test_connectivity_setter_reenables(harness):
    peer = harness.charm.state.peer
    peer.is_connectivity_enabled = False
    peer.is_connectivity_enabled = True
    assert _get_unit_db(harness, "connectivity") == "on"
    assert peer.is_connectivity_enabled is True


# -- CharmState.cluster_stanza (cross-scope reader) ----------------------------


def test_cluster_stanza_none_when_unset(harness):
    assert harness.charm.state.cluster_stanza is None


def test_cluster_stanza_prefers_app_databag(harness):
    """Once the leader adopted the stanza, the app copy wins over unit copies."""
    rel_id = _add_remote_unit(harness, "postgresql-single-kernel/1")
    harness.update_relation_data(rel_id, "postgresql-single-kernel/1", {"stanza": "unit-stanza"})
    harness.charm.state.application.stanza = "app-stanza"
    assert harness.charm.state.cluster_stanza == "app-stanza"


def test_cluster_stanza_falls_back_to_publishing_unit(harness):
    """Mid-initialization the stanza lives only on the primary's unit databag."""
    rel_id = _add_remote_unit(harness, "postgresql-single-kernel/1")
    harness.update_relation_data(rel_id, "postgresql-single-kernel/1", {"stanza": "unit-stanza"})
    assert harness.charm.state.cluster_stanza == "unit-stanza"


def test_cluster_stanza_reads_own_unit_databag(harness):
    harness.charm.state.peer.stanza = "own-unit-stanza"
    assert harness.charm.state.cluster_stanza == "own-unit-stanza"


def test_cluster_stanza_ignores_units_without_stanza(harness):
    """A unit carrying only S3 markers (no stanza yet) is skipped."""
    rel_id = _add_remote_unit(harness, "postgresql-single-kernel/1")
    harness.update_relation_data(
        rel_id, "postgresql-single-kernel/1", {"s3-initialization-done": "True"}
    )
    assert harness.charm.state.cluster_stanza is None


# -- ListBackupsError ----------------------------------------------------------


def test_list_backups_error_inherits_postgresql_base_error():
    assert issubclass(ListBackupsError, PostgreSQLBaseError)


def test_list_backups_error_is_raisable_and_catchable_as_base():
    with pytest.raises(PostgreSQLBaseError):
        raise ListBackupsError("pgbackrest list failed")
