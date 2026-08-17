# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the pgBackRest stanza and restore/PITR state accessors.

The stanza is coordinated between the leader (app databag) and the primary
(unit databag), so the same key name lives in both scopes and the read side has
to fall back from app to unit.
"""

import pytest


def _set_app_db(harness, values):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.app.name, values)


def _set_unit_db(harness, values):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, values)


# -- app scope ---------------------------------------------------------------

APP_FIELDS = [
    ("stanza", "stanza", "test-model.postgresql"),
    ("restore_stanza", "restore-stanza", "old-model.postgresql"),
    ("restoring_backup", "restoring-backup", "20260814-101112F"),
    ("restore_to_time", "restore-to-time", "2026-08-14 10:11:12"),
    ("restore_timeline", "restore-timeline", "3"),
    ("s3_initialization_start", "s3-initialization-start", "Thu Aug 14 10:11:12 2026"),
    ("s3_initialization_done", "s3-initialization-done", "True"),
    ("s3_initialization_block_message", "s3-initialization-block-message", "bad bucket"),
]


@pytest.mark.parametrize(("attribute", "key", "value"), APP_FIELDS)
def test_app_backup_field_defaults_to_none(harness, attribute, key, value):
    assert getattr(harness.charm.state.application, attribute) is None


@pytest.mark.parametrize(("attribute", "key", "value"), APP_FIELDS)
def test_app_backup_field_reads_its_own_key(harness, attribute, key, value):
    _set_app_db(harness, {key: value})

    assert getattr(harness.charm.state.application, attribute) == value


@pytest.mark.parametrize(("attribute", "key", "value"), APP_FIELDS)
def test_app_backup_field_writes_only_its_own_key(harness, attribute, key, value):
    """The setter must touch its own key and leave every sibling key alone."""
    with harness.hooks_disabled():
        harness.set_leader(True)
    app = harness.charm.state.application
    before = dict(app.data)

    setattr(app, attribute, value)

    assert dict(app.data) == before | {key: value}


def test_app_backup_fields_are_independent(harness):
    """Every app accessor must read a distinct key, not share one."""
    _set_app_db(harness, {key: value for _, key, value in APP_FIELDS})
    app = harness.charm.state.application

    assert [getattr(app, attribute) for attribute, _, _ in APP_FIELDS] == [
        value for _, _, value in APP_FIELDS
    ]


# -- unit scope --------------------------------------------------------------

UNIT_FIELDS = [
    ("stanza", "stanza", "test-model.postgresql"),
    ("s3_initialization_done", "s3-initialization-done", "True"),
    ("s3_initialization_block_message", "s3-initialization-block-message", "bad stanza"),
    ("last_pitr_fail_id", "last_pitr_fail_id", "2026-08-14 10:11:12 UTC"),
    ("rotate_logs_pid", "rotate-logs-pid", "4242"),
]


@pytest.mark.parametrize(("attribute", "key", "value"), UNIT_FIELDS)
def test_unit_backup_field_defaults_to_none(harness, attribute, key, value):
    assert getattr(harness.charm.state.peer, attribute) is None


@pytest.mark.parametrize(("attribute", "key", "value"), UNIT_FIELDS)
def test_unit_backup_field_reads_its_own_key(harness, attribute, key, value):
    _set_unit_db(harness, {key: value})

    assert getattr(harness.charm.state.peer, attribute) == value


@pytest.mark.parametrize(("attribute", "key", "value"), UNIT_FIELDS)
def test_unit_backup_field_writes_only_its_own_key(harness, attribute, key, value):
    """The setter must touch its own key and leave every sibling key alone."""
    peer = harness.charm.state.peer
    before = dict(peer.data)

    setattr(peer, attribute, value)

    assert dict(peer.data) == before | {key: value}


def test_unit_backup_fields_are_independent(harness):
    """Every unit accessor must read a distinct key, not share one."""
    _set_unit_db(harness, {key: value for _, key, value in UNIT_FIELDS})
    peer = harness.charm.state.peer

    assert [getattr(peer, attribute) for attribute, _, _ in UNIT_FIELDS] == [
        value for _, _, value in UNIT_FIELDS
    ]


# -- restore-in-progress views -----------------------------------------------

RESTORE_VIEWS = [
    ("is_cluster_restoring_backup", "restoring-backup"),
    ("is_cluster_restoring_to_time", "restore-to-time"),
]


@pytest.mark.parametrize(("view", "key"), RESTORE_VIEWS)
def test_restore_view_is_false_without_its_key(harness, view, key):
    assert getattr(harness.charm.state.application, view) is False


@pytest.mark.parametrize(("view", "key"), RESTORE_VIEWS)
def test_restore_view_tracks_only_its_own_key(harness, view, key):
    """Each view must answer for its own key, not for whichever restore field is set."""
    _set_app_db(harness, {key: "set"})
    app = harness.charm.state.application

    assert [getattr(app, name) for name, _ in RESTORE_VIEWS] == [
        name == view for name, _ in RESTORE_VIEWS
    ]


# -- cross-scope stanza view -------------------------------------------------


def test_stanza_prefers_the_app_databag(harness):
    _set_app_db(harness, {"stanza": "from-leader"})
    _set_unit_db(harness, {"stanza": "from-primary"})

    assert harness.charm.state.stanza == "from-leader"


def test_stanza_falls_back_to_the_unit_databag(harness):
    _set_unit_db(harness, {"stanza": "from-primary"})

    assert harness.charm.state.stanza == "from-primary"


def test_stanza_is_none_when_neither_scope_has_it(harness):
    assert harness.charm.state.stanza is None
