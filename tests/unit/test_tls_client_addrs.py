# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for TLS client address and common-name state accessors."""


def _set_unit_db(harness, key, value):
    rel_id = harness.model.get_relation("database-peers").id
    harness.update_relation_data(rel_id, harness.charm.unit.name, {key: value})


def test_database_address_reads_unit_databag(harness):
    _set_unit_db(harness, "database-address", "10.1.2.3")
    assert harness.charm.state.peer.database_address == "10.1.2.3"


def test_database_address_none_when_unset(harness):
    assert harness.charm.state.peer.database_address is None


def test_client_addresses_set(harness):
    _set_unit_db(harness, "database-address", "10.1.2.3")
    assert harness.charm.state.client_addresses == {"10.1.2.3"}


def test_client_addresses_empty_when_unset(harness):
    assert harness.charm.state.client_addresses == set()


def test_client_common_name_prefers_database_address(harness):
    _set_unit_db(harness, "database-address", "10.1.2.3")
    assert harness.charm.state.client_common_name == "10.1.2.3"


def test_client_common_name_falls_back_to_host(harness):
    assert harness.charm.state.client_common_name == harness.charm.state.host
