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


def test_client_common_name_prefers_database_address(substrate, harness):
    _set_unit_db(harness, "database-address", "10.1.2.3")
    state = harness.charm.state
    if substrate == "vm":
        # VM: CN follows the database-address databag value.
        assert state.client_common_name == "10.1.2.3"
    else:
        # K8s: CN is the endpoints FQDN, not the databag address.
        app = state.model.app.name
        assert state.client_common_name == f"{app}-0.{app}-endpoints"


def test_client_common_name_falls_back_to_host(substrate, harness):
    state = harness.charm.state
    if substrate == "vm":
        # VM: falls back to host when no databag address is set.
        assert state.client_common_name == state.host
    else:
        # K8s: no fallback — always the endpoints FQDN.
        app = state.model.app.name
        assert state.client_common_name == f"{app}-0.{app}-endpoints"
