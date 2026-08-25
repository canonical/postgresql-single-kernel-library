# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Unit tests for the typed external-client request model."""

import json

import pytest
from single_kernel_postgresql.core.external_clients import ExternalClientRequest


def test_request_parses_the_v0_requirer_databag():
    """The wire names the requirer writes map onto the typed fields."""
    request = ExternalClientRequest.model_validate({
        "database": "test_db",
        "extra-user-roles": "admin,createdb",
        "requested-secrets": json.dumps(["read-only-uris"]),
        "external-node-connectivity": "true",
    })
    assert request.database == "test_db"
    assert request.extra_user_roles == "admin,createdb"
    assert request.requested_secrets == ["read-only-uris"]
    assert request.external_node_connectivity is True


def test_request_database_defaults_to_empty():
    """A relation without a request yields an empty resource, like the raw fetch did."""
    assert ExternalClientRequest.model_validate({}).database == ""


def test_request_parses_the_entity_secret_field():
    """Undeclared request fields ride along as extras without failing parsing."""
    request = ExternalClientRequest.model_validate({
        "requested-entity-secret": "secret:9m4e2mr0ui3e8a215n4g",
        "prefix-matching": "all",
    })
    assert request.requested_entity_secret == "secret:9m4e2mr0ui3e8a215n4g"


def test_request_requested_secrets_absent_is_none():
    assert ExternalClientRequest.model_validate({"database": "db"}).requested_secrets is None


@pytest.mark.parametrize("payload", [["read-only-uris"], []])
def test_request_round_trips_already_typed_secrets(payload):
    """A list payload parses too, not only the JSON string the wire carries."""
    request = ExternalClientRequest.model_validate({"requested-secrets": payload})
    assert request.requested_secrets == payload


def test_request_decodes_the_entity_permissions_wire_format():
    """The v0 wire encodes entity permissions as a JSON string, not a list."""
    request = ExternalClientRequest.model_validate({
        "entity-permissions": json.dumps([
            {"resource_name": "db1", "resource_type": "DATABASE", "privileges": ["SELECT"]},
        ]),
    })
    assert request.entity_permissions[0].resource_name == "db1"
    assert request.entity_permissions[0].privileges == ["SELECT"]
