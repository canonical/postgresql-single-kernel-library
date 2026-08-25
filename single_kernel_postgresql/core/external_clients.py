#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Typed access to the external-client request payload.

The requirer's side of the ``database`` relation is parsed through dpcharmlibs'
shared data-platform models — valkey-operator's route — so every request-field
read goes through one validated shape instead of loose databag strings. The
request is readable on every unit, unlike the provider-side fields, which stay
on ``DatabaseProvides`` as leader-only reads.
"""

import json
from typing import Annotated

from dpcharmlibs.interfaces import EntityPermissionModel, RequirerCommonModel
from pydantic import BeforeValidator


def _load_json_list(value: object) -> object:
    """Deserialize a JSON-encoded list, passing any other shape through.

    The v0 requirer writes list-shaped request fields (``requested-secrets``,
    ``entity-permissions``) as their JSON encoding, not as native lists.
    """
    if isinstance(value, str):
        return json.loads(value)
    return value


class ExternalClientRequest(RequirerCommonModel):
    """One external client's request as written on its side of the relation.

    ``resource`` carries the requested database through the shared model's
    alias set; the postgresql-specific request fields are declared here.
    """

    requested_secrets: Annotated[list[str] | None, BeforeValidator(_load_json_list)] = None
    requested_entity_secret: str | None = None
    prefix_matching: str | None = None
    entity_permissions: Annotated[
        list[EntityPermissionModel] | None, BeforeValidator(_load_json_list)
    ] = None

    @property
    def database(self) -> str:
        """The requested database."""
        return self.resource
