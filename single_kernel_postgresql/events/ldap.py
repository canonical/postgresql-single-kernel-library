#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""LDAP events handler — owns the ``ldap`` requirer relation and the LDAP auth parameters.

Ported from the PostgreSQL VM and K8s charms' LDAP module. The substrate-specific
LDAP-sync sidecar restart stays behind the charm's ``restart_services`` bridge.
"""

import logging
from typing import Any

from ops import Relation
from ops.framework import Object
from ops.model import ActiveStatus

from single_kernel_postgresql.config.literals import LDAP_RELATION
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.lib.charms.glauth_k8s.v0.ldap import (
    LdapProviderData,
    LdapReadyEvent,
    LdapRequirer,
    LdapUnavailableEvent,
)

logger = logging.getLogger(__name__)


class LDAP(Object):
    """In this class, we manage PostgreSQL LDAP access."""

    def __init__(self, charm, state: CharmState):
        super().__init__(charm, "ldap")
        self.charm = charm
        self.state = state

        # LDAP relation handles the config options for LDAP access
        self.ldap = LdapRequirer(self.charm, LDAP_RELATION)
        self.framework.observe(self.ldap.on.ldap_ready, self._on_ldap_ready)
        self.framework.observe(self.ldap.on.ldap_unavailable, self._on_ldap_unavailable)

    @property
    def _relation(self) -> Relation | None:
        """Return the relation object."""
        return self.model.get_relation(LDAP_RELATION)

    def _on_ldap_ready(self, _: LdapReadyEvent) -> None:
        """Handler for the LDAP ready event."""
        logger.debug("Enabling LDAP connection")
        if self.charm.unit.is_leader():
            self.state.application.data.update({"ldap_enabled": "True"})

        self.charm.update_config()
        self.charm.set_unit_status(ActiveStatus())

    def _on_ldap_unavailable(self, _: LdapUnavailableEvent) -> None:
        """Handler for the LDAP unavailable event."""
        logger.debug("Disabling LDAP connection")
        if self.charm.unit.is_leader():
            self.state.application.data.update({"ldap_enabled": "False"})

        self.charm.update_config()

    def get_relation_data(self) -> LdapProviderData | None:
        """Get the LDAP info from the LDAP Provider class."""
        data = self.ldap.consume_ldap_relation_data(relation=self._relation)
        if data is None:
            logger.warning("LDAP relation is not ready")

        if not self.state.peer.is_connectivity_enabled:
            logger.warning("LDAP server will not be accessible")

        return data

    def get_ldap_parameters(self) -> dict[str, Any]:
        """Returns the LDAP configuration to use."""
        if not self.state.application.is_cluster_initialised:
            return {}
        if not self.state.application.is_ldap_charm_related:
            logger.debug("LDAP is not enabled")
            return {}

        relation_data = self.get_relation_data()
        if relation_data is None:
            return {}

        return {
            "ldapbasedn": relation_data.base_dn,
            "ldapbinddn": relation_data.bind_dn,
            "ldapbindpasswd": relation_data.bind_password,
            "ldaptls": relation_data.starttls,
            "ldapurl": relation_data.urls[0],
            # LDAP authentication parameters that are exclusive to
            # one of the two supported modes (simple bind or search+bind)
            # must be put at the very end of the parameters string
            "ldapsearchfilter": self.state.config.ldap_search_filter,
        }
