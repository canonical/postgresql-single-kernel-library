# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""PoC: demonstrate the TLSManager is wired into the charm end-to-end.

This is a throwaway spike for the "wiring up" step. It harnesses the lib's
test charm (which wires TLSManager via AbstractPostgreSQLCharm +
PostgreSQLEventsHandler), fires `leader-elected`, and shows the wired manager
generates the internal peer CA into CharmState. No DB and no real workload are
needed (the manager writes CA/cert into state; the file-push is out of scope).
"""

from unittest.mock import patch

import pytest
from ops.testing import Harness
from single_kernel_postgresql.charms import k8s_charm, vm_charm
from single_kernel_postgresql.config.literals import APP_SCOPE, PEER_RELATION
from single_kernel_postgresql.managers.tls import TLSManager


@pytest.fixture(autouse=True)
def harness(substrate, test_charm_path):
    with open(test_charm_path + "/metadata.yaml") as meta_file:
        meta = meta_file.read()
    with open(test_charm_path + "/actions.yaml") as actions_file:
        actions = actions_file.read()
    if substrate == "vm":
        harness = Harness(vm_charm.PostgreSQLVMCharm, meta=meta, actions=actions)
    else:
        harness = Harness(k8s_charm.PostgreSQLK8sCharm, meta=meta, actions=actions)
    peer_rel_id = harness.add_relation(PEER_RELATION, "postgresql-single-kernel")
    harness.add_relation_unit(peer_rel_id, "postgresql-single-kernel/0")
    harness.begin()
    yield harness
    harness.cleanup()


def test_tls_manager_is_wired_into_the_charm(harness):
    """The abstract charm instantiates the manager and shares the same instance.

    Proves the wiring exists: charm -> TLSManager -> PostgreSQLEventsHandler.
    """
    charm = harness.charm
    assert isinstance(charm.tls_manager, TLSManager)
    assert charm.postgresql_events_handler.tls_manager is charm.tls_manager


def test_leader_elected_drives_the_wired_tls_manager(harness):
    """Firing leader-elected routes through the handler into the wired manager.

    `PostgreSQLEventsHandler._on_leader_elected` calls
    `tls_manager.configure_internal_peer_ca()`, which writes the internal CA
    secret via CharmState. Adjacent subsystems (system passwords, config) are
    patched out so this asserts only the TLS wiring.
    """
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)

    assert charm.state.get_secret(APP_SCOPE, "internal-ca")
    assert charm.state.get_secret(APP_SCOPE, "internal-ca-key")
