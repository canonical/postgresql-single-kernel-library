# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from ops.testing import Harness
from single_kernel_postgresql.charms import k8s_charm, vm_charm
from single_kernel_postgresql.config.literals import PEER_RELATION


@pytest.fixture
def harness(substrate, test_charm_path):
    """A begun Harness for the substrate's test charm, with the peer relation added."""
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
