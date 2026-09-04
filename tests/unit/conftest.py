# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import pathlib
import shutil
from unittest.mock import patch, sentinel

import pytest
from ops.testing import Harness
from single_kernel_postgresql.charms import k8s_charm, vm_charm
from single_kernel_postgresql.config.literals import PEER_RELATION


class _MockRefresh:
    """Stands in for charm_refresh.Machines/Kubernetes in every unit test."""

    in_progress = False
    next_unit_allowed_to_refresh = True
    workload_allowed_to_start = True
    app_status_higher_priority = None
    unit_status_higher_priority = None

    def __init__(self, _, /):
        pass

    def unit_status_lower_priority(self, *, workload_is_running=True):
        return None


@pytest.fixture(autouse=True)
def mock_refresh(request, monkeypatch):
    """Shunt the charm_refresh integration.

    The refresh manager constructs its charm_refresh object whenever a charm is
    instantiated, and charm_refresh reads refresh_versions.toml (pinned charm and
    workload versions) from the working directory, which the unit environment does
    not have. Patch the entry points and provide a minimal versions file; the file
    is backed up and restored when one already exists.
    """
    monkeypatch.setattr("charm_refresh.Machines", _MockRefresh)
    monkeypatch.setattr("charm_refresh.Kubernetes", _MockRefresh)

    # Anchor on the setup-time working directory: other fixtures may chdir for the
    # duration of a test, and the shared monkeypatch undo runs after this teardown.
    base = pathlib.Path.cwd()
    path = base / "refresh_versions.toml"
    backup = base / "refresh_versions.toml.backup"
    status_path = base / ".last_refresh_unit_status.json"
    existed = path.exists()
    if existed:
        shutil.copy(path, backup)
    # Overwrite with minimal pinned versions; tests never read the real pins (the
    # charm_refresh entry points and the workload version getter are patched).
    path.write_text('charm = "16/0.0.0"\nworkload = "16.0"\n')

    yield

    status_path.unlink(missing_ok=True)
    path.unlink(missing_ok=True)
    if existed:
        shutil.move(backup, path)


@pytest.fixture
def patch_crypto():
    """Stub the tls-crypto generators so leader-elected paths skip real RSA keygen.

    set_leader() fires _on_leader_elected -> configure_internal_peer_ca, which would
    otherwise run real generate_private_key/generate_ca. The dedicated generate_* tests
    patch with their own sentinels to assert call args; this fixture is for the
    incidental paths that only need internal-ca to be present, not its content.
    """
    with (
        patch(
            "single_kernel_postgresql.managers.tls.generate_private_key",
            return_value=sentinel.ca_key,
        ),
        patch(
            "single_kernel_postgresql.managers.tls.generate_ca",
            return_value=sentinel.ca,
        ),
    ):
        yield


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
    # Set before begin(): Model.name (K8s namespace) is read by substrate-aware
    # state accessors (e.g. common_hosts Service FQDNs).
    harness.set_model_name("test-model")
    # The workload's versioned paths (K8sPaths) read the major version via
    # get_postgresql_version(), which reads refresh_versions.toml from cwd — absent
    # in the unit env. Patch it, mirroring tests/unit/test_postgresql.py.
    with patch(
        "single_kernel_postgresql.workload.base.BaseWorkload.get_postgresql_version",
        return_value="16.0",
    ):
        harness.begin()
        yield harness
    harness.cleanup()
