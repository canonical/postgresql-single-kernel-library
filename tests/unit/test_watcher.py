# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the watcher events handler (single_kernel_postgresql/events/watcher.py).

Ported from the VM charm's test_watcher_relation.py. The watcher relation is
VM-only, so every test skips on the k8s substrate.
"""

from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from ops import SecretNotFoundError
from single_kernel_postgresql.config.literals import (
    PEER_RELATION,
    WATCHER_OFFER_RELATION,
    WATCHER_SECRET_LABEL,
)


@pytest.fixture(autouse=True)
def vm_only(substrate):
    """The watcher relation is declared only by the VM charm's metadata."""
    if substrate == "k8s":
        pytest.skip("the watcher relation is VM-only")


def _set_leader(harness):
    """Elect the unit as leader, skipping the incidental leader-elected side effects."""
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)


@pytest.fixture
def update_config(harness):
    """Patch the charm's update_config bridge for the watcher-driven renders."""
    with patch.object(type(harness.charm), "update_config", Mock(return_value=True)) as mocked:
        yield mocked


@pytest.fixture
def postgresql(harness):
    """Patch the charm's postgresql client with a mock."""
    client = Mock()
    client.list_users.return_value = set()
    client.get_postgresql_version.return_value = "16"
    with patch.object(type(harness.charm), "postgresql", PropertyMock(return_value=client)):
        yield client


def _initialise(harness):
    """Mark the cluster as initialised."""
    peer_rel_id = harness.model.get_relation(PEER_RELATION).id
    harness.update_relation_data(
        peer_rel_id, harness.charm.app.name, {"cluster_initialised": "True"}
    )


def _add_watcher_relation(harness, app_name="postgresql-watcher") -> int:
    """Add a watcher-offer relation with a remote unit, without firing hooks."""
    with harness.hooks_disabled():
        rel_id = harness.add_relation(WATCHER_OFFER_RELATION, app_name)
        harness.add_relation_unit(rel_id, f"{app_name}/0")
    # The handler caches _relation/is_active; drop the caches so lookups see the new relation.
    harness.charm.watcher.__dict__.pop("_relation", None)
    harness.charm.watcher.__dict__.pop("is_active", None)
    harness.charm.watcher.__dict__.pop("watcher_raft_address", None)
    return rel_id


def _add_watcher_secret(harness) -> str:
    """Create the watcher secret the charm would have created (an owned secret)."""
    return harness.charm.app.add_secret(
        content={"raft-password": "raft-password"}, label=WATCHER_SECRET_LABEL
    ).id


def test_watcher_raft_address_without_relation_is_none(harness):
    assert harness.charm.watcher.watcher_raft_address is None


def test_watcher_raft_address_with_relation(harness):
    rel_id = _add_watcher_relation(harness)
    harness.update_relation_data(rel_id, "postgresql-watcher/0", {"unit-address": "10.0.0.10"})
    harness.update_relation_data(rel_id, "postgresql-watcher", {"watcher-raft-port": "2222"})

    assert harness.charm.watcher.watcher_raft_address == "10.0.0.10:2222"


def test_watcher_raft_address_with_invalid_port(harness):
    rel_id = _add_watcher_relation(harness)
    harness.update_relation_data(rel_id, "postgresql-watcher/0", {"unit-address": "10.0.0.10"})
    harness.update_relation_data(rel_id, "postgresql-watcher", {"watcher-raft-port": "http"})

    assert harness.charm.watcher.watcher_raft_address is None


def test_is_active_requires_connected_raft_status(harness):
    rel_id = _add_watcher_relation(harness)

    assert harness.charm.watcher.is_active is False

    harness.update_relation_data(rel_id, "postgresql-watcher", {"raft-status": "connected"})
    # is_active is cached; drop the cache so the re-read sees the new databag.
    harness.charm.watcher.__dict__.pop("is_active", None)
    assert harness.charm.watcher.is_active is True


def test_on_watcher_relation_joined_not_leader(harness, update_config, postgresql):
    _add_watcher_relation(harness)

    with (
        patch.object(harness.charm.watcher, "update_unit_address") as update_unit_address,
        patch.object(harness.charm.watcher, "_get_or_create_watcher_secret") as mock_secret,
    ):
        harness.charm.watcher._on_watcher_relation_joined(
            MagicMock(relation=harness.model.get_relation(WATCHER_OFFER_RELATION))
        )
        update_unit_address.assert_called_once()
        mock_secret.assert_not_called()


def test_on_watcher_relation_joined_leader_creates_secret(harness, update_config, postgresql):
    _initialise(harness)
    _set_leader(harness)
    _add_watcher_relation(harness)

    with patch.object(
        harness.charm.watcher, "_get_or_create_watcher_secret", return_value=None
    ) as mock_secret:
        harness.charm.watcher._on_watcher_relation_joined(
            MagicMock(relation=harness.model.get_relation(WATCHER_OFFER_RELATION))
        )
        mock_secret.assert_called_once()


def test_on_watcher_relation_changed_defers_without_initialised_cluster(harness, update_config):
    _add_watcher_relation(harness)

    event = MagicMock()
    harness.charm.watcher._on_watcher_relation_changed(event)

    event.defer.assert_called_once()
    update_config.assert_not_called()


def test_on_watcher_relation_changed_updates_config(harness, update_config, postgresql):
    _initialise(harness)
    _set_leader(harness)
    rel_id = _add_watcher_relation(harness)

    with patch.object(harness.charm, "cleanup_raft_cluster", create=True):
        # update_relation_data fires relation-changed, which drives the render.
        harness.update_relation_data(rel_id, "postgresql-watcher/0", {"unit-address": "10.0.0.10"})

    update_config.assert_called()


def test_update_relation_data_not_leader(harness, update_config):
    rel_id = _add_watcher_relation(harness)

    harness.charm.watcher._update_relation_data(harness.model.get_relation(WATCHER_OFFER_RELATION))

    assert not harness.get_relation_data(rel_id, harness.charm.app.name)


def test_update_relation_data_leader(harness, update_config, postgresql):
    _set_leader(harness)
    rel_id = _add_watcher_relation(harness)
    _add_watcher_secret(harness)

    with patch.object(harness.charm.tls_manager, "get_peer_ca_bundle", return_value="ca-bundle"):
        harness.charm.watcher._update_relation_data(
            harness.model.get_relation(WATCHER_OFFER_RELATION)
        )

    app_data = harness.get_relation_data(rel_id, harness.charm.app.name)
    assert app_data["cluster-name"] == "postgresql"
    assert app_data["raft-port"] == "2222"
    assert app_data["patroni-cas"] == "ca-bundle"
    assert app_data["tls-enabled"] == "false"


def test_update_unit_address_updates_az(harness):
    rel_id = _add_watcher_relation(harness)

    with (
        patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": "az1"}),
        patch.object(
            harness.charm.workload,
            "get_postgresql_version",
            return_value="16",
        ),
        patch.object(type(harness.charm.state), "unit_ip", PropertyMock(return_value="10.0.0.1")),
    ):
        harness.charm.watcher.update_unit_address(
            harness.model.get_relation(WATCHER_OFFER_RELATION)
        )

    unit_data = harness.get_relation_data(rel_id, harness.charm.unit.name)
    assert unit_data["unit-address"] == "10.0.0.1"
    assert unit_data["unit-az"] == "az1"


def test_update_watcher_secret_not_leader(harness):
    with patch.object(harness.charm.model, "get_secret") as mock_get:
        harness.charm.watcher.update_watcher_secret()
        mock_get.assert_not_called()


def test_update_watcher_secret_leader(harness):
    _set_leader(harness)
    mock_secret = MagicMock()
    mock_secret.get_content.return_value = {"watcher-password": "pw"}
    with (
        patch.object(harness.charm.model, "get_secret", return_value=mock_secret),
        patch.object(
            type(harness.charm.state.application),
            "raft_password",
            PropertyMock(return_value="new-raft-password"),
        ),
    ):
        harness.charm.watcher.update_watcher_secret()

        mock_secret.set_content.assert_called_once_with({
            "watcher-password": "pw",
            "raft-password": "new-raft-password",
        })


class TestWatcherRelationSecrets:
    """Tests for secret management in watcher relation."""

    @pytest.fixture(autouse=True)
    def _skip_k8s(self, substrate):
        if substrate == "k8s":
            pytest.skip("the watcher relation is VM-only")

    def test_get_or_create_watcher_secret_existing(self, harness):
        _set_leader(harness)
        mock_secret = MagicMock()

        with patch.object(harness.charm.model, "get_secret", return_value=mock_secret):
            result = harness.charm.watcher._get_or_create_watcher_secret()
            assert result == mock_secret

    def test_get_or_create_watcher_secret_creates_new(self, harness):
        _set_leader(harness)
        mock_secret = MagicMock()

        with (
            patch.object(
                harness.charm.model,
                "get_secret",
                side_effect=SecretNotFoundError("not found"),
            ),
            patch.object(
                harness.charm.model.app,
                "add_secret",
                return_value=mock_secret,
            ),
            patch.object(
                type(harness.charm.state.application),
                "raft_password",
                PropertyMock(return_value="raft-password"),
            ),
        ):
            result = harness.charm.watcher._get_or_create_watcher_secret()
            assert result == mock_secret

    def test_get_or_create_watcher_secret_no_raft_password(self, harness):
        _set_leader(harness)

        with (
            patch.object(
                harness.charm.model,
                "get_secret",
                side_effect=SecretNotFoundError("not found"),
            ),
            patch.object(
                type(harness.charm.state.application),
                "raft_password",
                PropertyMock(return_value=None),
            ),
        ):
            result = harness.charm.watcher._get_or_create_watcher_secret()
            assert result is None
