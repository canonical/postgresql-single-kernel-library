# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the PostgreSQL watcher relation handler."""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from ops import SecretNotFoundError
from single_kernel_postgresql.events.watcher import WatcherEventsHandler


def create_mock_charm():
    """Create a mock charm for testing."""
    mock_state = MagicMock()
    mock_state.cluster_name = "postgresql"
    mock_state.unit_ip = "10.0.0.1"
    mock_state.peer.is_app_leader = True
    mock_state.application.raft_password = "test-raft-password"

    mock_workload = MagicMock()

    mock_charm = MagicMock()
    mock_charm._patroni.unit_ip = "10.0.0.1"
    mock_charm._patroni.peers_ips = {"10.0.0.2"}
    mock_charm.is_cluster_initialised = True
    mock_charm.state = mock_state
    mock_charm.workload = mock_workload
    mock_charm.update_config = MagicMock()
    return mock_charm


def create_mock_relation():
    """Create a mock relation for testing."""
    mock_relation = MagicMock()
    mock_relation.data = {
        MagicMock(): {},  # app data
        MagicMock(): {},  # unit data
    }
    mock_relation.units = set()
    return mock_relation


class TestWatcherRelation:
    """Tests for PostgreSQLWatcherRelation class."""

    def test_watcher_address_no_relation(self, substrate):
        """Test watcher_address returns None when no relation exists."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")
        mock_charm = create_mock_charm()

        with patch.object(
            WatcherEventsHandler, "_relation", new_callable=PropertyMock, return_value=None
        ):
            relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
            assert relation.watcher_raft_address is None

    def test_watcher_address_with_relation(self, substrate):
        """Test watcher_address returns the watcher IP when available."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_relation = MagicMock()

        # Create a mock unit with unit-address
        mock_unit = MagicMock()
        mock_relation.units = {mock_unit}
        mock_relation.data = {
            mock_unit: {"unit-address": "10.0.0.10"},
            mock_relation.app: {"watcher-raft-port": "2222"},
        }

        with patch.object(
            WatcherEventsHandler,
            "_relation",
            new_callable=PropertyMock,
            return_value=mock_relation,
        ):
            relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
            assert relation.watcher_raft_address == "10.0.0.10:2222"

    def test_on_watcher_relation_joined_not_leader(self, substrate):
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        """Test relation joined event is ignored for non-leader units."""
        mock_charm = create_mock_charm()
        mock_charm.state.peer.is_app_leader = False
        mock_event = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with (
            patch.object(relation, "update_unit_address") as update_unit_address,
            patch.object(relation, "_get_or_create_watcher_secret") as mock_secret,
        ):
            relation._on_watcher_relation_joined(mock_event)
            update_unit_address.assert_called_once_with(mock_event.relation)
            mock_secret.assert_not_called()

    def test_on_watcher_relation_joined_leader(self, substrate):
        """Test relation joined event creates secret for leader."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_event = MagicMock()
        mock_secret = MagicMock()
        mock_secret.id = "secret:abc123"

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with (
            patch.object(relation, "_get_or_create_watcher_secret", return_value=mock_secret),
            patch.object(relation, "_update_relation_data") as mock_update,
        ):
            relation._on_watcher_relation_joined(mock_event)
            mock_secret.grant.assert_called_once_with(mock_event.relation)
            mock_update.assert_called_once_with(mock_event.relation)

    def test_on_watcher_relation_joined_no_secret(self, substrate):
        """Test relation joined event defers when secret creation fails."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_event = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.object(relation, "_get_or_create_watcher_secret", return_value=None):
            relation._on_watcher_relation_joined(mock_event)
            mock_event.defer.assert_called_once()

    def test_on_watcher_relation_changed_not_initialized(self, substrate):
        """Test relation changed event defers when cluster not initialized."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_charm.is_cluster_initialised = False
        mock_event = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
        relation._on_watcher_relation_changed(mock_event)

        mock_event.defer.assert_called_once()

    def test_on_watcher_relation_changed_updates_config(self, substrate):
        """Test relation changed event updates Patroni config."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_event = MagicMock()

        # Setup mock relation with watcher unit
        mock_unit = MagicMock()
        mock_event.relation.units = {mock_unit}
        mock_event.relation.data = {
            mock_unit: {"unit-address": "10.0.0.10"},
            mock_charm.state.peer.unit: {},
        }

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.object(relation, "_update_relation_data"):
            relation._on_watcher_relation_changed(mock_event)
            mock_charm.update_config.assert_called_once()

    def test_update_relation_data_not_leader(self, substrate):
        """Test _update_relation_data does nothing for non-leader."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_charm.unit.is_leader.return_value = False
        mock_relation = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
        relation._update_relation_data(mock_relation)

        # Should not try to update relation data
        assert not mock_relation.data[mock_charm.app].update.called

    def test_update_relation_data_leader(self, substrate):
        """Test _update_relation_data populates relation data correctly."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_charm._units_ips = ["10.0.0.1", "10.0.0.2"]  # Mock PostgreSQL endpoints
        mock_charm.state.unit_ip = "10.0.0.1"
        mock_relation = MagicMock()
        mock_relation.data = {
            mock_charm.state.peer.unit.app: {},
            mock_charm.state.peer.unit: {},
        }

        mock_secret = MagicMock()
        mock_secret.id = "secret:abc123"

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with (
            patch.object(relation.model, "get_secret", return_value=mock_secret),
            patch.object(relation, "_get_standby_clusters", return_value=[]),
        ):
            relation._update_relation_data(mock_relation)

        # Verify app data was updated
        app_data = mock_relation.data[mock_charm.state.peer.unit.app]
        assert app_data["cluster-name"] == "postgresql"
        assert "raft-secret-id" in app_data
        assert "raft-partner-addrs" in app_data
        assert "raft-port" in app_data

        # Verify unit data was updated
        unit_data = mock_relation.data[mock_charm.state.peer.unit]
        assert "unit-address" in unit_data

    def test_update_unit_address_updates_az(self, substrate):
        """Test update_unit_address also publishes unit AZ."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_relation = MagicMock()
        mock_relation.data = {
            mock_charm.state.peer.unit: {
                "unit-address": "10.0.0.1",
            }
        }

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": "az1"}, clear=False):
            relation.update_unit_address(mock_relation)

        assert mock_relation.data[mock_charm.state.peer.unit]["unit-az"] == "az1"

    def test_update_watcher_secret_not_leader(self, substrate):
        """Test update_watcher_secret does nothing for non-leader."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_charm.state.peer.is_app_leader = False

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.object(mock_charm.model, "get_secret") as mock_get:
            relation.update_watcher_secret()
            mock_get.assert_not_called()

    def test_update_watcher_secret_leader(self, substrate):
        """Test update_watcher_secret updates secret content."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_secret = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.object(relation.model, "get_secret", return_value=mock_secret):
            relation.update_watcher_secret()
            mock_secret.set_content.assert_called_once()


class TestWatcherRelationSecrets:
    """Tests for secret management in watcher relation."""

    def test_get_or_create_watcher_secret_existing(self, substrate):
        """Test _get_or_create_watcher_secret returns existing secret."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_secret = MagicMock()

        relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)

        with patch.object(relation.model, "get_secret", return_value=mock_secret):
            result = relation._get_or_create_watcher_secret()
            assert result == mock_secret

    def test_get_or_create_watcher_secret_creates_new(self, substrate):
        """Test _get_or_create_watcher_secret creates new secret."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_secret = MagicMock()

        with patch.object(WatcherEventsHandler, "model", new_callable=PropertyMock) as _model:
            _model.return_value.get_secret.side_effect = SecretNotFoundError("not found")
            _model.return_value.app.add_secret.return_value = mock_secret

            relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
            result = relation._get_or_create_watcher_secret()
            assert result == mock_secret
            _model.return_value.app.add_secret.assert_called_once()

    def test_get_or_create_watcher_secret_no_raft_password(self, substrate):
        """Test _get_or_create_watcher_secret returns None without password."""
        if substrate == "k8s":
            pytest.skip("Test only applicable for VM substrate")

        mock_charm = create_mock_charm()
        mock_charm.state.application.raft_password = None

        with patch.object(WatcherEventsHandler, "model", new_callable=PropertyMock) as _model:
            _model.return_value.get_secret.side_effect = SecretNotFoundError("not found")
            relation = WatcherEventsHandler(mock_charm, mock_charm.workload, mock_charm.state)
            result = relation._get_or_create_watcher_secret()
            assert result is None
