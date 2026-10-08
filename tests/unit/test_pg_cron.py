# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import yaml
from single_kernel_postgresql.core.config import CharmConfig, K8SCharmConfig


@pytest.fixture
def pg_config(substrate, test_charm_path):
    options = yaml.safe_load((Path(test_charm_path) / "config.yaml").read_text())["options"]
    defaults = {
        name.replace("-", "_"): option["default"]
        for name, option in options.items()
        if "default" in option
    }
    model = CharmConfig if substrate == "vm" else K8SCharmConfig
    return model(**defaults)


@pytest.fixture
def manager(harness, pg_config):
    manager = harness.charm.config_manager
    with patch.object(type(manager.state), "config", PropertyMock(return_value=pg_config)):
        yield manager


def test_pg_cron_defaults_off_from_charm_config(pg_config):
    assert pg_config.plugin_pg_cron_enable is False


def test_pg_cron_config_enables_plugin_discovery(harness, pg_config):
    config = type(pg_config)(**(pg_config.model_dump() | {"plugin_pg_cron_enable": True}))
    with patch.object(type(harness.charm.state), "config", PropertyMock(return_value=config)):
        assert "pg_cron" in harness.charm.database_manager.get_plugins()


@pytest.mark.parametrize("enabled", [False, True])
def test_pg_cron_patroni_configuration(manager, pg_config, enabled):
    config = pg_config.model_copy(update={"plugin_pg_cron_enable": enabled})
    with (
        patch.object(type(manager.state), "config", PropertyMock(return_value=config)),
        patch.object(type(manager), "_are_passwords_set", PropertyMock(return_value=True)),
        patch("single_kernel_postgresql.managers.config.render_file") as write,
    ):
        manager.render_patroni_yml_file()
    rendered = yaml.safe_load(write.call_args.args[2])
    for section in [rendered["bootstrap"]["dcs"]["postgresql"], rendered["postgresql"]]:
        parameters = section["parameters"]
        assert "pg_cron" in parameters["shared_preload_libraries"].split(",")
        cron_parameters = {
            key: value for key, value in parameters.items() if key.startswith("cron.")
        }
        assert cron_parameters == {
            "cron.database_name": "postgres",
            "cron.use_background_workers": "on",
            "cron.timezone": "GMT",
        }


@pytest.fixture
def database_connections(harness):
    connections = {}
    for database in ["postgres", "application"]:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = connection.cursor.return_value
        connection.cursor.return_value.fetchall.return_value = [("postgres",), ("application",)]
        connections[database] = connection
    with patch.object(
        type(harness.charm.postgresql),
        "_connect_to_database",
        side_effect=lambda database=None: connections[database or "postgres"],
    ):
        yield connections


@pytest.mark.parametrize("database", [None, "postgres", "application"])
@pytest.mark.parametrize("enabled", [False, True])
def test_pg_cron_reconciliation_only_in_postgres(harness, database_connections, database, enabled):
    harness.charm.postgresql.enable_disable_extensions(
        {"pg_cron": enabled, "hstore": enabled}, database
    )
    statement = "CREATE EXTENSION IF NOT EXISTS" if enabled else "DROP EXTENSION IF EXISTS"
    for name, connection in database_connections.items():
        queries = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
        expected_count = 1 if name == "postgres" and database != "application" else 0
        assert queries.count(f"{statement} pg_cron;") == expected_count
        assert queries.count(f"{statement} hstore;") == (1 if database in (None, name) else 0)


def test_pg_cron_skipped_when_creating_application_database(harness, database_connections):
    database_connections["postgres"].cursor.return_value.fetchone.return_value = None
    harness.charm.postgresql.create_database("application", ["pg_cron", "hstore"])
    queries = [
        call.args[0]
        for call in database_connections["application"].cursor.return_value.execute.call_args_list
    ]
    assert "CREATE EXTENSION IF NOT EXISTS pg_cron;" not in queries
    assert "CREATE EXTENSION IF NOT EXISTS hstore;" in queries
