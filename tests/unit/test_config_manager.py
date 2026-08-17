# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.
from unittest.mock import MagicMock, Mock, PropertyMock, patch, sentinel

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.workload.k8s import K8sWorkload
from single_kernel_postgresql.workload.vm import VMWorkload
from tenacity import stop_after_attempt, wait_fixed


@pytest.fixture(autouse=True)
def config(substrate):
    mock_charm = Mock()
    mock_container = Mock()
    workload = VMWorkload(".") if substrate == Substrates.VM else K8sWorkload(".", mock_container)
    with patch(
        "single_kernel_postgresql.workload.base.BaseWorkload.get_postgresql_version",
        return_value="16.6",
    ):
        config = ConfigManager(
            state=CharmState(charm=mock_charm, substrate=substrate),
            workload=workload,
            tls_manager=Mock(),
            patroni_manager=Mock(),
            resource_provider=Mock(),
            request_restart=Mock(),
            database_manager=Mock(),
            restart_services=Mock(),
        )
    yield config


def test_dict_to_hba_string(config):
    mock_data = {
        "ldapbasedn": "dc=example,dc=net",
        "ldapbinddn": "cn=serviceuser,dc=example,dc=net",
        "ldapbindpasswd": "password",
        "ldaptls": False,
        "ldapurl": "ldap://0.0.0.0:3893",
    }

    assert config._dict_to_hba_string(mock_data) == (
        'ldapbasedn="dc=example,dc=net" '
        'ldapbinddn="cn=serviceuser,dc=example,dc=net" '
        'ldapbindpasswd="password" '
        "ldaptls=0 "
        'ldapurl="ldap://0.0.0.0:3893"'
    )


def test_render_patroni_yml_file(substrate, config):
    with (
        patch(
            "single_kernel_postgresql.workload.base.BaseWorkload.get_postgresql_version",
            return_value="16.6",
        ),
        patch("single_kernel_postgresql.managers.config.render_file") as _render_file,
        patch("single_kernel_postgresql.managers.config.Template") as _template,
        patch(
            "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
        ) as _config,
        patch(
            "single_kernel_postgresql.core.state.CharmState.endpoint",
            new_callable=PropertyMock,
            return_value=sentinel.endpoint,
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.model_name",
            new_callable=PropertyMock,
            return_value=sentinel.model_name,
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.listen_ips",
            new_callable=PropertyMock,
            return_value=sentinel.listen_ips,
        ),
        patch(
            "single_kernel_postgresql.core.state.CharmState.unit_ip",
            new_callable=PropertyMock,
            return_value=sentinel.unit_ip,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.patroni_password",
            new_callable=PropertyMock,
            return_value=sentinel.patroni_pass,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.user_password",
            new_callable=PropertyMock,
            return_value=sentinel.user_pass,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.replication_password",
            new_callable=PropertyMock,
            return_value=sentinel.replication_pass,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.rewind_password",
            new_callable=PropertyMock,
            return_value=sentinel.rewind_pass,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.raft_password",
            new_callable=PropertyMock,
            return_value=sentinel.raft_pass,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.planned_units",
            new_callable=PropertyMock,
            return_value=1,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.cluster_name",
            new_callable=PropertyMock,
            return_value=sentinel.cluster_name,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.endpoints",
            new_callable=PropertyMock,
            return_value=["endpoint1", "endpoint2", "endpoint3"],
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.members_ips",
            new_callable=PropertyMock,
            return_value=["ip1", "ip2", "ip3"],
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.member_name",
            new_callable=PropertyMock,
            return_value=sentinel.member_name,
        ),
        patch("single_kernel_postgresql.managers.config.importlib.resources.files") as _files,
    ):
        _files.return_value.joinpath.return_value.read_text.return_value = "template"
        _config.return_value.synchronous_node_count = 1
        _config.return_value.durability_maximum_lag_on_failover = (
            sentinel.durability_maximum_lag_on_failover
        )
        _config.return_value.instance_password_encryption = sentinel.instance_password_encryption
        _template.return_value.render.return_value = sentinel.template_output

        config.render_patroni_yml_file()

        _template.assert_called_once_with("template")
        if substrate == Substrates.K8S:
            _template.return_value.render.assert_called_once_with(
                substrate="k8s",
                connectivity=False,
                enable_ldap=False,
                enable_tls=False,
                member_name=sentinel.member_name,
                superuser="operator",
                superuser_password=sentinel.user_pass,
                rewind_user="rewind",
                rewind_password=sentinel.rewind_pass,
                replication_password=sentinel.replication_pass,
                enable_pgbackrest_archiving=False,
                stanza=None,
                restore_stanza=None,
                restoring_backup=False,
                backup_id=None,
                pitr_target=None,
                restore_timeline=None,
                restore_to_latest=False,
                is_creating_backup=False,
                version="16",
                synchronous_node_count=0,
                maximum_lag_on_failover=sentinel.durability_maximum_lag_on_failover,
                pg_parameters=None,
                primary_cluster_endpoint=None,
                ldap_parameters="",
                patroni_password=sentinel.patroni_pass,
                user_databases_map=None,
                slots={},
                instance_password_encryption=sentinel.instance_password_encryption,
                extra_replication_endpoints=[],
                endpoint=sentinel.endpoint,
                endpoints=["endpoint1", "endpoint2", "endpoint3"],
                is_no_sync_member=False,
                namespace=sentinel.model_name,
                storage_path="/var/lib/pg/data",
                logs_storage_path="/var/lib/pg/logs",
                pgdata_path="/var/lib/pg/data/16/main",
            )
            _render_file.assert_called_once_with(
                substrate, "/var/lib/pg/data/patroni.yaml", sentinel.template_output, 0o644
            )
            # Same path object the Pebble layer launches Patroni with — never a literal.
            assert _render_file.call_args.args[1] == str(config.workload.paths.patroni_config)
        else:
            _template.return_value.render.assert_called_once_with(
                substrate="vm",
                connectivity=False,
                enable_ldap=False,
                enable_tls=False,
                member_name=sentinel.member_name,
                superuser="operator",
                superuser_password=sentinel.user_pass,
                rewind_user="rewind",
                rewind_password=sentinel.rewind_pass,
                replication_password=sentinel.replication_pass,
                enable_pgbackrest_archiving=False,
                stanza=None,
                restore_stanza=None,
                restoring_backup=False,
                backup_id=None,
                pitr_target=None,
                restore_timeline=None,
                restore_to_latest=False,
                is_creating_backup=False,
                version="16",
                synchronous_node_count=0,
                maximum_lag_on_failover=sentinel.durability_maximum_lag_on_failover,
                pg_parameters=None,
                primary_cluster_endpoint=None,
                ldap_parameters="",
                patroni_password=sentinel.patroni_pass,
                user_databases_map=None,
                slots={},
                instance_password_encryption=sentinel.instance_password_encryption,
                extra_replication_endpoints=[],
                conf_path="/var/snap/charmed-postgresql/current/etc/patroni",
                log_path="/var/snap/charmed-postgresql/common/var/log/patroni",
                postgresql_log_path="/var/snap/charmed-postgresql/common/var/log/postgresql",
                data_path="/var/snap/charmed-postgresql/common/var/lib/postgresql/16/main",
                wal_dir="/var/snap/charmed-postgresql/common/data/logs/16/main",
                partner_addrs=[],
                peers_ips=["ip1", "ip2", "ip3"],
                pgbackrest_configuration_file="--config=/var/snap/charmed-postgresql/current/etc/pgbackrest/pgbackrest.conf",
                scope=sentinel.cluster_name,
                self_ip=sentinel.unit_ip,
                listen_ips=sentinel.listen_ips,
                raft_password=sentinel.raft_pass,
                watcher=None,
            )
            _render_file.assert_called_once_with(
                substrate,
                "/var/snap/charmed-postgresql/current/etc/patroni/patroni.yaml",
                sentinel.template_output,
                0o600,
            )


def _worker_config(**overrides) -> Mock:
    """A Mock CharmConfig with all cpu_max_* fields at their pydantic "auto" default."""
    cfg = Mock()
    cfg.cpu_max_worker_processes = "auto"
    cfg.cpu_max_parallel_workers = "auto"
    cfg.cpu_max_parallel_maintenance_workers = "auto"
    cfg.cpu_max_logical_replication_workers = "auto"
    cfg.cpu_max_sync_workers_per_subscription = "auto"
    cfg.cpu_max_parallel_apply_workers_per_subscription = "auto"
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def test_calculate_max_worker_processes_auto_is_min_8_and_2x_cpu_cores(config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config()
        assert config._calculate_max_worker_processes(cpu_cores=2) == "4"
        assert config._calculate_max_worker_processes(cpu_cores=8) == "8"


def test_calculate_max_worker_processes_explicit_value_under_cap(config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config(cpu_max_worker_processes=6)
        assert config._calculate_max_worker_processes(cpu_cores=4) == "6"


def test_calculate_max_worker_processes_explicit_value_over_cap_raises(config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config(cpu_max_worker_processes=41)
        with pytest.raises(ValueError, match="cpu-max-worker-processes"):
            config._calculate_max_worker_processes(cpu_cores=4)


def test_calculate_max_worker_processes_none_is_omitted(config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config(cpu_max_worker_processes=None)
        assert config._calculate_max_worker_processes(cpu_cores=4) is None


def test_calculate_worker_process_config_auto_defaults_all_to_base_max_workers(config):
    """With everything on 'auto', the dependent params default to the computed base."""
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config()
        # auto max_worker_processes for 4 cores = min(8, 2*4) = 8
        assert config._calculate_worker_process_config(cpu_cores=4) == {
            "max_worker_processes": "8",
            "max_parallel_workers": "8",
            "max_parallel_maintenance_workers": "8",
            "max_logical_replication_workers": "8",
            "max_sync_workers_per_subscription": "8",
            "max_parallel_apply_workers_per_subscription": "8",
        }


def test_calculate_worker_process_config_parallel_workers_capped_by_base_max_workers(config):
    """cpu_max_parallel_workers is min()-constrained against base_max_workers, not just the cap."""
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config(
            cpu_max_worker_processes=4, cpu_max_parallel_workers=20
        )
        # base_max_workers = 4 (explicit, under the 10*8=80 cap); cpu_max_parallel_workers=20
        # is under its own 10*8=80 cap, but min(20, 4) == 4.
        result = config._calculate_worker_process_config(cpu_cores=8)
        assert result["max_worker_processes"] == "4"
        assert result["max_parallel_workers"] == "4"


def test_calculate_worker_process_config_dependent_param_over_cap_raises(config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config(cpu_max_parallel_maintenance_workers=41)
        with pytest.raises(ValueError, match="cpu-max-parallel-maintenance-workers"):
            config._calculate_worker_process_config(cpu_cores=4)


def test_calculate_worker_process_config_zero_cpu_count_is_zero(config):
    """With 0 cores, auto max_worker_processes is min(8, 2*0) = 0, not the 8 fallback."""
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _worker_config()
        assert config._calculate_worker_process_config(cpu_cores=0)["max_worker_processes"] == "0"


def _build_config(**overrides) -> Mock:
    """A Mock CharmConfig covering every field _build_postgresql_parameters reads."""
    cfg = _worker_config()
    cfg.profile_limit_memory = None
    cfg.cpu_wal_compression = None
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


@pytest.fixture
def postgresql_client():
    client = Mock()
    client.build_postgresql_parameters.return_value = {"shared_buffers": "128MB"}
    return client


def test_build_postgresql_parameters_merges_worker_config_and_wal_compression_default(
    config, postgresql_client
):
    """cpu_wal_compression defaults to "on" when unset, matching the config.yaml default."""
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _build_config()
        result = config._build_postgresql_parameters(postgresql_client, 4, 8_000_000_000)

        assert result["shared_buffers"] == "128MB"
        assert result["max_worker_processes"] == "8"
        assert result["wal_compression"] == "on"
        postgresql_client.build_postgresql_parameters.assert_called_once_with(
            config.state.model_config, 8_000_000_000, None
        )


def test_build_postgresql_parameters_cpu_wal_compression_explicit_false(config, postgresql_client):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _build_config(cpu_wal_compression=False)
        result = config._build_postgresql_parameters(postgresql_client, 4, 8_000_000_000)
        assert result["wal_compression"] == "off"


def test_build_postgresql_parameters_profile_limit_memory_converts_mb_to_bytes_and_caps(
    config, postgresql_client
):
    """profile_limit_memory is in MB (config.yaml unit); build_postgresql_parameters wants bytes."""
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _build_config(profile_limit_memory=2_000)
        config._build_postgresql_parameters(postgresql_client, 4, 16_000_000_000)
        postgresql_client.build_postgresql_parameters.assert_called_once_with(
            config.state.model_config, 16_000_000_000, 2_000_000_000
        )


def test_build_postgresql_parameters_none_base_falls_back_to_worker_config_dict(
    config, postgresql_client
):
    """When build_postgresql_parameters returns None, the result is just the worker config."""
    postgresql_client.build_postgresql_parameters.return_value = None
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value = _build_config()
        result = config._build_postgresql_parameters(postgresql_client, 2, 4_000_000_000)
        # auto max_worker_processes for 2 cores = min(8, 2*2) = 4
        assert result == {
            "max_worker_processes": "4",
            "max_parallel_workers": "4",
            "max_parallel_maintenance_workers": "4",
            "max_logical_replication_workers": "4",
            "max_sync_workers_per_subscription": "4",
            "max_parallel_apply_workers_per_subscription": "4",
            "wal_compression": "on",
        }


# --- is_tls_enabled -------------------------------------------------------------------


def test_is_tls_enabled_true_when_all_client_tls_files_present(config):
    config.tls_manager.get_client_tls_files.return_value = ("cert", "key", "ca")
    config.tls_manager.client_tls_files_on_disk.return_value = True
    assert config.is_tls_enabled is True


def test_is_tls_enabled_false_when_any_client_tls_file_missing(config):
    config.tls_manager.get_client_tls_files.return_value = ("cert", None, "ca")
    assert config.is_tls_enabled is False


def test_is_tls_enabled_false_until_the_files_reach_disk(config):
    """Issued certs are in the databag before the push writes them, so disk decides."""
    config.tls_manager.get_client_tls_files.return_value = ("cert", "key", "ca")
    config.tls_manager.client_tls_files_on_disk.return_value = False
    assert config.is_tls_enabled is False


# --- generate_config_hash (migration-compat) ------------------------------------------


def test_generate_config_hash_matches_charm_shake_128_of_model_dump(config):
    """The hash MUST byte-match the charm's shake_128(str(config.model_dump())).hexdigest(16)."""
    from hashlib import shake_128

    dumped = {"profile": "production", "plugin_hstore_enable": False}
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        _config.return_value.model_dump.return_value = dumped
        expected = shake_128(str(dumped).encode()).hexdigest(16)
        assert config.generate_config_hash == expected


# --- apply_api_config -----------------------------------------------------------------


@pytest.fixture
def api_config(config):
    """A config whose CharmState.config exposes every field apply_api_config reads."""
    with (
        patch(
            "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
        ) as _config,
        patch(
            "single_kernel_postgresql.core.state.CharmState.synchronous_configuration",
            new_callable=PropertyMock,
            return_value={"synchronous_node_count": 1},
        ),
    ):
        cfg = _worker_config()
        cfg.experimental_max_connections = None
        cfg.memory_max_prepared_transactions = 0
        cfg.memory_shared_buffers = None
        cfg.durability_wal_keep_size = 0
        cfg.durability_maximum_lag_on_failover = 1048576
        _config.return_value = cfg
        yield config


def test_apply_api_config_builds_cfg_and_base_patch(api_config):
    result = api_config.apply_api_config(4, async_primary_cluster_endpoint=None)
    assert result is True
    call = api_config.patroni_manager.bulk_update_parameters_controller_by_patroni.call_args
    cfg_patch, base_patch = call.args
    # max_connections auto = max(4*4, 100) = 100
    assert cfg_patch["max_connections"] == 100
    assert cfg_patch["max_replication_slots"] == 25
    assert cfg_patch["max_wal_senders"] == 25
    # worker params merged in (auto for 4 cores -> "8")
    assert cfg_patch["max_worker_processes"] == "8"
    assert cfg_patch["max_logical_replication_workers"] == "8"
    assert base_patch["synchronous_node_count"] == 1
    assert base_patch["maximum_lag_on_failover"] == 1048576
    assert "standby_cluster" not in base_patch


def test_apply_api_config_uses_experimental_max_connections_override(api_config):
    with patch(
        "single_kernel_postgresql.core.state.CharmState.config", new_callable=PropertyMock
    ) as _config:
        cfg = _worker_config()
        cfg.experimental_max_connections = 500
        cfg.memory_max_prepared_transactions = 0
        cfg.memory_shared_buffers = None
        cfg.durability_wal_keep_size = 0
        cfg.durability_maximum_lag_on_failover = 1048576
        _config.return_value = cfg
        api_config.apply_api_config(4, async_primary_cluster_endpoint=None)
        cfg_patch = (
            api_config.patroni_manager.bulk_update_parameters_controller_by_patroni.call_args.args[
                0
            ]
        )
        assert cfg_patch["max_connections"] == 500


def test_apply_api_config_adds_standby_cluster_when_async_primary_endpoint(api_config):
    api_config.apply_api_config(4, async_primary_cluster_endpoint="10.0.0.5")
    base_patch = (
        api_config.patroni_manager.bulk_update_parameters_controller_by_patroni.call_args.args[1]
    )
    assert base_patch["standby_cluster"] == {"host": "10.0.0.5"}


def test_apply_api_config_returns_false_on_retry_error(api_config):
    from tenacity import RetryError

    api_config.patroni_manager.bulk_update_parameters_controller_by_patroni.side_effect = (
        RetryError(last_attempt=None)
    )
    assert api_config.apply_api_config(4, async_primary_cluster_endpoint=None) is False


# --- handle_restart_need (restart-decision table) -------------------------------------


@pytest.fixture
def restart_engine(config):
    """A config with is_tls_enabled/_can_connect_to_postgresql/is_restart_pending patchable."""
    with (
        patch.object(type(config), "is_tls_enabled", new_callable=PropertyMock) as _is_tls,
        patch.object(config, "_can_connect_to_postgresql") as _can_connect,
        patch.object(config, "is_restart_pending") as _pending,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.tls",
            new_callable=PropertyMock,
        ) as _peer_tls,
    ):
        _is_tls.return_value = False
        _can_connect.return_value = True
        _pending.return_value = False
        config._is_tls = _is_tls
        config._can_connect = _can_connect
        config._pending = _pending
        config._peer_tls = _peer_tls
        yield config


def test_handle_restart_need_tls_flip_triggers_restart(restart_engine, postgresql_client):
    """Restart is required when the charm's TLS state disagrees with PostgreSQL's live state."""
    restart_engine._is_tls.return_value = True
    postgresql_client.is_tls_enabled.return_value = False
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    restart_engine.request_restart.assert_called_once_with()


def test_handle_restart_need_no_flip_no_config_change_does_not_restart(
    restart_engine, postgresql_client
):
    restart_engine._is_tls.return_value = True
    postgresql_client.is_tls_enabled.return_value = True
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    restart_engine.request_restart.assert_not_called()


def test_handle_restart_need_config_change_and_pending_restart_triggers(
    restart_engine, postgresql_client
):
    restart_engine._is_tls.return_value = True
    postgresql_client.is_tls_enabled.return_value = True
    restart_engine._pending.return_value = True
    restart_engine.handle_restart_need(postgresql_client, config_changed=True)
    restart_engine.request_restart.assert_called_once_with()


def test_handle_restart_need_config_change_but_no_pending_does_not_restart(
    restart_engine, postgresql_client
):
    restart_engine._is_tls.return_value = True
    postgresql_client.is_tls_enabled.return_value = True
    restart_engine._pending.return_value = False
    restart_engine.handle_restart_need(postgresql_client, config_changed=True)
    restart_engine.request_restart.assert_not_called()


def test_handle_restart_need_cannot_connect_forces_no_tls_restart(
    restart_engine, postgresql_client
):
    """When PostgreSQL is unreachable, the TLS-flip check is skipped (restart stays False)."""
    restart_engine._is_tls.return_value = True
    restart_engine._can_connect.return_value = False
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    postgresql_client.is_tls_enabled.assert_not_called()
    restart_engine.request_restart.assert_not_called()


def test_handle_restart_need_check_current_host_is_vm_only(
    substrate, restart_engine, postgresql_client
):
    """VM passes check_current_host=True to the live TLS probe; K8s omits it."""
    restart_engine._is_tls.return_value = False
    postgresql_client.is_tls_enabled.return_value = False
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    if substrate == Substrates.VM:
        postgresql_client.is_tls_enabled.assert_called_once_with(check_current_host=True)
    else:
        postgresql_client.is_tls_enabled.assert_called_once_with()


def test_handle_restart_need_persists_tls_flag_and_refreshes_endpoints(
    restart_engine, postgresql_client
):
    restart_engine._is_tls.return_value = True
    postgresql_client.is_tls_enabled.return_value = True
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    restart_engine._peer_tls.assert_called_once_with(True)
    restart_engine.database_manager.update_endpoints.assert_called_once_with()


def test_handle_restart_need_swallows_reload_patroni_error(restart_engine, postgresql_client):
    """A failing reload_patroni_configuration is logged and swallowed (faithful port)."""
    restart_engine._is_tls.return_value = False
    postgresql_client.is_tls_enabled.return_value = False
    restart_engine.patroni_manager.reload_patroni_configuration.side_effect = Exception("boom")
    # Must not raise.
    restart_engine.handle_restart_need(postgresql_client, config_changed=False)
    restart_engine.request_restart.assert_not_called()


# --- _can_connect_to_postgresql -------------------------------------------------------


def test_can_connect_to_postgresql_true_when_timezones_returned(config, postgresql_client):
    postgresql_client.get_postgresql_timezones.return_value = {"UTC"}
    assert config._can_connect_to_postgresql(postgresql_client) is True


def test_can_connect_to_postgresql_false_when_no_timezones(config, postgresql_client):
    """An empty timezones result drives the real retry loop to exhaustion -> False.

    Retrying itself runs for real (not mocked); only its stop/wait config is shortened
    so the test doesn't wait the real 10s delay. This exercises the actual
    "empty timezones -> CannotConnectError -> retry-exhaust -> RetryError -> False" path.
    """
    postgresql_client.get_postgresql_timezones.return_value = set()
    with (
        patch(
            "single_kernel_postgresql.managers.config.stop_after_delay",
            return_value=stop_after_attempt(1),
        ),
        patch(
            "single_kernel_postgresql.managers.config.wait_fixed",
            return_value=wait_fixed(0),
        ),
    ):
        assert config._can_connect_to_postgresql(postgresql_client) is False
    postgresql_client.get_postgresql_timezones.assert_called()


# --- is_restart_pending ---------------------------------------------------------------


def _wire_restart_pending_cursor(postgresql_client, count: int) -> Mock:
    """Wire the ``_connect_to_database() -> cursor()`` context-manager chain to a cursor."""
    cursor = Mock()
    cursor.fetchone.return_value = (count,)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    # MagicMock so the _connect_to_database() `with` context-manager protocol is supported.
    postgresql_client._connect_to_database = MagicMock()
    postgresql_client._connect_to_database.return_value.__enter__.return_value = connection
    return cursor


def test_is_restart_pending_true_when_pg_settings_reports_pending(config, postgresql_client):
    cursor = _wire_restart_pending_cursor(postgresql_client, 2)
    assert config.is_restart_pending(postgresql_client) is True
    cursor.execute.assert_called_once_with(
        "SELECT COUNT(*) FROM pg_settings WHERE pending_restart=True;"
    )


def test_is_restart_pending_false_when_none(config, postgresql_client):
    _wire_restart_pending_cursor(postgresql_client, 0)
    assert config.is_restart_pending(postgresql_client) is False


# --- update_config orchestration: early exits -----------------------------------------


@pytest.fixture
def orchestrate(config, postgresql_client):
    """A config with render + engine methods stubbed so update_config's branches are testable."""
    with (
        patch.object(config, "_build_postgresql_parameters", return_value={}),
        patch.object(config, "render_patroni_yml_file"),
        patch.object(type(config), "is_tls_enabled", new_callable=PropertyMock) as _is_tls,
        patch.object(type(config), "generate_config_hash", new_callable=PropertyMock) as _hash,
        patch.object(config, "handle_restart_need"),
        patch.object(config, "apply_api_config", return_value=True),
        patch.object(config, "_can_connect_to_postgresql", return_value=True),
        patch.object(config.workload, "is_patroni_running", return_value=True),
        patch.object(
            config,
            "resource_provider",
            return_value=Mock(get_available_resources=Mock(return_value=(4, 8_000_000_000))),
        ),
        # render_patroni_yml_file is mocked, but its kwargs are still eagerly evaluated
        # against the state — patch the accessors they read so they don't hit the Mock relation.
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.is_connectivity_enabled",
            new_callable=PropertyMock,
            return_value=True,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.is_ldap_enabled",
            new_callable=PropertyMock,
            return_value=False,
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.data",
            new_callable=PropertyMock,
            return_value={},
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.data",
            new_callable=PropertyMock,
            return_value={},
        ),
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.tls",
            new_callable=PropertyMock,
        ) as _peer_tls,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.config_hash",
            new_callable=PropertyMock,
        ) as _peer_cfg_hash,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.user_hash",
            new_callable=PropertyMock,
        ) as _peer_user_hash,
        patch(
            "single_kernel_postgresql.core.peer_relation.PostgreSQLApplication.user_hash",
            new_callable=PropertyMock,
        ) as _app_user_hash,
    ):
        _is_tls.return_value = False
        _hash.return_value = "newhash"
        _peer_cfg_hash.return_value = "oldhash"
        # patroni_manager is a Mock; member_started / ensure_slots are plain attributes here.
        config.patroni_manager.member_started = True
        config.patroni_manager.ensure_slots_controller_by_patroni.return_value = True
        config.database_manager.user_hash = "uh"
        config._is_tls = _is_tls
        config._hash = _hash
        config._peer_tls = _peer_tls
        config._peer_cfg_hash = _peer_cfg_hash
        config._peer_user_hash = _peer_user_hash
        config._app_user_hash = _app_user_hash
        yield config


def test_update_config_no_peers_returns_true_early(orchestrate, postgresql_client):
    assert orchestrate.update_config(postgresql_client, no_peers=True) is True
    orchestrate.render_patroni_yml_file.assert_called_once()
    orchestrate.database_manager.update_endpoints.assert_not_called()
    orchestrate.handle_restart_need.assert_not_called()


def test_update_config_threads_tls_flag_into_render(orchestrate, postgresql_client):
    """is_tls_enabled is what update_config passes as render_patroni_yml_file's enable_tls."""
    orchestrate._is_tls.return_value = True
    orchestrate.update_config(postgresql_client, no_peers=True)
    assert orchestrate.render_patroni_yml_file.call_args.kwargs["enable_tls"] is True

    orchestrate.render_patroni_yml_file.reset_mock()
    orchestrate._is_tls.return_value = False
    orchestrate.update_config(postgresql_client, no_peers=True)
    assert orchestrate.render_patroni_yml_file.call_args.kwargs["enable_tls"] is False


def test_update_config_workload_not_running_persists_tls_and_refreshes(
    orchestrate, postgresql_client
):
    orchestrate._is_tls.return_value = True
    with patch.object(orchestrate.workload, "is_patroni_running", return_value=False):
        assert orchestrate.update_config(postgresql_client) is True
    orchestrate._peer_tls.assert_called_once_with(True)
    orchestrate.database_manager.update_endpoints.assert_called_once_with()
    orchestrate.handle_restart_need.assert_not_called()


def test_update_config_member_not_started_with_tls_forces_handle_restart_true(
    orchestrate, postgresql_client
):
    orchestrate._is_tls.return_value = True
    orchestrate.patroni_manager.member_started = False
    assert orchestrate.update_config(postgresql_client) is True
    orchestrate.handle_restart_need.assert_called_once_with(postgresql_client, True)


def test_update_config_member_not_started_no_tls_returns_false(orchestrate, postgresql_client):
    orchestrate._is_tls.return_value = False
    orchestrate.patroni_manager.member_started = False
    assert orchestrate.update_config(postgresql_client) is False
    orchestrate.handle_restart_need.assert_not_called()


def test_update_config_cannot_connect_returns_false(substrate, orchestrate, postgresql_client):
    """The standalone connect gate is VM-only: K8s's Patroni API patch doesn't need it."""
    if substrate != Substrates.VM:
        pytest.skip("standalone connect gate is VM-only")
    with patch.object(orchestrate, "_can_connect_to_postgresql", return_value=False):
        assert orchestrate.update_config(postgresql_client) is False
    orchestrate.apply_api_config.assert_not_called()


def test_update_config_k8s_proceeds_without_standalone_connect_gate(
    substrate, orchestrate, postgresql_client
):
    """K8s has no standalone connect gate.

    It reaches apply_api_config even when _can_connect_to_postgresql would be False,
    as long as member_started is True.
    """
    if substrate != Substrates.K8S:
        pytest.skip("this asserts the K8s-only no-gate behavior")
    with patch.object(orchestrate, "_can_connect_to_postgresql", return_value=False):
        assert orchestrate.update_config(postgresql_client) is True
    orchestrate.apply_api_config.assert_called_once()


def test_update_config_api_apply_fails_returns_false(orchestrate, postgresql_client):
    with patch.object(orchestrate, "apply_api_config", return_value=False):
        assert orchestrate.update_config(postgresql_client) is False
    orchestrate.handle_restart_need.assert_not_called()


def test_update_config_happy_path_persists_hashes_and_calls_bridges(
    orchestrate, postgresql_client
):
    with patch(
        "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.is_app_leader",
        new_callable=PropertyMock,
        return_value=True,
    ):
        assert orchestrate.update_config(postgresql_client) is True
    # config_hash change (old "oldhash" != new "newhash") drives handle_restart_need(True).
    orchestrate.handle_restart_need.assert_called_once_with(postgresql_client, True)
    orchestrate.restart_services.assert_called_once_with()
    orchestrate._peer_cfg_hash.assert_called_with("newhash")
    orchestrate._peer_user_hash.assert_called_with("uh")
    orchestrate._app_user_hash.assert_called_with("uh")


def test_update_config_ensure_slots_is_k8s_only(substrate, orchestrate, postgresql_client):
    with patch(
        "single_kernel_postgresql.core.peer_relation.PostgreSQLPeer.is_app_leader",
        new_callable=PropertyMock,
        return_value=False,
    ):
        orchestrate.update_config(postgresql_client)
    if substrate == Substrates.K8S:
        orchestrate.patroni_manager.ensure_slots_controller_by_patroni.assert_called_once()
    else:
        orchestrate.patroni_manager.ensure_slots_controller_by_patroni.assert_not_called()


def test_update_config_vm_snap_gate_exits_before_restart_services(
    substrate, orchestrate, postgresql_client
):
    """VM: a stale snap revision returns True early, before restart_services / hash persist."""
    if substrate != Substrates.VM:
        pytest.skip("snap gate is VM-only")
    refresh = Mock()
    refresh.pinned_snap_revision = "999"
    from unittest.mock import call

    with patch.object(orchestrate.workload, "get_snap_revision", return_value="123"):
        assert orchestrate.update_config(postgresql_client, refresh=refresh) is True
    orchestrate.handle_restart_need.assert_called_once()
    orchestrate.restart_services.assert_not_called()
    # config_hash is read (getter, call()) for the restart decision but never PERSISTED
    # (setter, call("newhash")) — the snap gate returns before the hash write-back.
    assert call("newhash") not in orchestrate._peer_cfg_hash.mock_calls
