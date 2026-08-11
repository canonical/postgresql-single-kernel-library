# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.
from unittest.mock import Mock, PropertyMock, patch, sentinel

import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.core.state import CharmState
from single_kernel_postgresql.managers.config import ConfigManager
from single_kernel_postgresql.workload.k8s import K8sWorkload
from single_kernel_postgresql.workload.vm import VMWorkload


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
            state=CharmState(charm=mock_charm, substrate=substrate), workload=workload
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
                substrate, "/var/lib/pg/data/patroni.yml", sentinel.template_output, 0o644
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
