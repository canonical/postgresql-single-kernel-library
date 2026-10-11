# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the K8s workload seams: file push, trust-store sync, service builders."""

import json
from unittest.mock import Mock

import pytest
from single_kernel_postgresql.config.literals import DATABASE_PORT

CERTS_PATH = "/usr/local/share/ca-certificates"


@pytest.fixture
def workload(harness):
    """The K8s workload with a mocked container for the push/exec assertions."""
    workload = harness.charm.workload
    workload.container = Mock()
    workload.container.can_connect.return_value = True
    return workload


@pytest.fixture(autouse=True)
def _k8s_only(substrate):
    if substrate != "k8s":
        pytest.skip("the K8s workload seams are K8s-only")


def test_push_file_to_workload_uses_charm_permissions(workload):
    workload.push_file("/usr/local/share/ca-certificates/ca.crt", "CERT")
    container = workload.container
    container.push.assert_called_once_with(
        "/usr/local/share/ca-certificates/ca.crt",
        "CERT",
        make_dirs=True,
        permissions=0o400,
        user="postgres",
        group="postgres",
    )


def test_sync_tls_trust_store_pushes_ca_and_updates(workload):
    workload.sync_tls_trust_store("CA-PEM", certs_path=CERTS_PATH)
    pushed = workload.container.push.call_args.args
    assert pushed[0] == f"{CERTS_PATH}/ca.crt"
    assert pushed[1] == "CA-PEM"
    workload.container.exec.assert_called_once_with(["update-ca-certificates"])


def test_sync_tls_trust_store_skips_without_ca(workload):
    workload.sync_tls_trust_store(None, certs_path=CERTS_PATH)
    workload.container.push.assert_not_called()
    workload.container.exec.assert_not_called()


def test_generate_ldap_service_env(workload, patch_crypto):
    ldap_params = {
        "ldapurl": "ldap://ldap.example.com:389",
        "ldapbasedn": "dc=example,dc=net",
        "ldapbinddn": "cn=serviceuser",
        "ldapbindpasswd": "bindpw",
    }
    service = workload.generate_ldap_service(
        ldap_params,
        group_mappings={"group": "role"},
        operator_password="operator-pw",
        postgresql_service="postgresql",
    )
    assert service["command"] == "/start-ldap-synchronizer.sh"
    env = service["environment"]
    assert env["LDAP_HOST"] == "ldap.example.com"
    assert env["LDAP_GROUP_MAPPINGS"] == json.dumps({"group": "role"})
    assert env["POSTGRES_PASSWORD"] == "operator-pw"
    assert env["POSTGRES_PORT"] == DATABASE_PORT


def test_generate_metrics_service_uses_charm_command(workload, patch_crypto):
    service = workload.generate_metrics_service(
        monitoring_password=None, postgresql_service="postgresql"
    )
    assert service["command"] == "/usr/bin/prometheus-postgres-exporter"
    assert service["startup"] == "disabled"
    assert service["after"] == ["postgresql"]

    service = workload.generate_metrics_service(
        monitoring_password="pw", postgresql_service="postgresql"
    )
    assert service["startup"] == "enabled"
