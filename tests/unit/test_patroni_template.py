# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
"""Golden tests: the merged lib template must render the same configuration as each charm's original.

The single ``single_kernel_postgresql/templates/patroni.yml.j2`` branches on a ``substrate``
context var. For every conditional dimension of the render it must reproduce the configuration
the pre-merge charm template produced for that substrate. The fixtures under ``tests/fixtures/``
are byte copies of those originals; each case renders both with the same context and compares
the parsed documents, so any changed value, added key or reordered rule fails the suite.

Parsed rather than byte comparison because the merged template emits one field order for both
substrates instead of reproducing each charm's; mapping order is not part of the configuration.
"""

import importlib.resources
import itertools
from pathlib import Path

import pytest
import yaml
from jinja2 import Template

FIXTURES = Path(__file__).parent.parent / "fixtures"
ORIGINAL = {
    "vm": (FIXTURES / "patroni_vm_original.j2").read_text(),
    "k8s": (FIXTURES / "patroni_k8s_original.j2").read_text(),
}


def _merged_template_source() -> str:
    """Load the merged template as package data (proves it ships and is importable)."""
    return (
        importlib.resources
        .files("single_kernel_postgresql.templates")
        .joinpath("patroni.yml.j2")
        .read_text()
    )


# Compile once — the sources are static files, so per-case reparsing only slows the matrix.
# The production loader (importlib.resources) is exercised by _merged_template_source below.
_ORIGINAL_TEMPLATES = {substrate: Template(source) for substrate, source in ORIGINAL.items()}
_MERGED_TEMPLATE = Template(_merged_template_source())


def _base_context() -> dict:
    """Every variable both templates reference, at a neutral baseline.

    Individual cases override the dimensions under test; anything not overridden keeps
    these values so the render never hits an undefined variable by accident.
    """
    return {
        # identity / addresses
        "scope": "patroni-cluster",
        "member_name": "postgresql-0",
        "self_ip": "10.1.2.3",
        "endpoint": "postgresql-k8s-0",
        "namespace": "test-model",
        "listen_ips": ["10.1.2.3", "127.0.0.1"],
        "partner_addrs": ["10.1.2.4", "10.1.2.5"],
        "peers_ips": ["10.1.2.4", "10.1.2.5"],
        "endpoints": ["postgresql-k8s-1", "postgresql-k8s-2"],
        "extra_replication_endpoints": ["10.9.9.9", "10.9.9.10"],
        "watcher": None,
        "watcher_addr": None,
        # secrets
        "patroni_password": "patroni-secret",
        "raft_password": "raft-secret",
        "replication_password": "replication-secret",
        "rewind_user": "rewind",
        "rewind_password": "rewind-secret",
        "superuser": "operator",
        "superuser_password": "operator-secret",
        # paths (VM)
        "conf_path": "/var/snap/charmed-postgresql/current/etc/patroni",
        "log_path": "/var/snap/charmed-postgresql/common/var/log/patroni",
        "postgresql_log_path": "/var/snap/charmed-postgresql/common/var/log/postgresql",
        "data_path": "/var/snap/charmed-postgresql/common/var/lib/postgresql/16/main",
        "wal_dir": "/var/snap/charmed-postgresql/common/data/logs/16/main",
        # paths (K8s)
        "storage_path": "/var/lib/postgresql/data",
        "logs_storage_path": "/var/lib/postgresql/logs",
        "pgdata_path": "/var/lib/postgresql/data/16/main",
        # dcs / postgresql knobs
        "version": "16",
        "synchronous_node_count": 2,
        "maximum_lag_on_failover": 1048576,
        "instance_password_encryption": "scram-sha-256",
        "pg_parameters": None,
        "primary_cluster_endpoint": None,
        # archiving / restore
        "enable_pgbackrest_archiving": False,
        "pgbackrest_configuration_file": "--config=/var/snap/charmed-postgresql/current/etc/pgbackrest/pgbackrest.conf",
        "stanza": "test-stanza",
        "restore_stanza": "restore-stanza",
        "restoring_backup": False,
        "backup_id": None,
        "pitr_target": None,
        "restore_timeline": None,
        "restore_to_latest": False,
        # feature flags
        "connectivity": False,
        "enable_tls": False,
        "enable_ldap": False,
        "ldap_parameters": 'ldapbasedn="dc=example,dc=net" ldapbindpasswd="password"',
        "slots": {},
        "user_databases_map": {},
        "is_creating_backup": False,
        "is_no_sync_member": False,
    }


# Each dimension the merged template branches on, as (name, list-of-override-dicts). The
# product across dimensions is the matrix; a case that flips one dimension while holding the
# rest at baseline still exercises that branch, and the full product catches seam interactions.
_DIMENSIONS = {
    "tls": [
        {"enable_tls": False},
        {"enable_tls": True},
    ],
    "access": [
        # connectivity off -> reject block; on+ldap -> ldap block; on -> internal_access block
        {"connectivity": False, "enable_ldap": False},
        {"connectivity": True, "enable_ldap": False},
        {"connectivity": True, "enable_ldap": True},
    ],
    "restore": [
        {"restoring_backup": False},
        {"restoring_backup": True, "backup_id": "2024-01-01T00:00:00Z"},
        {
            "restoring_backup": True,
            "pitr_target": "2024-06-01 12:00:00",
            "restore_timeline": "2",
        },
        {"restoring_backup": True, "restore_to_latest": True},
        # standby cluster branch (elif primary_cluster_endpoint) — only when not restoring
        {"restoring_backup": False, "primary_cluster_endpoint": "10.20.30.40"},
    ],
    "slots": [
        {"slots": {}},
        {"slots": {"slot_one": "db1", "slot_two": "db2"}},
    ],
    "peers": [
        {},  # baseline peers present
        {"partner_addrs": [], "peers_ips": [], "endpoints": []},  # no_peers
    ],
    "watcher": [
        {"watcher": None, "watcher_addr": None},
        {"watcher": "10.5.5.5:2222", "watcher_addr": "10.5.5.5"},
    ],
    "extra_repl": [
        {},  # baseline extra endpoints present
        {"extra_replication_endpoints": []},
    ],
    "pg_params_and_dbs": [
        {},
        {
            "enable_pgbackrest_archiving": True,
            "pg_parameters": {"max_connections": "100", "work_mem": "4MB"},
            "user_databases_map": {
                "relation_1": "db_a,db_b",
                "pgbouncer_auth_relation_5": "db_c",
            },
        },
    ],
    "tags": [
        {},
        {"is_creating_backup": True, "is_no_sync_member": True},
    ],
}


def _matrix():
    """Cartesian product of every dimension, per substrate, as pytest params."""
    names = list(_DIMENSIONS)
    index_ranges = [range(len(_DIMENSIONS[name])) for name in names]
    params = []
    for substrate in ("vm", "k8s"):
        for indices in itertools.product(*index_ranges):
            overrides = {}
            for name, index in zip(names, indices, strict=True):
                overrides.update(_DIMENSIONS[name][index])
            case_id = f"{substrate}-" + "-".join(
                f"{name}{index}" for name, index in zip(names, indices, strict=True)
            )
            params.append(pytest.param(substrate, overrides, id=case_id))
    return params


@pytest.mark.parametrize(("substrate", "overrides"), _matrix())
def test_merged_template_matches_original(substrate, overrides):
    context = _base_context()
    context.update(overrides)

    expected = _ORIGINAL_TEMPLATES[substrate].render(**context)
    actual = _MERGED_TEMPLATE.render(substrate=substrate, **context)

    # Compared as parsed documents rather than bytes: the merged template emits one field
    # order for both substrates instead of each charm's, and YAML mapping order carries no
    # meaning. Sequences still compare in order, so pg_hba - where PostgreSQL takes the
    # first matching rule - and partner_addrs stay pinned.
    assert yaml.safe_load(actual) == yaml.safe_load(expected)


def test_template_loads_via_importlib_resources():
    """The merged template must resolve as package data, independent of the CWD."""
    source = _merged_template_source()
    assert "{%- set is_vm = substrate == 'vm' -%}" in source
    assert Template(source).render(substrate="vm", **_base_context())
