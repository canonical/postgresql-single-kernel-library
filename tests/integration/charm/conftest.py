# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for the integration tests of the PostgreSQL charm."""

import os
from collections.abc import Generator
from pathlib import Path

import pytest
import yaml

from tests.integration.charm.backup_helpers import AWS, GCP, cleanup_cloud, get_cloud_config
from tests.integration.charm.helpers import RoleAttributeValue
from tests.integration.types import Substrate

TEST_CHARMS_PATH = Path(__file__).parents[2] / "charms"


def _metadata(substrate: Substrate) -> dict:
    path = TEST_CHARMS_PATH / f"postgresql_{substrate}_test_charm" / "metadata.yaml"
    return yaml.safe_load(path.read_text())


@pytest.fixture(scope="session")
def app_name(substrate: Substrate) -> str:
    """Application name of the test charm (postgresql or postgresql-k8s)."""
    return _metadata(substrate)["name"]


@pytest.fixture(scope="session")
def charm(substrate: Substrate, app_name: str, architecture: str) -> str:
    """Path to the test charm built by scripts/build_charm.sh."""
    charm_dir = TEST_CHARMS_PATH / f"postgresql_{substrate}_test_charm"
    path = charm_dir / f"{app_name}_ubuntu@24.04-{architecture}.charm"
    if not path.exists():
        pytest.fail(
            f"{path} not found, build it with:"
            f" scripts/build_charm.sh -c tests/charms/postgresql_{substrate}_test_charm"
        )
    return str(path)


@pytest.fixture(scope="session")
def charm_resources(substrate: Substrate) -> dict[str, str] | None:
    """Resources to deploy the test charm with (the OCI image on K8s, none on VM)."""
    if substrate == Substrate.vm:
        return None
    resources = _metadata(substrate)["resources"]
    return {name: resource["upstream-source"] for name, resource in resources.items()}


@pytest.fixture(scope="module")
def aws_cloud_configs() -> Generator[tuple[dict[str, str], dict[str, str]]]:
    """AWS S3 integrator config and credentials; the test objects are deleted afterwards."""
    if (
        not os.environ.get("AWS_ACCESS_KEY", "").strip()
        or not os.environ.get("AWS_SECRET_KEY", "").strip()
    ):
        pytest.skip("AWS configs not set")

    config, credentials = get_cloud_config(AWS)
    yield config, credentials

    cleanup_cloud(config, credentials)


@pytest.fixture(scope="module")
def gcp_cloud_configs() -> Generator[tuple[dict[str, str], dict[str, str]]]:
    """GCP S3 integrator config and credentials; the test objects are deleted afterwards."""
    if (
        not os.environ.get("GCP_ACCESS_KEY", "").strip()
        or not os.environ.get("GCP_SECRET_KEY", "").strip()
    ):
        pytest.skip("GCP configs not set")

    config, credentials = get_cloud_config(GCP)
    yield config, credentials

    cleanup_cloud(config, credentials)


@pytest.fixture(scope="module")
def predefined_roles() -> dict:
    """Return the predefined roles with their expected permissions."""
    return {
        "": {
            "auto-escalate-to-database-owner": RoleAttributeValue.REQUESTED_DATABASE,
            "permissions": {
                "connect": RoleAttributeValue.REQUESTED_DATABASE,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.REQUESTED_DATABASE,
                "read-data": RoleAttributeValue.NO,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.NO,
            },
        },
        "charmed_stats": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.NO,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.NO,
            },
        },
        "charmed_read": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.ALL_DATABASES,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.NO,
            },
        },
        "charmed_dml": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.ALL_DATABASES,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.ALL_DATABASES,
            },
        },
        "charmed_backup": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.NO,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.YES,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.NO,
            },
        },
        "charmed_dba": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.ALL_DATABASES,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.YES,
                "write-data": RoleAttributeValue.ALL_DATABASES,
            },
        },
        "charmed_admin": {
            "auto-escalate-to-database-owner": RoleAttributeValue.ALL_DATABASES,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.NO,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.ALL_DATABASES,
                "read-data": RoleAttributeValue.ALL_DATABASES,
                "read-stats": RoleAttributeValue.ALL_DATABASES,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.NO,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.ALL_DATABASES,
            },
        },
        "CREATEDB": {
            "auto-escalate-to-database-owner": RoleAttributeValue.NO,
            "permissions": {
                "connect": RoleAttributeValue.ALL_DATABASES,
                "create-databases": RoleAttributeValue.YES,
                "create-objects": RoleAttributeValue.NO,
                "escalate-to-database-owner": RoleAttributeValue.NO,
                "read-data": RoleAttributeValue.NO,
                "read-stats": RoleAttributeValue.NO,
                "run-backup-commands": RoleAttributeValue.NO,
                "set-up-predefined-catalog-roles": RoleAttributeValue.YES,
                "set-user": RoleAttributeValue.NO,
                "write-data": RoleAttributeValue.NO,
            },
        },
    }


@pytest.fixture(scope="module")
def predefined_roles_combinations() -> list:
    """Return the valid combinations of predefined roles."""
    return [
        ("",),
        ("charmed_stats",),
        ("charmed_read",),
        ("charmed_dml",),
        ("charmed_admin",),
        ("charmed_admin", "CREATEDB"),
    ]
