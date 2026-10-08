# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Normalization matrix for S3ConnectionInfo.retrieve_s3_parameters."""

from unittest.mock import Mock

import pytest
from single_kernel_postgresql.core.s3 import S3ConnectionInfo
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.s3 import S3Requirer

REQUIRED = {"bucket": "backups", "access-key": "key", "secret-key": "secret"}


def _info_with(connection_info: dict) -> S3ConnectionInfo:
    requirer = Mock(spec=S3Requirer)
    requirer.get_s3_connection_info.return_value = dict(connection_info)
    return S3ConnectionInfo(requirer)


@pytest.mark.parametrize(
    ("connection_info", "expected_missing"),
    [
        ({}, ["bucket", "access-key", "secret-key"]),
        ({"access-key": "key"}, ["bucket", "secret-key"]),
    ],
)
def test_missing_required_parameters(connection_info, expected_missing):
    parameters, missing = _info_with(connection_info).retrieve_s3_parameters()

    assert parameters == {}
    assert missing == expected_missing


def test_defaults_are_applied_for_optional_parameters():
    parameters, missing = _info_with(REQUIRED).retrieve_s3_parameters()

    assert missing == []
    assert parameters["endpoint"] == "https://s3.amazonaws.com"
    assert parameters["path"] == "/"
    assert parameters["s3-uri-style"] == "host"
    assert parameters["delete-older-than-days"] == "9999999"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # Whitespace is stripped from all string parameters.
        (
            {"bucket": " backups ", "access-key": " key\t", "secret-key": "\nsecret"},
            {"bucket": "backups", "access-key": "key", "secret-key": "secret"},
        ),
        # Trailing endpoint slashes are removed.
        ({"endpoint": "https://ceph.internal:8000/"}, {"endpoint": "https://ceph.internal:8000"}),
        # The path gets a leading slash (required by pgBackRest) and no trailing slash.
        ({"path": "sub/dir/"}, {"path": "/sub/dir"}),
        # Surrounding bucket slashes are stripped.
        ({"bucket": "/backups/"}, {"bucket": "backups"}),
    ],
)
def test_parameter_normalization(overrides, expected):
    parameters, missing = _info_with({**REQUIRED, **overrides}).retrieve_s3_parameters()

    assert missing == []
    for key, value in expected.items():
        assert parameters[key] == value
