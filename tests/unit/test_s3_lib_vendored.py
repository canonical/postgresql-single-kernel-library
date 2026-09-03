# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""The vendored s3 charm lib stays importable through the package namespace."""

import single_kernel_postgresql.lib.charms.data_platform_libs.v0.s3 as s3_lib


def test_vendored_s3_lib_exposes_the_requirer():
    assert hasattr(s3_lib, "S3Requirer")
    assert hasattr(s3_lib, "CredentialsChangedEvent")
    assert hasattr(s3_lib, "CredentialsGoneEvent")
