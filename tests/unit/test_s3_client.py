# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""S3Client behaviour, mocked at the boto3 Session boundary."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, ConnectTimeoutError, SSLError
from single_kernel_postgresql.managers.s3_client import S3Client

BASE_PARAMETERS = {
    "bucket": "backups",
    "access-key": "key",
    "secret-key": "secret",
    "endpoint": "https://s3.amazonaws.com",
    "path": "/sub",
}


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Operation")


@pytest.fixture
def session():
    with patch("single_kernel_postgresql.managers.s3_client.Session") as session:
        session.return_value.resource.return_value.Bucket.return_value = MagicMock()
        yield session


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"region": "us-east-2"}, "https://s3.us-east-2.amazonaws.com"),
        ({}, "https://s3.amazonaws.com"),
        (
            {"endpoint": "https://ceph.internal:8000", "region": "us-east-1"},
            "https://ceph.internal:8000",
        ),
    ],
)
def test_construct_endpoint(overrides, expected):
    # AWS endpoints are rewritten to the region host; other endpoints are kept.
    assert S3Client()._construct_endpoint({**BASE_PARAMETERS, **overrides}) == expected


@pytest.mark.parametrize(
    ("ca_chain", "verify"),
    [
        (None, None),
        (
            "/etc/pgbackrest/pgbackrest-tls-ca-chain.crt",
            "/etc/pgbackrest/pgbackrest-tls-ca-chain.crt",
        ),
    ],
)
def test_session_resource(session, ca_chain, verify):
    client = S3Client(tls_ca_chain_filename=ca_chain)

    client._get_s3_session_resource({**BASE_PARAMETERS, "region": "us-east-2"})

    session.assert_called_once_with(
        aws_access_key_id="key", aws_secret_access_key="secret", region_name="us-east-2"
    )
    _, kwargs = session.return_value.resource.call_args
    assert kwargs["endpoint_url"] == "https://s3.us-east-2.amazonaws.com"
    assert kwargs["verify"] == verify
    config = kwargs["config"]
    assert config.request_checksum_calculation == "when_required"
    assert config.response_checksum_validation == "when_required"


def test_upload_content(session):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    uploaded = {}
    bucket.upload_file.side_effect = lambda name, path: uploaded.update(
        path=path,
        content=open(name).read(),  # noqa: SIM115
    )

    assert S3Client().upload_content("contents", "backup.conf", BASE_PARAMETERS) is True

    assert uploaded == {"path": "sub/backup.conf", "content": "contents"}


def test_upload_content_failure_returns_false(session):
    session.return_value.resource.return_value.Bucket.return_value.upload_file.side_effect = (
        OSError("disk full")
    )

    assert S3Client().upload_content("contents", "backup.conf", BASE_PARAMETERS) is False


def test_read_content(session):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    bucket.download_fileobj.side_effect = lambda _, buf: buf.write(b"contents")

    assert S3Client().read_content("backup.conf", BASE_PARAMETERS) == "contents"


def test_read_content_without_bucket_returns_none(session):
    # VM-charm guard kept here: no bucket means no read attempt.
    assert S3Client().read_content("backup.conf", {"path": "/"}) is None
    session.assert_not_called()


@pytest.mark.parametrize("code", ["404", "AccessDenied"])
def test_read_content_error_returns_none(session, code):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    bucket.download_fileobj.side_effect = _client_error(code)

    assert S3Client().read_content("backup.conf", BASE_PARAMETERS) is None


def test_create_bucket_missing_parameters_is_noop(session):
    """Missing required parameters degrade to a no-op, as in the charms."""
    S3Client().create_bucket_if_not_exists({"path": "/bucket"})
    session.return_value.resource.assert_not_called()


def test_create_bucket_keeps_existing_bucket(session):
    bucket = session.return_value.resource.return_value.Bucket.return_value

    S3Client().create_bucket_if_not_exists(BASE_PARAMETERS)

    bucket.meta.client.head_bucket.assert_called_once_with(Bucket="backups")
    bucket.create.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        ConnectTimeoutError(endpoint_url=BASE_PARAMETERS["endpoint"]),
        SSLError(endpoint_url=BASE_PARAMETERS["endpoint"], error="certificate verify failed"),
    ],
    ids=["connect-timeout", "ssl"],
)
def test_create_bucket_reraises_connection_errors(session, error):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    bucket.meta.client.head_bucket.side_effect = error

    with pytest.raises(type(error)):
        S3Client().create_bucket_if_not_exists(BASE_PARAMETERS)
    bucket.create.assert_not_called()


REGION_CONFIG = {"CreateBucketConfiguration": {"LocationConstraint": "us-east-2"}}


@pytest.mark.parametrize(
    ("create_side_effects", "expected_create_kwargs"),
    [
        ([None], [REGION_CONFIG]),
        ([_client_error("InvalidLocationConstraint"), None], [REGION_CONFIG, {}]),
    ],
    ids=["with-region", "invalid-location-constraint"],
)
def test_create_bucket(session, create_side_effects, expected_create_kwargs):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    bucket.meta.client.head_bucket.side_effect = _client_error("NoSuchBucket")
    bucket.create.side_effect = create_side_effects

    S3Client().create_bucket_if_not_exists({**BASE_PARAMETERS, "region": "us-east-2"})

    assert [call.kwargs for call in bucket.create.call_args_list] == expected_create_kwargs
    bucket.wait_until_exists.assert_called()


def test_create_bucket_reraises_other_client_errors(session):
    bucket = session.return_value.resource.return_value.Bucket.return_value
    bucket.meta.client.head_bucket.side_effect = _client_error("NoSuchBucket")
    bucket.create.side_effect = _client_error("AccessDenied")

    with pytest.raises(ClientError):
        S3Client().create_bucket_if_not_exists(BASE_PARAMETERS)
