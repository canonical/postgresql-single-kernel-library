# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""boto3-based client for reading from and writing to an S3 bucket."""

import logging
import os
import tempfile
from io import BytesIO

from boto3.session import Session
from botocore.client import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, SSLError
from botocore.loaders import create_loader
from botocore.regions import EndpointResolver

logger = logging.getLogger(__name__)


class S3Client:
    """Client for uploading to and downloading from an S3 bucket.

    The only construction-time input is the TLS CA-chain path used to verify the
    S3 endpoint; the rest is passed per call as an ``s3_parameters`` dict, as
    produced by ``core.s3.S3ConnectionInfo.retrieve_s3_parameters``.
    """

    def __init__(self, tls_ca_chain_filename: str | None = None):
        """Initialize the S3 client with the TLS CA-chain path, or None for the system trust store."""
        self._tls_ca_chain_filename = tls_ca_chain_filename

    def _construct_endpoint(self, s3_parameters: dict) -> str:
        """Construct the S3 endpoint using the region, needed for AWS endpoints without one."""
        # Use the provided endpoint unless it is an AWS endpoint missing the region.
        endpoint = s3_parameters["endpoint"]

        # Construct the endpoint using the region, and use it for AWS endpoints.
        loader = create_loader()
        data = loader.load_data("endpoints")
        resolver = EndpointResolver(data)
        endpoint_data = resolver.construct_endpoint("s3", s3_parameters.get("region"))
        if endpoint_data and endpoint.endswith(endpoint_data["dnsSuffix"]):
            endpoint = f"{endpoint.split('://')[0]}://{endpoint_data['hostname']}"

        return endpoint

    def _get_s3_session_resource(self, s3_parameters: dict):
        kwargs = {
            "aws_access_key_id": s3_parameters["access-key"],
            "aws_secret_access_key": s3_parameters["secret-key"],
        }
        if "region" in s3_parameters:
            kwargs["region_name"] = s3_parameters["region"]
        session = Session(**kwargs)
        return session.resource(
            "s3",
            endpoint_url=self._construct_endpoint(s3_parameters),
            verify=(self._tls_ca_chain_filename or None),
            config=Config(
                # https://github.com/boto/boto3/issues/4400#issuecomment-2600742103
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    def upload_content(self, content: str, s3_path: str, s3_parameters: dict) -> bool:
        """Upload ``content`` to ``s3_path`` relative to the configured path; returns success."""
        bucket_name = s3_parameters["bucket"]
        processed_s3_path = os.path.join(s3_parameters["path"], s3_path).lstrip("/")
        location = f"bucket={bucket_name}, path={processed_s3_path}"
        try:
            logger.debug(f"Uploading content to {location}")
            s3 = self._get_s3_session_resource(s3_parameters)
            bucket = s3.Bucket(bucket_name)

            with tempfile.NamedTemporaryFile() as temp_file:
                temp_file.write(content.encode("utf-8"))
                temp_file.flush()
                bucket.upload_file(temp_file.name, processed_s3_path)
        except Exception as e:
            logger.exception(f"Failed to upload content to S3 {location}", exc_info=e)
            return False

        return True

    def read_content(self, s3_path: str, s3_parameters: dict) -> str | None:
        """Read ``s3_path`` relative to the configured path, or None if it does not exist."""
        if not (bucket_name := s3_parameters.get("bucket")):
            logger.debug("No bucket set")
            return None
        processed_s3_path = os.path.join(s3_parameters["path"], s3_path).lstrip("/")
        location = f"bucket={bucket_name}, path={processed_s3_path}"
        try:
            logger.debug(f"Reading content from {location}")
            s3 = self._get_s3_session_resource(s3_parameters)
            bucket = s3.Bucket(bucket_name)
            with BytesIO() as buf:
                bucket.download_fileobj(processed_s3_path, buf)
                return buf.getvalue().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.info(f"No such object to read from S3 {location}")
            else:
                logger.exception(f"Failed to read content from S3 {location}", exc_info=e)
        except Exception as e:
            logger.exception(f"Failed to read content from S3 {location}", exc_info=e)

        return None

    def create_bucket_if_not_exists(self, s3_parameters: dict) -> None:
        """Create the configured bucket if it does not already exist.

        ConnectTimeoutError and SSLError are re-raised so the user can fix the
        network/TLS problem and call ``juju resolve`` to re-trigger the hook.

        Missing required parameters are a no-op, exactly as the charms'
        ``_create_bucket_if_not_exists`` early return: the initialization flow
        degrades into the graceful stanza block message downstream.
        """
        missing_parameters = [
            param for param in ("bucket", "access-key", "secret-key") if param not in s3_parameters
        ]
        if missing_parameters:
            logger.warning(
                f"Missing required S3 parameters in relation with S3 integrator: {missing_parameters}"
            )
            return
        bucket_name = s3_parameters["bucket"]
        region = s3_parameters.get("region", "")

        try:
            s3 = self._get_s3_session_resource(s3_parameters)
        except ValueError:
            logger.exception("Failed to create a session '%s' in region=%s.", bucket_name, region)
            raise
        bucket = s3.Bucket(bucket_name)
        try:
            bucket.meta.client.head_bucket(Bucket=bucket_name)
            logger.debug("Bucket %s exists.", bucket_name)
            return
        except ConnectTimeoutError as e:
            # Re-raise the error if the connection timeouts, so the user has the possibility to
            # fix network issues and call juju resolve to re-trigger the hook that calls
            # this method.
            logger.error(f"error: {e!s} - please fix the error and call juju resolve on this unit")
            raise
        except SSLError as e:
            logger.error(f"error: {e!s} - Is TLS enabled and CA chain set on S3?")
            raise
        except ClientError:
            logger.warning("Bucket %s doesn't exist or you don't have access to it.", bucket_name)

        try:
            bucket.create(CreateBucketConfiguration={"LocationConstraint": region})
            bucket.wait_until_exists()
            logger.info("Created bucket '%s' in region=%s", bucket_name, region)
        except ClientError as error:
            if error.response["Error"]["Code"] != "InvalidLocationConstraint":
                logger.exception(
                    "Couldn't create bucket named '%s' in region=%s.", bucket_name, region
                )
                raise
            logger.info("Specified location-constraint is not valid, trying create without it")
            try:
                bucket.create()
                bucket.wait_until_exists()
                logger.info("Created bucket '%s', ignored region=%s", bucket_name, region)
            except ClientError:
                logger.exception(
                    "Couldn't create bucket named '%s' in region=%s.", bucket_name, region
                )
                raise
