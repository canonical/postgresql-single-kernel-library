# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Live-fetch view over the S3 (s3-parameters) relation data."""

import logging

from single_kernel_postgresql.lib.charms.data_platform_libs.v0.s3 import S3Requirer

logger = logging.getLogger(__name__)


class S3ConnectionInfo:
    """Reads and normalizes S3 connection parameters from the s3-parameters relation."""

    def __init__(self, s3_requirer: S3Requirer):
        """Initialize the S3 connection info with the requirer bound to the relation."""
        self.s3_requirer = s3_requirer

    def retrieve_s3_parameters(self) -> tuple[dict, list[str]]:
        """Retrieve S3 parameters from the S3 integrator relation.

        Returns:
            a tuple of (parameters, missing_required_parameters). If any required
            parameter is missing, parameters is empty and missing lists the absent
            keys. Otherwise parameters has defaults applied and values normalized.
        """
        s3_parameters = self.s3_requirer.get_s3_connection_info()
        required_parameters = [
            "bucket",
            "access-key",
            "secret-key",
        ]
        missing_required_parameters = [
            param for param in required_parameters if param not in s3_parameters
        ]
        if missing_required_parameters:
            logger.warning(
                f"Missing required S3 parameters in relation with S3 integrator: {missing_required_parameters}"
            )
            return {}, missing_required_parameters

        # Add some sensible defaults (as expected by the code) for missing optional parameters
        s3_parameters.setdefault("endpoint", "https://s3.amazonaws.com")
        s3_parameters.setdefault("path", "")
        s3_parameters.setdefault("s3-uri-style", "host")
        s3_parameters.setdefault("delete-older-than-days", "9999999")

        # Strip whitespaces from all parameters.
        for key, value in s3_parameters.items():
            if isinstance(value, str):
                s3_parameters[key] = value.strip()

        # Clean up extra slash symbols to avoid issues on 3rd-party storages
        # like Ceph Object Gateway (radosgw).
        s3_parameters["endpoint"] = s3_parameters["endpoint"].rstrip("/")
        s3_parameters["path"] = (
            f"/{s3_parameters['path'].strip('/')}"  # The slash in the beginning is required by pgBackRest.
        )
        s3_parameters["bucket"] = s3_parameters["bucket"].strip("/")

        return s3_parameters, []
