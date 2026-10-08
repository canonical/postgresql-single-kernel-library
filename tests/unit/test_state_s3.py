# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""CharmState owns the S3Requirer and exposes the live-fetch S3 connection info."""

from single_kernel_postgresql.config.literals import S3_RELATION_NAME
from single_kernel_postgresql.core.s3 import S3ConnectionInfo
from single_kernel_postgresql.lib.charms.data_platform_libs.v0.s3 import S3Requirer

CREDENTIALS = {
    "bucket": " backups ",
    "access-key": "key",
    "secret-key": "secret",
    "endpoint": "https://ceph.internal:8000/",
    "path": "backups",
}


def test_state_owns_the_s3_requirer(harness):
    state = harness.charm.state

    assert isinstance(state.s3_requirer, S3Requirer)
    assert state.s3_requirer.relation_name == S3_RELATION_NAME
    info = state.s3_connection_info
    assert isinstance(info, S3ConnectionInfo)
    assert info.s3_requirer is state.s3_requirer
    assert state.s3_relation is None

    harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    assert state.s3_relation is not None


def test_s3_connection_info_is_live_fetched_from_the_relation(harness):
    rel_id = harness.add_relation(S3_RELATION_NAME, "s3-integrator")
    harness.update_relation_data(rel_id, "s3-integrator", CREDENTIALS)

    parameters, missing = harness.charm.state.s3_connection_info.retrieve_s3_parameters()

    assert missing == []
    assert parameters["bucket"] == "backups"
    assert parameters["endpoint"] == "https://ceph.internal:8000"
    assert parameters["path"] == "/backups"


def test_s3_connection_info_reports_missing_required_parameters(harness):
    harness.add_relation(S3_RELATION_NAME, "s3-integrator")

    parameters, missing = harness.charm.state.s3_connection_info.retrieve_s3_parameters()

    assert parameters == {}
    assert missing == ["bucket", "access-key", "secret-key"]
