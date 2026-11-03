# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import jubilant
import pytest

from . import architecture

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def substrate() -> str:
    """Override the vm/k8s parametrized fixture from tests/conftest.py.

    The watcher only runs on VM; without this override every test would run twice.
    """
    return "vm"


@pytest.fixture(scope="session")
def charm():
    # Return str instead of pathlib.Path since python-libjuju's model.deploy(), juju deploy, and
    # juju bundle files expect local charms to begin with `./` or `/` to distinguish them from
    # Charmhub charms.
    return f"./postgresql-watcher_ubuntu@24.04-{architecture.architecture}.charm"


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Pytest fixture that wraps :meth:`jubilant.with_model`.

    This adds command line parameter ``--keep-models`` (see help for details).
    """
    model = request.config.getoption("--model")
    keep_models = bool(request.config.getoption("--keep-models"))

    if model:
        juju = jubilant.Juju(model=model)
        yield juju
    else:
        with jubilant.temp_model(keep=keep_models) as juju:
            yield juju
