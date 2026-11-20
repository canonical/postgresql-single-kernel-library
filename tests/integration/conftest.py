# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared fixtures for the integration tests of every charm (substrate and Juju model)."""

import subprocess
from collections.abc import Generator

import jubilant
import pytest
from single_kernel_postgresql.config.enums import Substrates

# Juju model type for each substrate, used to check the controller matches --substrate.
MODEL_TYPES = {Substrates.VM: "iaas", Substrates.K8S: "caas"}

# Stash key of a module whose abort_on_fail test failed (value: nodeid of that test).
ABORTED_BY = pytest.StashKey[str]()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--controller",
        action="store",
        help="Juju controller to use; defaults to the current controller.",
    )
    parser.addoption(
        "--model",
        action="store",
        help="Juju model to use; defaults to a temporary model for each test module.",
    )
    parser.addoption(
        "--keep-models",
        action="store_true",
        help="Keep temporary models after the tests.",
    )
    parser.addoption(
        "--no-deploy",
        action="store_true",
        help="Skip tests marked skip_if_deployed; requires --model.",
    )
    parser.addoption(
        "--substrate",
        choices=[substrate.value for substrate in Substrates],
        required=True,
        help="Substrate to run the integration tests on.",
    )


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--no-deploy") and not config.getoption("--model"):
        raise pytest.UsageError("must specify --model when using --no-deploy")
    config.addinivalue_line(
        "markers", "skip_if_deployed: skip deployment when --no-deploy is specified"
    )
    config.addinivalue_line("markers", "vm_only: run the test only on the VM substrate")
    config.addinivalue_line("markers", "k8s_only: run the test only on the K8s substrate")
    config.addinivalue_line(
        "markers", "abort_on_fail: skip the rest of the module if this test fails"
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    report = yield
    if report.failed and item.get_closest_marker("abort_on_fail"):
        item.getparent(pytest.Module).stash[ABORTED_BY] = item.nodeid
    return report


def pytest_runtest_setup(item: pytest.Item) -> None:
    if item.get_closest_marker("skip_if_deployed") and item.config.getoption("--no-deploy"):
        pytest.skip("Skipping deployment because --no-deploy was specified.")
    aborted_by = item.getparent(pytest.Module).stash.get(ABORTED_BY, None)
    if aborted_by:
        pytest.skip(f"aborted: {aborted_by} failed")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    substrate = Substrates(config.getoption("--substrate"))
    for item in items:
        if substrate != Substrates.VM and item.get_closest_marker("vm_only"):
            item.add_marker(pytest.mark.skip(reason="VM only test"))
        if substrate != Substrates.K8S and item.get_closest_marker("k8s_only"):
            item.add_marker(pytest.mark.skip(reason="K8s only test"))


@pytest.fixture(scope="session", autouse=True)
def substrate(request: pytest.FixtureRequest) -> Substrates:
    """The substrate selected with --substrate.

    Overrides the fixture in tests/conftest.py, which runs every test on both substrates.
    """
    return Substrates(request.config.getoption("--substrate"))


@pytest.fixture(scope="session")
def architecture() -> str:
    """The architecture of the machine running the tests (e.g. amd64)."""
    return subprocess.run(
        ["dpkg", "--print-architecture"], capture_output=True, check=True, encoding="utf-8"
    ).stdout.strip()


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest, substrate: Substrates) -> Generator[jubilant.Juju]:
    """Juju model for the test module: --model if given, a temporary one otherwise."""
    controller = request.config.getoption("--controller")
    model = request.config.getoption("--model")
    keep_models = request.config.getoption("--keep-models")

    if model:
        juju = jubilant.Juju(model=f"{controller}:{model}" if controller else model)
        _check_model_type(juju, substrate)
        yield juju
    else:
        with jubilant.temp_model(keep=keep_models, controller=controller) as juju:
            _check_model_type(juju, substrate)
            yield juju


def _check_model_type(juju: jubilant.Juju, substrate: Substrates) -> None:
    model_type = juju.status().model.type
    if model_type != MODEL_TYPES[substrate]:
        pytest.fail(
            f"--substrate={substrate.value} needs a {MODEL_TYPES[substrate]} model, but the model is"
            f" {model_type}: use --controller to pick a {substrate.value} controller"
        )
