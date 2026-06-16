# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError
from single_kernel_postgresql.core.config import CharmConfig, K8SCharmConfig

_LOCALE_FIELDS = ("response_lc_monetary", "response_lc_numeric", "response_lc_time")


def _locale_values(model: type, field: str) -> set[str]:
    annotation = model.model_fields[field].annotation
    literal = next(arm for arm in get_args(annotation) if arm is not type(None))
    return set(get_args(literal))


def test_charmconfig_imports_and_keys_resolve():
    keys = CharmConfig.keys()
    for field in _LOCALE_FIELDS:
        assert field in keys


def test_k8s_config_adds_exactly_the_rock_only_locales():
    """K8s locale fields are the shared set plus exactly C.utf8 and POSIX."""
    for field in _LOCALE_FIELDS:
        base = _locale_values(CharmConfig, field)
        k8s = _locale_values(K8SCharmConfig, field)
        assert "C" in base and "C.utf8" not in base and "POSIX" not in base
        assert k8s - base == {"C.utf8", "POSIX"}
        assert base - k8s == set()


def test_locale_fields_validate_per_substrate():
    """The VM model rejects C.utf8/POSIX; the K8s model accepts them."""
    for field in _LOCALE_FIELDS:
        base = TypeAdapter(CharmConfig.model_fields[field].annotation)
        k8s = TypeAdapter(K8SCharmConfig.model_fields[field].annotation)
        for locale in ("C.utf8", "POSIX"):
            with pytest.raises(ValidationError):
                base.validate_python(locale)
            k8s.validate_python(locale)
        base.validate_python("C")
        k8s.validate_python("C")
