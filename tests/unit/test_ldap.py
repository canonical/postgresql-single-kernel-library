# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
"""Tests for the LDAP events handler (single_kernel_postgresql/events/ldap.py)."""

from unittest.mock import MagicMock, PropertyMock, patch

from ops.model import ActiveStatus
from single_kernel_postgresql.lib.charms.glauth_k8s.v0.ldap import LdapProviderData


def _set_leader(harness):
    """Elect the unit as leader, skipping the incidental leader-elected side effects."""
    charm = harness.charm
    with (
        patch.object(charm.cluster_manager, "configure_system_passwords"),
        patch.object(charm.config_manager, "update_config"),
    ):
        harness.set_leader(True)


def _add_secret(harness) -> str:
    """Register the bind-account secret the provider would have created and grant it."""
    secret_id = harness.add_model_secret(owner="glauth-k8s", content={"password": "password"})
    harness.grant_secret(secret_id, harness.charm.app.name)
    return secret_id


def _set_provider_data(harness, rel_id: int, secret_id: str = "") -> None:
    """Write the provider app databag the provider charm writes."""
    with harness.hooks_disabled():
        data = LdapProviderData(
            auth_method="simple",
            base_dn="dc=example,dc=net",
            bind_dn="cn=serviceuser,dc=example,dc=net",
            bind_password="password",
            bind_password_secret=secret_id or None,
            starttls=False,
            ldaps_urls=[],
            urls=["ldap://0.0.0.0:3893"],
        ).model_dump(exclude_none=True)
        harness.update_relation_data(rel_id, "glauth-k8s", data)


def _add_ldap_relation(harness) -> int:
    """Add an ``ldap`` relation."""
    return harness.add_relation("ldap", "glauth-k8s")


def _relate_provider(harness) -> int:
    """Wire the full provider side: relation, granted secret, and provider databag."""
    rel_id = _add_ldap_relation(harness)
    secret_id = _add_secret(harness)
    _set_provider_data(harness, rel_id, secret_id)
    return rel_id


def _patch_config(search_filter: str = "(uid=$username)"):
    """Stub CharmState.config.

    The harness fixture does not load config.yaml, so the required
    plugin_/profile fields would fail validation on a real parse.
    """
    return patch(
        "single_kernel_postgresql.core.state.CharmState.config",
        new_callable=PropertyMock,
        return_value=MagicMock(ldap_search_filter=search_filter),
    )


def test_on_ldap_ready_sets_flag_and_updates_config(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    with patch.object(charm.config_manager, "update_config") as update_config:
        charm.ldap._on_ldap_ready(MagicMock())

    assert charm.state.application.data["ldap_enabled"] == "True"
    update_config.assert_called_once()
    assert charm.unit.status == ActiveStatus()


def test_on_ldap_ready_skips_flag_write_when_not_leader(harness):
    charm = harness.charm
    with patch.object(charm.config_manager, "update_config") as update_config:
        charm.ldap._on_ldap_ready(MagicMock())

    # only the leader writes the peer flag; the config update happens on every unit
    update_config.assert_called_once()
    assert "ldap_enabled" not in charm.state.application.data


def test_on_ldap_unavailable_clears_flag_and_updates_config(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    charm.state.application.data.update({"ldap_enabled": "True"})
    with patch.object(charm.config_manager, "update_config") as update_config:
        charm.ldap._on_ldap_unavailable(MagicMock())

    assert charm.state.application.data["ldap_enabled"] == "False"
    update_config.assert_called_once()


def test_get_relation_data_without_relation_is_none(harness):
    assert harness.charm.ldap.get_relation_data() is None


def test_get_relation_data_resolves_bind_password_from_secret(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    _relate_provider(harness)

    data = charm.ldap.get_relation_data()

    assert data is not None
    assert data.base_dn == "dc=example,dc=net"
    assert data.bind_dn == "cn=serviceuser,dc=example,dc=net"
    assert data.bind_password == "password"
    assert data.urls == ["ldap://0.0.0.0:3893"]
    assert data.starttls is False


def test_get_ldap_parameters_requires_initialised_cluster(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    charm.state.application.data.update({"ldap_enabled": "True"})
    _relate_provider(harness)

    assert charm.ldap.get_ldap_parameters() == {}


def test_get_ldap_parameters_requires_flag(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    charm.state.application.data.update({"cluster_initialised": "True"})
    _relate_provider(harness)

    assert charm.ldap.get_ldap_parameters() == {}


def test_get_ldap_parameters_maps_provider_data(harness, patch_crypto):
    charm = harness.charm
    _set_leader(harness)
    charm.state.application.data.update({"cluster_initialised": "True", "ldap_enabled": "True"})
    _relate_provider(harness)

    with _patch_config() as config:
        parameters = charm.ldap.get_ldap_parameters()

    assert parameters == {
        "ldapbasedn": "dc=example,dc=net",
        "ldapbinddn": "cn=serviceuser,dc=example,dc=net",
        "ldapbindpasswd": "password",
        "ldaptls": False,
        "ldapurl": "ldap://0.0.0.0:3893",
        "ldapsearchfilter": config.return_value.ldap_search_filter,
    }
    # exclusive simple-bind/search+bind parameters must sit at the very end
    assert list(parameters)[-1] == "ldapsearchfilter"
