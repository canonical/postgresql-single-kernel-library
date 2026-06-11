---
myst:
  html_meta:
    description: "Overview of Charmed PostgreSQL major versions, the legacy and modern charm generations, supported Charmhub channels, and migration guidance."
---

(charm-versions)=
# PostgreSQL major versions
{{vm_k8s}}

````{tab-set}
```{tab-item} VM
:sync: vm

There are two [generations](https://documentation.ubuntu.com/juju/3.6/reference/charm/#by-generation) of charms stored under the same charm name: `postgresql`. In these docs, we refer to them as "legacy" and "modern".
```
```{tab-item} K8s
:sync: k8s

There are two [generations](https://documentation.ubuntu.com/juju/3.6/reference/charm/#by-generation) of charms stored under the same charm name: `postgresql-k8s`. In these docs, we refer to them as "legacy" and "modern".
```
````

They are shipped in the following [Charmhub tracks](https://documentation.ubuntu.com/juju/3.6/reference/charm/#track):

| Charm name              | Charmhub channel | Type   | Status                                                            |
| ----------------------- | ---------------- | ------ | ----------------------------------------------------------------- |
| PostgreSQL 16           | `16/stable`      | modern | ![check] Latest version - new features are released here. See: [PostgreSQL 16 documentation](https://documentation.ubuntu.com/charmed-postgresql/16/) |
| PostgreSQL 14           | `14/stable`      | modern | ![check] In maintenance mode - bug fixes and security updates only. |
| Legacy PostgreSQL charm | `latest/stable`  | legacy | ![cross] Deprecated. Migrate to 14 as soon as possible. |

Legacy charm (deprecated)
: Also known as a [Reactive charm](https://documentation.ubuntu.com/juju/3.6/reference/charm/#reactive-charm). Found in the Charmhub channel `latest/stable`.
: Provided `db` and `db-admin` endpoints for the `pgsql` interface.

Modern charm
: Also known as an [Ops charm](https://documentation.ubuntu.com/juju/3.6/reference/charm/#ops-charm). Found in the Charmhub channels `14/stable` and `16/stable`.
: `14/stable` provides legacy endpoints and new `database` endpoint for the `postgresql_client` interface.
: `16/stable` **does not** provide legacy endpoints - only the new `database` `database` endpoint for the `postgresql_client` interface.

{{seealso}} {ref}`interfaces-and-endpoints`

## Choosing a version

* **For new deployments**: Use **PostgreSQL 16** for the latest features and long-term support
* **For existing PostgreSQL 14 deployments**: Continue using PostgreSQL 14 or plan migration to 16
* **For legacy charm users**: Migrate to PostgreSQL 14 as soon as possible

## PostgreSQL 16

**Latest stable version** with active feature development.

* **Base:** Ubuntu 24.04 LTS (Noble)
* **Supported architectures:** `amd64`, `arm64`
* **Channel:** `16/stable` (latest development available for testing in `16/edge`)
* **Juju version:** Requires Juju 3.6+ LTS
* **Support status:** ![check] Active development and full support

### New features

* [**Juju spaces**](juju-spaces) - Enhanced networking capabilities for complex deployment scenarios
* [**Juju user secrets**](https://documentation.ubuntu.com/juju/latest/reference/secret/index.html#user-secret) - Secure management of the charm's [internal passwords](manage-passwords)
* **Improved** [**security hardening**](security-hardening) - Enhanced security posture and best practices
* **TLS v4 library migration**
  * New endpoints `client-certificates` and `peer-certificates`
  * Endpoint `peer-interfaces` uses TLS by default
  * See all endpoints on [Charmhub](https://charmhub.io/postgresql/integrations?channel=14/stable)
* [**Timescale Community Edition**](enable-timescaledb) replaces Timescale Apache 2
* **Improved built-in** [**roles**](/explanation/roles) - Enhanced role-based access control system
* **New** **refresh process** for in-place upgrades

### Deprecated or removed

Important changes to keep in mind when migrating from 14 to 16:

* **Legacy interface `psql`** - Endpoints `db` and `db-admin` are no longer supported
  * See [](/explanation/interfaces-and-endpoints) for current supported interfaces
* **Support for Juju < `v3.6` removed**
  * Charmed PostgreSQL 16 requires Juju `3.6+ LTS` due to [Juju secrets](https://documentation.ubuntu.com/juju/3.6/reference/secret/index.html) support
* **Juju actions `get-password` and `set-password` removed**
  * Replaced by [Juju secrets](https://documentation.ubuntu.com/juju/3.6/reference/secret/index.html) for enhanced security
* **[Timescale Apache 2 edition](https://docs.timescale.com/about/latest/timescaledb-editions/) replaced**
  * Now uses [Timescale Community edition](https://docs.timescale.com/about/latest/timescaledb-editions/)
* **Charm action `set-tls-private-key` removed**
  * Will be re-introduced as Juju User Secrets in future releases
* **Charm actions renamed for consistency:**
  * `pre-upgrade-check` → `pre-refresh-check`
  * `resume-upgrade` → `resume-refresh`
  * Changes align with `juju refresh` terminology
* **Charm endpoint `certificates` split into separate endpoints:**
  * `client-certificates` - For client certificate management
  * `peer-certificates` - For peer-to-peer certificate management

For detailed information about all PostgreSQL 16 releases, see {ref}`release-notes`.

## PostgreSQL 14

**Maintenance mode** with bug fixes and security updates only.

* **Base:** Ubuntu 22.04 LTS (Jammy)
* **Supported architectures:** `amd64`, `arm64`
* **Channel:** `14/stable`
* **Juju version:** Partially compatible with older Juju versions down to 2.9
* **Support status:** 🔧 Bug fixes and security updates only

### Features

* [**Deployment on multiple cloud services**](deploy), including Sunbeam, MAAS, AWS, GCE, and Azure
* [**Juju storage**](juju-storage) - Flexible storage configuration options
* [**Back up and restore**](back-up-and-restore), including point-in-time recovery
* [**COS integration**](observability-cos) - Enable observability tools like Grafana, Loki, Tempo, and Parca
* [**TLS integration**](enable-tls)
* [**LDAP integration**](enable-ldap) - Centralised authentication for PostgreSQL clusters
* [**`amd64` and `arm64`architecture** support](system-requirements)

For detailed information about all PostgreSQL 14 releases, see the [PostgreSQL 14 Releases page](https://documentation.ubuntu.com/charmed-postgresql/14/release-notes).

(legacy-charm)=
## Legacy charm

The legacy PostgreSQL charm is a [Reactive charm](https://documentation.ubuntu.com/juju/3.6/reference/charm/#reactive-charm) in the Charmhub channel `latest/stable`. 

It provided `db` and `db-admin` endpoints for the `pgsql` interface.

**We strongly advise against using the now deprecated `latest/` track**. It will be removed from Charmhub in the near future.

The [default track](https://docs.openstack.org/charm-guide/yoga/project/charm-delivery.html) was switched from the `latest/` to `14/` to ensure all new deployments use a modern codebase. See [this Discourse post](https://discourse.charmhub.io/t/request-switch-default-track-from-latest-to-14-for-postgresql-k8s-charms/10314) for more information about the switch.

### How to migrate from legacy to modern

To migrate from the legacy PostgreSQL charm to the modern PostgreSQL 14 charm, you can use two approaches:

**Quick method**: Since PostgreSQL 14 provides the legacy databases, you can simply relate your application with the new charm with the same `db` endpoint without any extra changes:

````{tab-set}
```{tab-item} VM
:sync: vm

	postgresql:
		charm: postgresql
		channel: 14/stable
```
```{tab-item} K8s
:sync: k8s

	postgresql:
		charm: postgresql-k8s
		channel: 14/stable
		trust: true
```
````

**Recommended method**: Migrate your application to the new [`postgresql_client` interface](https://github.com/canonical/charm-relation-interfaces). 

The application will connect to PostgreSQL using the [`data_interfaces`](https://charmhub.io/data-platform-libs/libraries/data_interfaces) library from [data-platform-libs](https://github.com/canonical/data-platform-libs/) via the `database` endpoint.

{{seealso}} {ref}`integrate-with-your-charm`.

### How to deploy the legacy PostgreSQL charm

Deploy the charm using the channel `latest/stable`:

````{tab-set}
```{tab-item} VM
:sync: vm

	postgresql:
		charm: postgresql
		channel: latest/stable
```
```{tab-item} K8s
:sync: k8s

	postgresql:
		charm: postgresql-k8s
		channel: latest/stable
```
````

```{caution}
Remove the charm store prefix `cs:` from the bundle. Otherwise, the modern charm will be chosen by Juju (due to the default track pointing to `14/stable` and not `latest/stable`).

A common error message is: `cannot deploy application "postgresql": unknown option "..."`.
```

### Configuration options

The legacy charm config options were not moved to the modern charms. Modern charms apply the best possible configuration automatically. 

Feel free to {ref}`contact us <contact>` about the database tuning and configuration options.

### Extensions supported by modern charm

The legacy charm provided plugins/extensions enabling through the relation (interface `pgsql`).This is NOT supported by modern charms (neither `pgsql` nor `postgresql_client` interfaces). Please enable the necessary extensions using appropriate `plugin_*_enable` [config option](https://charmhub.io/postgresql/configure) of the modern charm. After enabling the modern charm, it will provide plugins support for both `pgsql` (only if it's PostgreSQL 14) and `postgresql_client` interfaces.

{{seealso}} {ref}`supported-extensions`

Feel free to {ref}`contact us <contact>` if there is a particular extension you are interested in.

### Roles supported by modern charm

In the legacy charm, the user could request roles by setting the `roles` field to a comma separated list of desired roles. This is NOT supported by the `14/` modern charm implementation of the legacy `pgsql` interface. 

The same functionality is provided via the modern `postgresql_client` using {ref}`extra user roles <users>`. 

For more information about migrating the new interface on PostgreSQL 14, see {ref}`integrate-with-your-charm`.

### Workload artifacts

The legacy charm used to deploy PostgreSQL from APT/Debian packages,
while the modern charm installs and operates PostgreSQL snap [charmed-postgresql](https://snapcraft.io/charmed-postgresql). 

{{seealso}} {ref}`architecture`

### How to report issues and contact authors

````{tab-set}
```{tab-item} VM
:sync: vm

The legacy charm (from `latest/stable`) is stored on [Launchpad](https://git.launchpad.net/postgresql-charm/). Report legacy charm issues [here](https://bugs.launchpad.net/postgresql-charm).

The modern charms are stored on GitHub: [PostgreSQL 14 branch](https://github.com/canonical/postgresql-operator/tree/main) and [PostgreSQL 16 branch](https://github.com/canonical/postgresql-operator/tree/14/stable) . Report modern charm issues [here](https://github.com/canonical/postgresql-operator/issues/new/choose).
```
```{tab-item} K8s
:sync: k8s

The "legacy charm" (from `latest/stable`) is stored on [Launchpad](https://git.launchpad.net/charm-k8s-postgresql), here is the link to report all [legacy charm issues](https://bugs.launchpad.net/charm-k8s-postgresql).

The "modern charm" (from `14/stable`) is stored on [GitHub](https://github.com/canonical/postgresql-k8s-operator), here is the link to report [modern charm issues](https://github.com/canonical/postgresql-k8s-operator/issues/new/choose).
```
````

Do you have questions? {ref}`Contact us <contact>`!

<!--Links-->
[cross]: https://img.icons8.com/?size=16&id=CKkTANal1fTY&format=png&color=D00303
[check]: https://img.icons8.com/color/20/checkmark--v1.png