---
myst:
  html_meta:
    description: "Overview of Charmed PostgreSQL major versions, the legacy and modern charm generations, supported Charmhub channels, and migration guidance."
---

(charm-versions)=
# PostgreSQL major versions
{{vm}}{{k8s}}

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
| PostgreSQL 16           | `16/stable`      | modern | ![check] Latest version - new features are released here          |
| PostgreSQL 14           | `14/stable`      | modern | ![check] In maintenance mode - bug fixes and security updates only. See: [PostgreSQL 14 documentation](https://documentation.ubuntu.com/charmed-postgresql/14/) |
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

## PostgreSQL 16 features

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
  * See all endpoints on [Charmhub](https://charmhub.io/postgresql/integrations?channel=16/stable)
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

## PostgreSQL 14 features

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

<!--Links-->
[cross]: https://img.icons8.com/?size=16&id=CKkTANal1fTY&format=png&color=D00303
[check]: https://img.icons8.com/color/20/checkmark--v1.png