---
myst:
  html_meta:
    description: "Integrate Charmed PostgreSQL with other Juju charms using the postgresql_client interface, or connect non-Juju applications to the database."
---

(integrate)=
# How to integrate with PostgreSQL
{{vm_k8s}}

[Integrations](https://juju.is/docs/juju/relation), also known as “relations” are connections between two applications with compatible endpoints. These connections simplify the creation and management of users, passwords, and other shared data.

Charmed PostgreSQL can be integrated with any charmed application that supports its interfaces.

If you are a charm developer who wants to implement PostgreSQL-compatible endpoints, skip ahead to {ref}`integrate-with-your-charm`.

If you are a charm user who wants to know more about integrating PostgreSQL with existing charms or applications, continue reading {ref}`integrate-with-a-client-application`.

(integrate-with-a-client-application)=
## Integrate with a client application

Integrations with charmed applications are supported via the modern [`postgresql_client`](https://github.com/canonical/charm-relation-interfaces/blob/main/interfaces/postgresql_client/v0/README.md) interface, and the legacy `psql` interface from the [original version](https://launchpad.net/postgresql-charm) of the charm.

### Modern `postgresql_client` interface

To integrate with a charmed application that already supports the `postgresql_client` interface (for example, PgBouncer or Temporal K8s) run

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate postgresql:database <charm>

To remove the integration, run

    juju remove-relation postgresql <charm>
```
```{tab-item} K8s
:sync: k8s

    juju integrate postgresql-k8s:database <charm>

To remove the integration, run

    juju remove-relation postgresql-k8s <charm>
```
````

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju integrate` becomes `juju relate` for Juju 2.9.
```

{{seealso}} [All compatible charms](https://charmhub.io/integrations/postgresql_client)

### Legacy `pgsql` interface

Note that this interface is **deprecated**. See {ref}`legacy-charm`.

````{tab-set}
```{tab-item} VM
:sync: vm

To integrate via the legacy interface, run

    juju integrate postgresql:db <charm>

Extended permissions can be requested using the `db-admin` endpoint:

    juju integrate postgresql:db-admin <charm>
```
```{tab-item} K8s
:sync: k8s

Using the `mattermost-k8s` charm as an example, an integration with the legacy interface could be created as follows:

    juju integrate postgresql-k8s:db mattermost-k8s:db

Extended permissions can be requested using the `db-admin` endpoint:

    juju integrate postgresql-k8s:db-admin mattermost-k8s:db
```
````

### Other applications

To integrate with a charm that doesn't support the interface or a client application that lives outside of Juju, use the [`data-integrator`](https://charmhub.io/data-integrator) charm to create the required credentials and endpoints.

Deploy `data-integrator`:

```shell
juju deploy data-integrator --config database-name=<name>
```

Integrate with PostgreSQL:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate data-integrator postgresql
```

```{tab-item} K8s
:sync: k8s

    juju integrate data-integrator postgresql-k8s
```
````

Use the `get-credentials` action to retrieve credentials from `data-integrator`:

```shell
juju run data-integrator/leader get-credentials
```

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

{{seealso}} {ref}`manage-passwords` for information about credentials, such as password rotation.
(integrate-with-your-charm)=
## Integrate with your charm

To add native support for PostgreSQL integrations in your charm, see [`data-platform-libs`](https://github.com/canonical/data-platform-libs).

{{seealso}} [Ops | Integrate your charm with PostgreSQL](https://ops.readthedocs.io/en/latest/tutorial/from-zero-to-hero-write-your-first-kubernetes-charm/integrate-your-charm-with-postgresql.html)