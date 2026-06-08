(integrate)=
# How to integrate with PostgreSQL
[Integrations](https://juju.is/docs/juju/relation), also known as “relations” are connections between two applications with compatible endpoints. These connections simplify the creation and management of users, passwords, and other shared data.

Charmed PostgreSQL can be integrated with any charmed application that supports its interfaces.

If you are a charm developer who wants to implement PostgreSQL-compatible endpoints, skip ahead to {ref}`integrate-with-your-charm`.

If you are a charm user who wants to know more about integrating PostgreSQL with existing charms or applications, continue reading {ref}`integrate-with-a-client-application`.

(integrate-with-a-client-application)=
## Integrate with a client application

PostgreSQL can be integrated with other charms that support the `postgresql_client` interface and with non-Juju applications.

### Charms with the `postgresql_client` interface

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

{{seealso}} [All compatible charms](https://charmhub.io/integrations/postgresql_client)

### Non-Juju applications

To integrate with an application outside of Juju, you use the [`data-integrator`](https://charmhub.io/data-integrator) charm to create the required credentials and endpoints.

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

{{seealso}} {ref}`manage-passwords` for information about credentials, such as password rotation or requesting a custom username.

(integrate-with-your-charm)=
## Integrate with your charm

To add native support for PostgreSQL integrations in your charm, see [`data-platform-libs`](https://github.com/canonical/data-platform-libs).

{{seealso}} [Ops | Integrate your charm with PostgreSQL](https://ops.readthedocs.io/en/latest/tutorial/from-zero-to-hero-write-your-first-kubernetes-charm/integrate-your-charm-with-postgresql.html)