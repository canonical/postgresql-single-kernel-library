---
myst:
  html_meta:
    description: "Reference for Charmed PostgreSQL 14 modern and legacy Juju interfaces and endpoints."
---

(interfaces-and-endpoints)=
# Interfaces and endpoints
{{vm_k8s}}

The charm supports modern `postgresql_client` and legacy `pgsql` interfaces (in a backward compatible mode).

```{dropdown} Do not relate both modern and legacy interfaces simultaneously
:class-container: dropdown-caution
:icon: alert-fill

This might cause the charm to enter a blocked state. Relate one interface at a time.
```

## Modern interfaces

This charm provides modern [`postgresql_client` interface](https://github.com/canonical/charm-relation-interfaces). Applications can easily connect PostgreSQL using [`data_interfaces`](https://charmhub.io/data-platform-libs/libraries/data_interfaces) library from [`data-platform-libs`](https://github.com/canonical/data-platform-libs/).

### Modern `postgresql_client` interface (`database` endpoint)

{{seealso}} {ref}`integrate-with-your-charm`.

Adding a relation is accomplished with `juju integrate` (or `relate` for Juju 2.9) via the `database` endpoint.

For example:

````{tab-set}
```{tab-item} VM
:sync: vm

    # Deploy Charmed PostgreSQL cluster with 3 nodes
    juju deploy postgresql -n 3 --channel 14/stable

    # Deploy the relevant application charms
    juju deploy mycharm

    # Relate PostgreSQL with your application
    juju relate postgresql:database mycharm:database

    # Check established relation (using postgresql_client interface):
    juju status --relations

    # Example of a properly established relation:
    # > Relation provider      Requirer          Interface          Type
    # > postgresql:database    mycharm:database  postgresql_client  regular
```
```{tab-item} K8s
:sync: k8s

    # Deploy Charmed PostgreSQL cluster with 3 nodes
    juju deploy postgresql-k8s --channel 14/stable -n 3 --trust

    # Deploy the relevant application charms
    juju deploy mycharm

    # Relate PostgreSQL with your application
    juju relate postgresql-k8s:database mycharm:database

    # Check established relation (using postgresql_client interface):
    juju status --relations

    # Example of a properly established relation:
    # > Relation provider          Requirer          Interface          Type
    # > postgresql-k8s:database    mycharm:database  postgresql_client  regular
```
````

{octicon}`arrow-right` See the {ref}`users` page for more details about default and additional database user roles.

## Legacy interfaces

Legacy relations are deprecated and will be discontinued on future releases. Their usage should be avoided. 

Check the the limitations of legacy interface implementations in {ref}`legacy-charm`.

### Legacy `pgsql` interface (`db` and `db-admin` endpoints):

This charm supports legacy interface `pgsql` from the previous [PostgreSQL charm](https://launchpad.net/postgresql-charm).

For example:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju relate postgresql:db mailman3-core
    juju relate postgresql:db-admin landscape-server
```
```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 14/stable --trust
    juju deploy finos-waltz-k8s --channel edge
    juju relate postgresql-k8s:db finos-waltz-k8s
```
````

```{dropdown} <code>db-admin</code> security warning
:open:
:class-container: dropdown-caution
:icon: alert-fill

The endpoint `db-admin` provides the same legacy interface `pgsql` with PostgreSQL admin-level privileges. 

It is NOT recommended to use it due to security limitations.
```

