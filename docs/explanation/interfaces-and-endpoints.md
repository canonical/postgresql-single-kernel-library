(interfaces-and-endpoints)=
# Interfaces/endpoints
{{vm}}{{k8s}}

<!--TODO: update for PG 16? -->

The charm supports modern `postgresql_client` and legacy `pgsql` interfaces (in a backward compatible mode).

```{caution}
Do **not** relate both modern and legacy interfaces simultaneously!
```

## Modern interfaces

This charm provides modern ['postgresql_client' interface](https://github.com/canonical/charm-relation-interfaces). Applications can easily connect PostgreSQL using ['data_interfaces' ](https://charmhub.io/data-platform-libs/libraries/data_interfaces) library from ['data-platform-libs'](https://github.com/canonical/data-platform-libs/).

### Modern `postgresql_client` interface (`database` endpoint):

Adding a relation is accomplished with `juju integrate` via the `database` endpoint.

For example:

````{tab-set}
```{tab-item} VM
:sync: vm

    # Deploy Charmed PostgreSQL cluster with 3 nodes
    juju deploy postgresql -n 3 --channel 16/stable

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
    juju deploy postgresql-k8s --channel 16/stable -n 3 --trust

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

Find more details about default and additional database user roles in {ref}`users`.

## Legacy interfaces

```{note}
Legacy relations are deprecated and will be discontinued on future releases. Their usage should be avoided.

Check the legacy interfaces implementation limitations in {ref}`charm-versions`.
```

### Legacy `pgsql` interface (`db` and `db-admin` endpoints):

This charm supports legacy interface `pgsql` from the previous [PostgreSQL charm](https://launchpad.net/postgresql-charm):

````{tab-set}
```{tab-item} VM
:sync: vm

    juju relate postgresql:db mailman3-core
    juju relate postgresql:db-admin landscape-server
```
```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 16/stable --trust
    juju deploy finos-waltz-k8s --channel edge
    juju relate postgresql-k8s:db finos-waltz-k8s
```
````

```{note}
The endpoint `db-admin` provides the same legacy interface `pgsql` with PostgreSQL admin-level privileges. It is NOT recommended to use it due to security limitations.
```
