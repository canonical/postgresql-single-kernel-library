(interfaces-and-endpoints)=
# Interfaces and endpoints
{{vm_k8s}}

This charm provides the modern ['postgresql_client' interface](https://github.com/canonical/charm-relation-interfaces). Applications can easily connect PostgreSQL using ['data_interfaces' ](https://charmhub.io/data-platform-libs/libraries/data_interfaces) library from ['data-platform-libs'](https://github.com/canonical/data-platform-libs/).

{{seealso}} {ref}`integrate-with-your-charm`.

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

{octicon}`arrow-right` See the {ref}`users` page for more details about default and additional database user roles.
