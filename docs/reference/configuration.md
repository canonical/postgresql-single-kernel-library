---
orphan: True
---

# Configuration

The PostgreSQL charm exposes several configuration parameters you can use to fine tune your deployment via [`juju config`](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/config/).

````{tab-set}
```{tab-item} VM
:sync: vm

For example:

    juju deploy postgresql --channel 14/stable --config profile=testing

The full list can be accessed on [Charmhub](https://charmhub.io/postgresql/configurations?channel=16/stable) or by running `juju config postgresql`
```
```{tab-item} K8s
:sync: k8s

The full list can be accessed on [Charmhub](https://charmhub.io/postgresql-k8s/configurations?channel=16/stable) or by running `juju config postgresql-k8s`
```
````

---

```{dropdown} This page is under construction.
:open:
:class-container: dropdown-note
:icon: pencil

For more complete information about PostgreSQL configuration options, see [Charmed PostgreSQL 16 - Configuration](https://canonical.com/data/postgresql/docs/16/reference/configuration/)
```
