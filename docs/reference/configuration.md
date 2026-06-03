(configuration)=
# Configuration

The PostgreSQL charm exposes several configuration parameters you can use to fine tune your deployment via [`juju config`](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/config/).

````{tab-set}
```{tab-item} VM
:sync: vm

For example:

    juju deploy postgresql --channel 16/stable --config profile=testing

The full list can be accessed on [Charmhub](https://charmhub.io/postgresql/configurations?channel=16/stable) or by running `juju config postgresql`
```
```{tab-item} K8s
:sync: k8s

The full list can be accessed on [Charmhub](https://charmhub.io/postgresql-k8s/configurations?channel=16/stable) or by running `juju config postgresql-k8s`
```
````

Below is a list of selected configuration parameters that warrant some additional context.

## Performance (profiles)

Charmed PostgreSQL resource allocation can be controlled via the charm's `profile` config option.

|Value|Description|Details|
| --- | --- | ----- |
|`production`<br>(default)|[Maximum performance](https://github.com/canonical/postgresql-operator/blob/main/lib/charms/postgresql_k8s/v0/postgresql.py#L437-L446)| 25% of the available memory for `shared_buffers` and the remain as cache memory (defaults mimic legacy charm behaviour).<br/>The `max_connections=max(4 * os.cpu_count(), 100)`.<br/> Use [pgbouncer](https://charmhub.io/pgbouncer?channel=1/stable) if `max_connections` are not enough ([reasoning](https://www.percona.com/blog/scaling-postgresql-with-pgbouncer-you-may-need-a-connection-pooler-sooner-than-you-expect/)).|
|`testing`|[Minimal resource usage](https://github.com/canonical/postgresql-operator/blob/main/lib/charms/postgresql_k8s/v0/postgresql.py#L437-L446)|  PostgreSQL 14 defaults. |

## CPU

Full list: `juju config postgresql | grep cpu`

## System users

## Max connections

## Storage

## Plugins

## Optimizer

## LDAP

## WAL/durability?

## Instance

## Logging

## Memory