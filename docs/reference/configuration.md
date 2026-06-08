(configuration)=
# Configuration

<!--TODO: re-categorize with existing prefixes-->

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

This page contains additional context about some categories of configuration parameters. Note that this is not a comprehensive list - always use `juju config` to see all available configurations.

(system-profiling)=
## System profiling

Options dedicated to defining the performance profile and creating dedicated memory pools for shared caching, sorting, and utility operations.

### `profile`

Profile representing the scope of deployment, and used to tune resource allocation. `production` (default) will tune PostgreSQL for maximum performance, while `testing` will tune for minimal running performance.

* `production`: Maximum performance. 25% of the available memory for `shared_buffers` and the remain as cache memory (defaults mimic legacy charm behaviour). The `max_connections=max(4 * os.cpu_count(), 100)`. Use [PgBouncer](https://charmhub.io/pgbouncer?channel=1/stable) if `max_connections` are not enough ([reasoning](https://www.percona.com/blog/scaling-postgresql-with-pgbouncer-you-may-need-a-connection-pooler-sooner-than-you-expect/))
* `testing`: Minimal resource usage. PostgreSQL 14 defaults.

More profiling and memory allocation configurations: `juju config postgresql | grep -e profile- -e memory-`

## CPU concurrency and background workers

Settings regulating how background workers are scaled and how heavily PostgreSQL splits query execution paths across CPU cores.

See all options with `juju config postgresql | grep cpu-`.

## Logging and performance

Telemetry diagnostic filters to track performance and check audit files.

See related options with `juju config postgresql | grep -e logging- -e -track`

## Optimizer

Toggle permissions for the optimizer's usage of specific data retrieval methods (e.g. scan, aggregation, execution plans, query costs, JIT)

See related options with `juju config postgresql | grep optimizer-`

## System autovacuum

Settings related to background cleanup tasks responsible for recovering space from dead rows, maintaining statistics, and preventing transaction ID wraparounds.

See related options with `juju config postgresql | grep vacuum-`

## Connection rules and authentication

See options related to network timeout management with `juju config postgresql | grep -e connection- -e -timeout`.

See options related to authentication with `juju config postgresql | grep -e -encryption -e ldap -e -users`.

### `experimental-max-connections`

<!--TODO connection-pooling.md -->

## Experimental

experimental-delete-older-than-days (manage-backup-retention.md)

## Data formatting and query parsing behavior

`juju config postgresql | grep -e request- -e ldap response- `

## Plugins/extensions

All `plugin-<name>-enable` config parameters provide automatic activation for extensions like PgAudit, PgVector, and TimescaleDB.

See:
* {ref}`supported-extensions`
* {ref}`enable-extension`