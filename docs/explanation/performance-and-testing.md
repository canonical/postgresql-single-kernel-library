---
myst:
  html_meta:
    description: "Performance tuning and testing guidance for Charmed PostgreSQL, covering resource profiles, hardware constraints, and functional validation methods."
---

(performance-and-testing)=
# Performance and testing
{{vm_k8s}}

This page summarizes the configuration parameters and testing methods that can help optimize and validate Charmed PostgreSQL deployments. It provides operational guidelines for adjusting resource allocation profiles, applying hardware constraints, and executing functional validation tests.

## Resource allocation

Charmed PostgreSQL resource allocation can be controlled via the charm's `profile` config option:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql profile=<production|testing>
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s profile=<production|testing>
```
````

* `production`: maximum performance, higher {ref}`hardware requirements <hardware>`
* `testing`: minimal resource usage, lower {ref}`hardware requirements <hardware>`

See: {ref}`system-profiling`

## Hardware constraints

The Juju [`--constraints`](https://juju.is/docs/juju/constraint) flag sets RAM and CPU limits for [Juju units](https://juju.is/docs/juju/unit):

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 14/stable --constraints cores=8 mem=16G
```
```{tab-item} K8s
:sync: k8s

    juju deploy postgresql --channel 14/stable --constraints cores=8 mem=16G --trust
```
````

Juju constraints can be set together with the charm's profile:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 14/stable --constraints cores=8 mem=16G --config profile=testing
```
```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 14/stable --constraints cores=8 mem=16G --config profile=testing --trust
```
````

(testing)=
## Testing your deployment

### Benchmarking

For performance testing and benchmarking charms, we recommend using the [Charmed Sysbench](https://charmhub.io/sysbench) operator. This is a tool for benchmarking database applications that includes monitoring and CPU/RAM/IO performance measurement.

### Smoke test

This type of test ensures that basic functionality works over a short amount of time.

One way to do this is by integrating your PostgreSQL application with the [PostgreSQL Test Application](https://charmhub.io/postgresql-test-app), and running the "continuous writes" test:

```shell
juju run postgresql-test-app/leader start-continuous-writes
```

The expected behaviour is:
* `postgresql-test-app` will continuously insert records into the database received through the integration (the table `continuous_writes`).
* The counters (amount of records in table) will grow on all cluster members

````{tab-set}
```{tab-item} VM
:sync: vm

    juju add-model smoke-test

    juju deploy postgresql --channel 14/stable
    juju add-unit postgresql -n 2

    juju deploy postgresql-test-app
    juju integrate postgresql-test-app:database postgresql

    # Optionally configure write speed (default is 500 miliseconds)
    juju config postgresql-test-app sleep_interval=1000

    juju run postgresql-test-app/leader start-continuous-writes

    juju run postgresql-test-app/leader show-continuous-writes
```
```{tab-item} K8s
:sync: k8s

    juju add-model smoke-test

    juju deploy postgresql-k8s --channel 14/stable --trust
    juju scale-application postgresql-k8s 3

    juju deploy postgresql-test-app
    juju integrate postgresql-test-app:database postgresql

    # Optionally configure write speed (default is 500 miliseconds)
    juju config postgresql-test-app sleep_interval=1000

    juju run postgresql-test-app/leader start-continuous-writes

    juju run postgresql-test-app/leader show-continuous-writes
```
````

To stop the "continuous write" test, run

```shell
juju run postgresql-test-app/leader stop-continuous-writes
```

To truncate the "continuous write" table (i.e. delete all records from database), run

```shell
juju run postgresql-test-app/leader clear-continuous-writes
```

Watch the data being written:

````{tab-set}
```{tab-item} VM
:sync: vm

    export user=operator
    export pass=$(juju run postgresql/leader get-password username=${user} | yq '.. | select(. | has("password")).password')
    export relname=first-database
    export ip=$(juju show-unit postgresql/0 --endpoint database | yq '.. | select(. | has("public-address")).public-address')
    export db=$(juju show-unit postgresql/0 --endpoint database | yq '.. | select(. | has("database")).database')
    export relid=$(juju show-unit postgresql/0 --endpoint database | yq '.. | select(. | has("relation-id")).relation-id')
    export query="select count(*) from continuous_writes"

    watch -n1 -x juju run postgresql-test-app/leader run-sql dbname=${db} query="${query}" relation-id=${relid} relation-name=${relname}

    # OR

    watch -n1 -x juju ssh --container postgresql postgresql/leader "psql postgresql://${user}:${pass}@${ip}:5432/${db} -c \"${query}\""
```
```{tab-item} K8s
:sync: k8s

    export user=operator
    export pass=$(juju run postgresql-k8s/leader get-password username=${user} | yq '.. | select(. | has("password")).password')
    export relname=first-database
    export ip=$(juju show-unit postgresql-k8s/0 --endpoint database | yq '.. | select(. | has("public-address")).public-address')
    export db=$(juju show-unit postgresql-k8s/0 --endpoint database | yq '.. | select(. | has("database")).database')
    export relid=$(juju show-unit postgresql-k8s/0 --endpoint database | yq '.. | select(. | has("relation-id")).relation-id')
    export query="select count(*) from continuous_writes"

    watch -n1 -x juju run postgresql-test-app/leader run-sql dbname=${db} query="${query}" relation-id=${relid} relation-name=${relname}

    # OR

    watch -n1 -x juju ssh --container postgresql postgresql-k8s/leader "psql postgresql://${user}:${pass}@${ip}:5432/${db} -c \"${query}\""
```
````

### System test

To perform a system test, deploy the PostgreSQL charm bundle ([VM](https://charmhub.io/postgresql-bundle) | [K8s](https://charmhub.io/postgresql-k8s-bundle)). This charm bundle automatically deploys and tests all the necessary parts at once.