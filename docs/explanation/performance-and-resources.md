(performance-and-resources)=
# Performance and testing

## Resource allocation

See also system requirements

### Profile config option

Charmed PostgreSQL resource allocation can be controlled via the charm's `profile` config option.

### Juju constraints

The Juju [`--constraints`](https://juju.is/docs/juju/constraint) flag sets RAM and CPU limits for [Juju units](https://juju.is/docs/juju/unit):

```text
juju deploy postgresql --channel 16/stable --constraints cores=8 mem=16G
```

Juju constraints can be set together with the charm's profile:

```text
juju deploy postgresql --channel 16/stable --constraints cores=8 mem=16G --config profile=testing
```

## Benchmarking

For performance testing and benchmarking charms, we recommend using the [Charmed Sysbench](https://charmhub.io/sysbench) operator. This is a tool for benchmarking database applications that includes monitoring and CPU/RAM/IO performance measurement.

## Smoke test

This type of test ensures that basic functionality works over a short amount of time.

### VM

One way to do this is by integrating your PostgreSQL application with the [PostgreSQL Test Application](https://charmhub.io/postgresql-test-app), and running the "continuous writes" test:

```shell
juju run postgresql-test-app/leader start-continuous-writes
```

The expected behaviour is:
* `postgresql-test-app` will continuously inserts records into the database received through the integration (the table `continuous_writes`).
* The counters (amount of records in table) will grow on all cluster members

```{dropdown} Full example

    juju add-model smoke-test

    juju deploy postgresql --channel 16/stable
    juju add-unit postgresql -n 2

    juju deploy postgresql-test-app
    juju integrate postgresql-test-app:database postgresql

    # Optionally configure write speed (default is 500 miliseconds)
    juju config postgresql-test-app sleep_interval=1000

    juju run postgresql-test-app/leader start-continuous-writes

    juju run postgresql-test-app/leader show-continuous-writes
```

To stop the "continuous write" test, run

```shell
juju run postgresql-test-app/leader stop-continuous-writes
```

To truncate the "continuous write" table (i.e. delete all records from database), run

```shell
juju run postgresql-test-app/leader clear-continuous-writes
```
### K8s

1. Deploy database with test application
2. Start "continuous write" test

<details><summary>Example</summary>

```text
juju add-model smoke-test

juju deploy postgresql-k8s --channel 14/edge --trust
juju scale-application postgresql-k8s 3 # (optional)

juju deploy postgresql-test-app
juju integrate postgresql-test-app:first-database postgresql-k8s

# Start "continuous write" test:
juju run postgresql-test-app/leader start-continuous-writes

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

# Watch that the counter is growing!
```
</details>

Expected results:

* `postgresql-test-app` continuously inserts records into the database received through the integration (the table `continuous_writes`).
* The counters (amount of records in table) are growing on all cluster members

Tips:

To stop the "continuous write" test, run

```text
juju run postgresql-test-app/leader stop-continuous-writes
```

To truncate the "continuous write" table (i.e. delete all records from database), run

```text
juju run postgresql-test-app/leader clear-continuous-writes
```

## System test

To perform a system test, deploy [`postgresql-k8s-bundle`](https://charmhub.io/postgresql-k8s-bundle). This charm bundle automatically deploys and tests all the necessary parts at once.

