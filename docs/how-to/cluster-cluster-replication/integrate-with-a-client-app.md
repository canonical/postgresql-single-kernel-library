(integrate-with-a-client-app)=
# Integrate with a client application

This guide describes how to integrate a client application with a cluster-cluster async setup using an example PostgreSQL deployment with two servers: one in Rome and one in Lisbon.

## Configure database endpoints

To make your database available to a client application, you must first offer and consume database endpoints.

### Offer database endpoints

[Offer](https://juju.is/docs/juju/offer) the `database` endpoint on each of the `postgresql` applications.

```shell
juju switch rome
juju offer db1:database db1database

juju switch lisbon
juju offer db2:database db2database
```

### Consume endpoints on client app

It is good practice to use a separate model for the client application rather than using one of the database host models.

```shell
juju add-model app
juju switch app
juju consume rome.db1database
juju consume lisbon.db2database
```

## Internal client

If the client application is another charm, deploy them and connect them with `juju integrate`.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju switch app

    juju deploy postgresql-test-app
    juju deploy pgbouncer --channel 1/stable

    juju integrate postgresql-test-app:database pgbouncer
    juju integrate pgbouncer db1database
```

```{tab-item} K8s
:sync: k8s

    juju switch app

    juju deploy postgresql-test-app
    juju deploy pgbouncer-k8s --trust --channel 1/stable

    juju relate postgresql-test-app:first-database pgbouncer-k8s
    juju relate pgbouncer-k8s db1database
```
````

## External client

If the client application is external, they must be integrated via the [`data-integrator` charm](https://charmhub.io/data-integrator).

````{tab-set}
```{tab-item} VM
:sync: vm

    juju switch app

    juju deploy data-integrator --config database-name=mydatabase
    juju deploy pgbouncer pgbouncer-external --channel 1/stable

    juju relate data-integrator pgbouncer-external
    juju relate pgbouncer-external db1database

    juju run data-integrator/leader get-credentials
```

```{tab-item} K8s
:sync: k8s

    juju switch app

    juju deploy data-integrator --config database-name=mydatabase
    juju deploy pgbouncer-k8s pgbouncer-external --trust --channel 1/stable

    juju relate data-integrator pgbouncer-external
    juju relate pgbouncer-external db1database

    juju run data-integrator/leader get-credentials
```
````
