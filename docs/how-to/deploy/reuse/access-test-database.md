This section walks you through creating and accessing a test database in your newly configured cloud.

Create a Juju model:

```{terminal}
:copy:

juju add-model <model-name>
```

The following command deploys PostgreSQL and the [data-integrator charm](https://charmhub.io/data-integrator) to request a test database:

```{terminal}
:copy:

juju deploy postgresql --channel 16/stable
```
```{terminal}
:copy:

juju deploy data-integrator --config database-name=test-db
```
```{terminal}
:copy:

juju integrate postgresql data-integrator
```

Once `juju status` shows the apps as `active` and `idle`, request the credentials for your newly bootstrapped PostgreSQL database:

```{terminal}
:copy:

juju run data-integrator/leader get-credentials
```

Take note of the values for `<username>`, `<password>`, and `<endpoint>`.

At this point, you can access your cloud database using the internal IP address.

All further Juju applications will use the database through the internal network:

```{terminal}
:copy:

psql postgresql://<username>:<password>@<endpoint>/test-db

psql (15.6 (Ubuntu 15.6-0ubuntu0.23.10.1), server 16.9 (Ubuntu 14.12-0ubuntu0.24.04.1))
Type "help" for help.

test-db=>
```