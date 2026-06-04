---
relatedlinks: https://documentation.ubuntu.com/juju/3.6/reference/relation/, [postgresql_client&#32-&#32GitHub](https://github.com/canonical/charm-relation-interfaces/tree/main/interfaces/postgresql_client/v0)
---

(integrate-with-another-application)=
# How to integrate with a client application
{{vm}}{{k8s}}

[Integrations](https://juju.is/docs/juju/relation), also known as “relations” are connections between two applications with compatible endpoints. These connections simplify the creation and management of users, passwords, and other shared data.

This guide shows how to integrate Charmed PostgreSQL with both charmed and non-charmed applications.

For charm developer information about how to integrate your own charmed application with PostgreSQL, see {ref}`integrate-with-your-charm`.

Integrations with charmed applications are supported via the modern [`postgresql_client`](https://github.com/canonical/charm-relation-interfaces/blob/main/interfaces/postgresql_client/v0/README.md) interface, and the legacy `psql` interface from the [original version](https://launchpad.net/postgresql-charm) of the charm.

```{note}
You can see which existing charms are compatible with PostgreSQL in the {guilabel}`Integrations` tab on Charmhub:
* [VM charm integrations](https://charmhub.io/postgresql/integrations)
* [K8s charm integrations](https://charmhub.io/postgresql-k8s/integrations)
```

## Integrate with a charmed application

To integrate with a charmed application that supports the `postgresql_client` interface, run

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate postgresql:database <charm>

To remove the integration, run

    juju remove-relation postgresql <charm>
```
```{tab-item} K8s
:sync: k8s

    juju integrate postgresql-k8s:database <charm>

To remove the integration, run

    juju remove-relation postgresql-k8s <charm>
```
````

## Integrate with a non-charmed application

To integrate with an application outside of Juju, you must use the [`data-integrator` charm](https://charmhub.io/data-integrator) to create the required credentials and endpoints.

Deploy `data-integrator`:

```shell
juju deploy data-integrator --config database-name=<name>
```

Integrate with PostgreSQL:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate data-integrator postgresql
```

```{tab-item} K8s
:sync: k8s

    juju integrate data-integrator postgresql-k8s
```
````

Use the `get-credentials` action to retrieve credentials from `data-integrator`:

```shell
juju run data-integrator/leader get-credentials
```

## Rotate application passwords

To rotate the passwords of users created for integrated applications, the integration should be removed and integrated again. This process will generate a new user and password for the application.

See: {ref}`manage-passwords`

## Request a custom username

Charms can request a custom username to be used in their relation with PostgreSQL 16.

The simplest way to test it is to use `requested-entities-secret` field via the [`data-integrator` charm](https://charmhub.io/data-integrator).

`````{dropdown} Example
````{tab-set}
```{tab-item} VM
:sync: vm

    $ juju deploy postgresql --channel 16/stable

    $ juju add-secret myusername mylogin=mypassword
    secret:d5l3do605d8c4b1gn9a0

    $ juju deploy data-integrator --channel latest/edge --config database-name=mydbname --config requested-entities-secret=d5l3do605d8c4b1gn9a0
    Deployed "data-integrator" from charm-hub charm "data-integrator", revision 307 in channel latest/edge on ubuntu@24.04/stable

    $ juju grant-secret d5l3do605d8c4b1gn9a0 data-integrator

    $ juju relate postgresql data-integrator

    $ juju run data-integrator/leader get-credentials
    ...
    postgresql:
    database: mydbname
    username: mylogin
    password: mypassword
    uris: postgresql://mylogin:mypassword@10.218.34.199:5432/mydbname
    version: "16.11"
    ...

    $ psql postgresql://mylogin:mypassword@10.218.34.199:5432/mydbname -c "SELECT SESSION_USER, CURRENT_USER"
    session_user |       current_user
    --------------+---------------------------
    mylogin      | charmed_mydbname_owner
    (1 row)
```
```{tab-item} K8s
:sync: k8s

    $ juju deploy postgresql-k8s --channel 16/stable --trust

    $ juju add-secret myusername mylogin=mypassword
    secret:d5l3do605d8c4b1gn9a0

    $ juju deploy data-integrator --channel latest/edge --config database-name=mydbname --config requested-entities-secret=d5l3do605d8c4b1gn9a0
    Deployed "data-integrator" from charm-hub charm "data-integrator", revision 307 in channel latest/edge on ubuntu@24.04/stable

    $ juju grant-secret d5l3do605d8c4b1gn9a0 data-integrator

    $ juju relate postgresql-k8s data-integrator

    $ juju run data-integrator/leader get-credentials
    ...
    postgresql-k8s:
    database: mydbname
    username: mylogin
    password: mypassword
    uris: postgresql://mylogin:mypassword@10.218.34.199:5432/mydbname
    version: "16.13"
    ...

    $ psql postgresql://mylogin:mypassword@10.218.34.199:5432/mydbname -c "SELECT SESSION_USER, CURRENT_USER"
    session_user |       current_user
    --------------+---------------------------
    mylogin      | charmed_mydbname_owner
    (1 row)
```
````
`````