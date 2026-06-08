(manage-passwords)=
# How to manage passwords
{{vm_k8s}}

Charmed PostgreSQL 16 uses [Juju secrets](https://documentation.ubuntu.com/juju/latest/reference/secret/#secret) to manage passwords.

{{seealso}} [Juju | How to manage secrets](https://documentation.ubuntu.com/juju/latest/howto/manage-secrets/#manage-secrets)

## Create a secret

To create a secret in Juju containing one or more user passwords:

```shell
juju add-secret <secret_name> <user_a>=<password_a> <user_b>=<password_b>
```

The command above will output a secret URI, which you'll need for configuring `system-users`.

Admin users that were not included in the `add-secret` command will use an automatically created password.

To grant the secret to the PostgreSQL charm:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju grant-secret <secret_name> postgresql
```
```{tab-item} K8s
:sync: k8s

    juju grant-secret <secret_name> postgresql-k8s
```
````

## Configure `system-users`

To set the `system-users` config option to the secret URI:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql system-users=<secret_URI>
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s system-users=<secret_URI>
```
````

```{tip}
Note that `<secret_URI>` includes the the scheme (`secret:`).
```

When the `system-users` config option is set, the charm will:
* Use the content of the secret specified by the `system-users` config option instead of the one generated.
* Update the passwords of the internal `system-users` in its user database.

If the config option is **not** specified, the charm will automatically generate passwords for the internal system-users and store them in a secret.

To retrieve the password of an internal system-user, run the `juju show-secret` command with the respective secret URI.

## Update a secret

To update an existing secret:

```shell
juju update-secret <secret_name> <user_a>=<new_password_a> <user_c>=<password_c>
```

In this example,
* `user_a`'s password was updated from `password_a` to `new_password_a`
* `user_c`'s password was updated from an auto-generated password to `password_c`
* `user_b`'s password remains as it was when the secret was added, but **`user_b` is no longer part of the secret**.

{{seealso}} {ref}`users`

## Rotate application passwords

To rotate the passwords of users created for integrated applications, the integration should be removed and integrated again. This process will generate a new user and password for the application.

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