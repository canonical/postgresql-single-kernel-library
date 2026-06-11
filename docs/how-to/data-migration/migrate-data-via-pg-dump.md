---
myst:
  html_meta:
    description: "Migrate PostgreSQL data from a legacy charm to Charmed PostgreSQL 14 using pg_dump for a complete backup and restore data migration."
---

(migrate-data-via-pg-dump)=
# Migrate data via `pg_dump`

<!--TODO: replace "legacy"? -->

This guide describes database **data** migration from a {ref}`legacy PostgreSQL charm <charm-versions>` running PostgreSQL 14 to a modern PostgreSQL 14 charm.

To migrate charms on new Juju interfaces, refer to the guide {ref}`integrate-with-your-charm`.

A minor difference in commands might be necessary for different revisions and/or Juju versions, but the general logic remains:

* Deploy the modern charm nearby
* Request credentials from legacy charm
* Remove relation to legacy charm (to stop data changes)
* Perform legacy DB dump (using the credentials above)
* Upload the legacy charm dump into the modern charm
* Add relation to modern charm
* Validate results and remove legacy charm

```{dropdown} Always try this in a test environment before performing it in production!
:class-container: dropdown-caution
:icon: alert-fill
:class-title: sd-font-weight-normal

{ref}`Contact us <contact>` for more guidance.
```

## Prerequisites

- **Your application supports modern PostgreSQL interfaces**
    - See: {ref}`interfaces-and-endpoints` and {ref}`integrate-with-your-charm`
- A client machine with access to the deployed legacy charm
- Enough storage in the cluster to support backup/restore of the databases.

## Obtain existing database credentials

To obtain credentials for existing databases, execute the following commands for **each** database that will be migrated. Take note of these credentials for future steps.

First, define and tune your application and database (db) names. For example:

````{tab-set}
```{tab-item} VM
:sync: vm

    CLIENT_APP=<my-application/0>
    OLD_DB_APP=<legacy-postgresql/leader | postgresql/0>
    NEW_DB_APP=<new-postgresql/leader | postgresql/0>
    DB_NAME=<your_db_name_to_migrate>
```

```{tab-item} K8s
:sync: k8s

    CLIENT_APP=<my-application-k8s/0>
    OLD_DB_APP=<legacy-postgresql-k8s/leader | postgresql-k8s/0>
    NEW_DB_APP=<new-postgresql-k8s/leader | postgresql-k8s/0>
    DB_NAME=<your_db_name_to_migrate>
```
````

Then, obtain the username from the existing legacy database via its relation info:

```shell
OLD_DB_USER=$(juju show-unit ${CLIENT_APP} | yq '.[] | .relation-info | select(.[].endpoint == "db") | .[0].application-data.user')
```

## Deploy new PostgreSQL databases and obtain credentials

Deploy new PostgreSQL database charm:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql ${NEW_DB_APP} --channel 14/stable
```

```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s ${NEW_DB_APP} --channel 14/stable --trust
```
````

Obtain the `operator` user password of the new PostgreSQL database via Juju secrets. See {ref}`save-current-cluster-credentials` for more details.

```shell
NEW_DB_USER=operator
NEW_DB_PASS=<your password>
```

## Migrate database

Use the credentials and information obtained in previous steps to perform the database migration with the following procedure.

Make sure no new connections were made and that the database has not been altered.

### Create dump from legacy charm

Remove the relation between application charm and legacy charm:

```shell
juju remove-relation  ${CLIENT_APP}  ${OLD_DB_APP}
```

Connect to the database VM of a legacy charm:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju ssh ${OLD_DB_APP} bash
```

```{tab-item} K8s
:sync: k8s

    juju ssh --remote ${OLD_DB_APP} bash
```
````

Create a dump via Unix socket using credentials from the relation:

```shell
mkdir -p /srv/dump/
OLD_DB_DUMP="legacy-postgresql-${DB_NAME}.sql"
pg_dump -Fc -h /var/run/postgresql/ -U ${OLD_DB_USER} -d ${DB_NAME} > "/srv/dump/${OLD_DB_DUMP}"
```

Exit the database VM or workload container:

```shell
exit
```

### Upload dump to new charm

Fetch dump locally and upload it to the new Charmed PostgreSQL charm:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju scp ${OLD_DB_APP}:/srv/dump/${OLD_DB_DUMP}  ./${OLD_DB_DUMP}
    juju scp ./${OLD_DB_DUMP}  ${NEW_DB_APP}:.
```

```{tab-item} K8s
:sync: k8s

    juju scp --remote ${OLD_DB_APP}:/srv/dump/${OLD_DB_DUMP}  ./${OLD_DB_DUMP}
    juju scp --container postgresql-k8s ./${OLD_DB_DUMP}  ${NEW_DB_APP}:.
```
````

`ssh` into new Charmed PostgreSQL charm and create a new database using `${NEW_DB_PASS}`:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju ssh ${NEW_DB_APP} bash
    createdb -h localhost -U ${NEW_DB_USER} --password ${DB_NAME}
```

```{tab-item} K8s
:sync: k8s

    juju ssh --container postgresql-k8s ${NEW_DB_APP} bash
    createdb -h localhost -U ${NEW_DB_USER} --password ${DB_NAME}
```
````

Restore the dump (using `${NEW_DB_PASS}`):

```shell
pg_restore -h localhost -U ${NEW_DB_USER} --password -d ${DB_NAME} --no-owner --clean --if-exists ${OLD_DB_DUMP}
```

## Integrate with modern charm

Integrate your application and new PostgreSQL database charm using the modern `database` endpoint:

```shell
juju integrate ${CLIENT_APP}  ${NEW_DB_APP}:database
```

If the `database` endpoint (from the `postgresql_client` interface) is not yet supported, use instead the `db` endpoint from the legacy `pgsql` interface:

```shell
juju integrate ${CLIENT_APP}  ${NEW_DB_APP}:db
```

## Verify database migration

Test your application to make sure the data is available and in a good condition.

## Remove old databases

Test your application and if you are happy with a data migration, do not forget to remove legacy charms to keep the house clean:

```shell
juju remove-application --destroy-storage <legacy_postgresql>
```
