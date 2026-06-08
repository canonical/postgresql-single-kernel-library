(logical-replication)=
# How to enable logical replication
{{vm_k8s}}

Logical replication is a feature that allows replicating a subset of one PostgreSQL cluster data to another PostgreSQL cluster.

Under the hood, it uses the publication and subscriptions mechanisms from the [PostgreSQL logical replication](https://www.postgresql.org/docs/16/logical-replication.html) feature.

## Prequisites
* Juju `v.3.6.8+`
* Charmed PostgreSQL VM: Revision 863+
* Charmed PostgreSQL K8s: Revision 630+
* Make sure your machine(s) fulfil the {ref}`system-requirements`

---

## Set up PostgreSQL clusters and client application

Start by deploying two PostgreSQL clusters:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 16/stable postgresql1
    juju deploy postgresql --channel 16/stable postgresql2
```

```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 16/stable --trust postgresql1
    juju deploy postgresql-k8s --channel 16/stable --trust postgresql2
```
````

For testing purposes, you can deploy two applications of the [data integrator charm](https://charmhub.io/data-integrator) and then integrate them to the two PostgreSQL clusters you want to replicate data between.

```shell
juju deploy data-integrator di1 --config database-name=testdb
juju deploy data-integrator di2 --config database-name=testdb

juju integrate postgresql1 di1
juju integrate postgresql2 di2
```

Then, integrate both PostgreSQL clusters:

```shell
juju integrate postgresql1:logical-replication-offer postgresql2:logical-replication
```

This will create a publication on the first cluster and a subscription on the second cluster, allowing data to be replicated from the first to the second.

## Replicate data across clusters

Request the credentials for the first PostgreSQL cluster.

```shell
juju run di1/leader get-credentials
```

Output example:

`````{tab-set}
````{tab-item} VM
:sync: vm

```yaml
postgresql:
  data: '{"database": "testdb", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: testdb
  endpoints: 10.166.227.78:5432
  password: G7Qu77SU0qeadnhn
  read-only-endpoints: 10.166.227.78:5432
  read-only-uris: postgresql://relation-8:G7Qu77SU0qeadnhn@10.166.227.78:5432/testdb
  tls: "False"
  tls-ca: ""
  uris: postgresql://relation-8:G7Qu77SU0qeadnhn@10.166.227.78:5432/testdb
  username: relation-8
  version: "16.9"
```
````
````{tab-item} K8s
:sync: k8s

```yaml
postgresql:
  data: '{"database": "testdb", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: testdb
  endpoints: postgresql1-primary.dev.svc.cluster.local:5432
  password: NTgtJkVfUHLiYDk5
  read-only-endpoints: postgresql1-primary.dev.svc.cluster.local:5432
  read-only-uris: postgresql://relation_id_8:NTgtJkVfUHLiYDk5@postgresql1-primary.dev.svc.cluster.local:5432/testdb
  tls: "False"
  tls-ca: ""
  uris: postgresql://relation_id_8:NTgtJkVfUHLiYDk5@postgresql1-primary.dev.svc.cluster.local:5432/testdb
  username: relation_id_8
  version: "16.9"
```
````
`````

Then create a table and insert some data into it on the first cluster:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-8:G7Qu77SU0qeadnhn@10.166.227.78:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd (message int); insert into asd values (123);
    CREATE TABLE
    INSERT 0 1
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_8:NTgtJkVfUHLiYDk5@postgresql1-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd (message int); insert into asd values (123);
    CREATE TABLE
    INSERT 0 1
```
````

After that, you need to create the same table on the second cluster so that the data can be replicated. Start by getting the credentials for the second cluster:

```shell
juju run di2/leader get-credentials
```

Output example:

`````{tab-set}
````{tab-item} VM
:sync: vm

```yaml
postgresql:
  data: '{"database": "testdb", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: testdb
  endpoints: 10.166.227.109:5432
  password: FHZbyAPGQjbDpj65
  read-only-endpoints: 10.166.227.109:5432
  read-only-uris: postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
  tls: "False"
  tls-ca: ""
  uris: postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
  username: relation-9
  version: "16.9"
```
````
````{tab-item} K8s
:sync: k8s

```yaml
postgresql:
  data: '{"database": "testdb", "external-node-connectivity": "true", "provided-secrets":
    "[\"mtls-cert\"]", "requested-secrets": "[\"username\", \"password\", \"tls\",
    \"tls-ca\", \"uris\", \"read-only-uris\"]"}'
  database: testdb
  endpoints: postgresql2-primary.dev.svc.cluster.local:5432
  password: nfWkAiEtSA3iA7t2
  read-only-endpoints: postgresql2-primary.dev.svc.cluster.local:5432
  read-only-uris: postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
  tls: "False"
  tls-ca: ""
  uris: postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
  username: relation_id_9
  version: "16.9"
```
````
`````

Then create the same table on the second cluster:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd (message int);
    CREATE TABLE
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd (message int);
    CREATE TABLE
```
````

Configure the replication of that specific database and table (remember to specify the table schema; it's the `public` schema in this example):

```shell
juju config postgresql2 logical-replication-subscription-request='{"testdb": ["public.asd"]}'
```

After a few seconds, you can check that the data has been replicated:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd;
    message
    ---------
        123
    (1 row)
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd;
    message
    ---------
        123
    (1 row)
```
````

You can then add more data to the table in the first cluster, and it will be replicated to the second cluster automatically.

It's also possible to replicate tables in the other direction, from the second cluster to the first, while keeping the replication from the first cluster to the second. To do so, integrate the clusters in the opposite direction:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd2 (message int); insert into asd2 values (123);
    CREATE TABLE
    INSERT 0 1
    testdb=> \q

    psql postgresql://relation-8:G7Qu77SU0qeadnhn@10.166.227.78:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd2 (message int);
    CREATE TABLE
    testdb=> \q

    juju integrate postgresql1:logical-replication postgresql2:logical-replication-offer

    juju config postgresql1 logical-replication-subscription-request='{"testdb": ["public.asd2"]}'

    psql postgresql://relation-8:G7Qu77SU0qeadnhn@10.166.227.78:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd2;
    message
    ---------
        123
    (1 row)
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd2 (message int); insert into asd2 values (123);
    CREATE TABLE
    INSERT 0 1
    testdb=> \q

    psql postgresql://relation_id_8:NTgtJkVfUHLiYDk5@postgresql1-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> create table asd2 (message int);
    CREATE TABLE
    testdb=> \q

    juju integrate postgresql1:logical-replication postgresql2:logical-replication-offer

    juju config postgresql1 logical-replication-subscription-request='{"testdb": ["public.asd2"]}'

    psql postgresql://relation_id_8:NTgtJkVfUHLiYDk5@postgresql1-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd2;
    message
    ---------
        123
    (1 row)
```
````

And the same table, or even different tables, can be replicated to multiple clusters at the same time. For example, you can replicate the `asd` table from the first cluster to both a second and a third clusters, or you can replicate it only to the second cluster and replicate a different table to the third cluster.

If the relation between the PostgreSQL clusters is broken, the data will be kept in both clusters, but the replication will stop. You can re-enable logical replication by following the steps in {ref}`re-enable`

The same will happen for that specific table if you change the table in the `logical-replication-subscription-request` config option to a different table or remove it completely. If one or more tables other than the current one are specified, the replication will continue for those tables, but the current table will not be replicated any more.

(re-enable)=
## Re-enable logical replication

If the relation between the PostgreSQL clusters is broken, you can re-enable logical replication by following these steps.

Drop and re-create the table on the second cluster:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> drop table asd; create table asd (message int);
    DROP TABLE
    CREATE TABLE
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> drop table asd; create table asd (message int);
    DROP TABLE
    CREATE TABLE
```
````

If the table is not dropped and re-created, the second cluster will enter a blocked state.

Example output from Juju debug logs:

```text
unit-postgresql2-0: 11:55:43 ERROR unit.postgresql2/0.juju-log logical-replication:11: relations.logical_replication:Logical replication validation: table public.asd in database testdb isn't empty
```

Then, integrate the clusters again:

```shell
juju integrate postgresql1:logical-replication-offer postgresql2:logical-replication
```

And you'll be able to see the data replicated from the first cluster to the second:

````{tab-set}
```{tab-item} VM
:sync: vm

    psql postgresql://relation-9:FHZbyAPGQjbDpj65@10.166.227.109:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd;
    message
    ---------
        123
    (1 row)
```
```{tab-item} K8s
:sync: k8s

    psql postgresql://relation_id_9:nfWkAiEtSA3iA7t2@postgresql2-primary.dev.svc.cluster.local:5432/testdb
    psql (16.9 (Ubuntu 16.9-0ubuntu0.24.04.1))
    Type "help" for help.

    testdb=> select * from asd;
    message
    ---------
        123
    (1 row)
```
````
