---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL with Juju network spaces to separate client, replication, cluster, and backup traffic on different network interfaces."
---

(juju-spaces)=
# How to deploy on Juju spaces
{{vm}}

The Charmed PostgreSQL operator supports [Juju spaces](https://documentation.ubuntu.com/juju/latest/reference/space/index.html) to separate network traffic for:
- **Client** - PostgreSQL instance to client data
- **Instance-replication** - cluster instances replication data
- **Cluster-replication** - cluster to cluster replication data
- **Backup** - backup and restore data

## Prerequisites

* Configured network spaces
  * See [Juju | How to manage network spaces](https://documentation.ubuntu.com/juju/latest/reference/juju-cli/list-of-juju-cli-commands/add-space/)

## Deploy

On application deployment, constraints are required to ensure the unit(s) have address(es) on the specified network space(s), and endpoint binding(s) for the space(s).

For example, with spaces configured for instance replication and client traffic:

```shell
❯ juju spaces
Name      Space ID  Subnets
alpha     0         10.163.154.0/24
client    1         10.0.0.0/24
peers     2         10.10.10.0/24
```

The space `alpha` is default and cannot be removed. To deploy Charmed PostgreSQL Operator using the spaces:

```shell
juju deploy postgresql --channel 16/stable \
  --constraints spaces=client,peers \
  --bind "database-peers=peers database=client"
```

```{dropdown} There is no support for the juju <code>bind</code> command.
:open:
:class-container: dropdown-caution
:icon: alert-fill
:class-title: sd-font-weight-normal

Network space binding must be defined at deploy time only.
```

Consequently, a client application must use the `client` space on the model, or a space for the same subnet in another model, for example:

```shell
juju deploy client-app \
  --constraints spaces=client \
  --bind database=client
```

The two application can be then related using:

```shell
juju integrate postgresql:database client-app:database
```

The client application will receive network endpoints on the `10.0.0.0/24` subnet.

The Charmed PostgreSQL operator endpoints are:

| Endpoint                       | Traffic              |
| ------------------------------ | -------------------- |
| database                       | Client               |
| database-peers                 | Instance-replication |
| replication-offer, replication | Cluster-replication  |
| s3-parameters                  | Backup               |

```{dropdown} Using spaces for backup traffic
:open:
:class-container: dropdown-caution
:icon: alert-fill
:class-title: sd-font-weight-normal

If using a network space for the backup traffic, the user is responsible for ensuring that the target object storage URL traffic is routed via the specified network space.
```
