---
relatedlinks: "[Charmhub&#32;|&#32;PostgreSQL&#32;VM](https://charmhub.io/postgresql?channel=16/stable), [Charmhub&#32;|&#32;PostgreSQL&#32;K8s](https://charmhub.io/postgresql-k8s?channel=16/stable)"
---

(architecture)=
# Architecture
{{vm_k8s}}

Charmed PostgreSQL is a Juju-based operator to deploy and operate [PostgreSQL](https://www.postgresql.org/) on machines or Kubernetes. It is based on [PostgreSQL Community Edition](https://www.postgresql.org/community/), and uses [Patroni](https://github.com/zalando/patroni) to manage PostgreSQL cluster via [synchronous replication](https://patroni.readthedocs.io/en/latest/replication_modes.html#postgresql-synchronous-replication).

```{figure} architecture-diagram.png
:width: 690
:alt: Diagram of a sample PostgreSQL deployment with 3 units, and integrations with PgBouncer, Prometheus, Grafana, and Ceph.

Example Charmed PostgreSQL deployment with 3 replicas. Integrations include PgBouncer for load balancing, Prometheus and Grafana for observability, and Ceph for backups.
```

## Juju layer

`````{tab-set}
````{tab-item} VM
:sync: vm

The machine charm leverages the [charmed-postgresql](https://snapcraft.io/charmed-postgresql) snap, which is deployed by Juju on the specified VM/MAAS/bare-metal machine based on Ubuntu Noble/24.04.

The snap runs PostgreSQL service(s) in a secure and isolated environment. For more information, see this blog post about [strict confinement](https://ubuntu.com/blog/demystifying-snap-confinement).

Since both the charm code and workload run in the same host machine, they can communicate with one another without the need of an intermediate layer (unlike the Kubernetes charm, which uses Pebble)
````
````{tab-item} K8s
:sync: k8s

The Kubernetes charm leverages the [sidecar](https://kubernetes.io/blog/2015/06/the-distributed-system-toolkit-patterns/#example-1-sidecar-containers) pattern to allow multiple containers in each pod with [Pebble](https://documentation.ubuntu.com/juju/3.6/reference/pebble/) running as the workload container’s entrypoint.

Pebble is a lightweight, API-driven process supervisor that is responsible for configuring processes to run in a container and controlling those processes throughout the workload lifecycle.

Pebble `services` are configured through [layers](https://github.com/canonical/pebble#layer-specification), and the following containers represent each one a layer forming the effective Pebble configuration, or `pebble plan`:

* a charm container runs Juju operator code: `juju ssh postgresql-k8s/0 bash`
* a workload container runs the [PostgreSQL application](https://www.postgresql.org/) along with other services (like monitoring metrics exporters, etc): `juju ssh --container postgresql postgresql-k8s/0 bash`

As a result, if you run a `kubectl get pods` on a namespace named for the Juju model you’ve deployed the "Charmed PostgreSQL K8s" charm into, you’ll see something like the following:

```shell
NAME                READY   STATUS    RESTARTS   AGE
postgresql-k8s-0    2/2     Running   0          65m
```

This shows there are 2 containers in the pod: `charm` and `workload` mentioned above.

And if you run `kubectl describe pod postgresql-k8s-0`, all the containers will have as Command `/charm/bin/pebble`. That’s because Pebble is responsible for the processes startup as explained above.
````
`````

## High-level design

`````{tab-set}
````{tab-item} VM
:sync: vm

```{dropdown} The charmed-postgresql snap installs <code>14/stable</code> by default.
:open:
:class-container: dropdown-note
:icon: info
:class-title: sd-font-weight-normal

For `16/stable`, use the `--channel` flag when installing or refreshing the snap.
```

The snap ships the following components:

* **PostgreSQL** (based on Ubuntu APT package [postgresql](https://packages.ubuntu.com/jammy/postgresql))
* **PgBouncer** (based on Canonical [backport](https://launchpad.net/~data-platform/+archive/ubuntu/pgbouncer))
* **Patroni** (based on Canonical [backport](https://launchpad.net/~data-platform/+archive/ubuntu/patroni))
* **pgBackRest** (based on Canonical  [backport](https://launchpad.net/~data-platform/+archive/ubuntu/pgbackrest))
* **Prometheus PostgreSQL Exporter** (based on Canonical [backport](https://launchpad.net/~data-platform/+archive/ubuntu/postgres-exporter))
* **Prometheus PgBouncer Exporter** (based on Canonical [backport](https://launchpad.net/~data-platform/+archive/ubuntu/pgbouncer-exporter))
* Prometheus Grafana dashboards and Loki alert rules are part of the charm revision (and missing in the snap).

Versions of all the components above are carefully chosen to fit functionality of each other.

The Charmed PostgreSQL unit consisting of a several services which are enabled/activated according to the setup:

```shell
$ juju ssh postgresql/0 snap services charmed-postgresql

Service                                          Startup   Current  Notes
charmed-postgresql.ldap-sync                     enabled   active   -
charmed-postgresql.patroni                       enabled   active   -
charmed-postgresql.pgbackrest-exporter           enabled   active   -
charmed-postgresql.pgbackrest-service            enabled   active   -
charmed-postgresql.prometheus-postgres-exporter  enabled   active   -
charmed-postgresql.pgbackrest-exporter           enabled   active   -
```

The `ldap-sync` service starts a background process synchronizing LDAP server usernames into PostgreSQL usernames, assigning mapped roles in the process.

The `patroni` snap service is a main PostgreSQL instance which is normally up and running right after the charm deployment.

The `pgbackrest` snap service is a backup framework for PostgreSQL. It is disabled if {ref}`backups <back-up-and-restore>` are not configured along with the `pgbackrest-exporter` service.

The `prometheus-postgres-exporter` service is activated after integration with {ref}`COS monitoring <enable-monitoring>` only.

The charmed-postgresql snap also ships list of tools used by charm:
* `charmed-postgresql.psql` (alias `psq`) - is PostgreSQL interactive terminal.
* `charmed-postgresql.patronictl` - a tool to monitor and manage Patroni.
* `charmed-postgresql.pgbackrest` - a tool to backup/restore PostgreSQL DB.

```{dropdown} Managing snap services manually
:open:
:class-container: dropdown-caution
:icon: alert
:class-title: sd-font-weight-normal

To avoid a {term}`split-brain scenario`, we recommend you do **not** start, stop, and restart snap services manually. See {ref}`this guide <troubleshooting>` for troubleshooting guidance.

All snap resources must be executed under the special **snap user `_daemon_`** only!
```

````
````{tab-item} K8s
:sync: k8s

The Charmed PostgreSQL K8s (`workload` container) based on `postgresql-image` resource defined in the [charm metadata.yaml](https://github.com/canonical/postgresql-k8s-operator/blob/main/metadata.yaml). It is an official Canonical [charmed-postgresql](https://github.com/canonical/charmed-postgresql-rock) [OCI/Rock](https://documentation.ubuntu.com/rockcraft/en/latest/explanation/rockcraft/) image, which is based on the Canonical [`charmed-postgresql` snap].

[Charmcraft](https://canonical-charmcraft.readthedocs-hosted.com/stable/) uploads an image as a [charm resource](https://charmhub.io/postgresql-k8s/resources/postgresql-image) to [Charmhub](https://charmhub.io/postgresql-k8s) during the [publishing](https://github.com/canonical/postgresql-k8s-operator/blob/main/.github/workflows/release.yaml).

The charm supports Juju deployment on several Kubernetes environments like MicroK8s, Canonical K8s, GKE, AKS, and EKS. See {ref}`k8s-clouds`.

The OCI/Rock ships the following components:

* PostgreSQL Community Edition (based on [`charmed-postgresql` snap])
* Patroni (based on [`charmed-postgresql` snap])
* PgBouncer (based on [`charmed-postgresql` snap])
* PgBackRest (based on [`charmed-postgresql` snap])
* Prometheus PostgreSQL Exporter (based on [`charmed-postgresql` snap])
* Prometheus PgBouncer Exporter (based on [`charmed-postgresql` snap])
* Prometheus Grafana dashboards and Loki alert rules are part of the charm revision (and missing in SNAP).

SNAP-based rock images guarantee the same components versions and functionality between VM and K8s charm flavours.

Pebble runs layers of all the currently enabled services, e.g. monitoring, backups, etc:

```shell
$ juju ssh --container postgresql postgresql-k8s/0  /charm/bin/pebble services
Service            Startup   Current   Since
metrics_server     enabled   active    today at 21:42 UTC
pgbackrest server  disabled  inactive  -
postgresql         enabled   active    today at 21:42 UTC
```

The `postgresql` is a main Pebble service which is normally up and running right after the charm deployment.

All `metrics_server` Pebble service is only activated after integrating with {ref}`COS Monitoring <enable-monitoring>`.

```{dropdown} Managing Pebble services manually
:open:
:class-container: dropdown-caution
:icon: alert
:class-title: sd-font-weight-normal

To avoid a {term}`split-brain scenario`, we recommend you do **not** start, stop, and restart Pebble services manually.

All Pebble resources must be executed with the proper user defined in the `user:group` options of the Pebble layer.
```

The rock "charmed-postgresql" also ships list of tools used by charm:
* `psql` - PostgreSQL client to connect the database.
* `patronictl` - a tool to monitor/manage Patroni.
* `pgbackrest` - a framework to backup and restore PostgreSQL.
````
`````

{octicon}`arrow-right` See the {ref}`troubleshooting` guide for more information.

## Integrations

The following charms can be integrated with PostgreSQL out of the box.

### PgBouncer

[PgBouncer](https://charmhub.io/pgbouncer) & [PgBouncer K8s](https://charmhub.io/pgbouncer-k8s) is a lightweight connection pooler for PostgreSQL that provides transparent routing between your application and back-end PostgreSQL Servers.


### TLS certificates operator

The [TLS Certificates](https://charmhub.io/tls-certificates-operator) charm is responsible for distributing certificates through Juju configs. For test deployments, the [self-signed certificates](https://charmhub.io/self-signed-certificates) charm is available as well.

{{seealso}} {ref}`enable-tls`

### S3 integrator

[S3 Integrator](https://charmhub.io/s3-integrator) is an integrator charm for providing S3 credentials to Charmed PostgreSQL to access shared S3 data. Store the credentials centrally in the integrator charm and relate consumer charms as needed.

{{seealso}} {ref}`configure-s3-aws`

### Data integrator

The [data integrator](https://charmhub.io/data-integrator) charm requests credentials for non-native Juju applications. Not all applications implement a `data_interfaces` relation, but do allow setting credentials via config. Additionally, some of applications are run outside of juju. This integrator charm allows receiving credentials which can be passed into application config directly without implementing a Juju-native relation.

{{seealso}} {ref}`integrate-with-a-client-application`

### PostgreSQL test app

The [PostgreSQL Test App](https://charmhub.io/postgresql-test-app) charm is a Canonical test application to validate the charm installation / functionality and perform basic performance tests.

### GLAuth

[GLAuth](https://github.com/glauth/glauth) is a secure, easy-to-use and open-sourced LDAP server which provides capabilities to centrally manage accounts across infrastructures. The charm is only available for Kubernetes clouds via the [GLAuth-K8s operator](https://charmhub.io/glauth-k8s), so a cross-controller relation is needed in order to integrate both charms.

### Grafana

[Grafana](https://grafana.com/) is an open-source visualisation tools that allows to query, visualise, alert on, and visualise metrics from mixed data sources in configurable dashboards for observability. This charms is shipped with its own Grafana dashboard and supports integration with the [Grafana Operator](https://charmhub.io/grafana-k8s) to simplify observability.

{{seealso}} {ref}`enable-monitoring`

#### Loki

[Loki](https://grafana.com/docs/loki/latest/) is an open-source fully-featured logging system. This charms is shipped with support for the [Loki Operator](https://charmhub.io/loki-k8s) to collect the generated logs.

{{seealso}} {ref}`enable-monitoring`

### Prometheus

[Prometheus](https://prometheus.io/docs/introduction/overview/) is an open-source systems monitoring and alerting toolkit with a dimensional data model, flexible query language, efficient time series database and modern alerting approach. Charmed PostgreSQL is shipped with a Prometheus exporters, alerts and support for integrating with the [Prometheus charm](https://charmhub.io/prometheus-k8s) to automatically scrape the targets.

{{seealso}} {ref}`enable-monitoring` and {ref}`enable-alert-rules`.

<!--TODO: unhide when charm flowcharts are updated
## LLD (Low Level Design)

Check the charm state machines displayed in the {ref}`charm-event-flowcharts`. The low-level logic is mostly common for both VM and K8s charms.
-->

### Juju events

An event is a data structure that encapsulates part of the execution context of a charm.

`````{tab-set}
````{tab-item} VM
:sync: vm

For this charm, the following events are observed:

1. [`on_install`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#install): install the snap "charmed-postgresql" and perform basic preparations to bootstrap the cluster on the first leader (or join the already configured cluster).
2. [`leader-elected`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#leader-elected): generate all the secrets to bootstrap the cluster.
3. [`leader-settings-changed`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#leader-settings-changed): Handle the leader settings changed event.
4. [`start`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#start): Init/setting up the cluster node.
5. [`config_changed`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#config-changed): usually fired in response to a configuration change using the GUI or CLI. Create and set default cluster and cluster-set names in the peer relation databag (on the leader only).
6. [`update-status`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#update-status): Takes care of workload health checks.
<!--- 7. database_storage_detaching: TODO: ops? event?
1. TODO: any other events?
--->

````
````{tab-item} K8s
:sync: k8s
For this charm, the following events are observed:

1. [`postgresql_pebble_ready`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#container-pebble-ready): informs charm about the availability of the rock "charmed-postgresql"-based `workload` K8s container. Also performs basic preparations to bootstrap the cluster on the first leader (or join the already configured cluster).
2. [`leader`-elected](https://documentation.ubuntu.com/juju/3.6/reference/hook/#leader-elected): generate all the secrets to bootstrap the cluster.
5. [`config_changed`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#config-changed): usually fired in response to a configuration change using the GUI or CLI. Create and set default cluster and cluster-set names in the peer relation databag (on the leader only).
6. [`update-status`](https://documentation.ubuntu.com/juju/3.6/reference/hook/#update-status): Takes care of workload health checks.
<!--- 7. database_storage_detaching: TODO: ops? event?
1. TODO: any other events? relation_joined/changed/created/broken
--->
````
`````

### Charm code overview

[`src/charm.py`](https://github.com/canonical/postgresql-operator/blob/main/src/charm.py) is the default entry point for a charm and has the `PostgresqlOperatorCharm` Python class which inherits from `CharmBase`.

`CharmBase` is the base class from which all Charms are formed, defined by [Ops](https://ops.readthedocs.io/en/latest/) (Python framework for developing charms). See more information in the [Ops documentation for `CharmBase`](https://ops.readthedocs.io/en/latest/reference/ops.html#ops.CharmBase).

The `__init__` method guarantees that the charm observes all events relevant to its operation and handles them.

The VM and K8s charm flavours shares the codebase via [`postgresql-single-kernel-library`](https://github.com/canonical/postgresql-single-kernel-library).
