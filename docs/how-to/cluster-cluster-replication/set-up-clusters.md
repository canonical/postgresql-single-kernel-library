---
myst:
  html_meta:
    description: "Set up Charmed PostgreSQL cross-regional async replication across two Juju models in different servers for disaster recovery."
---

(set-up-clusters)=
# Set up clusters

Cross-regional (or multi-server) asynchronous replication focuses on disaster recovery by distributing data across different servers.

This guide will show you the basics of initiating a cross-regional async setup using an example PostgreSQL deployment with two servers: one in Rome and one in Lisbon.

## Prerequisites

* Juju `v.3.4.2+`
* Your machine(s) fulfil the {ref}`system-requirements`.
* Your models are hosted in compatible clouds. See: {ref}`supported-cloud-model-combinations`.

---

## Deploy

To deploy two clusters in different servers, create two juju models - one for the `rome` cluster, one for the `lisbon` cluster. In the example below, we use the config flag `profile=testing` to limit memory usage.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju add-model rome
    juju add-model lisbon

    juju switch rome # active model must correspond to cluster
    juju deploy postgresql --channel 16/stable db1

    juju switch lisbon
    juju deploy postgresql --channel 16/stable db2
```
```{tab-item} K8s
:sync: k8s

    juju add-model rome
    juju add-model lisbon

    juju switch rome # active model must correspond to cluster
    juju deploy postgresql-k8s --trust --channel 16/stable db1

    juju switch lisbon
    juju deploy postgresql-k8s --trust --channel 16/stable db2
```
````

## Offer

[Offer](https://juju.is/docs/juju/offer) asynchronous replication in one of the clusters.

```shell
juju switch rome
juju offer db1:replication-offer replication-offer
```

## Consume

Consume asynchronous replication on planned `Standby` cluster (Lisbon):
```shell
juju switch lisbon
juju consume rome.replication-offer
juju integrate replication-offer db2:replication
```

## Promote or switchover a cluster

To define the primary cluster, use the `create-replication` action.

```shell
juju run -m rome db1/leader create-replication
```

To switchover and use `lisbon` as the primary instead, run

```shell
juju run -m lisbon db2/leader promote-to-primary scope=cluster
```

## Scale a cluster

The two clusters work independently, which means that it’s possible to scale each cluster separately. The `-m` flag defines the target of this action, so it can be performed within any active model.

For example:

```shell
juju add-unit db1 -n 2 -m rome
juju add-unit db2 -n 2 -m lisbon
```

Scaling is possible both before and after the asynchronous replication is established.


