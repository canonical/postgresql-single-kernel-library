---
myst:
  html_meta:
    description: "Scale a Charmed PostgreSQL cluster by adding or removing Juju units to control the number of replicas for high availability and performance."
---

(scale-cluster)=
# How to scale a cluster
{{vm_k8s}}

Replication in PostgreSQL is the process of creating copies of the stored data. This provides redundancy, which means the application can provide self-healing capabilities in case one replica fails. In this context, each replica is equivalent to one juju unit.

This guide will show you how to establish and change the amount of Juju units used to replicate your data.

## Deploy PostgreSQL with n replicas

To deploy PostgreSQL with multiple replicas, specify the number of desired units with the `-n` option.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 16/stable -n <number_of_replicas>
```
```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 16/stable -n <number_of_replicas> --trust
```
````

```{dropdown} It is recommended to deploy an *odd* number of replicas
:icon: light-bulb
:class-container: dropdown-tip
:class-title: sd-font-weight-normal

This helps prevent a [split-brain](https://en.wikipedia.org/wiki/Split-brain_(computing)) scenario.
```

## Scale replicas on an existing application

The amount of replicas (Juju units) can also be modified after deployment.

```{dropdown} Data loss warning
:open:
:class-container: dropdown-important
:icon: no-entry-fill

Removing the last unit will destroy your data permanently!
```

````{tab-set}
```{tab-item} VM
:sync: vm

To scale up the cluster, use `juju add-unit`:

    juju add-unit postgresql --num-units <amount_of_units_to_add>

To scale down the cluster, use `juju remove-unit`:

    juju remove-unit postgresql/<unit_id_to_remove>
```

```{tab-item} K8s
:sync: k8s

In Kubernetes, scaling operations are performed using `juju scale-application` and specifying the total amount of units you want to have in the cluster:

    juju scale-application postgresql-k8s <total number of units>
```
````

{{seealso}} {ref}`units`