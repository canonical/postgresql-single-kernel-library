---
myst:
  html_meta:
    description: "Connect to Charmed PostgreSQL from outside the local network using PgBouncer with virtual IPs or cross-controller Juju relations."
---

(external-network-access)=
# How to connect to your database outside the local network
{{vm}} {{k8s}}

This page summarises resources for setting up deployments where an external application must connect to a PostgreSQL database from outside the local area network, or outside the Kubernetes cluster.

## External application (non-Juju)

### VM use case
{{vm}}

*The client application is a non-Juju application outside of the local area network where Juju and the database are running.*

There are many possible ways to connect the Charmed PostgreSQL database from outside of the LAN where the database cluster is located. The available options are heavily dependent on the cloud/hardware/virtualisation in use.

One of the possible options is to use [virtual IP addresses (VIP)](https://en.wikipedia.org/wiki/Virtual_IP_address) which the charm PgBouncer provides with assistance from the charm/interface `hacluster`. Please follow the [PgBouncer documentation](https://charmhub.io/pgbouncer/docs/h-external-access) for such configuration.

See also: {ref}`tls-vip-access`

### K8s use case
{{k8s}}
*The client application is a non-Juju application outside of the database's Kubernetes deployment.*

To connect the Charmed PostgreSQL K8s database from outside the Kubernetes cluster, the charm PgBouncer K8s should be deployed. Please follow the instructions in the [PgBouncer K8s documentation](https://charmhub.io/pgbouncer-k8s/docs/h-external-access).

## External relation (Juju)
{{vm}} {{k8s}}

The client application is a Juju application outside the database deployment - such as a hybrid Juju deployment with different VM clouds/controllers, or mixed VM and K8s applications.

In this case, a cross-hybrid or cross-controller relation is necessary. Please {ref}`contact <contacts>` the Data team to discuss possible options for your use case.
