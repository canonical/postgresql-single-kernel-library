(enable-ldap)=
# How to enable LDAP authentication

<!--TODO: check this whole guide for correctness (I removed the cross-controller relation on K8s to simplify) -->

The Lightweight Directory Access Protocol (LDAP) enables centralised authentication for PostgreSQL clusters, reducing the overhead of managing local credentials and access policies.

This guide goes over the steps to integrate LDAP as an authentication method with the PostgreSQL charm, all within the Juju ecosystem.

```{caution}
In this guide, we use [self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate) provided by the [`self-signed-certificates` operator](https://github.com/canonical/self-signed-certificates-operator).

**This is not recommended for a production environment.**

Check the collection of [Charmhub operators](https://charmhub.io/?q=tls-certificates) that implement the `tls-certificate` interface.
```

## Prerequisites

* Juju `v3.6` or higher

---

## Deploy an LDAP server on Kubernetes

````{tab-set}
```{tab-item} VM
:sync: vm

With PostgreSQL for machines, you'll need a separate Juju controller with a K8s model in order to deploy the [`glauth-k8s` charm](https://charmhub.io/glauth-k8s).

Then, we'll create a cross-controller relation to the PostgreSQL VM model.

    juju switch <k8s-controller-name>
    juju add-model <k8s-model-name>
```

```{tab-item} K8s
:sync: k8s

With PostgreSQL for Kubernetes, you can simply deploy GLAuth alongside PostgreSQL without a separate Juju model.
```
````

Deploy `glauth-k8s`, `self-signed-certificates`, and `postgresql-k8s`:

```shell
juju deploy glauth-k8s --channel edge --trust --config ldaps_enabled=true
juju deploy self-signed-certificates
juju deploy postgresql-k8s --channel 16/edge --trust
```

Integrate the three applications:

```shell
juju integrate glauth-k8s:certificates self-signed-certificates
juju integrate glauth-k8s:pg-database postgresql-k8s
```

Deploy the [`glauth-utils` charm](https://charmhub.io/glauth-utils) to manage LDAP users, and integrate it with the GLAuth application:

```shell
juju deploy glauth-utils --channel edge --trust
juju integrate glauth-k8s glauth-utils
```

Users and groups can now be created using `glauth-utils`.

## Create a cross-model relation (VM only)

`````{tab-set}
````{tab-item} VM
:sync: vm

## Expose cross-controller URLs

Enable the required MicroK8s plugin:

```shell
IPADDR=$(ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc')
sudo microk8s enable metallb $IPADDR-$IPADDR
```

Deploy the [Traefik charm](https://charmhub.io/traefik-k8s) in order to expose endpoints from the K8s cluster:

```shell
juju deploy traefik-k8s --trust
```

Integrate the two applications:

```shell
juju integrate traefik-k8s glauth-k8s:ldaps-ingress
```

## Expose cross-model relations

To offer the GLAuth interfaces, run:

```shell
juju offer glauth-k8s:ldap ldap
juju offer glauth-k8s:send-ca-cert send-ca-cert
```

## Consume offers

Switch to the VM controller:

```shell
juju switch <lxd_controller>:<my-model>
```

Consume the LDAP offers:

```shell
juju consume <k8s_controller>:admin/<k8s-model-name>.ldap
juju consume <k8s_controller>:admin/<k8s-model-name>.send-ca-cert
```
````
````{tab-item} K8s
:sync: k8s

This step is not needed with PostgreSQL K8s. Proceed to the next section: {ref}`map-ldap-users-to-postgresql`.
````
`````

(map-ldap-users-to-postgresql)=
## Map LDAP users to PostgreSQL

To have LDAP users available in PostgreSQL, provide a comma separated list of LDAP groups to already created PostgreSQL authorisation groups. To create those groups before hand, refer to the [Data Integrator charm](https://charmhub.io/data-integrator).

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql ldap-map="<ldap_group>=<psql_group>"
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s ldap-map="<ldap_group>=<psql_group>"
```
````

## Disable LDAP

You can disable LDAP removing the following relations:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju remove-relation postgresql.receive-ca-cert send-ca-cert
    juju remove-relation postgresql.ldap ldap
```
```{tab-item} K8s
:sync: k8s

    juju remove-relation postgresql-k8s:receive-ca-cert send-ca-cert
    juju remove-relation postgresql-k8s:ldap ldap
```
````
