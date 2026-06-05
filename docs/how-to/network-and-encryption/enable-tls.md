(enable-tls)=
# How to enable TLS encryption
{{vm_k8s}}

This guide will show how to enable TLS/SSL on a PostgreSQL cluster using the [`self-signed-certificates` operator](https://github.com/canonical/self-signed-certificates-operator) as an example.

```{dropdown} Do **not** use self-signed certificates in production.
:color: warning
:icon: alert
:class-title: sd-font-weight-normal

In this guide, we use [self-signed certificates](https://en.wikipedia.org/wiki/Self-signed_certificate) provided by the [`self-signed-certificates` operator](https://github.com/canonical/self-signed-certificates-operator).

**This is not recommended for a production environment.**

Check the collection of [Charmhub operators](https://charmhub.io/?q=tls-certificates) that implement the `tls-certificate` interface.
```

This guide assumes everything is deployed within the same network and Juju model.

{{seealso}} {ref}`tls-vip-access`

## Enable TLS

First, deploy the TLS charm:

```shell
juju deploy self-signed-certificates
```

To enable TLS integrate the two applications:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate postgresql:client-certificates self-signed-certificates:certificates
```
```{tab-item} K8s
:sync: k8s

    juju integrate postgresql-k8s:certificates self-signed-certificates:certificates
```
````

## Check certificates in use

To check the certificates in use by PostgreSQL, run

```shell
openssl s_client -starttls postgres -connect <leader_unit_IP>:<port> | grep issuer
```

## Manage keys

<!--TODO: verify if this section is K8s-only-->

Updates to private keys for certificate signing requests (CSR) can be made via the `set-tls-private-key` action. Note that passing keys to external/internal keys should *only be done with* `base64 -w0`, *not* `cat`.

With three replicas, this schema should be followed:

Generate a shared internal key:

```shell
openssl genrsa -out internal-key.pem 3072
```

Generate external keys for each unit:

```shell
openssl genrsa -out external-key-0.pem 3072
openssl genrsa -out external-key-1.pem 3072
openssl genrsa -out external-key-2.pem 3072
```

Apply both private keys to each unit. The shared internal key will be applied only to the juju leader.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
    juju run postgresql/1 set-tls-private-key "external-key=$(base64 -w0 external-key-1.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
    juju run postgresql/2 set-tls-private-key "external-key=$(base64 -w0 external-key-2.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/0 set-tls-private-key "external-key=$(base64 -w0 external-key-0.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
    juju run postgresql-k8s/1 set-tls-private-key "external-key=$(base64 -w0 external-key-1.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
    juju run postgresql-k8s/2 set-tls-private-key "external-key=$(base64 -w0 external-key-2.pem)"  "internal-key=$(base64 -w0 internal-key.pem)"
```
````

Updates can also be done with auto-generated keys with

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/0 set-tls-private-key
    juju run postgresql/1 set-tls-private-key
    juju run postgresql/2 set-tls-private-key
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/0 set-tls-private-key
    juju run postgresql-k8s/1 set-tls-private-key
    juju run postgresql-k8s/2 set-tls-private-key
```
````

## Disable TLS

Disable TLS by removing the integration.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju remove-relation postgresql:client-certificates self-signed-certificates:certificates
```
```{tab-item} K8s
:sync: k8s

    juju remove-relation postgresql-k8s:certificates self-signed-certificates:certificates
```
````
