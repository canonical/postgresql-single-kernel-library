---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL on Canonical Kubernetes using Juju, with steps to enable local storage and bootstrap the cluster."
---

(canonical-k8s)=
# How to deploy on Canonical K8s
{{k8s}}

[Canonical Kubernetes](https://ubuntu.com/kubernetes) is a Kubernetes service built on Ubuntu and optimised for most major public clouds.

## Prerequisites

* A physical or virtual machine running Ubuntu 22.04+
* Juju 2.9+ installed via snap
* See {ref}`system-requirements`

---

## Install Canonical Kubernetes

Follow the instructions in the [official Canonical Kubernetes documentation](https://documentation.ubuntu.com/canonical-kubernetes/release-1.35/snap/howto/install/snap/)

Once Canonical K8s is up and running, enable local storage (or any another persistent volume provider, to be used by [Juju storage](https://juju.is/docs/juju/storage) later):

```{terminal}
:copy:

sudo k8s enable local-storage
```
```{terminal}
:copy:
sudo k8s status --wait-ready
```
{{seealso}} [Canonical Kubernetes | Enable local storage](https://documentation.ubuntu.com/canonical-kubernetes/latest/snap/tutorial/getting-started/#enable-local-storage)

````{dropdown} Optionally, install the <code>kubectl</code> tool and dump the K8s config.
:open:
:class-container: dropdown-tip
:icon: light-bulb
:class-title: sd-font-weight-normal

```{terminal}
:copy:

sudo snap install kubectl --classic
```
```{terminal}
:copy:

mkdir ~/.kube
```
```{terminal}
:copy:

sudo k8s config > ~/.kube/config
```
```{terminal}
:copy:
kubectl get namespaces # to test the credentials
```
````

## Bootstrap Juju on Canonical K8s

Add a Juju K8s cloud:

```{terminal}
:copy:

juju add-k8s ck8s --client --context-name="k8s"
```

Bootstrap a Juju controller:

```{terminal}
:copy:

juju bootstrap ck8s
```

## Deploy Charmed PostgreSQL

Create a Juju model:

```{terminal}
:copy:

juju add-model <model-name>
```

Deploy the PostgreSQL charm for K8s:

```{terminal}
:copy:

juju deploy postgresql-k8s --channel 14/stable --trust
```

---

{octicon}`arrow-right` For more information, see the [official Canonical Kubernes documentation](https://documentation.ubuntu.com/canonical-kubernetes/)