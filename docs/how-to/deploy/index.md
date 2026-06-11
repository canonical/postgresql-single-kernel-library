---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL via Juju CLI or Terraform on VM or Kubernetes clouds, with guides for multi-AZ and airgapped environments."
---

(deploy)=
# How to deploy

The basic requirements for deploying a charm are the [**Juju client**](https://documentation.ubuntu.com/juju/3.6/) and a machine [**cloud**](https://juju.is/docs/juju/cloud). For more details, see {ref}`system-requirements`.

If you are not sure where to start, or would like a more guided walkthrough for setting up your environment, start with the {ref}`tutorial`.

(deploy-quickstart)=
## Quickstart

Charmed PostgreSQL can be deployed using the Juju CLI directly, or via Terraform.

To deploy via the **Juju CLI**, you need to first [bootstrap](https://juju.is/docs/juju/juju-bootstrap) a cloud controller and create a [model](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/model/):

```shell
juju bootstrap <cloud name> <controller name>
juju add-model <model name>
```

Then, use the `juju deploy` command:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 14/stable

See all available config options for the charm with `juju config postgresql --format json`.
```

```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 14/stable --trust

See all available config options for the charm with `juju config postgresql-k8s --format json`.
```
````

See all available Juju flags in the [`juju deploy` CLI reference](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/deploy/). For example, you can set CPU and memory constraints with `--constraints`.

To deploy via **Terraform**, see the {ref}`Terraform guide <terraform>`.

(deploy-clouds)=
## Clouds

Charmed PostgreSQL can be deployed on several machine and Kubernetes cloud services.

```{toctree}
:titlesonly:

VM clouds <vm-clouds/index>
K8s clouds <k8s-clouds/index>
```

Deploy a cluster on a cloud using different availability zones, using Google Cloud as an example:

```{toctree}
:titlesonly:
:maxdepth: 2

Multi-AZ <multi-az/index>
```

## Terraform

Deploy PostgreSQL and automate your infrastructure with the Juju Terraform Provider:

```{toctree}
:titlesonly:

Terraform <terraform>
```

## Airgapped

Install PostgreSQL in an airgapped environment via Charmhub and the Snap Store Proxy:

```{toctree}
:titlesonly:

Air-gapped <air-gapped>
```

## Networking and TLS encryption

Example setup of external TLS/SSL access via Virtual IP (VIP):

```{toctree}
:titlesonly:

TLS VIP access <tls-vip-access>
```

## Juju storage

Use volume provided by different clouds via [Juju storage](https://documentation.ubuntu.com/juju/3.6/reference/storage/):

```{toctree}
:titlesonly:

Juju storage <juju-storage>
```