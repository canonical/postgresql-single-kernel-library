(deploy)=
# How to deploy

The basic requirements for deploying a charm are the [**Juju client**](https://documentation.ubuntu.com/juju/3.6/) and a machine [**cloud**](https://juju.is/docs/juju/cloud).

For more details, see {ref}`system-requirements`.

If you are not sure where to start, or would like a more guided walkthrough for setting up your environment, see the {ref}`tutorial`.

(deploy-quickstart)=
## Quickstart

Charmed PostgreSQL can be deployed using the Juju CLI directly, or via Terraform.

To deploy via the **Juju CLI**, you need to first [bootstrap](https://juju.is/docs/juju/juju-bootstrap) a cloud controller and create a [model](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/model/):

```shell
juju bootstrap <cloud name> <controller name>
juju add-model <model name>
```

Then, use the [`juju deploy`](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/juju-cli/list-of-juju-cli-commands/deploy/) command:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 16/stable
```

```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 16/stable --trust
```
````

To deploy via **Terraform**, see the {ref}`Terraform guide <terraform>`.

If you are not sure where to start or would like a more guided walkthrough for setting up your environment, see the {ref}`tutorial`.

(deploy-clouds)=
## Clouds

Charmed MySQL can be deployed on several machine and Kubernetes cloud services.

```{toctree}
:titlesonly:

VM clouds <vm-clouds/index>
K8s clouds <k8s-clouds/index>
```

Deploy a cluster on a cloud using different availability zones:

```{toctree}
:titlesonly:

Multi-AZ <multi-az>
```

## Terraform

Deploy PostgreSQL and automate your infrastructure with the Juju Terraform Provider:

```{toctree}
:titlesonly:

Terraform <terraform>
```

## Networking and TLS encryption

Example setup of external TLS/SSL access via Virtual IP (VIP):

```{toctree}
:titlesonly:

TLS VIP access <tls-vip-access>
```

Configure Juju spaces to separate network traffic:

```{toctree}
:titlesonly:

Juju spaces <juju-spaces>
```

## Airgapped

Install PostgreSQL in an airgapped environment via Charmhub and the Snap Store Proxy:

```{toctree}
:titlesonly:

Air-gapped <air-gapped>
```

## Juju storage

Use volume provided by different clouds via [Juju storage](https://documentation.ubuntu.com/juju/3.6/reference/storage/):

```{toctree}
:titlesonly:

Juju storage <juju-storage>
```
