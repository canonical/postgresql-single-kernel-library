(system-requirements)=
# System requirements

The following are the minimum software and hardware requirements to run Charmed PostgreSQL.

{ref}`Contact us <contact>` if you need support or have a feature request.

## Software

````{tab-set}
```{tab-item} VM
:sync: vm

* Ubuntu 24.04 LTS (Noble) or later
```

```{tab-item} K8s
:sync: k8s

* Ubuntu 24.04 LTS (Noble) or later
* Kubernetes 1.27+
* Canonical MicroK8s 1.27+
  * snap channel `1.27-strict/stable` and newer
```
````

### Juju

Charmed PostgreSQL 16 supports several Juju releases from 3.6 LTS onward. The table below shows which minor versions of each major Juju release are supported by the stable Charmhub releases of PostgreSQL.

| Juju major release | Supported minor versions | Compatible charm revisions |
|:-------------------|:-------------------------|:---------------------------|
| ![3.6 LTS]         | `3.6.1+`                 | All                        |

## Hardware

Minimum recommended hardware for a **production** deployment:
* vCPU: 4
* RAM: 8GB
* Disk: 20GB

Minimum recommended hardware for **testing**:
* vCPU: 2
* RAM: 2GB
* Disk: 10GB

Actual hardware requirements will depend on the workload you are running against your database. See: {ref}`performance-and-resources`.

The charm is based on the [charmed-postgresql snap](https://snapcraft.io/charmed-postgresql).

It currently supports the following architectures:
* `amd64`
* `arm64`

## Networking

*Access to the internet is required for downloading required snaps and charms
* Only IPv4 is supported at the moment
  * See more information about this limitation in [this Jira issue](https://warthogs.atlassian.net/browse/DPE-4695)


[3.6 LTS]: https://img.shields.io/badge/3.6_LTS-%23E95420?label=Juju
