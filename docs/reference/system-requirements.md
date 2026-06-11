---
myst:
  html_meta:
    description: "Minimum hardware and software requirements for running Charmed PostgreSQL, including Ubuntu version, Juju, Kubernetes, and resource specifications."
---

(system-requirements)=
# System requirements

The following are the minimum software and hardware requirements to run Charmed PostgreSQL.

{ref}`Contact us <contact>` if you need support or have a feature request.

## Software

````{tab-set}
```{tab-item} VM
:sync: vm

* Ubuntu 22.04 (Jammy) or later
```

```{tab-item} K8s
:sync: k8s

* Ubuntu 22.04 (Jammy) or later
* Kubernetes 1.27+
* Canonical MicroK8s 1.27+
  * snap channel `1.27-strict/stable` and newer
```
````

### Juju

Charmed PostgreSQL 14 supports several Juju releases, starting from [2.9 LTS](https://documentation.ubuntu.com/juju/3.6/releasenotes/juju_2.9.x/#juju-2-9-0).

| Juju major release | Supported minor versions | Compatible charm revisions |Comment |
|:--------|:-----|:-----|:-----|
| [![3.6 LTS]](https://documentation.ubuntu.com/juju/3.6/releasenotes/juju_3.6.x/) | `3.6.1+` | 444/445+ | Recommended for production. |
| [![3.5]](https://documentation.ubuntu.com/juju/3.6/releasenotes/unsupported/juju_3.x.x/#juju-3-5) | `3.5.1+` | 280+  | [Known Juju issue](https://bugs.launchpad.net/juju/+bug/2066517) in `3.5.0` |
| [![3.4]](https://documentation.ubuntu.com/juju/3.6/releasenotes/unsupported/juju_3.x.x/#juju-3-4) | `3.4.3+` | 280+  | Know Juju issues with previous minor versions |
| [![3.3]](https://documentation.ubuntu.com/juju/3.6/releasenotes/unsupported/juju_3.x.x/#juju-3-3) | `3.3.0+` | from 177 to 193  | No known issues |
| [![3.2]](https://documentation.ubuntu.com/juju/3.6/releasenotes/unsupported/juju_3.x.x/#juju-3-2) | `3.2.0+` | from 177 to 193 | No known issues |
| [![3.1]](https://documentation.ubuntu.com/juju/3.6/releasenotes/unsupported/juju_3.x.x/#juju-3-1) | `3.1.7+` | from 177 to 193| Juju secrets were stabilised in `3.1.7` |
| [![2.9 LTS]](https://documentation.ubuntu.com/juju/3.6/releasenotes/juju_2.9.x/#) | `2.9.49+` | 73+ |
|  | `2.9.32+` | 73 to 193 | No tests for older Juju versions. |

(hardware)=
## Hardware

Minimum recommended hardware for a **production** deployment:
* vCPU: 4
* RAM: 8GB
* Disk: 20GB

Minimum recommended hardware for **testing**:
* vCPU: 2
* RAM: 2GB
* Disk: 10GB

Actual hardware requirements will depend on the workload you are running against your database.

{{seealso}} {ref}`performance-and-testing`

The charm is based on the [charmed-postgresql snap](https://snapcraft.io/charmed-postgresql).

It currently supports the following architectures:
* `amd64`
* `arm64`
  * Starting from **Revision 211** for K8s
  * Starting from **Revision 396** for VM

## Networking

Access to the internet is required for downloading required snaps and charms.

{{seealso}} {ref}`air-gapped`

Only IPv4 is supported at the moment. See more information about this limitation in [this Jira issue](https://warthogs.atlassian.net/browse/DPE-4695)

<!-- Badges -->

[2.9 LTS]: https://img.shields.io/badge/2.9_LTS-%23E95420?label=Juju
[3.1]: https://img.shields.io/badge/3.1-%23E95420?label=Juju
[3.2]: https://img.shields.io/badge/3.2-%23E95420?label=Juju
[3.3]: https://img.shields.io/badge/3.3-%23E95420?label=Juju
[3.4]: https://img.shields.io/badge/3.4-%23E95420?label=Juju
[3.5]: https://img.shields.io/badge/3.5-%23E95420?label=Juju
[3.6 LTS]: https://img.shields.io/badge/3.6_LTS-%23E95420?label=Juju