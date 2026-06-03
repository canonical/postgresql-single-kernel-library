(system-requirements)=
# System requirements

The following are the minimum software and hardware requirements to run Charmed PostgreSQL.

{ref}`Contact us <contact>` if you need support or have a feature request.

## Software

* Ubuntu 24.04 (Noble) or later.

### Juju

Charmed PostgreSQL 16 supports several Juju releases from 3.6 LTS onward. The table below shows which minor versions of each major Juju release are supported by the stable Charmhub releases of PostgreSQL.

| Juju major release | Supported minor versions | Compatible charm revisions |
|:-------------------|:-------------------------|:---------------------------|
| ![3.6 LTS]         | `3.6.1+`                 | All                        |

## Hardware

profile    testing     production
vCPU         2+             4+
RAM          2GiB+         8GiB+
Disk         10GiB+       20GiB+

Actual hardware requirements will depend on the workload you are running against your database.

The charm is based on the [charmed-postgresql snap](https://snapcraft.io/charmed-postgresql) and [rock]()

It currently supports the following architectures:
* `amd64`
* `arm64`

## Networking

*Access to the internet is required for downloading required snaps and charms
* Only IPv4 is supported at the moment
  * See more information about this limitation in [this Jira issue](https://warthogs.atlassian.net/browse/DPE-4695)


[3.6 LTS]: https://img.shields.io/badge/3.6_LTS-%23E95420?label=Juju
