---
myst:
  html_meta:
    description: "Overview of all stable Charmed PostgreSQL K8s charm revisions with PostgreSQL versions, Juju requirements, and supported features per release."
---

(release-notes-k8s)=
# Release notes (K8s)
{{k8s}}

This page provides a high-level overview of the dependencies and features that are supported by each revision in every stable release of the [PostgreSQL charm for Kubernetes](https://charmhub.io/postgresql-k8s).

To learn more about the different release tracks and channels, see the [Juju documentation about channels](https://documentation.ubuntu.com/juju/3.6/reference/charm/#risk).

To see all releases and commits, see [Charmed PostgreSQL on GitHub](https://github.com/canonical/postgresql-k8s-operator).

## Dependencies and supported features

Several [revisions](https://documentation.ubuntu.com/juju/3.6/reference/charm/#charm-revision) are released simultaneously for different [bases/series](https://juju.is/docs/juju/base) using the same charm code. In other words, one release contains multiple revisions.

If you do not specify a revision on deploy time, Juju will automatically choose the revision that matches your base and architecture.

All revisions of PostgreSQL described below are built for *Ubuntu 24.04 LTS (Noble)*.

| Charmhub revision</br>(amd, arm)  |  PostgreSQL version | Minimum Juju version |
|:---------------------------------:|:------------------:|:--------------------:|
| {ref}`901, 902 <rev-901-902>`     | 16.13              | `3.6.21+` |

```{toctree}
:titlesonly:
:hidden:

901-902
```