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

All revisions of PostgreSQL described below are built for *Ubuntu 22.04 LTS (Jammy)*.

| Charmhub revision</br>(amd, arm)  |  PostgreSQL version | Recommended Juju version |
|:---------------------------------|:-------------------|:--------------------|
| {ref}`925, 924 <rev-924-925>`     | 14.23               | `3.6.24+`             |
| {ref}`774, 775 <rev-774-775>`     | 14.20               | `3.6.1+`             |
| {ref}`495, 494 <rev-494-495>`     | 14.15               | `3.6+`               |
| {ref}`462, 463 <rev-462-463>`     | 14.13               | `3.6+`               |
| {ref}`444, 445 <rev-444-445>`     | 14.13               | `3.6+`               |
| {ref}`381, 382 <rev-381-382>`     | 14.12               | `3.4.5+`             |
| {ref}`281, 280 <rev-280-281>`     | 14.11               | `3.4.5+`             |
| {ref}`193 <rev-193>`              | 14.10               | `3.4.2+`             |
| {ref}`177 <rev-177>`              | 14.9                | `3.1.6+`             |
| {ref}`158 <rev-158>`              | 14.9                | `3.1.5+`             |
| {ref}`73 <rev-73>`                | 14.7                | `2.9.32+`            |

```{toctree}
:titlesonly:
:hidden:

924-925
774-775
494-495
462-463
444-445
381-382
280-281
193
177
158
73
```

