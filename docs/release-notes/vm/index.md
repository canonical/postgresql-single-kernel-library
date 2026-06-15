---
myst:
  html_meta:
    description: "Overview of all stable Charmed PostgreSQL VM charm revisions with PostgreSQL versions, Juju requirements, and supported features per release."
---

(release-notes-vm)=
# Release notes (VM)
{{vm}}

This page provides a high-level overview of the dependencies and features that are supported by each revision in every stable release of the [PostgreSQL charm for machines/VM](https://charmhub.io/postgresql).

To learn more about the different release tracks and channels, see the [Juju documentation about channels](https://documentation.ubuntu.com/juju/3.6/reference/charm/#risk).

To see all releases and commits, see [Charmed PostgreSQL on GitHub](https://github.com/canonical/postgresql-operator).

## Dependencies and supported features

Several [revisions](https://documentation.ubuntu.com/juju/3.6/reference/charm/#charm-revision) are released simultaneously for different [bases/series](https://juju.is/docs/juju/base) using the same charm code. In other words, one release contains multiple revisions.

If you do not specify a revision on deploy time, Juju will automatically choose the revision that matches your base and architecture.

All revisions of PostgreSQL described below are built for *Ubuntu 22.04 LTS (Jammy)*.

| Charmhub revision</br>(amd, arm)   | Snap revision</br>(amd, arm) | PostgreSQL version | Minimum Juju version |
|:----------------------------------|:----------------------------|:------------------|:--------------------|
| {ref}`1090, 1091 <rev-1090-1091>`  |         281,280              | 14.22              | `3.6.21+`           |
| {ref}`1044, 1045 <rev-1044-1045>`  |         247,246              | 14.20              | `3.6.14+`           |
| {ref}`986, 987 <rev-986-987>`      |         245,243              | 14.20              | `3.6.1+`            |
| {ref}`935, 936 <rev-935-936>`      |         229,230              | 14.19              | `3.6.1+`            |
| {ref}`552, 553 <rev-552-553>`      |                              | 14.15              | `3.6.1+`            |
| {ref}`467, 468 <rev-467-468>`      |                              | 14.12              | `3.4.3+`            |
| {ref}`429, 430 <rev-429-430>`      |                              | 14.11              | `3.4.2+`            |
| {ref}`363 <rev-363>`               |                              | 14.10              | `3.4.2+`            |
| {ref}`351 <rev-351>`               |             89               | 14.9               | `3.1.6+`            |
| {ref}`336 <rev-336>`               |                              | 14.9               | `3.1.5+`            |
| {ref}`288 <rev-288>`               |                              | 14.7               | `2.9.32+`           |


```{toctree}
:titlesonly:
:hidden:

1090-1091
1044-1045
986-987
935-936
552-553
467-468
429-430
363
351
336
288
```