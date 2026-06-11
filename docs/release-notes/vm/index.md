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

| Charmhub revision</br>(amd, arm)  | Snap revision</br>(amd, arm) | PostgreSQL version | Minimum Juju version |
|:---------------------------------:|:------------------------:|:------------------:|:--------------------:|

```{toctree}
:titlesonly:
:hidden:


```