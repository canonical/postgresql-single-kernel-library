---
myst:
  html_meta:
    description: "How-to guides for Charmed PostgreSQL on VMs and Kubernetes, covering deployment, scaling, authentication, backups, observability, and data migration."
---

(how-to)=
# How-to guides
{{vm_k8s}}

The following guides cover key processes and common tasks for setting up and managing Charmed PostgreSQL on bare metal and virtual machines.

## Deployment and setup

Available deployment methods and specialised setups:

```{toctree}
:titlesonly:
:maxdepth: 2

Deploy <deploy/index>
```

## Operations and maintenance

Essential operations to configure and manage a PostgreSQL cluster:

```{toctree}
:titlesonly:

Scale a cluster <scale-cluster>
Integrate <integrate>
Authentication <authentication/index>
Network and encryption <network-and-encryption/index>
```

### Backups and data migration

Configuration of storage providers and backup management for safety and data migration from a different cluster:

```{toctree}
:titlesonly:
:maxdepth: 2

Back up and restore <back-up-and-restore/index>
```

Other data migration guides:

```{toctree}
:titlesonly:
:maxdepth: 2

Data migration <data-migration/index>
```

### Refresh (upgrade)

Instructions for performing an in-place application refresh:

```{toctree}
:titlesonly:

Refresh (upgrade) <refresh/index>
```

### Observability (COS)

Set up observability services like Grafana, Prometheus, Loki, and Tempo through the Canonical Observability Stack (COS):

```{toctree}
:maxdepth: 2

Observability (COS) <observability-cos/index>
```

### High availability & replication

Walkthrough of a cross-regional, cluster-cluster deployment and disaster recovery operations:

```{toctree}
:maxdepth: 2
:titlesonly:

Custer-cluster replication <cluster-cluster-replication/index>
```

## Extensions (plugins)

```{toctree}
:maxdepth: 2
:titlesonly:

PostgreSQL extensions <extensions/index>
```

## Troubleshooting and disaster recovery

```{toctree}
:titlesonly:

Troubleshooting <troubleshooting/index>
```

