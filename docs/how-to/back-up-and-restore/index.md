---
myst:
  html_meta:
    description: "How-to guides for backing up and restoring Charmed PostgreSQL, covering S3 configuration, creating backups, restoring, and cluster migration."
---

(back-up-and-restore)=
# How to back up and restore
{{vm_k8s}}

<!--
TODO: clarify difference between:

`back-up-and-restore/restore-a-backup`
`back-up-and-restore/migrate-a-cluster`
`data-migration/migrate-data-via-backup-restore`
`data-migration/migrate-data-via-pg-dump`
https://canonical-information-systems-documentation.readthedocs-hosted.com/en/latest/how-to/prodstack/sql-migration/
-->

Configure AWS S3 storage with the `s3-integrator` charm to manage backups of your cluster:

```{toctree}
:titlesonly:

Configure S3 AWS <configure-s3-aws>
Configure S3 RadosGW <configure-s3-radosgw>
```

Essential backup management operations:

```{toctree}
Create a backup <create-a-backup>
Restore a backup <restore-a-backup>
Manage backup retention <manage-backup-retention>
```

Migrate a PostgreSQL cluster via backup and restore:

```{toctree}
Migrate a cluster <migrate-a-cluster>
```