---
myst:
  html_meta:
    description: "Restore a Charmed PostgreSQL backup from S3 storage by listing available backups and running the restore Juju action, with point-in-time recovery notes."
---

(restore-a-backup)=
# How to restore a local backup

This is a guide on how to restore a locally made backup.

To restore a backup that was made from the a *different* cluster, (i.e. cluster migration via restore), see {ref}`migrate-a-cluster`.

## Prerequisites

* A PostgreSQL deployment {ref}`scaled down <scale-replicas>` to one unit (scale it up again after the backup is restored)
- Access to S3 storage
- {ref}`A backup in your S3 storage <create-a-backup>`

---

## Apply cluster credentials

When restoring a backup that was taken from the same cluster and the `operator`, `monitoring`, `replication`, and `rewind` passwords have not changed since then, you **do not** need to do this step.

```{include} migrate-a-cluster.md
    :start-after: "<!--begin include-->"
    :end-before: "<!--end include-->"
```

## List backups

To view the available backups to restore, use the command `list-backups`:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader list-backups
```

```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader list-backups
```
````

This should show your available backups like in the sample output below:

```shell
list-backups: |-
  Storage bucket name: canonical-postgres
  Backups base path: /test/backup/

  backup-id            | action             | ... | timeline
  ---------------------------------------------------------------------------
  2024-07-22T13:11:56Z | full backup        | ... | 1
  2024-07-22T14:12:45Z | incremental backup | ... | 1
  2024-07-22T15:34:24Z | restore            | ... | 2
  2024-07-22T16:26:48Z | incremental backup | ... | 2
  2024-07-22T17:17:59Z | full               | ... | 2
  2024-07-22T18:05:32Z | restore            | ... | 3
```

Below is a complete list of parameters shown for each backup/restore operation:

* `backup-id`: unique identifier of the backup.
* `action`: indicates the action performed by the user through one of the charm action; can be any of full backup, incremental backup, differential backup or restore.
* `status`: either finished (successfully) or failed.
* `reference-backup-id`
* `LSN start/stop`: a database specific number (or timestamp) to identify its state.
* `start-time`: records start of the backup operation.
* `finish-time`: records end of the backup operation.
* `backup-path`: path of the backup related files in the S3 repository.
* `timeline`: number which identifies different branches in the database transactions history; every time a restore or PITR is made, this number is incremented by 1.

## Point-in-time recovery

Point-in-time recovery (PITR) enables restorations to the database state at specific points in time.

After performing a PITR in a PostgreSQL cluster, a new timeline is created to track from the point to where the database was restored. They can be tracked via the `timeline` parameter in the `list-backups` output.

## Restore backup

To restore a backup from that list, run the `restore` command and pass the parameter corresponding to the backup type.

When the user needs to restore a specific backup that was made, they can use the `backup-id` that is listed in the `list-backups` output.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader restore backup-id=YYYY-MM-DDTHH:MM:SSZ
```

```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader restore backup-id=YYYY-MM-DDTHH:MM:SSZ
```
````

However, if the user needs to restore to a specific point in time between different backups (e.g. to restore only specific transactions made between those backups), they can use the `restore-to-time` parameter to pass a timestamp related to the moment they want to restore. The format matches PostgreSQL's `SELECT current_timestamp` output (`YYYY-MM-DD HH:MM:SS` with a space separator, optional fractional seconds, and an optional `+HH` / `-HH:MM` timezone offset):

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader restore restore-to-time="YYYY-MM-DD HH:MM:SS"
```

```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader restore restore-to-time="YYYY-MM-DD HH:MM:SS"
```
````

It is also possible to restore to the latest point from a specific timeline by passing the ID of a backup taken on that timeline and `restore-to-time=latest` when requesting a restore:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader restore restore-to-time=latest
```

```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader restore restore-to-time=latest
```
````
