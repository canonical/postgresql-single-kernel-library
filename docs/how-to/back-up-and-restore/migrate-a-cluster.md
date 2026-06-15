---
myst:
  html_meta:
    description: "Migrate a Charmed PostgreSQL cluster by restoring a backup from a source cluster to a new cluster and synchronizing passwords."
---

(migrate-a-cluster)=
# How to migrate a cluster
{{vm_k8s}}

This is a guide on how to restore a backup that was made from a different cluster, (i.e. cluster migration via restore).

{octicon}`arrow-right` See {ref}`restore-a-backup` to perform a basic restore from a *local* backup

## Prerequisites

* A PostgreSQL deployment {ref}`scaled down <scale-cluster>` to one unit (scale it up again after the backup is restored)
* A {ref}`backup <create-a-backup>` from the previous cluster in your S3 storage
* Passwords from your previous cluster
  * See: {ref}`manage-passwords` and {ref}`save-current-cluster-credentials`

---

## Apply cluster credentials

When you restore a backup from an old cluster, it will restore the password from the previous cluster to your current cluster. 

Set the password of your current cluster to the previous cluster’s password:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader set-password username=operator password=<previous cluster password> 
    juju run postgresql/leader set-password username=replication password=<previous cluster password> 
    juju run postgresql/leader set-password username=rewind password=<previous cluster password> 
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader set-password username=operator password=<previous cluster password>
    juju run postgresql-k8s/leader set-password username=replication password=<previous cluster password> 
    juju run postgresql-k8s/leader set-password username=rewind password=<previous cluster password>
```
````

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
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

Take note of the `backup-id` that corresponds to the previous cluster.

## Restore backup

To restore your current cluster to the state of the previous cluster, the `restore` command with your `backup-id`:

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

Your restore will then be in progress, once it is complete your cluster will represent the state of the previous cluster.

