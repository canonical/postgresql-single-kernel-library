---
myst:
  html_meta:
    description: "Migrate a Charmed PostgreSQL cluster by restoring a backup from a source cluster to a new cluster and synchronize passwords."
---

(migrate-a-cluster)=
# How to migrate a cluster
{{vm_k8s}}

This is a guide on how to restore a backup that was made from a different cluster, (i.e. cluster migration via restore).

```{seealso}
To perform a basic restore from a *local* backup, see {ref}`restore-a-backup`.
```

## Prerequisites


* A PostgreSQL deployment {ref}`scaled down <scale-replicas>` to one unit (scale it up again after the backup is restored)
* A {ref}`backup <create-a-backup>` from the previous cluster in your S3 storage
* Passwords from your previous cluster
  * See: {ref}`manage-passwords` and {ref}`save-current-cluster-credentials`

---

## Apply cluster credentials

Passwords are not re-generated when a cluster is restored. To make sure the new cluster uses the credentials from the previous cluster, apply the credentials you {ref}`saved during the backup process <save-current-cluster-credentials>` **before** restoring.

<!--begin include-->
Create a secret with the password values you saved when creating the backup:

```shell
juju add-secret <secret name> monitoring=<password1> operator=<password2> replication=<password3> rewind=<password4>
```

where `<secret name>` can be any name you'd like for the restored secrets.

Then, grant the secret to the PostgreSQL application that will initiate the restore:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju grant-secret <secret name> postgresql
```

```{tab-item} K8s
:sync: k8s

    juju grant-secret <secret name> postgresql-k8s
```
````
<!--end include-->

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

