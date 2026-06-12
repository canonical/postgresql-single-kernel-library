(minor-rollback)=
# Perform a minor rollback
{{vm_k8s}}

**Example**: PostgreSQL 14.9 -> PostgreSQL 14.8
(including simple charm revision bump: from revision 3 to revision 1)

After a `juju refresh`, if there are any version incompatibilities in charm revisions, its dependencies, or any other unexpected failure in the upgrade process, the process will be halted and enter a failure state.

Even if the underlying PostgreSQL cluster continues to work, it’s important to roll back the charm to a previous revision so that an update can be attempted after further inspection of the failure.

```{dropdown} Do **not** trigger a rollback during a running upgrade!
:open:
:class-container: dropdown-important
:icon: no-entry-fill

It may cause an unpredictable PostgreSQL cluster state!
```

## Summary of the rollback steps

````{tab-set}
```{tab-item} VM
:sync: vm

* {ref}`prepare-rollback` the Charmed PostgreSQL application for the in-place rollback. 
* {ref}`roll-back`. Once started, all units in a cluster will be executed sequentially. The rollback will be aborted (paused) if the unit rollback has failed.
* {ref}`post-rollback-check`: Make sure the charm and cluster are in a healthy state again.
```
```{tab-item} K8s
:sync: k8s

* {ref}`prepare-rollback` the Charmed PostgreSQL application for the in-place rollback.  Perform the first charm rollback on the first unit only. The unit
* {ref}`Roll back one unit<roll-back>`. The unit with the maximal ordinal will be chosen. If it rolls back successfully, continue with the other units. 
* {ref}`post-rollback-check`: Make sure the charm and cluster are in a healthy state again.
```
````

(prepare-rollback)=
## Prepare

To execute a rollback, we use a similar procedure to the upgrade. The difference is the charm revision to upgrade to. 

````{tab-set}
```{tab-item} VM
:sync: vm

In this guide's example, we will refresh the charm back to revision `182`.

It is necessary to re-run `pre-upgrade-check` action on the leader unit in order to enter the upgrade recovery state:

    juju run postgresql/leader pre-upgrade-check
```
```{tab-item} K8s
:sync: k8s

In this guide's example, we will refresh the charm back to revision `88`.

It is necessary to re-run `pre-upgrade-check` action on the leader unit to enter the upgrade recovery state:

    juju run postgresql-k8s/leader pre-upgrade-check
```
````

(roll-back)=
## Roll back

````{tab-set}
```{tab-item} VM
:sync: vm

When using a charm from charmhub:

    juju refresh postgresql --revision=182

When deploying from a local charm file, one must have the previous revision charm file and run the following command:

    juju refresh postgresql --path=./postgresql_ubuntu-22.04-amd64.charm

where `postgresql_ubuntu-22.04-amd64.charm` is the previous revision charm file.

The first unit will be rolled out and should rejoin the cluster after settling down. After the refresh command, the juju controller revision for the application will be back in sync with the running Charmed PostgreSQL revision.
```
```{tab-item} K8s
:sync: k8s

When using a charm from Charmhub:

    juju refresh postgresql-k8s --revision=88

When deploying from a local charm file, one must have the previous revision charm file and the `postgresql-image` resource, then run

    juju refresh postgresql-k8s --path=./postgresql-k8s_ubuntu-22.04-amd64.charm \
        --resource postgresql-image=ghcr.io/canonical/charmed-postgresql:797a2132

...where `postgresql-k8s_ubuntu-22.04-amd64.charm` is the previous revision charm file. The reference for the resource for a given revision can be found at the `metadata.yaml` file in the [charm repository](https://github.com/canonical/postgresql-k8s-operator/blob/main/metadata.yaml#L31).

The first unit will be rolled out and should rejoin the cluster after settling down. After the refresh command, the juju controller revision for the application will be back in sync with the running Charmed PostgreSQL K8s revision.

### Resume

We still need to resume the upgrade on the remaining units, which is done with the `resume-upgrade` action.

    juju run postgresql-k8s/leader resume-upgrade

This will roll out the pods in the remaining units, but to the same charm revision.
```
````

(post-rollback-check)=
## Post-rollback check

Future [improvements are planned](https://warthogs.atlassian.net/browse/DPE-2621) to check the state of a pod/cluster on a low level. 

For now, use `juju status` to make sure the {ref}`cluster state <charm-statuses>` is OK.

