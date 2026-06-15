(minor-upgrade)=
# Perform a minor upgrade

**Example**: PostgreSQL 14.8 -> PostgreSQL 14.9

(including charm revision bump: e.g. Revision 1 -> Revision 3)

## Summary

````{tab-set}
```{tab-item} VM
:sync: vm

* {ref}`Pre-upgrade checks <pre-upgrade-checks>`: Important information to consider before starting an upgrade.
* {ref}`Record the revision of the original charm <record-original-charm-revision>`.
* {ref}`Prepare <prepare-upgrade>` your Charmed PostgreSQL Juju application for the in-place upgrade.
* {ref}`Upgrade <upgrade>`. Once started, all units in a cluster will be executed sequentially. The upgrade will be aborted (paused) if the unit upgrade has failed.
* {ref}`Roll back <roll-back-optional>` in case of disaster. 
* {ref}`post-upgrade-check`: Make sure all units are in the proper state and the cluster is healthy.
```
```{tab-item} K8s
:sync: k8s

* {ref}`Pre-upgrade checks <pre-upgrade-checks>`: Important information to consider before starting an upgrade.
* {ref}`Record the revision of the original charm <record-original-charm-revision>`.
* {ref}`Prepare <prepare-upgrade>` your Charmed PostgreSQL K8s application for the in-place upgrade, including scaling up by one unit.
* {ref}`Upgrade one unit <upgrade>`. Once started, only one unit (pod) in the cluster will be upgraded. If the new pod is OK after the refresh, the upgrade can be resumed for all other units in the cluster.
* {ref}`Roll back <roll-back-optional>` in case of disaster. 
* Post-upgrade check: Make sure the cluster is healthy and remove any pods that are no longer needed.
content
```
````

(pre-upgrade-checks)=
## Pre-upgrade checks

Key topics to take into consideration before upgrading.

### Avoid concurrency with other operations

**We strongly recommend to NOT perform any other extraordinary operations on Charmed PostgreSQL cluster while upgrading.** 

Some examples are operations like (but not limited to) the following:

* Adding or removing units
* Creating or destroying new relations
* Changes in workload configuration
* Upgrading other connected/related/integrated applications simultaneously

Concurrency with other operations is not supported, and it can lead the cluster into inconsistent states.

### Verify your backups

**Make sure to have a backup of your data when running any type of upgrade.**

If you do not have a backup, see {ref}`create-a-backup`.

Verify the integrity of the backup by performing a test {ref}`restore on another application <migrate-a-cluster>`.

Check the restored data by ensuring that:
* recent data is present
* the data size is correct
* the data matches what you expected in the backup

### Review release notes

Review the release notes for every charm version between the version that you are refreshing from and to to understand what changed and if any action is required from you before, during, or after the refresh.

If the PostgreSQL versions that you are refreshing from and to are different, refer to the [upstream PostgreSQL release notes](https://www.postgresql.org/docs/release/) to understand what changed and if any action is required from you.

### Service disruption

**It is recommended to deploy your application in conjunction with the [Charmed PgBouncer](https://charmhub.io/pgbouncer) operator.** 

This will ensure minimal service disruption, if any.

(record-original-charm-revision)=
## Record original charm revision

```{dropdown} This step is only valid when deploying from Charmhub
:open:
:icon: info
:class-container: dropdown-note
:class-title: sd-font-weight-normal

If a [local charm](https://juju.is/docs/sdk/deploy-a-charm) is deployed (revision is small, e.g. 0-10), make sure the proper/current local revision of the `.charm` file is available BEFORE going further. You might need it for a rollback.
```

The first step is to record the revision of the running application as a safety measure for a rollback action. To accomplish this, run the `juju status` command and look for the deployed Charmed PostgreSQL revision in the command output, e.g.:

````{tab-set}
```{tab-item} VM
:sync: vm

    Model        Controller  Cloud/Region         Version  SLA          Timestamp
    welcome-lxd  lxd         localhost/localhost  3.1.6    unsupported  11:35:36+02:00

    App         Version  Status  Scale  Charm       Channel       Rev  Exposed  Message
    postgresql  14.9     active      3  postgresql  14/stable     330  no       

    Unit           Workload  Agent  Machine  Public address  Ports     Message
    postgresql/3*  active    idle   3        10.3.217.74     5432/tcp  
    postgresql/4   active    idle   4        10.3.217.95     5432/tcp  
    postgresql/5   active    idle   5        10.3.217.108    5432/tcp  

    Machine  State    Address       Inst id        Base          AZ  Message
    3        started  10.3.217.74   juju-d483b7-3  ubuntu@22.04      Running
    4        started  10.3.217.95   juju-d483b7-4  ubuntu@22.04      Running
    5        started  10.3.217.108  juju-d483b7-5  ubuntu@22.04      Running

In this example, the current revision is `330`. Store it safely to use in case of a rollback!
```
```{tab-item} K8s
:sync: k8s

    Model        Controller  Cloud/Region        Version  SLA          Timestamp
    welcome-k8s  microk8s    microk8s/localhost  3.1.6    unsupported  12:23:03+02:00

    App             Version  Status  Scale  Charm           Channel    Rev  Address         Exposed  Message
    postgresql-k8s  14.9     active      3  postgresql-k8s  14/stable  145  10.152.183.166  no       

    Unit               Workload  Agent  Address     Ports  Message
    postgresql-k8s/0*  active    idle   10.1.12.12         Primary
    postgresql-k8s/1   active    idle   10.1.12.19         
    postgresql-k8s/2   active    idle   10.1.12.20

For this example, the current revision is `145`. Store it safely to use in case of a rollback!
```
````

(prepare-upgrade)=
## Prepare

`````{tab-set}
````{tab-item} VM
:sync: vm

Prepare for the upgrade by checking the cluster health and configuring the charm to minimize primary switchover.

### Run `pre-upgrade-check` action

Before running the [`juju refresh`](https://juju.is/docs/juju/juju-refresh) command, it’s necessary to run the `pre-upgrade-check` action against the leader unit:

    juju run postgresql/leader pre-upgrade-check

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

Make sure there are no errors in the result output.

This action will configure the charm to minimise the amount of primary switchover, among other preparations for a safe upgrade process. After successful execution, the charm is ready to be upgraded.
````
````{tab-item} K8s
:sync: k8s

Prepare for the upgrade by scaling up by one unit and checking the cluster health.

### Scale up

It is recommended to scale the application up by one unit before starting the upgrade process.

The new unit will be the first one to be updated, and it will assert that the upgrade is possible. In case of failure, having the extra unit will ease the rollback procedure without disrupting service - simply remove the pod that was recently added when scaling up.

You can read more about this in the {ref}`rollback guide <minor-rollback>`.

Scale your application using the following command:

    juju scale-application postgresql-k8s <current_units_count+1>

Wait for the new unit to be up and ready.

### Run `pre-upgrade-check` action

Before running the [`juju refresh`](https://juju.is/docs/juju/juju-refresh) command, it’s necessary to run the `pre-upgrade-check` action against the leader unit:

    juju run postgresql-k8s/leader pre-upgrade-check

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

Make sure there are no errors in the result output.

This action will configure the charm to minimise the amount of primary switchover, among other preparations for a safe upgrade process. After successful execution, the charm is ready to be upgraded.
````
`````

(upgrade)=
## Upgrade

Use the  `juju refresh` command to trigger the charm upgrade process.

`````{tab-set}
````{tab-item} VM
:sync: vm

Example with channel selection:

```shell
juju refresh postgresql --channel 14/edge
```

Example with specific revision selection:

```shell
juju refresh postgresql --revision=342
```

Example with a local charm file:

```shell
juju refresh postgresql --path ./postgresql_ubuntu-22.04-amd64.charm
```

All units will be refreshed (i.e. receive new charm content), and the upgrade will execute one unit at a time. 

**The order in which the units are upgraded is based on roles**.

First the `replica` units, then the `sync-standby` units, and lastly, the `leader`(or `primary`) unit. This helps reduce connection disruptions. 

`juju status` will look like similar to the output below:

```text
Model        Controller  Cloud/Region         Version  SLA          Timestamp
welcome-lxd  lxd         localhost/localhost  3.1.6    unsupported  11:36:18+02:00

App         Version  Status  Scale  Charm       Channel   Rev  Exposed  Message
postgresql  14.9     active      3  postgresql  14/stable 331  no       

Unit           Workload  Agent      Machine  Public address  Ports     Message
postgresql/3*  waiting   idle       3        10.3.217.74     5432/tcp  other units upgrading first...
postgresql/4   waiting   idle       4        10.3.217.95     5432/tcp  other units upgrading first...
postgresql/5   waiting   executing  5        10.3.217.108    5432/tcp  waiting for database initialisation

Machine  State    Address       Inst id        Base          AZ  Message
3        started  10.3.217.74   juju-d483b7-3  ubuntu@22.04      Running
4        started  10.3.217.95   juju-d483b7-4  ubuntu@22.04      Running
5        started  10.3.217.108  juju-d483b7-5  ubuntu@22.04      Running
```

After each unit completes the upgrade, the message will go blank, and a next unit will follow:

```text
Model        Controller  Cloud/Region         Version  SLA          Timestamp
welcome-lxd  lxd         localhost/localhost  3.1.6    unsupported  11:36:31+02:00

App         Version  Status  Scale  Charm       Channel   Rev  Exposed  Message
postgresql  14.9     active      3  postgresql  14/stable 331  no       

Unit           Workload     Agent      Machine  Public address  Ports     Message
postgresql/3*  waiting      idle       3        10.3.217.74     5432/tcp  other units upgrading first...
postgresql/4   maintenance  executing  4        10.3.217.95     5432/tcp  refreshing the snap
postgresql/5   active       idle       5        10.3.217.108    5432/tcp  

Machine  State    Address       Inst id        Base          AZ  Message
3        started  10.3.217.74   juju-d483b7-3  ubuntu@22.04      Running
4        started  10.3.217.95   juju-d483b7-4  ubuntu@22.04      Running
5        started  10.3.217.108  juju-d483b7-5  ubuntu@22.04      Running
```
````
````{tab-item} K8s
:sync: k8s

Example with channel selection:

```shell
juju refresh postgresql-k8s --channel 14/stable --trust
```

Example with specific revision selection (do not miss the OCI resource!):

```shell
juju refresh postgresql-k8s --revision=189 --resource postgresql-image=...
```

**The upgrade will execute only on the highest ordinal unit.** 

For the running example `postgresql-k8s/3`, the `juju status` will look like:

```text
Model        Controller  Cloud/Region        Version  SLA          Timestamp
welcome-k8s  microk8s    microk8s/localhost  3.1.6    unsupported  12:26:32+02:00

App             Version  Status   Scale  Charm           Channel    Rev  Address         Exposed  Message
postgresql-k8s  14.9     waiting      4  postgresql-k8s  14/stable  154  10.152.183.166  no       installing agent

Unit               Workload     Agent      Address     Ports  Message
postgresql-k8s/0*  waiting      idle       10.1.12.12         other units upgrading first...
postgresql-k8s/1   waiting      idle       10.1.12.19         other units upgrading first...
postgresql-k8s/2   waiting      idle       10.1.12.20         other units upgrading first...
postgresql-k8s/3   maintenance  executing  10.1.12.23         upgrading unit
```

### Resume

After the unit is upgraded, the charm will set the unit upgrade state as completed. If deemed necessary, the user can further assert the success of the upgrade. 

Given that the unit is healthy within the cluster, the next step is to resume the upgrade process by running:

```shell
juju run postgresql-k8s/leader resume-upgrade 
```

The `resume-upgrade` command will roll out the upgrade for the following unit, always from highest to lowest. For each successful upgraded unit, the process will roll out the next one automatically.

Sample `juju status` output:

```text
Model        Controller  Cloud/Region        Version  SLA          Timestamp
welcome-k8s  microk8s    microk8s/localhost  3.1.6    unsupported  12:28:38+02:00

App             Version  Status   Scale  Charm           Channel  Rev  Address         Exposed  Message
postgresql-k8s  14.9     waiting      4  postgresql-k8s  14/edge  154  10.152.183.166  no       installing agent

Unit               Workload     Agent      Address     Ports  Message
postgresql-k8s/0*  waiting      executing  10.1.12.12         other units upgrading first...
postgresql-k8s/1   waiting      executing  10.1.12.19         other units upgrading first...
postgresql-k8s/2   maintenance  executing  10.1.12.24         (config-changed) upgrading unit
postgresql-k8s/3   maintenance  executing  10.1.12.23         upgrade completed
```
````
`````

**Please be patient during huge installations.**
Each unit should recover shortly after the upgrade, but time can vary depending on the amount of data written to the cluster while the unit was not part of it. 

**Incompatible charm revisions or dependencies will halt the process.**
After a `juju refresh`, if there are any version incompatibilities in charm revisions, its dependencies, or any other unexpected failure in the upgrade process, the upgrade process will be halted and enter a failure state.

(roll-back-optional)=
## Roll back (optional)

This is only necessary if the upgrade failed in some way.

Although the underlying PostgreSQL cluster continues to work, it’s important to roll back the charm to a previous revision so that an update can be attempted after further inspection of the failure. 

```{dropdown} Do **not** trigger a rollback during a running upgrade procedure!
:open:
:class-container: dropdown-important
:icon: no-entry-fill

It is expected to have some status changes during the process: `waiting`, `maintenance`, `active`. 

Make sure the upgrade has failed/stopped and cannot be fixed/continued before triggering `rollback`!
```

{{see}} {ref}`minor-rollback`

(post-upgrade-check)=
## Post-upgrade check

````{tab-set}
```{tab-item} VM
:sync: vm

Future [improvements are planned](https://warthogs.atlassian.net/browse/DPE-2621) to check the state of a cluster on a low level. 

For now, use `juju status` to make sure the {ref}`cluster state <charm-statuses>` is OK.
```
```{tab-item} K8s
:sync: k8s

Future [improvements are planned](https://warthogs.atlassian.net/browse/DPE-2621) to check the state of a pod on a low level. 

For now, use `juju status` to make sure the {ref}`cluster state <charm-statuses>` is OK.

### Scale back up

If the application scale was changed for the upgrade procedure, it is now safe to scale it back to the desired unit count:

    juju scale-application postgresql-k8s <unit_count>

```
````
