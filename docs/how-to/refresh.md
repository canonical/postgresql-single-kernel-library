(refresh)=
# Refresh (upgrade)
{{vm_k8s}}

````{dropdown} Emergency stop button
:open:
:color: danger
:icon: no-entry-fill

Halt an in-progress refresh with

```shell
juju config <app name> pause-after-unit-refresh=all
```

Then, consider {ref}`rolling back <roll-back>`.
````

Charmed PostgreSQL supports minor version in-place refresh via the [`juju refresh`](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/refresh/#details) command.

## Determine which version to refresh to

Get the current charm revision of the application with [`juju status`](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/status/).

(recommended-refreshes)=
### Recommended refreshes

These refreshes are well-tested and should be preferred.

`````{tab-set}
````{tab-item} VM
:sync: vm

```{eval-rst}
+--------------+------------+----------+--------------+------------+----------+-------------------------------------------------------------------------------------------------+
|  From                                |  To                                  | Charm release notes to review                                                                   |
+--------------+------------+----------+--------------+------------+----------+                                                                                                 |
| Charm        | PostgreSQL | Snap     | Charm        | PostgreSQL | Snap     |                                                                                                 |
| revision     | Version    | revision | revision     | Version    | revision |                                                                                                 |
+==============+============+==========+==============+============+==========+=================================================================================================+
| 843 (amd64)  | 16.9       | 201, 202 | 1047 (amd64) | 16.11      | 242, 244 | | `951, 952 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.135.0>`__    |
+--------------+            |          +--------------+            |          | | `989, 990 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.165.0>`__    |
| 844 (arm64)  |            |          | 1046 (arm64) |            |          | | `1046, 1047 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.206.0>`__  |
+--------------+------------+----------+--------------+------------+----------+-------------------------------------------------------------------------------------------------+
| 952 (amd64)  | 16.10      | 239, 240 | 1047 (amd64) | 16.11      | 242, 244 | | `989, 990 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.165.0>`__    |
+--------------+            |          +--------------+            |          | | `1046, 1047 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.206.0>`__  |
| 951 (arm64)  |            |          | 1046 (arm64) |            |          |                                                                                                 |
+--------------+------------+----------+--------------+------------+----------+-------------------------------------------------------------------------------------------------+
| 990 (amd64)  | 16.11      | 242, 244 | 1047 (amd64) | 16.11      | 242, 244 | | `1046, 1047 <https://github.com/canonical/postgresql-operator/releases/tag/v16%2F1.206.0>`__  |
+--------------+            |          +--------------+            |          |                                                                                                 |
| 989 (arm64)  |            |          | 1046 (arm64) |            |          |                                                                                                 |
+--------------+------------+----------+--------------+------------+----------+-------------------------------------------------------------------------------------------------+
```
````

````{tab-item} K8s
:sync: k8s

```{eval-rst}
+--------------+------------+----------+--------------+------------+----------+-------------------------------------+
|  From                                |  To                                  | Charm release notes to review       |
+--------------+------------+----------+--------------+------------+----------+                                     |
| Charm        | PostgreSQL | OCI      | Charm        | PostgreSQL | OCI      |                                     |
| revision     | Version    | revision | revision     | Version    | revision |                                     |
+==============+============+==========+==============+============+==========+=====================================+
|    N/A       |            |          |              |            |          |                                     |
+--------------+------------+----------+--------------+------------+----------+-------------------------------------+
```
````
`````

### Supported refreshes

If possible, use a {ref}`recommended refresh <recommended-refreshes>` instead.

`````{tab-set}
````{tab-item} VM
:sync: vm

```{eval-rst}
+------------+------------+----------+------------+------------+----------+
| From                    | To                                            |
+------------+------------+----------+------------+------------+----------+
| Charm      | PostgreSQL | Snap     | Charm      | PostgreSQL | Snap     |
| revision   | Version    | revision | revision   | Version    | revision |
+============+============+==========+============+============+==========+
| 843, 844   | 16.9       | 201, 202 | 951, 952   | 16.10      | 239, 240 |
|            |            |          +------------+------------+----------+
|            |            |          | 989, 990   | 16.11      | 242, 244 |
|            |            |          +------------+------------+----------+
|            |            |          | 1046, 1047 | 16.11      | 242, 244 |
+------------+------------+----------+------------+------------+----------+
| 951, 952   | 16.10      | 239, 240 | 989, 990   | 16.11      | 242, 244 |
|            |            |          +------------+------------+----------+
|            |            |          | 1046, 1047 | 16.11      | 242, 244 |
+------------+------------+----------+------------+------------+----------+
| 989, 990   | 16.11      | 242, 244 | 1046, 1047 | 16.11      | 242, 244 |
+------------+------------+----------+------------+------------+----------+
```
````

````{tab-item} K8s
:sync: k8s

```{eval-rst}
+------------+------------+----------+------------+------------+----------+
| From                    | To                                            |
+------------+------------+----------+------------+------------+----------+
| Charm      | PostgreSQL | OCI      | Charm      | PostgreSQL | OCI      |
| revision   | Version    | revision | revision   | Version    | revision |
+============+============+==========+============+============+==========+
|   N/A      |            |          |            |            |          |
+------------+------------+----------+------------+------------+----------+
```
````
`````

### Unsupported refreshes

These are examples of refreshes that are not supported in-place.
In some of these cases, it may be possible to perform an out-of-place upgrade or downgrade.

* Minor in-place downgrade from PostgreSQL 16.10 to 16.9
* Major in-place upgrade from PostgreSQL 14 to 16
* Major in-place downgrade from PostgreSQL 16 to 14
* Any refresh from or to a non-stable version (e.g. 16/edge)

## Verify your backups

If you do not have a backup, see {ref}`create-a-backup`.

Verify the integrity of the backup by performing a test {ref}`restore on another application <migrate-a-cluster>`.

Check the restored data by ensuring that:
* recent data is present
* the data size is correct
* the data matches what you expected in the backup

## Read the rollback instructions

In the event that something goes wrong (e.g. the refresh fails, the new version of PostgreSQL is not performant enough, a database client is incompatible with the new version), you may want to quickly roll back.

Prepare for this possibility by reading through the entire refresh documentation—with special attention to the {ref}`halt-the-refresh` and {ref}`roll-back` sections—before starting the refresh.

## Review release notes

Review the release notes for every charm version between the version that you are refreshing from and to to understand what changed and if any action is required from you before, during, or after the refresh.

For {ref}`recommended refreshes <recommended-refreshes>`, refer to the rightmost column of the table.

If the PostgreSQL versions that you are refreshing from and to are different, refer to the [upstream PostgreSQL release notes](https://www.postgresql.org/docs/release/) to understand what changed and if any action is required from you.

(test-in-a-staging-environment)=
## Test in a staging environment

We recommend testing the entire refresh procedure in a staging environment before refreshing your production environment.

In a staging environment, we also encourage you to simulate failure of the refresh and to practice recovery by restoring from the {ref}`backup <create-a-backup>`.

## Check that clients are compatible

Ensure that your clients are compatible with the PostgreSQL version that you're refreshing to.

It may be necessary to refresh your clients before refreshing PostgreSQL.

## Inform users and schedule a maintenance window

Tell your users when you will perform the refresh and remain in contact with them so that you are aware of any issues.

If possible, schedule a maintenance window during a period of low traffic.

The duration of the refresh may depend on the size of your data and volume of traffic. To estimate the duration, we recommend {ref}`testing in a staging environment <test-in-a-staging-environment>`.

## Consider scaling up

During the refresh of the application, units will be restarted one by one.
While a unit is restarting, the performance of the cluster will be degraded.

To ensure that the cluster can handle all traffic during the refresh, consider scaling up the application by 1 unit.

```{dropdown} The PostgreSQL charm does not support scaling up while a refresh is in progress.
:open:
:color: warning
:icon: alert
:class-title: sd-font-weight-normal

If you anticipate that the refresh will be in progress for an extended duration (e.g. days, weeks), scale up the application before the refresh so that it can handle the maximum load during that period.
```

(pre-refresh-check)=
## Pre-refresh check

Run the `pre-refresh-check` action on the leader unit to prepare the application for refresh.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader pre-refresh-check
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader pre-refresh-check
```
````

If the action does not succeed, **do not refresh**.

If the action succeeds, copy down the rollback command.

Keep the command available in case you need to {ref}`roll back <roll-back>`.

(configure-pause-after-unit-refresh)=
## Configure `pause-after-unit-refresh`

After each unit is refreshed, the charm will perform automatic health checks.

We recommend supplementing the automatic checks with manual checks.

Examples of manual checks:

* Database clients are healthy and can connect to the refreshed units
* Transactions per second and resource consumption (CPU, memory, disk) are similar on refreshed and non-refreshed units
* Leaving the application in a partially-refreshed state (only some units refreshed) for several weeks and monitoring that the new version is stable in your environment

To facilitate your manual checks, the application can be configured to pause the refresh and wait for your confirmation.

Set the `pause-after-unit-refresh` config option to:

* `all` to wait for your confirmation after each unit refreshes
* `first` (default) to wait for your confirmation once, after the first unit refreshes
* `none` to never wait for your confirmation

For example:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql pause-after-unit-refresh=all
```
```{tab-item} K8s
:sync: k8s

   juju config postgresql-k8s pause-after-unit-refresh=all
```
````

```{dropdown} Automatic pause on health check failure
:open:
:color: info
:icon: info
:class-title: sd-font-weight-normal

If the charm's automatic health checks fail, the refresh will be paused (until those health checks succeed) regardless of the value of the `pause-after-unit-refresh` config option.
```

## Avoid operations while a refresh is in progress

While a refresh is in progress, the application is in a vulnerable state.

These operations are not supported while a refresh is in progress:
* Scaling up the application
* Scaling down the application—unless it is necessary for recovery
* Creating or removing relations
* Creating or restoring a backup (on the Juju application)
* Changes to config values (except `pause-after-unit-refresh`)

## Start the refresh

Use `juju refresh` and specify the charm revision that you are refreshing to.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju refresh postgresql --revision <revision-number>
```
```{tab-item} K8s
:sync: k8s

    juju refresh postgresql-k8s --revision <revision-number>
```
````

(halt-the-refresh)=
## Halt the refresh

If something goes wrong, halt the refresh by running:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql pause-after-unit-refresh=all
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s pause-after-unit-refresh=all
```
````

In the command above, replace `postgresql` with the name of the Juju application.

Next, assess the situation and plan the recovery. Often, the safest recovery path is to {ref}`roll back <roll-back>`.

Consider {ref}`contacting us <contact>` for guidance.

(roll-back)=
## Roll back

If something went wrong, the safest recovery path is often to roll back to the original version.

First, {ref}`halt the refresh <halt-the-refresh>`.

Run the rollback command you copied down earlier in {ref}`pre-refresh-check`.

In most cases, the rollback command is also displayed in the application's status message in `juju status`.

### Resume the rollback

If more than one unit was refreshed before the rollback was started and `pause-after-unit-refresh` is set to `all` or `first`, your manual confirmation will be needed to complete the rollback.
The procedure for the rollback is the same as described in {ref}`monitor-the-refresh.

### Reflect

After the application has been rolled back and you have confirmed that service has been fully restored, investigate what went wrong.

If applicable, please file a {ref}`bug report <contact>`.

Once you understand what went wrong and have tested that it has been fixed, the refresh can be attempted again.

## Monitor the refresh

Use `juju status` to monitor the progress of the refresh.

In some cases, it may take a few minutes for the statuses to update after the refresh has started.

If the application status or any of the unit statuses are `blocked`, your action is required. Follow the instructions in the status messages.

If the application status or any of the unit statuses are `error`, your action may be required. Monitor `juju debug-log`.
The error may have been a temporary issue.
If the error persists, your action is required—consider [rolling back](#roll-back).

Monitor the refresh until it successfully finishes.
When the refresh completes, the application status will go from a message beginning with "Refreshing" to an `active` status with no message.

### Resume refresh

If `pause-after-unit-refresh` is set to `all` or `first` (default), your confirmation will be needed during the refresh.

The application status in `juju status` will instruct you when your confirmation is needed with the `resume-refresh` action.

Before running the `resume-refresh` action:
* Wait until all of the application's unit agent statuses are `idle`
* Wait until all of the refreshed units' workload statuses are `active`
* Perform [manual checks](#configure-pause-after-unit-refresh) to ensure that everything is healthy

Example of running the `resume-refresh` action on unit 1:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/1 resume-refresh
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/1 resume-refresh
```
````
