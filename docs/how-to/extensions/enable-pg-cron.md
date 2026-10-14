---
myst:
  html_meta:
    description: "Enable pg_cron in Charmed PostgreSQL and schedule SQL jobs in PostgreSQL databases."
---

(enable-pg-cron)=
# How to enable pg_cron
{{vm_k8s}}

The [pg_cron extension](https://github.com/citusdata/pg_cron) runs SQL jobs on a schedule. It is disabled by default.

## Enable pg_cron

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql plugin-pg-cron-enable=true
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s plugin-pg-cron-enable=true
```
````

Wait for all units to show `active` and `idle` in `juju status`.

The charm installs pg_cron only in the `postgres` database. Jobs use background workers and schedules use GMT. The charm does not grant application users permission to schedule jobs.

pg_cron shares worker slots with other extensions, parallel queries, and logical replication. Small deployments may need a higher `cpu-max-worker-processes` value to accommodate the scheduler, concurrent jobs, and other workers. Enabling pg_cron does not increase this limit automatically.

## Connect to PostgreSQL

Find the secret labelled `database-peers.postgresql.app` (VM) or `database-peers.postgresql-k8s.app` (K8s):

```shell
juju secrets --format=yaml
```

Reveal the secret and use its `operator-password` value when `psql` prompts for a password. Replace `<secret-id>` with the secret's ID:

```shell
juju show-secret <secret-id> --reveal
```

Find the PostgreSQL primary and its address. Replace `<primary-unit>` with the unit returned by `get-primary`, then open its shell:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader get-primary
    juju status postgresql
    juju ssh <primary-unit> bash
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader get-primary
    juju status postgresql-k8s
    juju ssh --container postgresql <primary-unit> bash
```
````

In that shell, connect to `postgres` as `operator`. Replace `<primary-address>` with the primary unit's address from `juju status`:

```shell
psql --host=<primary-address> --username=operator --password postgres
```

## Schedule and inspect jobs

For example, to record a timestamp every minute:

```sql
CREATE TABLE public.cron_example (run_at timestamptz DEFAULT now());
SELECT cron.schedule(
    'record-time', '* * * * *',
    'INSERT INTO public.cron_example DEFAULT VALUES'
);
```

After a minute, check the table and job history:

```sql
SELECT * FROM public.cron_example;
SELECT jobid, jobname, schedule, database, active FROM cron.job;
SELECT jobid, status, return_message, start_time
FROM cron.job_run_details ORDER BY start_time DESC LIMIT 10;
```

To target an existing application database, call `cron.schedule_in_database()` from `postgres`. Replace `application_db` with the database name:

```sql
SELECT cron.schedule_in_database(
    'vacuum-application', '0 3 * * *', 'VACUUM', 'application_db'
);
```

Jobs run with the scheduling user's permissions. Job definitions are replicated, and scheduling resumes on the new primary after a switchover. Standbys do not execute jobs.

## Pause or remove a job

To pause a job while keeping its definition and history:

```sql
SELECT cron.alter_job(
    (SELECT jobid FROM cron.job WHERE jobname = 'record-time'), active := false
);
```

Use `active := true` to resume it.

To remove its schedule:

```sql
SELECT cron.unschedule('record-time');
```

## Disable pg_cron

Disabling pg_cron drops the extension, including all job definitions and run history. Re-enabling it does not restore them. Pause jobs instead if you need to keep this data.

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql plugin-pg-cron-enable=false
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s plugin-pg-cron-enable=false
```
````

Wait for all units to show `active` and `idle`. If database objects depend on the extension, the charm reports `blocked`; remove those dependencies before disabling it.
