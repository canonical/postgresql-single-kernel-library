---
myst:
  html_meta:
    description: "Enable the TimescaleDB extension in Charmed PostgreSQL for time-series data using the juju config plugin-timescaledb-enable option."
---

(enable-timescaledb)=
# How to enable TimescaleDB
{{vm_k8s}}
<!-- TODO: see ticket about K8s bug -->

Charmed PostgreSQL separates TimescaleDB editions for different [Charmhub tracks](https://canonical-charmcraft.readthedocs-hosted.com/en/stable/howto/manage-channels/):

[Charmed PostgreSQL 16](https://charmhub.io/postgresql?channel=16/stable) ships [Timescale Community edition](https://docs.timescale.com/about/latest/timescaledb-editions/).

## Enable TimescaleDB

````{tab-set}
```{tab-item} VM
:sync: vm

To enable TimescaleDB plugin/extension simply run:

	juju config postgresql plugin-timescaledb-enable=true

The plugin has been enabled on all units once the config-change event finished and all units reports idle:

	$ juju status
	...
	Unit           Workload  Agent      Machine  Public address  Ports     Message
	postgresql/3*  active    executing  3        10.189.210.124  5432/tcp  (config-changed) Primary
	postgresql/5   active    executing  5        10.189.210.166  5432/tcp  (config-changed)
	postgresql/6   active    executing  6        10.189.210.150  5432/tcp  (config-changed)
	...
	Unit           Workload  Agent  Machine  Public address  Ports     Message
	postgresql/3*  active    idle   3        10.189.210.124  5432/tcp  Primary
	postgresql/5   active    idle   5        10.189.210.166  5432/tcp
	postgresql/6   active    idle   6        10.189.210.150  5432/tcp
	...
```
```{tab-item} K8s
:sync: k8s

To enable TimescaleDB plugin/extension simply run:

	juju config postgresql-k8s plugin-timescaledb-enable=true

The plugin has been enabled on all units once the config-change event finished and all units reports idle:

	$ juju status
	...
	Unit               Workload  Agent      Public address  Ports     Message
	postgresql-k8s/0*  active    executing  10.1.142.171		      (config-changed) Primary
	postgresql-k8s/1   active    executing  10.1.142.169		      (config-changed)
	postgresql-k8s/2   active    executing  10.1.142.170		      (config-changed)
	...
	Unit               Workload  Agent  Public address  Ports     Message
	postgresql-k8s/0*  active    idle   10.1.142.171              Primary
	postgresql-k8s/1   active    idle   10.1.142.169
	postgresql-k8s/2   active    idle   10.1.142.170
	...
```
````

## Disable TimescaleDB

To disable it explicitly, simply run:

````{tab-set}
```{tab-item} VM
:sync: vm

	juju config postgresql plugin-timescaledb-enable=false

```
```{tab-item} K8s
:sync: k8s

	juju config postgresql-k8s plugin-timescaledb-enable=false
```
````

The plugin has been disabled on all units once the config-change event finishes and all units reports idle.

The extension will NOT be disable when database objects uses/depends on plugin is being disabled (clean the database to disable the plugin).

For example:

```shell
$ juju status
...
Unit           Workload  Agent  Machine  Public address  Ports     Message
postgresql/3*  blocked   idle   3        10.189.210.124  5432/tcp  Cannot disable plugins: Existing objects depend on it. See logs
...
```

Another option is to reset the manually enabled config option (as it is disabled by default):

````{tab-set}
```{tab-item} VM
:sync: vm

	juju config postgresql --reset plugin-timescaledb-enable

```
```{tab-item} K8s
:sync: k8s

	juju config postgresql-k8s --reset plugin-timescaledb-enable
```
````

## Test TimescaleDB status

Prepare the `user_defined_action` procedure:

```shell
postgres=# CREATE OR REPLACE PROCEDURE user_defined_action(job_id int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  RAISE NOTICE 'Executing action % with config %', job_id, config;
END
$$;
```
