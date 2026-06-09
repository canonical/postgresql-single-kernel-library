---
myst:
  html_meta:
    description: "Manage Charmed PostgreSQL backups with a retention policy."
---

(manage-backup-retention)=
# Manage backup retention
{{vm_k8s}}

Charmed PostgreSQL backups can be managed via a retention policy. This retention can be set by the user in the form of a configuration parameter in the charm [`s3-integrator`](https://charmhub.io/s3-integrator) via the config option  [`experimental-delete-older-than-days`](https://charmhub.io/s3-integrator/configuration?channel=latest/edge#experimental-delete-older-than-days).

This guide will teach you how to set this configuration and how it works in managing existing backups.

```{caution}
This is an experimental parameter; use it with caution.
```

## Configure S3-integrator charm

Deploy and run the `s3-integrator` charm:

```shell
juju deploy s3-integrator
juju run s3-integrator/leader sync-s3-credentials access-key=<access-key-here> secret-key=<secret-key-here>
```

Then, use `juju config` to add the desired retention time in days:

```shell
juju config s3-integrator experimental-delete-older-than-days=<number-of-days>
```

To pass these configurations to a Charmed PostgreSQL application, integrate the two applications:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate s3-integrator postgresql
```

```{tab-item} K8s
:sync: k8s

    juju integrate s3-integrator postgresql-k8s
```
````

To remove this option at any time, the configuration can be erased from the charm:

```shell
juju config s3-integrator --reset experimental-delete-older-than-days
```

This configuration will be enforced in every Charmed PostgreSQL application that is related to the configured S3-integrator charm

```{dropdown} Backups older than retention time will only expire once a newer backup is created
:open:
:class-container: dropdown-note
:icon: info
:class-title: sd-font-weight-normal

The retention is **not** enforced automatically once a backup is older than the set amount of days. This behaviour prevents complete backup deletion if there has been no newer backups created in the charm.
```

See the [s3-integrator charm on Charmhub](https://charmhub.io/s3-integrator/configure) for a list of all its configuration parameters.

