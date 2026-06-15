---
myst:
  html_meta:
    description: "Configure the s3-integrator charm for AWS S3 to enable Charmed PostgreSQL backups, including credentials, bucket setup, and application integration."
---

(configure-s3-aws)=
# Configure S3 for AWS
{{vm_k8s}}

A Charmed PostgreSQL backup can be stored on any S3-compatible storage. S3 access and configurations are managed with the [s3-integrator charm](https://charmhub.io/s3-integrator).

This guide will teach you how to deploy and configure the s3-integrator charm for [AWS S3](https://aws.amazon.com/s3/), send the configurations to the Charmed PostgreSQL application, and update it.

{{seealso}} {ref}`configure-s3-radosgw`.

```{dropdown} pgBackRest limitations
:open:
:class-container: dropdown-caution
:icon: alert-fill

The backup tool [pgBackRest](https://pgbackrest.org/) can only interact with S3-compatible storage if they work with [SSL/TLS](https://github.com/pgbackrest/pgbackrest/issues/2340).

Backup via the plain HTTP is currently not supported.
```

## Set up `s3-integrator`

Deploy and configure the `s3-integrator` charm for AWS S3:

```shell
juju deploy s3-integrator --channel=1/stable
juju run s3-integrator/leader sync-s3-credentials access-key=<access-key-here> secret-key=<secret-key-here>

juju config s3-integrator \
    endpoint="https://s3.us-west-2.amazonaws.com" \
    bucket="postgresql-test-bucket-1" \
    path="/postgresql-test" \
    region="us-west-2"
```

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

There is an experimental configuration option that sets up a retention time (in days) for backups stored in S3: [`experimental-delete-older-than-days`](https://charmhub.io/s3-integrator/configuration?channel=latest/edge#experimental-delete-older-than-days). 

See: {ref}`manage-backup-retention`.

```{dropdown} The S3 endpoint must be specified as <code>s3.\<region\>.amazonaws.com</code> within the **first 24 hours** of creating the bucket.
:class-container: dropdown-caution
:icon: alert-fill
:class-title: sd-font-weight-normal

For older buckets, the endpoint `s3.amazonaws.com` can be used.

See [this post in the AWS forum](https://repost.aws/knowledge-center/s3-http-307-response) for more information.
```

## Integrate with Charmed PostgreSQL

To pass these configurations to Charmed PostgreSQL, integrate the two applications:

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

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju integrate` becomes `juju relate` for Juju 2.9.
```

You can create, list, and restore backups now:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader list-backups
    juju run postgresql/leader create-backup
    juju run postgresql/leader list-backups
    juju run postgresql/leader restore backup-id=<backup-id>
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader list-backups
    juju run postgresql-k8s/leader create-backup
    juju run postgresql-k8s/leader list-backups
    juju run postgresql-k8s/leader restore backup-id=<backup-id>
```
````

You can also update your S3 configuration options after relating:

```shell
juju config s3-integrator <option>=<value>
```

See the [s3-integrator charm on Charmhub](https://charmhub.io/s3-integrator/configure) for a list of all its configuration parameters.
