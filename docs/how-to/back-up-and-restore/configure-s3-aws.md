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

## Set up `s3-integrator`

Deploy and configure the `s3-integrator` charm for AWS S3:

<!--TODO: Check if s3-integrator needs juju secrets-->
```shell
juju deploy s3-integrator
juju run s3-integrator/leader sync-s3-credentials access-key=<access-key-here> secret-key=<secret-key-here>

juju config s3-integrator \
    endpoint="https://s3.us-west-2.amazonaws.com" \
    bucket="postgresql-test-bucket-1" \
    path="/postgresql-test" \
    region="us-west-2"
```

There is an experimental configuration option that sets up a retention time (in days) for backups stored in S3: [`experimental-delete-older-than-days`](https://charmhub.io/s3-integrator/configuration?channel=latest/edge#experimental-delete-older-than-days). See: {ref}`manage-backup-retention`.


```{dropdown} The S3 endpoint must be specified as <code>s3.\<region\>.amazonaws.com</code> within the **first 24 hours** of creating the bucket.
:open:
:color: warning
:icon: alert
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
