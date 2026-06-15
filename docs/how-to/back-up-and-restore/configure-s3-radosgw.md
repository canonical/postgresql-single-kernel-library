---
myst:
  html_meta:
    description: "Configure the s3-integrator charm to use Ceph RadosGW S3-compatible storage for Charmed PostgreSQL backups using the MinIO client."
---

(configure-s3-radosgw)=
# Configure S3 for RadosGW
{{vm_k8s}}

A PostgreSQL backup can be stored on any S3-compatible storage. S3 access and configurations are managed with the [s3-integrator charm](https://charmhub.io/s3-integrator).

This guide will teach you how to deploy and configure the s3-integrator charm on Ceph via [RadosGW](https://docs.ceph.com/en/quincy/man/8/radosgw/), send the configuration to a Charmed PostgreSQL application, and update it.

{{seealso}} {ref}`configure-s3-aws`

```{dropdown} pgBackRest limitations
:open:
:class-container: dropdown-caution
:icon: alert-fill
:class-title: sd-font-weight-normal

The backup tool [pgBackRest](https://pgbackrest.org/) can only interact with S3-compatible storage if they work with [SSL/TLS](https://github.com/pgbackrest/pgbackrest/issues/2340).

Backup via the plain HTTP is currently not supported.
```

## Configure `s3-integrator`

First, install the MinIO client and create a bucket:

```shell
mc config host add dest https://radosgw.mycompany.fqdn <access-key> <secret-key> --api S3v4 --lookup path
mc mb dest/backups-bucket
```

Then, deploy and run the charm:

```shell
juju deploy s3-integrator --channel=1/stable
juju run s3-integrator/leader sync-s3-credentials access-key=<access-key> secret-key=<secret-key>
```

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

Lastly, use `juju config` to add your configuration parameters. For example:

```shell
juju config s3-integrator \
    endpoint="https://radosgw.mycompany.fqdn" \
    bucket="backups-bucket" \
    path="/postgresql" \
    region="" \
    s3-api-version="" \
    s3-uri-style="path" \
    tls-ca-chain="$(base64 -w0 /path-to-your-server-ca-file)"
```

## Integrate with Charmed PostgreSQL

```{include} configure-s3-aws.md
:start-after: "## Integrate with Charmed PostgreSQL"
```