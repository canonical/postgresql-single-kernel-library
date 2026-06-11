---
myst:
  html_meta:
    description: "Security hardening guide for Charmed PostgreSQL covering cloud environments, Juju security, OS hardening, encryption, authentication, and monitoring."
---

(security-hardening-overview)=
# Security hardening overview
{{vm_k8s}}

This document provides an overview of security features and guidance for hardening the security of Charmed PostgreSQL deployments, including setting up and managing a secure environment.

## Environment

The environment where Charmed PostgreSQL operates can be divided into two components:

1. Cloud
2. Juju

### Cloud

````{tab-set}
```{tab-item} VM
:sync: vm

Charmed PostgreSQL can be deployed on top of several clouds and virtualisation layers:

| Cloud | Security guides |
| ----- | --------------- |
| OpenStack |[OpenStack Security Guide](https://docs.openstack.org/security-guide/)|
| AWS       |[Best Practices for Security, Identity and Compliance](https://aws.amazon.com/architecture/security-identity-compliance), [AWS security credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/security-creds.html)|
| Azure     |[Azure security best practices and patterns](https://learn.microsoft.com/en-us/azure/security/fundamentals/best-practices-and-patterns), [Managed identities for Azure resource](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/)|
| GCP       |[Google security overview](https://cloud.google.com/docs/security)|
```
```{tab-item} K8s
:sync: k8s

Charmed PostgreSQL can be deployed on top of several Kubernetes distributions. The following table provides references for the security documentation for the main supported cloud platforms.

| Cloud | Security guides |
| ----- | --------------- |
| Canonical Kubernetes |[Security overview](https://ubuntu.com/kubernetes/docs/security), [How to secure a cluster](https://ubuntu.com/kubernetes/docs/how-to-security)|
| MicroK8s             |[CIS compliance](https://microk8s.io/docs/cis-compliance), [Cluster hardening guide](https://microk8s.io/docs/how-to-cis-harden)|
| AWS EKS              |[Best Practices for Security, Identity and Compliance](https://aws.amazon.com/architecture/security-identity-compliance), [AWS security credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/security-creds.html), [Security in EKS](https://docs.aws.amazon.com/eks/latest/userguide/security.html)|
| Azure AKS            |[Azure security best practices and patterns](https://learn.microsoft.com/en-us/azure/security/fundamentals/best-practices-and-patterns), [Managed identities for Azure resource](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/), [Security in AKS](https://learn.microsoft.com/en-us/azure/aks/concepts-security)|
| GCP GKE              |[Google security overview](https://cloud.google.com/kubernetes-engine/docs/concepts/security-overview), [Harden your cluster’s security](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster)|
```
````

### Juju

Juju is the component responsible for orchestrating the entire lifecycle, from deployment to Day 2 operations. For more information on Juju security hardening, see the [Juju security page](https://canonical-juju.readthedocs-hosted.com/en/latest/user/explanation/juju-security/) and the [How to harden your deployment](https://documentation.ubuntu.com/juju/latest/howto/manage-your-juju-deployment/harden-your-juju-deployment/#harden-your-deployment) guide.

#### Cloud credentials

````{tab-set}
```{tab-item} VM
:sync: vm

When configuring cloud credentials to be used with Juju, ensure that users have correct permissions to operate at the required level. Juju superusers responsible for bootstrapping and managing controllers require elevated permissions to manage several kinds of resources, such as virtual machines, networks, storage, etc. Please refer to the links below for more information on the policies required to be used depending on the cloud.

| Cloud | Cloud user policies|
| ----- | ------------------ |
| OpenStack |N/A|
| AWS       |[Juju AWS Permission](https://discourse.charmhub.io/t/juju-aws-permissions/5307), [AWS Instance Profiles](https://discourse.charmhub.io/t/using-aws-instance-profiles-with-juju-2-9/5185), [Juju on AWS](https://juju.is/docs/juju/amazon-ec2)|
| Azure     |[Juju Azure Permission](https://juju.is/docs/juju/microsoft-azure), [How to use Juju with Microsoft Azure](https://discourse.charmhub.io/t/how-to-use-juju-with-microsoft-azure/15219)|
| GCP       |[Google Cloud's Identity and Access Management](https://cloud.google.com/iam/docs/overview), [GCE role recommendations](https://cloud.google.com/policy-intelligence/docs/role-recommendations-overview), [Google GCE cloud and Juju](https://canonical-juju.readthedocs-hosted.com/en/latest/user/reference/cloud/list-of-supported-clouds/the-google-gce-cloud-and-juju/)|
```
```{tab-item} K8s
:sync: k8s

When configuring cloud credentials to be used with Juju, ensure that users have the correct permissions to operate at the required level on the Kubernetes cluster. Juju superusers responsible for bootstrapping and managing controllers require elevated permissions to manage several kinds of resources. For this reason, the K8s user for bootstrapping and managing the deployments should have full permissions, such as:

* create, delete, patch, and list:
  * namespaces
  * services
  * deployments
  * stateful sets
  * pods
  * PVCs

In general, it is common practice to run Juju using the admin role of K8s, to have full permissions on the Kubernetes cluster.
```
````

#### Juju users

It is very important that Juju users are set up with minimal permissions depending on the scope of their operations. Please refer to the [User access levels](https://juju.is/docs/juju/user-permissions) documentation for more information on the access levels and corresponding abilities.

Juju user credentials must be stored securely and rotated regularly to limit the chances of unauthorised access due to credentials leakage.

## Applications

In the following sections, we provide guidance on how to harden your deployment through your substrate, security upgrades, encryption, and other best practices.

````{tab-set}
```{tab-item} VM
:sync: vm

### Operating system

Charmed PostgreSQL and Charmed PgBouncer run on top of Ubuntu 22.04. Deploy a [Landscape Client Charm](https://charmhub.io/landscape-client?) to connect the underlying VM to a Landscape User Account to manage security upgrades and integrate [Ubuntu Pro](https://ubuntu.com/pro) subscriptions.

### Security upgrades

[Charmed PostgreSQL](https://charmhub.io/postgresql) and [Charmed PgBouncer](https://charmhub.io/pgbouncer) operators install pinned versions of their respective snaps to provide reproducible and secure environments.

New versions (revisions) of the charmed operators can be released to update the operator's code, workloads, or both. It is important to refresh the charms regularly to make sure the workloads are as secure as possible.

For more information on upgrading Charmed PostgreSQL, see:
* {ref}`How to upgrade PostgreSQL <refresh>`
* [How to upgrade PgBouncer](https://charmhub.io/pgbouncer/docs/h-upgrade)
* {ref}`PostgreSQL release notes <release-notes>`
* [PgBouncer release notes](https://charmhub.io/pgbouncer/docs/r-releases)

### Encryption

To utilise encryption at transit for all internal and external cluster connections, integrate Charmed PostgreSQL with a TLS certificate provider. Please refer to the [Charming Security page](https://charmhub.io/topics/security-with-x-509-certificates) for more information on how to select the right certificate provider for your use case.

Encryption in transit for backups is provided by the storage service (Charmed PostgreSQL is a client for an S3-compatible storage).

For more information on encryption, see {ref}`cryptography` and {ref}`network-and-encryption`.

### Authentication

Charmed PostgreSQL supports the password-based `scram-sha-256` authentication method for authentication between:

* External connections to clients
* Internal connections between members of cluster
* PgBouncer connections

For more implementation details, see the [upstream PostgreSQL documentation](https://www.postgresql.org/docs/16/auth-password.html).

### Monitoring and auditing

Charmed PostgreSQL provides native integration with the [Canonical Observability Stack (COS)](https://charmhub.io/topics/canonical-observability-stack). To reduce the blast radius of infrastructure disruptions, the general recommendation is to deploy COS and the observed application into separate environments, isolated from one another. Refer to the [COS production deployments best practices](https://charmhub.io/topics/canonical-observability-stack/reference/best-practices) for more information, see the how-to guides about {ref}`observability-cos`.

PostgreSQL logs are stored in `/var/snap/charmed-postgresql/common/var/log/postgresql` within the PostgreSQL container of each unit. It’s recommended to integrate the charm with {ref}`COS <enable-monitoring>`, from where the logs can be easily persisted and queried using [Loki](https://charmhub.io/loki-k8s)/[Grafana](https://charmhub.io/grafana).

### Security event logging

Charmed PostgreSQL VM provides [PostgreSQL Audit Extension (or pgAudit)](https://www.pgaudit.org/) enabled by default. These logs are stored in the `/var/snap/charmed-postgresql/common/var/log/postgresql` directory of each unit along with the regular workload logs, and rotated minutely. If COS is enabled, audit logs are also persisted there.

The following information is configured to be logged:

* Statements related to roles and privileges, such as GRANT, REVOKE, CREATE, ALTER, and DROP ROLE.
* Data Definition Language (DDL) statements.
* Miscellaneous commands like DISCARD, FETCH, CHECKPOINT, VACUUM, SET.
* Miscellaneous SET commands.

Other events, like connections and disconnections, are logged depending on the value of the charm configuration options related to them. For more information, check the configuration options with the `logging` prefix in the [configuration reference](https://charmhub.io/postgresql/configurations#logging_log_connections).

Also, all operations performed by the charm as a result of user actions — such as enabling or disabling plugins, managing TLS, creating or restoring backups, and configuring replication between clusters (asynchronous or logical) — are executed through the underlying workload components (PostgreSQL, Patroni, or pgBackRest). Consequently, these operations are recorded in the respective workload log files, which are accessible in the directories below and also forwarded to COS:

* `/var/snap/charmed-postgresql/common/var/log/patroni`
* `/var/snap/charmed-postgresql/common/var/log/pgbackrest`
* `/var/snap/charmed-postgresql/common/var/log/postgresql`

No secrets are logged.

```
```{tab-item} K8s
:sync: k8s

### Base images

Charmed PostgreSQL K8s and Charmed PgBouncer K8s run on top of rockcraft-based images shipping the PostgreSQL and PgBouncer distribution binaries built by Canonical. These images (rocks) are available in a GitHub registry for [PostgreSQL](https://github.com/canonical/charmed-postgresql-rock/pkgs/container/charmed-postgresql) and [PgBouncer](https://github.com/orgs/canonical/packages/container/package/charmed-pgbouncer) respectively. Both images are based on Ubuntu 22.04.

### Charmed operator security upgrades

[Charmed PostgreSQL K8s](https://charmhub.io/postgresql-k8s) operator and [Charmed PgBouncer K8s](https://charmhub.io/pgbouncer-k8s) operator install pinned versions of their respective rocks to provide reproducible and secure environments.

New versions (revisions) of the charmed operators can be released to update the operator's code, workloads, or both. It is important to refresh the charms regularly to make sure the workloads are as secure as possible.

For more information on upgrading Charmed PostgreSQL K8s, see:

* {ref}`How to upgrade PostgreSQL <refresh>`
* [How to upgrade PgBouncer](https://charmhub.io/pgbouncer-k8s/docs/h-upgrade)
* {ref}`PostgreSQL release notes <release-notes>`
* [PgBouncer release notes](https://charmhub.io/pgbouncer/docs-k8s/r-releases)

### Encryption

To utilise encryption at transit for all internal and external cluster connections, integrate Charmed PostgreSQL K8s and Charmed PgBouncer K8s with a TLS certificate provider. Please refer to the [Charming Security page](https://charmhub.io/topics/security-with-x-509-certificates) for more information on how to select the right certificate provider for your use case.

Encryption in transit for backups is provided by the storage service (Charmed PostgreSQL K8s is a client for an S3-compatible storage).

For more information on encryption, see {ref}`cryptography` and {ref}`enable-tls`.

### Authentication

Charmed PostgreSQL K8s supports the password-based `scram-sha-256` authentication method for authentication between:

* External connections to clients
* Internal connections between members of cluster
* PgBouncer connections

For more implementation details, see the [PostgreSQL documentation](https://www.postgresql.org/docs/14/auth-password.html).

### Monitoring and auditing

Charmed PostgreSQL K8s provides native integration with the [Canonical Observability Stack (COS)](https://charmhub.io/topics/canonical-observability-stack). To reduce the blast radius of infrastructure disruptions, the general recommendation is to deploy COS and the observed application into separate environments, isolated from one another. Refer to the [COS production deployments best practices](https://charmhub.io/topics/canonical-observability-stack/reference/best-practices) for more information, see the how-to guides about {ref}`observability-cos`.

PostgreSQL logs are stored in `/var/lib/pg/logs/16/main/pg_logs` within the `postgresql` container of each unit. The legacy `/var/log/postgresql` path is kept as a compatibility symlink to that directory. It’s recommended to integrate the charm with {ref}`COS <enable-monitoring>, from where the logs can be easily persisted and queried using [Loki](https://charmhub.io/loki-k8s)/[Grafana](https://charmhub.io/grafana).

### Security event logging

Charmed PostgreSQL K8s provides [PostgreSQL Audit Extension (or pgAudit)](https://www.pgaudit.org/) enabled by default. These logs are stored in `/var/lib/pg/logs/16/main/pg_logs` along with the regular PostgreSQL workload logs, and rotated minutely. If COS is enabled, audit logs are also persisted there.

The following information is configured to be logged:

* Statements related to roles and privileges, such as GRANT, REVOKE, CREATE, ALTER, and DROP ROLE.
* Data Definition Language (DDL) statements.
* Miscellaneous commands like DISCARD, FETCH, CHECKPOINT, VACUUM, SET.
* Miscellaneous SET commands.

Other events, like connections and disconnections, are logged depending on the value of the charm configuration options related to them. For more information, check the configuration options with the `logging` prefix in the [configuration reference](https://charmhub.io/postgresql-k8s/configurations?channel=14/stable#logging-log-connections).

Also, all operations performed by the charm as a result of user actions — such as enabling or disabling plugins, managing TLS, creating or restoring backups, and configuring replication between clusters (asynchronous or logical) — are executed through the underlying workload components (PostgreSQL, Patroni, or pgBackRest). Consequently, these operations are recorded in the respective workload log files under `/var/lib/pg/logs/16/main/` and also forwarded to COS.

No secrets are logged.
```
````

