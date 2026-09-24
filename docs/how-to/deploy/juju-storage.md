---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL with Juju storage pools to configure archive, data, logs, and temp storage volumes on supported clouds."
---

(juju-storage)=
# How to deploy on Juju storage
{{vm}} <!--TODO: create K8s docs -->

Charmed PostgreSQL uses the [Juju storage](https://documentation.ubuntu.com/juju/3.6/reference/storage/) abstraction to utilise data volume provided by different [clouds](https://documentation.ubuntu.com/juju/3.6/reference/cloud/#cloud) while keeping the same UI/UX for end users.

Charmed PostgreSQL 14 supports a single storage: `pgdata`. 

The {ref}`legacy PostgreSQL charm <charm-versions>` does **not** support the Juju storage abstraction.

## Prerequisites

If you are deploying with Terraform, note that the Juju Terraform Provider introduced storage support in version [0.13](https://github.com/juju/terraform-provider-juju/releases/tag/v0.13.0)

---

## Check Juju storage details

Check the [metadata.yaml](https://github.com/canonical/postgresql-operator/blob/main/metadata.yaml) for find Juju storage name and tech details:

````{dropdown} Charmed PostgreSQL 14 storage list
:open:
:color: light
:icon: list-unordered
:class-title: sd-font-weight-normal
:class-body: sd-py-0

```
storage:
  pgdata:
    type: filesystem
    location: /var/snap/charmed-postgresql/common
```
````

Charmed PostgreSQL 14 supports single storage `pgdata` attaching it on `juju deploy` and mounted inside the Snap common folder `/var/snap/charmed-postgresql/common`.

<details><summary>Example</summary>

```text
$ juju deploy postgresql --channel 14/stable

$ juju storage
Unit          Storage ID  Type        Size  Status   Message
postgresql/0  pgdata/0    filesystem        pending  

$ juju storage
Unit          Storage ID  Type        Pool    Size    Status    Message
postgresql/0  pgdata/0    filesystem  rootfs  97 GiB  attached  

$ juju show-storage pgdata/0 
pgdata/0:
  kind: filesystem
  life: alive
  status:
    current: attached
    since: 01 May 2025 18:47:04+02:00
  persistent: false
  attachments:
    units:
      postgresql/0:
        machine: "0"
        location: /var/snap/charmed-postgresql/common
        life: alive

$ juju ssh postgresql/0 mount | grep /var/snap/charmed-postgresql/common
/dev/sda1 on /var/snap/charmed-postgresql/common type ext4 (rw,relatime,discard,errors=remount-ro)
```
</details>


## Define storage size

```shell
$ juju deploy postgresql --channel 14/stable --storage pgdata=10G

$ juju storage
Unit          Storage ID  Type        Pool  Size    Status    Message
postgresql/1  pgdata/1    filesystem  lxd   10 GiB  attached
```

## Define storage location

Juju supports wide list of different [storage pools](https://bobcares.com/blog/lxd-create-storage-pool/):

```shell
$ juju create-storage-pool mystoragepool lxd

$ juju storage-pools | grep mystoragepool
mystoragepool  lxd

$ juju deploy postgresql --channel 14/stable --storage pgdata=5G,mystoragepool

$ juju storage
Unit          Storage ID  Type        Pool           Size    Status    Message
postgresql/2  pgdata/2    filesystem  mystoragepool  5 GiB   attached
```

## Example: Re-deploy detached storage

To re-deploy the application with the old Juju storage, it is necessary to provide all charm/database  credentials to the new charm or app. 

Charmed PostgreSQL 14 uses the Juju action `set-password` to handle credentials:

```shell
# Note old passwords
$ juju show-secret --reveal database-peers.postgresql.app

# Find old storage id:
$ juju storage

# Re-deploy new app one unit only to set old passwords
$ juju deploy postgresql --channel 14/stable -n 1

# Once deployed, set old passwords from old app
$ juju run postgresql/leader set-password username=operator password=cpUJnNRJ6Qt2Hgli 
$ juju run postgresql/leader set-password username=patroni password=0N7pKAutKCstPuvx
$ juju run postgresql/leader set-password username=raft password=8xSZvTLfyHpglGfI
$ juju run postgresql/leader set-password username=replication password=5xN9gj9uu5Um3PWB 
$ juju run postgresql/leader set-password username=rewind password=wuEsPmsdpc6L8qhT 
$ juju run postgresql/leader set-password username=monitoring password=AgOxXzcRD5iohE6C

# Scale down to zero units by removing all Juju units
$ juju remove-unit postgresql/1 --destroy-storage=yes

# Scale up to one unit and attach the old Juju storage
$ juju add-unit postgresql --attach-storage pgdata/x

# If no issues noticed, scale to HA (3 units total)
$ juju add-unit postgresql -n 2
```

