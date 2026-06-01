---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL in an airgapped environment without internet access using Juju 3 and locally available resources from offline stores."
---

(air-gapped)=
# Deploy in an offline or air-gapped environment
{{vm}} {{k8s}}

An air-gapped environment refers to a system that does not have access to the public internet.
This guide goes through the special configuration steps for installing Charmed PostgreSQL VM in an air-gapped environment.

## Requirements

Canonical does not prescribe how you should set up your specific air-gapped environment. However, it is assumed that it meets the following conditions:

* DNS is configured to the local nameservers.
* Juju 3
* [Juju is configured](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#configure-juju) to use local airgapped services.
* The [`store-admin`](https://snapcraft.io/store-admin) tool is installed and configured.
* [Air-gapped CharmHub](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub) is installed and running.
* Local APT and LXD Images caches are reachable.

On machines:
* A VM/hardware resources are available for Juju

On Kubernetes:
* A K8s cluster is running
* An air-gapped container registry (such as [Artifactory](https://jfrog.com/artifactory/)) is reachable from the K8s cluster over HTTPS
  *  **Note**: Secure (HTTPS) OCI access is important, otherwise Juju won’t work!

## Summary of an airgapped setup

The general steps for setting up an airgapped deployment for charms are as follows:
1. Export the necessary artifacts: charms, snaps, or OCI resources
2. Transfer the binary blobs into your airgapped environment
3. Import or upload the artifacts into their corresponding store or registry in the airgapped environment
4. Deploy PostgreSQL

## Export artifacts from online stores

Exporting charms, snaps, and OCI resources are currently independent processes. The [`store-admin`](https://snapcraft.io/store-admin) tool is designed to simplify the process.

### Export snaps (VM only)

Machine charms usually require snaps (and some manually pin them).

To learn how to manually exports snaps, follow the official Enterprise Store documentation:

* [Offline Charmhub > Export snap resources](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#export-snap-resources).

```{caution}
Always use the `snap.yaml` and `bundle.yaml` from the same Git commit!

The [`snap.yaml`](https://github.com/canonical/postgresql-bundle/blob/main/releases/latest/) shipped by the PostgreSQL bundle is mapped to the published [`bundle.yaml`](https://github.com/canonical/postgresql-bundle/blob/main/releases/latest/).
```

For example:

```shell
store-admin export snaps --from-yaml snaps.yaml
```

### Export OCI images (K8s only)

Follow the official Enterprise Store documentation:

* [Offline Charmhub > Export OCI images](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#export-oci-images).

### Export charms

The necessary charm(s) can be exported as bundle or independently (charm-by-charm). See the Snap Proxy documentation:

* [Offline Charmhub > Export charm bundle](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#export-charm-bundles)
* [Offline Charmhub > Export charms](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#export-charms)


For example, to export all charms in the PostgreSQL 14 bundle: <!--TODO: clarify bundle for PG 16? -->

````{tab-set}
```{tab-item} VM
:sync: vm

    store-admin export bundle postgresql-bundle --channel=14/edge --series=jammy --arch=amd64
```
```{tab-item} K8s
:sync: k8s

    store-admin export bundle postgresql-k8s-bundle --channel=14/edge --series=jammy --arch=amd64
```
````

### Transfer the binary blobs

Transfer the binary blobs using the way of your choice into the air-gapped environment.

For example:

````{tab-set}
```{tab-item} VM
:sync: vm
    cp /home/ubuntu/snap/store-admin/common/export/*.tar.gz /media/usb/
    ...
    cp /media/usb/*.tar.gz /var/snap/snap-store-proxy/common/<charms-to-push>/
```

```{tab-item} K8s
:sync: k8s
    cp /home/ubuntu/snap/store-admin/common/export/postgresql-k8s-bundle-20241003T104903.tar.gz /media/usb/
    ...
    cp /media/usb/postgresql-k8s-bundle-20241003T104903.tar.gz /var/snap/snap-store-proxy/common/<charms-to-push>/
```
````

```{tip}
Always verify the [checksum](https://en.wikipedia.org/wiki/Checksum) for the transferred blobs!
```

## Import artifacts to airgapped stores

Artifacts must now be uploaded into their corresponding stores in the airgapped environment.

### Import snaps (VM only)

When importing machine charms that depend on a snap for functionality, **you must first manually import the required snap**.

See:

* [Operate offline > Importing (pushing) snaps](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap/#importing-pushing-snaps).

For example:

```shell
sudo snap-store-proxy push-snap /var/snap/snap-store-proxy/common/snaps-to-push/charmed-postgresql-20241008T082122.tar.gz
```

### Import OCI images (K8s only)

Before importing Kubernetes charms, ensure that the corresponding OCI image is copied to the local registry, maintaining its original path.

### Import charms

Upload the charm blobs into the local airgapped Charmhub. See:
* [Offline Charmhub > Import Packages](https://documentation.ubuntu.com/enterprise-store/main/how-to/airgap-charmhub/#import-packages)

````{tab-set}
```{tab-item} VM
:sync: vm

    sudo snap-store-proxy push-charm-bundle /var/snap/snap-store-proxy/common/<charms-to-push>/postgresql-bundle-20241003T104903.tar.gz
```
```{tab-item} K8s
:sync: k8s

    sudo snap-store-proxy push-charm-bundle /var/snap/snap-store-proxy/common/<charms-to-push>/postgresql-k8s-bundle-20241003T104903.tar.gz
```
````

```{tip}
When re-importing charms or importing other revisions, make sure to provide the `--push-channel-map`.
```

## Deploy PostgreSQL

Deploy and operate Juju charms normally:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju deploy postgresql --channel 16/stable
```

```{tab-item} K8s
:sync: k8s

    juju deploy postgresql-k8s --channel 16/stable --trust
```
````

```{caution}
All the charm, snap, and OCI revisions deployed in the airgapped environment must match the official Charmhub and Snap Store revisions.

Use the official {ref}`release notes <release-notes>` as a reference.
```

## Additional resources

* [Enterprise Store documentation](https://documentation.ubuntu.com/enterprise-store/main/) (formerly Snap Store Proxy)
* [Installing Charmed Kubernetes offline](https://ubuntu.com/kubernetes/docs/install-offline)
* [Wikipedia > Air gap (networking)](https://en.wikipedia.org/wiki/Air_gap_(networking))

