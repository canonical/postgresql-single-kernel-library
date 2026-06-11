---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL on Google Kubernetes Engine (GKE) using Juju, with step-by-step Google Cloud CLI and kubectl configuration."
---

(gke)=
# How to deploy on GKE
{{k8s}}

[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine?hl=en) (GKE) is a highly scalable and fully automated Kubernetes service.

{octicon}`browser` GKE web interface: [console.cloud.google.com/compute](https://console.cloud.google.com/compute)

## Prerequisites

* A physical or virtual machine running Ubuntu 22.04+
* Juju 3.6+ installed via snap

---

## Install GKE tooling

Install the Google Cloud command-line tools via snap:

```{terminal}
:copy:

sudo snap install google-cloud-cli --classic
```

Install the [`kubectl` CLI tools](https://kubernetes.io/docs/tasks/tools/) via snap:

```{terminal}
:copy:

sudo snap install kubectl --classic
```

To check they are correctly installed, run

```{terminal}
:copy:

gcloud --version

Google Cloud SDK 474.0.0
...
```
```{terminal}
:copy:

kubectl version --client

Client Version: v1.28.2
Kustomize Version: v5.0.4-0.20230601165947-6ce0bf390ce3
```

### Authenticate

Log in to Google Cloud:

```{terminal}
:copy:

gcloud auth login
```
This should open a page in your browser starting with  `https://accounts.google.com/o/oauth2/...` where you can complete the login.

If successful, the command prompt will show:

```text
You are now logged in as [<account>@gmail.com].
```

### Configure project ID

Next, you must associate this installation with GCloud project using "Project ID" from [resource-management](https://console.cloud.google.com/cloud-resource-manager):

```{terminal}
:copy:

gcloud config set project <PROJECT_ID>

Updated property [core/project].
```

### Install additional auth plugin

As a last step, install the Debian package `google-cloud-sdk-gke-gcloud-auth-plugin` using this [official Google Cloud documentation](https://cloud.google.com/sdk/docs/install#deb).

## Create a new GKE cluster

The following command will start three [compute engines](https://cloud.google.com/compute/) on Google Cloud and deploy a K8s cluster. You can imagine the compute engines as three physical servers in clouds.

```{terminal}
:copy:

gcloud container clusters create --zone <region-name>-c $USER-$RANDOM --cluster-version 1.25 --machine-type <compute-engine> --num-nodes=3 --no-enable-autoupgrade
```

Next, assign your account as an admin of the newly created K8s cluster:

```{terminal}
:copy:

kubectl create clusterrolebinding cluster-admin-binding-$USER --clusterrole=cluster-admin --user=$(gcloud config get-value core/account)
```

## Bootstrap Juju on GKE

Add a Juju K8s cloud:

```{terminal}
:copy:

/snap/juju/current/bin/juju add-k8s <k8s-cloud-name> --storage=standard --client
```

```{dropdown} K8s credentials on Juju
:open:
:class-container: dropdown-note
:icon: info
:class-title: sd-font-weight-normal

[This known issue](https://bugs.launchpad.net/juju/+bug/2007575) forces non-snap Juju usage to add-k8s credentials on Juju.
```

Bootstrap a Juju controller:

```{terminal}
:copy:

juju bootstrap <controller-name>
```
{{seealso}} [Juju | Google GKE and Juju](https://documentation.ubuntu.com/juju/3.6/reference/cloud/list-of-supported-clouds/the-google-gke-cloud-and-juju/)

## Deploy charms

Create a Juju model (K8s namespace):

```{terminal}
:copy:

juju add-model <model-name>
```

At this stage, Juju is ready to use GKE. Check the list of currently running K8s pods with:

```{terminal}
:copy:

kubectl get pods -n <model-name>
```

The following commands deploy PostgreSQL and PgBouncer:

```{terminal}
:copy:

juju deploy postgresql-k8s --channel 14/stable --trust
```
```{terminal}
:copy:

juju deploy pgbouncer-k8s --trust
```

### Display deployment information

To list GKE clusters:

```{terminal}
:copy:

gcloud container clusters list

>NAME          LOCATION         MASTER_VERSION   MASTER_IP      MACHINE_TYPE      NODE_VERSION     >NUM_NODES  STATUS
>mykola-18187  <region-name>-c  1.25.9-gke.2300  31.210.22.127  <compute-engine>  1.25.9-gke.2300  3          >RUNNING
>taurus-7485   <region-name>-c  1.25.9-gke.2300  142.142.21.25  <compute-engine>  1.25.9-gke.2300  3          >RUNNING
```

Juju can handle multiple clouds simultaneously. To see a list of clouds with registered credentials on Juju, run:

```{terminal}
:copy:

juju clouds

>Clouds available on the controller:
>Cloud      Regions  Default       Type
><k8s-cloud-name>  1        <region-name>  k8s
>
>Clouds available on the client:
>Cloud           Regions  Default       Type  Credentials  Source    Description
><k8s-cloud-name>       1        <region-name>  k8s   1            local     A Kubernetes Cluster
>localhost       1        localhost     lxd   1            built-in  LXD Container Hypervisor
>microk8s        0                      k8s   1            built-in  A local Kubernetes context
>
```

## Clean up

```{include} ../reuse/clean-cloud-resources.md
```

To delete the Juju cloud, run

```{terminal}
:copy:

juju remove-cloud <k8s-cloud-name>
```

To delete GKE clusters, run

```{terminal}
:copy:

gcloud container clusters list
```
```{terminal}
:copy:

gcloud container clusters delete <cluster_name> --zone <region-name>-c
```

Revoke the Google Cloud user credentials (you should see a confirmation output):

```{terminal}
:copy:

gcloud auth revoke <account>@gmail.com

>Revoked credentials:
 >- <account>@gmail.com
```

