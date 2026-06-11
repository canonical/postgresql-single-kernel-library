---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL on Azure Kubernetes Service (AKS) using Juju, with step-by-step instructions for Azure CLI and kubectl setup."
---

(aks)=
# How to deploy on AKS
{{k8s}}

[Azure Kubernetes Service](https://learn.microsoft.com/en-us/azure/aks/) (AKS) allows you to quickly deploy a production ready Kubernetes cluster in Azure.

{octicon}`browser` AKS web interface: [https://portal.azure.com/](https://portal.azure.com/)

## Prerequisites

* A physical or virtual machine running Ubuntu 22.04+
* Juju 3.6+ installed via snap

---

## Install AKS CLI tooling

Install the Azure CLI for Linux by following the [official Azure documentation](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt).

Install the [`kubectl` CLI tools](https://kubernetes.io/docs/tasks/tools/) via snap:

```{terminal}
:copy:

sudo snap install kubectl --classic
```

To check they are correctly installed, run

```{terminal}
:copy:

az --version

azure-cli                         2.65.0
core                              2.65.0
telemetry                          1.1.0

Dependencies:
msal                              1.31.0
azure-mgmt-resource               23.1.1
...

Your CLI is up-to-date.
```
```{terminal}
:copy:

kubectl version --client

Client Version: v1.28.2
Kustomize Version: v5.0.4-0.20230601165947-6ce0bf390ce3
```

### Authenticate

Login to your Azure account:

```{terminal}
:copy:

az login
```

## Create a new AKS cluster

Export the deployment name for later use:

```{terminal}
:copy:

export JUJU_NAME=aks-$USER-$RANDOM
```

Create a new [Azure Resource Group](https://learn.microsoft.com/en-us/cli/azure/manage-azure-groups-azure-cli) in the region that best suits you:

```{terminal}
:copy:

az group create --name aks --location <region-name>
```

Create a new Kubernetes cluster on AKS with the following command (increase node count and size if necessary):

```{terminal}
:copy:

az aks create -g aks -n ${JUJU_NAME} --enable-managed-identity --node-count 1 --node-vm-size=Standard_D4s_v4 --generate-ssh-keys

{
  "aadProfile": null,
  "addonProfiles": null,
  "agentPoolProfiles": [
    {
      "availabilityZones": null,
      "capacityReservationGroupId": null,
      "count": 1,
      "creationData": null,
      "currentOrchestratorVersion": "1.28.9",
      "enableAutoScaling": false,
      "enableEncryptionAtHost": false,
      "enableFips": false,
      "enableNodePublicIp": false,
...
```

Dump the new AKS credentials:

```{terminal}
:copy:

az aks get-credentials --resource-group aks --name ${JUJU_NAME} --context aks

...
Merged "aks" as current context in ~/.kube/config
```

## Bootstrap Juju on AKS

Bootstrap a Juju controller:

```{terminal}
:copy:

juju bootstrap aks <controller-name>

Creating Juju controller "aks" on aks/<region-name>
Bootstrap to Kubernetes cluster identified as azure/<region-name>
Creating k8s resources for controller "controller-aks"
Downloading images
Starting controller pod
Bootstrap agent now started
Contacting Juju controller at 20.231.233.33 to verify accessibility...

Bootstrap complete, controller "aks" is now available in namespace "controller-aks"

Now you can run
	juju add-model <model-name>
to create a new model to deploy k8s workloads.
```

{{seealso}} [Juju | Microsoft Azure options](https://documentation.ubuntu.com/juju/3.6/reference/cloud/list-of-supported-clouds/the-microsoft-aks-cloud-and-juju/)

## Deploy Charmed PostgreSQL on AKS

Create a Juju model:

```{terminal}
:copy:

juju add-model <model-name>
```

The following command deploys 3 nodes of PostgreSQL on Kubernetes:

```{terminal}
:copy:

juju deploy postgresql-k8s --channel 14/stable --trust -n 3

Deployed "postgresql-k8s" from charm-hub charm "postgresql-k8s", revision <number> in channel 14/stable on ubuntu@22.04/edge
```

## Display deployment information

Display information about the current deployments with `kubectl` and `az`:

```{terminal}
:copy:

kubectl cluster-info

Kubernetes control plane is running at https://aks-user-aks-aaaaa-bbbbb.hcp.<region-name>.azmk8s.io:443
CoreDNS is running at https://aks-user-aks-aaaaa-bbbbb.hcp.<region-name>.azmk8s.io:443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
Metrics-server is running at https://aks-user-aks-aaaaa-bbbbb.hcp.<region-name>.azmk8s.io:443/api/v1/namespaces/kube-system/services/https:metrics-server:/proxy
```

```{terminal}
:copy:
az aks list

...
        "count": 1,
        "currentOrchestratorVersion": "1.28.9",
        "enableAutoScaling": false,
...
```

```{terminal}
:copy:

kubectl get node

NAME                                STATUS   ROLES   AGE   VERSION
aks-nodepool1-31246187-vmss000000   Ready    agent   11m   v1.28.9
```

## Clean up

```{include} ../reuse/clean-cloud-resources.md
```

List all services and then delete those that have an associated `EXTERNAL-IP` value (e.g. load balancers):

```{terminal}
:copy:

kubectl get svc --all-namespaces

(...)
```

```{terminal}
:copy:

kubectl delete svc <service-name>
```

Next, delete the AKS resources:

```{terminal}
:copy:

az aks delete -g aks -n ${JUJU_NAME}
```

{{seealso}} [Azure documentation | Delete all Azure resources](https://learn.microsoft.com/en-us/cli/azure/delete-azure-resources-at-scale#delete-all-azure-resources-of-a-type)

Finally, log out from AKS to clean the local credentials (to avoid forgetting and leaking):

```{terminal}
:copy:

az logout
```
