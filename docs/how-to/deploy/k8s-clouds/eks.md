---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL on Amazon Elastic Kubernetes Service (EKS) using Juju, with setup for eksctl, AWS CLI, and kubectl tools."
---

(eks)=
# How to deploy on EKS
{{k8s}}

The [Amazon Elastic Kubernetes Service](https://aws.amazon.com/eks/) (EKS) is a popular, fully automated Kubernetes service.

{octicon}`browser` EKS web interface: [console.aws.amazon.com/eks/home](https://console.aws.amazon.com/eks/home)

## Prerequisites

* A physical or virtual machine running Ubuntu 24.04+
* Juju 3.6+ installed via snap

---

## Install EKS tooling

Install the Amazon EKS CLI by following the [official `eksctl` documentation](https://eksctl.io/installation/).

Install the Amazon Web Services CLI by following the [official AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

Install the [`kubectl` CLI tools](https://kubernetes.io/docs/tasks/tools/) via snap:

```{terminal}
:copy:

sudo snap install kubectl --classic
```

To check they are correctly installed, run

```{terminal}
:copy:

eksctl info

eksctl version: 0.159.0
kubectl version: v1.28.2
```
```{terminal}
:copy:
aws --version

aws-cli/2.13.25 Python/3.11.5 Linux/6.2.0-33-generic exe/x86_64.ubuntu.23 prompt/off
```

```{terminal}
:copy:

kubectl version --client

Client Version: v1.28.2
Kustomize Version: v5.0.4-0.20230601165947-6ce0bf390ce3
```

## Authenticate

Create an IAM account or use legacy access keys to operate AWS:

```{terminal}
:copy:

aws configure

AWS Access Key ID [None]: SECRET_ACCESS_KEY_ID
AWS Secret Access Key [None]: SECRET_ACCESS_KEY_VALUE
Default region name [None]: eu-west-3
Default output format [None]:
```
```{terminal}
:copy:

aws sts get-caller-identity

{
    "UserId": "1234567890",
    "Account": "1234567890",
    "Arn": "arn:aws:iam::1234567890:root"
}
```

## Create a new EKS cluster

Export the deployment name for later use:

```{terminal}
:copy:

export JUJU_NAME=aks-$USER-$RANDOM
```

Example `cluster.yaml`:

```{terminal}
:copy:

cat <<-EOF > cluster.yaml

---
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
    name: ${JUJU_NAME}
    region: <region-name>
    version: "1.27"
iam:
  withOIDC: true

addons:
- name: aws-ebs-csi-driver
  wellKnownPolicies:
    ebsCSIController: true

nodeGroups:
    - name: ng-1
      minSize: 3
      maxSize: 5
      iam:
        attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
        - arn:aws:iam::aws:policy/AmazonS3FullAccess
      instancesDistribution:
        maxPrice: 0.15
        instanceTypes: ["m5.xlarge", "m5.2xlarge"] # At least two instance types should be specified
        onDemandBaseCapacity: 0
        onDemandPercentageAboveBaseCapacity: 50
        spotInstancePools: 2
EOF
```

Create a new Kubernetes cluster on EKS with the following command:

```{terminal}
:copy:

eksctl create cluster -f cluster.yaml

...
2023-10-12 11:13:58 [ℹ]  using region <region-name>
2023-10-12 11:13:59 [ℹ]  using Kubernetes version 1.27
...
2023-10-12 11:40:00 [✔]  EKS cluster "eks-taurus-27506" in "<region-name>" region is ready
```

## Bootstrap Juju on EKS

Add a Juju K8s cloud:

```{terminal}
:copy:

juju add-k8s <k8s-cloud-name>
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
{{seealso}} [Juju | Amazon EKS and Juju](https://documentation.ubuntu.com/juju/3.6/reference/cloud/list-of-supported-clouds/the-amazon-eks-cloud-and-juju/#the-amazon-eks-cloud-and-juju)

## Deploy charms

Create a Juju model (K8s namespace):

```{terminal}
:copy:

juju add-model <model-name>
```

The following command deploys 3 nodes of PostgreSQL on Kubernetes:

```{terminal}
:copy:

juju deploy postgresql-k8s --channel 16/stable --trust -n 3

Deployed "postgresql-k8s" from charm-hub charm "postgresql-k8s", revision <number> in channel 16/stable on ubuntu@24.04/edge
```

### Display deployment information

Display information about the current deployments with `kubectl` and `eksctl`:

```{terminal}
:copy:

kubectl cluster-info

Kubernetes control plane is running at https://AAAAAAAAAAAAAAAAAAAAAAA.gr7.<region-name>.eks.amazonaws.com
CoreDNS is running at https://AAAAAAAAAAAAAAAAAAAAAAA.gr7.<region-name>.eks.amazonaws.com/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
```
```{terminal}
:copy:

eksctl get cluster -A

NAME			        REGION		    EKSCTL   CREATED
eks-taurus-27506	<region-name>	True
```
```{terminal}
:copy:

kubectl get node

NAME                                               STATUS   ROLES    AGE   VERSION
ip-192-168-14-61.<region-name>.compute.internal    Ready    <none>   19m   v1.27.5-eks-43840fb
ip-192-168-51-96.<region-name>.compute.internal    Ready    <none>   19m   v1.27.5-eks-43840fb
ip-192-168-78-167.<region-name>.compute.internal   Ready    <none>   19m   v1.27.5-eks-43840fb
```

## Clean up

```{include} ../reuse/clean-cloud-resources.md
```

To delete the Juju cloud, run

```{terminal}
:copy:

juju remove-cloud <k8s-cloud-name>
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

Next, delete the EKS cluster:

```{terminal}
:copy:

eksctl get cluster -A
```
```{terminal}
:copy:

eksctl delete cluster <cluster-name> --region <region-name> --force --disable-nodegroup-eviction
```

{{seealso}} [Amazon documentation | Delete an EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/delete-cluster.html)

Finally, remove AWS CLI user credentials (to avoid forgetting and leaking):

```{terminal}
:copy:

rm -f ~/.aws/credentials
```

