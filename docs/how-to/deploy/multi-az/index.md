---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL across multiple availability zones for high availability, eliminating single points of failure on GCE and GKE."
---

(multi-az)=
# How to deploy on multiple availability zones (AZ)
{{vm_k8s}}

During the deployment to hardware/VMs, it is important to spread all the database copies (Juju units) to different hardware servers, or even better, to different [availability zones](https://en.wikipedia.org/wiki/Availability_zone) (AZ). This will guarantee no shared service-critical components across the database cluster (eliminate the case with all eggs in the same basket).

## Prerequisites

**Machines/VM**: This feature is enabled by default on EC2/GCE and is supported by LXD/MicroCloud.

**Kubernetes**: Your cloud must support and provide availability zones concepts such as the K8s label `topology.kubernetes.io/zone`. This is enabled by default on EKS/GKE/AKS and supported by MicroK8s/Charmed Kubernetes.

## Examples

These guides will take you through deploying a PostgreSQL cluster on Google Cloud using 3 available zones. All Juju units (or pods, on Kubernetes) will be set up to sit in their dedicated zones only, which effectively guarantees database copy survival across all available availability zones.

```{toctree}
:titlesonly:

GCE (VM) <gce>
GKE (K8s) <gke>
```