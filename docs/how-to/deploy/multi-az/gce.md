---
myst:
  html_meta:
    description: "Deploy a Charmed PostgreSQL machine cluster across three availability zones on Google Compute Engine using Juju zone constraints for high availability."
---

(multi-az-gce)=
# Multi-AZ example on GCE
{{vm}}

This guide goes through an example deployment of PostgreSQL on GCE (Google Cloud Engine) with Juju zone constraints.

## Set up GCE on Google Cloud Engine

Let's deploy the {ref}`PostgreSQL Cluster on GCE (us-east4) <gce>` using all 3 zones there (`us-east4-a`, `us-east4-b`, `us-east4-c`) and make sure all pods always sits in the dedicated zones only.

```{dropdown} Creating the following resources may cost you money
:open:
:color: warning
:icon: alert
:class-title: sd-font-weight-normal

Be sure to monitor your Google Cloud costs.
```

Log into Google Cloud and bootstrap GCE:

```shell
gcloud auth login
gcloud iam service-accounts keys create sa-private-key.json  --iam-account=juju-gce-account@[your-gcloud-project-12345].iam.gserviceaccount.com
sudo mv sa-private-key.json /var/snap/juju/common/sa-private-key.json
sudo chmod a+r /var/snap/juju/common/sa-private-key.json

juju add-credential google
juju bootstrap google gce
juju add-model mymodel
```

## Deploy PostgreSQL with Juju zones constraints

Juju provides support for availability zones using [**constraints**](https://juju.is/docs/juju/constraint#zones).

The command below demonstrates how Juju automatically deploys Charmed PostgreSQL VM using Juju constraints:

```shell
juju deploy postgresql --channel 16/stable -n 3 \
  --constraints zones=us-east1-b,us-east1-c,us-east1-d
```

After a successful deployment, `juju status` will show an active application:

```shell
Model    Controller  Cloud/Region     Version    SLA          Timestamp
mymodel  gce         google/us-east1  3.5.4      unsupported  00:16:52+02:00

App         Version  Status  Scale  Charm       Channel    Rev  Exposed  Message
postgresql  16.9     active      3  postgresql  16/stable  843  no

Unit           Workload  Agent  Machine  Public address  Ports     Message
postgresql/0   active    idle   0        34.148.44.51    5432/tcp
postgresql/1   active    idle   1        34.23.202.220   5432/tcp
postgresql/2*  active    idle   2        34.138.167.85   5432/tcp  Primary

Machine  State    Address        Inst id        Base          AZ          Message
0        started  34.148.44.51   juju-e7c0db-0  ubuntu@22.04  us-east1-d  RUNNING
1        started  34.23.202.220  juju-e7c0db-1  ubuntu@22.04  us-east1-c  RUNNING
2        started  34.138.167.85  juju-e7c0db-2  ubuntu@22.04  us-east1-b  RUNNING
```

and each unit/vm will sit in the separate AZ out of the box:

```shell
> gcloud compute instances list
NAME           ZONE        MACHINE_TYPE  PREEMPTIBLE  INTERNAL_IP  EXTERNAL_IP    STATUS
juju-a82dd9-0  us-east1-b  n1-highcpu-4               10.142.0.30  34.23.252.144  RUNNING  # Juju Controller
juju-e7c0db-2  us-east1-b  n2-highcpu-2               10.142.0.32  34.138.167.85  RUNNING  # postgresql/2
juju-e7c0db-1  us-east1-c  n2-highcpu-2               10.142.0.33  34.23.202.220  RUNNING  # postgresql/1
juju-e7c0db-0  us-east1-d  n2-highcpu-2               10.142.0.31  34.148.44.51   RUNNING  # postgresql/0
```

### Simulation: A node gets lost

Let's destroy a GCE node and recreate it using the same AZ:

```shell
> gcloud compute instances delete juju-e7c0db-1
No zone specified. Using zone [us-east1-c] for instance: [juju-e7c0db-1].
The following instances will be deleted. Any attached disks configured to be auto-deleted will be deleted unless they are attached to any other instances or the `--keep-disks` flag is given and specifies them for keeping. Deleting a disk is
irreversible and any data on the disk will be lost.
 - [juju-e7c0db-1] in [us-east1-c]

Do you want to continue (Y/n)?  Y

Deleted [https://www.googleapis.com/compute/v1/projects/data-platform-testing-354909/zones/us-east1-c/instances/juju-e7c0db-1].
```

```shell
Model    Controller  Cloud/Region     Version    SLA          Timestamp
mymodel  gce         google/us-east1  3.5.4      unsupported  00:25:14+02:00

App         Version  Status  Scale  Charm       Channel    Rev  Exposed  Message
postgresql  16.9     active    2/3  postgresql  16/stable  843  no

Unit           Workload  Agent  Machine  Public address  Ports     Message
postgresql/0   active    idle   0        34.148.44.51    5432/tcp
postgresql/1   unknown   lost   1        34.23.202.220   5432/tcp  agent lost, see 'juju show-status-log postgresql/1'
postgresql/2*  active    idle   2        34.138.167.85   5432/tcp  Primary

Machine  State    Address        Inst id        Base          AZ          Message
0        started  34.148.44.51   juju-e7c0db-0  ubuntu@22.04  us-east1-d  RUNNING
1        down     34.23.202.220  juju-e7c0db-1  ubuntu@22.04  us-east1-c  RUNNING
2        started  34.138.167.85  juju-e7c0db-2  ubuntu@22.04  us-east1-b  RUNNING
```

Here we should remove the no-longer available `server/vm/GCE` node and add a new one. Juju will create it in the same AZ `us-east4-c`:

```shell
> juju remove-unit postgresql/1 --force --no-wait
WARNING This command will perform the following actions:
will remove unit postgresql/1

Continue [y/N]? y
```

The command `juju status` shows the machines in a healthy state, but PostgreSQL HA recovery is necessary:

```shell
Model    Controller  Cloud/Region     Version    SLA          Timestamp
mymodel  gce         google/us-east1  3.5.4      unsupported  00:30:09+02:00

App         Version  Status  Scale  Charm       Channel    Rev  Exposed  Message
postgresql  16.9     active      2  postgresql  16/stable  843  no

Unit           Workload  Agent  Machine  Public address  Ports     Message
postgresql/0   active    idle   0        34.148.44.51    5432/tcp
postgresql/2*  active    idle   2        34.138.167.85   5432/tcp  Primary

Machine  State    Address        Inst id        Base          AZ          Message
0        started  34.148.44.51   juju-e7c0db-0  ubuntu@22.04  us-east1-d  RUNNING
2        started  34.138.167.85  juju-e7c0db-2  ubuntu@22.04  us-east1-b  RUNNING
```

Request Juju to add a new unit in the proper AZ:

```shell
juju add-unit postgresql -n 1
```

Juju uses the right AZ where the node is missing. Run `juju status`:

```shell
Model    Controller  Cloud/Region     Version    SLA          Timestamp
mymodel  gce         google/us-east1  3.5.4      unsupported  00:30:42+02:00

App         Version  Status  Scale  Charm       Channel    Rev  Exposed  Message
postgresql           active    2/3  postgresql  16/stable  843  no

Unit           Workload  Agent       Machine  Public address  Ports     Message
postgresql/0   active    idle        0        34.148.44.51    5432/tcp
postgresql/2*  active    idle        2        34.138.167.85   5432/tcp  Primary
postgresql/3   waiting   allocating  3                                  waiting for machine

Machine  State    Address        Inst id        Base          AZ          Message
0        started  34.148.44.51   juju-e7c0db-0  ubuntu@22.04  us-east1-d  RUNNING
2        started  34.138.167.85  juju-e7c0db-2  ubuntu@22.04  us-east1-b  RUNNING
3        pending                 juju-e7c0db-3  ubuntu@22.04  us-east1-c  starting
```

## Remove GCE setup

Always clean cloud resources that are no longer necessary; they could be costly!

Check the list of currently running GCE instances:

```shell
> gcloud compute instances list
NAME           ZONE        MACHINE_TYPE   PREEMPTIBLE  INTERNAL_IP  EXTERNAL_IP    STATUS
juju-a82dd9-0  us-east1-b  n1-highcpu-4                10.142.0.30  34.23.252.144  RUNNING
juju-e7c0db-2  us-east1-b  n2-highcpu-2                10.142.0.32  34.138.167.85  RUNNING
juju-e7c0db-3  us-east1-c  n2d-highcpu-2               10.142.0.34  34.23.202.220  RUNNING
juju-e7c0db-0  us-east1-d  n2-highcpu-2                10.142.0.31  34.148.44.51   RUNNING
```

Request Juju to clean all GCE resources:

```shell
juju destroy-controller gce --no-prompt --force --destroy-all-models
```

Re-check that there are no running GCE instances left (it should be empty):

```shell
gcloud compute instances list
```

