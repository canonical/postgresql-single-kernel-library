---
myst:
  html_meta:
    description: "Deploy Charmed PostgreSQL using Terraform and the Juju Terraform Provider with ready-to-use canonical module examples."
---

(terraform)=
# How to deploy with Terraform
{{vm_k8s}}

[Terraform](https://www.terraform.io/) is an infrastructure automation tool to provision and manage resources in clouds or data centres. To deploy Charmed PostgreSQL using Terraform and Juju, you can use the [Juju Terraform Provider](https://registry.terraform.io/providers/juju/juju/latest)

The easiest way is to start from [these examples of terraform modules](https://github.com/canonical/terraform-modules) prepared by Canonical.

{{seealso}} In-depth [introduction to the Juju Terraform Provider](https://discourse.charmhub.io/t/6939).

## Install Terraform tooling

This guide assumes Juju is installed and you have a controller already bootstrapped. For more information, check see {ref}`deploy`.

First, install Terraform Provider and example modules:

```shell
sudo snap install terraform --classic
```

````{tab-set}
```{tab-item} VM
:sync: vm

Switch to the LXD provider and create a new model:

    juju switch lxd
    juju add-model my-model

Clone examples and navigate to the PostgreSQL machine module:

    git clone https://github.com/canonical/terraform-modules.git
    cd terraform-modules/modules/machine/postgresql
```
```{tab-item} K8s
:sync: k8s

Switch to the K8s provider and create a new model:

    juju switch microk8s
    juju add-model my-model

Clone examples and navigate to the PostgreSQL machine module:

    git clone https://github.com/canonical/terraform-modules.git
    cd terraform-modules/modules/k8s/postgresql
```
````

Initialise the Juju Terraform Provider:

```shell
terraform init
```

## Verify the deployment

Open the `main.tf` file to see the brief contents of the Terraform module:

`````{tab-set}
````{tab-item} VM
:sync: vm

```tf
resource "juju_application" "machine_postgresql" {
  name  = "postgresql"
  model = "my-model"

  charm {
    name    = "postgresql"
    channel = "16/stable"
  }

  config = {
    plugin-hstore-enable  = true
    plugin-pg-trgm-enable = true
  }

  units = 1
}
```
````
````{tab-item} K8s
:sync: k8s

```tf
resource "juju_application" "k8s_postgresql" {
  name  = var.postgresql_application_name
  model = var.juju_model_name
  trust = true

  charm {
    name    = "postgresql-k8s"
    channel = var.postgresql_charm_channel
  }

  units = 1
}
```
````
`````

Run `terraform plan` to get a preview of the changes that will be made:

```shell
terraform plan -var "juju_model_name=my-model"
```

## Apply the deployment

If everything looks correct, deploy the resources (skip the approval):

```shell
terraform apply -auto-approve -var "juju_model_name=my-model"
```

## Check deployment status

Check the deployment status with

````{tab-set}
```{tab-item} VM
:sync: vm

    juju status --model lxd:my-model --watch 1s

Sample output:

    Model         Controller  Cloud/Region         Version  SLA          Timestamp
    my-model  lxd         localhost/localhost  3.5.2    unsupported  14:04:26+02:00

    App         Version  Status  Scale  Charm       Channel    Rev  Exposed  Message
    postgresql  16.9     active      1  postgresql  16/stable  843  no

    Unit           Workload  Agent  Machine  Public address  Ports     Message
    postgresql/0*  active    idle   0        10.142.152.90   5432/tcp  Primary

    Machine  State    Address        Inst id        Base          AZ  Message
    0        started  10.142.152.90  juju-1ea4a4-0  ubuntu@22.04      Running
```
```{tab-item} K8s
:sync: k8s

    juju status --model k8s:my-model --watch 1s

Sample output:

    Model     Controller  Cloud/Region        Version  SLA          Timestamp
    my-model  k8s         microk8s/localhost  3.5.3    unsupported  12:09:38Z

    App             Version  Status  Scale  Charm           Channel    Rev  Address         Exposed  Message
    postgresql-k8s  16.9     active      1  postgresql-k8s  16/stable  615  10.152.183.137  no

    Unit               Workload  Agent  Address     Ports  Message
    postgresql-k8s/0*  active    idle   10.1.77.74         Primary
```
````

Continue to operate the charm as usual from here or apply further Terraform changes.

## Clean up

To keep the house clean, remove the newly deployed Charmed PostgreSQL by running

```shell
terraform destroy -var "juju_model_name=my-model"
```

Sample output:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju_application.machine_postgresql: Refreshing state... [id=my-model:postgresql]

    Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
    - destroy

    Terraform will perform the following actions:

    # juju_application.machine_postgresql will be destroyed
    - resource "juju_application" "machine_postgresql" {
        - config      = {
            - "plugin-hstore-enable"  = "true"
            - "plugin-pg-trgm-enable" = "true"
            } -> null
        - constraints = "arch=amd64" -> null
        - id          = "my-model:postgresql" -> null
        - model       = "my-model" -> null
        - name        = "postgresql" -> null
        - placement   = "0" -> null
        - storage     = [
            - {
                - count = 1 -> null
                - label = "pgdata" -> null
                - pool  = "rootfs" -> null
                - size  = "99G" -> null
                },
            ] -> null
        - trust       = true -> null
        - units       = 1 -> null

        - charm {
            - base     = "ubuntu@24.04" -> null
            - channel  = "16/stable" -> null
            - name     = "postgresql" -> null
            - revision = 843 -> null
            - series   = "noble" -> null
            }
        }

    Plan: 0 to add, 0 to change, 1 to destroy.

    Changes to Outputs:
    - application_name = "postgresql" -> null

    Do you really want to destroy all resources?
    Terraform will destroy all your managed infrastructure, as shown above.
    There is no undo. Only 'yes' will be accepted to confirm.

    Enter a value: yes

    juju_application.machine_postgresql: Destroying... [id=my-model:postgresql]
    juju_application.machine_postgresql: Destruction complete after 1s

    Destroy complete! Resources: 1 destroyed.
```
```{tab-item} K8s
:sync: k8s

    juju_application.k8s_postgresql: Refreshing state... [id=my-model:postgresql-k8s]

    Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
    - destroy

    Terraform will perform the following actions:

    # juju_application.k8s_postgresql will be destroyed
    - resource "juju_application" "k8s_postgresql" {
        - constraints = "arch=amd64" -> null
        - id          = "my-model:postgresql-k8s" -> null
        - model       = "my-model" -> null
        - name        = "postgresql-k8s" -> null
        - placement   = "" -> null
        - storage     = [
            - {
                - count = 1 -> null
                - label = "pgdata" -> null
                - pool  = "kubernetes" -> null
                - size  = "1G" -> null
                },
            ] -> null
        - trust       = true -> null
        - units       = 1 -> null

        - charm {
            - base     = "ubuntu@22.04" -> null
            - channel  = "14/stable" -> null
            - name     = "postgresql-k8s" -> null
            - revision = 281 -> null
            - series   = "jammy" -> null
            }
        }

    Plan: 0 to add, 0 to change, 1 to destroy.

    Changes to Outputs:
    - application_name = "postgresql-k8s" -> null

    Do you really want to destroy all resources?
    Terraform will destroy all your managed infrastructure, as shown above.
    There is no undo. Only 'yes' will be accepted to confirm.

    Enter a value: yes

    juju_application.k8s_postgresql: Destroying... [id=my-model:postgresql-k8s]
    juju_application.k8s_postgresql: Destruction complete after 0s

    Destroy complete! Resources: 1 destroyed.
```
````

For more examples of Terraform modules for VM, including PostgreSQL HA and PostgreSQL + PgBouncer, see the other directories in the [`terraform-modules` repository](https://github.com/canonical/terraform-modules/tree/main/modules/machine).

Feel free to {ref}`contact us <contact>` if you have any question and [collaborate with us on GitHub](https://github.com/canonical/terraform-modules)!
