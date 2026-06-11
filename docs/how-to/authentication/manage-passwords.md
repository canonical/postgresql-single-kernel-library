---
myst:
  html_meta:
    description: "Manage Charmed PostgreSQL system user passwords using Juju secrets, including creating, granting, and configuring the system-users option."
---

(manage-passwords)=
# How to manage passwords
{{vm_k8s}}

In Charmed PostgreSQL 14, user credentials are managed with Juju's `get-password` and `set-password` actions.

## Get password

To retrieve the operator's password:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju run postgresql/leader get-password
```
```{tab-item} K8s
:sync: k8s

    juju run postgresql-k8s/leader get-password
```
````

## Set password

To change the operator's password to a new, randomised password:

````{tab-set}
```{tab-item} VM
:sync: vm

juju run postgresql/leader set-password
```
```{tab-item} K8s
:sync: k8s

juju run postgresql-k8s/leader set-password
```
````

To set a manual password for the operator/admin user:

````{tab-set}
```{tab-item} VM
:sync: vm

juju run postgresql/leader set-password password=<password>
```
```{tab-item} K8s
:sync: k8s

juju run postgresql-k8s/leader set-password password=<password>
```
````

To set a manual password for another user:

````{tab-set}
```{tab-item} VM
:sync: vm

juju run postgresql/leader set-password username=<username> password=<password>
```
```{tab-item} K8s
:sync: k8s

juju run postgresql-k8s/leader set-password username=<username> password=<password>
```
````
