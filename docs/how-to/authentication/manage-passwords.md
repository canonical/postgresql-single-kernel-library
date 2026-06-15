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

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju run <action name>` becomes `juju run-action <action name> --wait` for Juju 2.9.
```

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

## Rotate application passwords

To rotate the passwords of users created for integrated applications, the integration should be removed and created again. This process will generate a new user and password for the application.


````{tab-set}
```{tab-item} VM
:sync: vm

    juju remove-relation <application> postgresql
    juju integrate <application> postgresql
```
```{tab-item} K8s
:sync: k8s

    juju remove-relation <application> postgresql-k8s
    juju integrate <application> postgresql-k8s
```
````

```{dropdown} Juju 2.9 users
:class-container: dropdown-note
:icon: info

Remember that `juju integrate` becomes `juju relate` for Juju 2.9.
```

In the case of connecting with a non-charmed application, `<application>` would be `data-integrator`.