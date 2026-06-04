(enable-extension)=
# How to enable an extension
{{vm_k8s}}

This guide outlines the steps for enabling an extension (plugin) in a Charmed PostgreSQL deployment.

```{seealso}
* {ref}`List of supported extensions <supported-extensions>`
```

## Enable extension

Enable the extension by setting `True` as the value of its respective config option, like in the following example:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju config postgresql plugin-<extension name>-enable=True
```
```{tab-item} K8s
:sync: k8s

    juju config postgresql-k8s plugin-<extension name>-enable=True
```
````

```{note}
The word "plugin" is used interchangeably with "extension". Both words refer to PostgreSQL extensions.
```

## Integrate your application

Integrate your charm with the PostgreSQL charm:

````{tab-set}
```{tab-item} VM
:sync: vm

    juju integrate <your-charm> postgresql
```
```{tab-item} K8s
:sync: k8s

    juju integrate <your-charm> postgresql-k8s
```
````

If your application charm requests extensions through `db` or `db-admin` relation data, but the extension is not enabled yet, you'll see that the PostgreSQL application goes into a blocked state with the following message:

````{tab-set}
```{tab-item} VM
:sync: vm

    postgresql/0*  blocked   idle   10.1.123.30      extensions requested through relation
```
```{tab-item} K8s
:sync: k8s

    postgresql-k8s/0*  blocked   idle   10.1.123.30      extensions requested through relation
```
````

In the [Juju debug logs](https://juju.is/docs/juju/juju-debug-log) we can see the list of extensions that need to be enabled. The example below shows a debug log for a PostgreSQL machine charm:

```text
unit-postgresql-0: 18:04:51 ERROR unit.postgresql/0.juju-log db:5: ERROR - `extensions` (pg_trgm, unaccent) cannot be requested through relations - Please enable extensions through `juju config` and add the relation again.
```

After enabling the needed extensions through the config options, the charm will unblock. If you have removed the relation, you can add it back again.

If the application charm uses the new `postgresql_client` interface, it can use the [is_postgresql_plugin_enabled](https://charmhub.io/data-platform-libs/libraries/data_interfaces) helper method from the data interfaces library to check whether the plugin/extension is already enabled in the database.

```{seealso}
{ref}`List of supported extensions <supported-extensions>`
```

