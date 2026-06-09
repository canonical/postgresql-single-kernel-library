To access the database from outside of the cloud, open the the cloud's firewall using [`juju expose`](https://juju.is/docs/juju/juju-expose):

```{terminal}
:copy:

juju expose postgresql
```

```{dropdown} Be wary of opening ports to the public
:open:
:color: warning
:icon: alert
:class-title: sd-font-weight-normal

Make sure you understand the risks before doing this in production.
```

Once exposed, you can connect your database using the same credentials as above except the IP. This time, **use the public IP assigned by the cloud provider to the PostgreSQL instance**.

You can find it it with `juju status`:

```{terminal}
:copy:

juju status postgresql

...
Unit           Workload  Agent  Machine  Public address  Ports     Message
postgresql/0*  active    idle   0        <public-ip>     5432/tcp  Primary
...
```
```{terminal}
:copy:

psql postgresql://<username>:<password>@<public-ip>  :5432/test-db

psql (15.6 (Ubuntu 15.6-0ubuntu0.23.10.1), server 16.9 (Ubuntu 14.12-0ubuntu0.24.04.1))
Type "help" for help.

test-db=>
```

To close public access, run:

```{terminal}
:copy:

juju unexpose postgresql
```