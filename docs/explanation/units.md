(units)=
# PostgreSQL units

## Primary vs. leader unit

The [PostgreSQL primary server](https://www.postgresql.org/docs/current/runtime-config-replication.html#RUNTIME-CONFIG-REPLICATION-PRIMARY) unit may or may not be the same as the [juju leader unit](https://juju.is/docs/juju/leader).

The juju leader unit is the represented in `juju status` by an asterisk (*) next to its name.

To retrieve the juju unit that corresponds to the PostgreSQL primary, use the action `get-primary` on any of the units running ` postgresql-k8s`:

```text
juju run postgresql-k8s/leader get-primary
```

Similarly, the primary replica is displayed as a status message in `juju status`. However, one should note that this hook gets called on regular time intervals and the primary may be outdated if the status hook has not been called recently.

````{note}
**We highly suggest configuring the `update-status` hook to run frequently.** In addition to reporting the primary, secondaries, and other statuses, the [status hook](https://documentation.ubuntu.com/juju/3.6/reference/hook/#update-status) performs self-healing in the case of a network cut.

To change the frequency of the `update-status` hook, run

```text
juju model-config update-status-hook-interval=<time(s/m/h)>
```

This hook executes a read query to PostgreSQL. On a production level server, this should be configured to occur at a frequency that doesn't overload the server with read requests. Similarly, the hook should not be configured at too quick of a frequency, as this can delay other hooks from running.
````