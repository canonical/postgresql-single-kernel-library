(connection-pooling)=
# Connection pooling
{{vm_k8s}}

Connection pooling is a strategy to reduce the amount of active connections and the costs of reopening connections. It requires maintaining a set of persistently opened connections, called a pool, that can be reused by clients.

Since PostgreSQL spawns separate processes for client connections, it can be beneficial in some use cases to maintain a client side connection pool rather than increase the database connection limits and resource consumption.

A way to achieve this with Charmed PostgreSQL is by integrating with the [PgBouncer charm](https://charmhub.io/pgbouncer?channel=1/stable).

## Increasing maximum allowed connections

If using PgBouncer is not enough to handle the connections load of your application, you can increase the amount of connections that PostgreSQL can open via the `experimental-max-connections` config parameter ([VM charm](https://charmhub.io/postgresql/configurations?channel=16/edge#experimental-max-connections)| [K8s charm](https://charmhub.io/postgresql/configurations?channel=16/edge#experimental-max-connections)).

```{dropdown} Resource usage warning
:open:
:color: warning
:icon: alert
:class-title: sd-font-weight-normal

Each connection opened by PostgreSQL spawns a new process, which is resource-intensive. Use this option as a last resort.

{ref}`Contact us <contact>` for more guidance for your use-case.
```
