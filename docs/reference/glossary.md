---
myst:
  html_meta:
    description: "Glossary of Charmed PostgreSQL terms including the Canonical Observability Stack, application endpoints, PostgreSQL extensions, and split-brain scenarios."
---

# Glossary

<!--TODO: definitions and apply {term} label throughout docs -->

```{glossary}
Canonical Observability Stack
    A set of charms that facilitates integration with open-source telemetry tools like Grafana, Prometheus, and Loki.

Application endpoint
    A struct defined in the charm's metadata that helps define a relation. See: [Juju | Application endpoint](https://documentation.ubuntu.com/juju/3.6/reference/application/#application-endpoint)

PostgreSQL extension
    (also "plugin") A modular package that adds new functionality to PostgreSQL. {ref}`Several extensions <supported-extensions>` are included in the charm.

PostgreSQL plugin
    (also "extension") A modular package that adds new functionality to PostgreSQL. {ref}`Several plugins <supported-extensions>` are included in the charm.

Split-brain scenario
```

<!--

Failover

Juju interface

Patroni

PgBouncer

Juju refresh

PostgreSQL replica

Replication

Charm revision

Role

Juju secret

Juju status

Switchover

Juju unit

User

Charm workload
-->