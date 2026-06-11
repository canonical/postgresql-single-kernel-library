---
myst:
  html_meta:
    description: "Explanations for Charmed PostgreSQL covering architecture, Juju integration, users, roles, logs, security, performance, and key interfaces."
---

(explanation)=
# Explanation
{{vm_k8s}}

Additional context about the PostgreSQL charm, including design, operational concepts, and security.

## Charm design

Core concepts about the high-level design of the PostgreSQL charm and its interfaces:

```{toctree}
:titlesonly:

Architecture <architecture>
Charm versions <charm-versions>
Interfaces and endpoints <interfaces-and-endpoints>
```

## Operation

Standard PostgreSQL operational concepts explained in the context of charms and Juju - such as how Juju units map to PostgreSQL replicas, what kinds of users and roles are exposed through the charm, and what kind of logs get written:

```{toctree}
:titlesonly:

Units <units>
Users <users>
Roles <roles>
Logs <logs>
Performance and testing <performance-and-testing>
Connection pooling <connection-pooling>
```

## Security

Overview of security and cryptography features in the charm:

```{toctree}
:titlesonly:

Security hardening <security-hardening/index>
```