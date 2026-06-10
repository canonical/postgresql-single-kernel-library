---
myst:
  html_meta:
    description: "Troubleshooting guides for Charmed PostgreSQL, including status checks, switchover/failover, SOS report collection, and CLI helper tools."
---

(troubleshooting)=
# Troubleshooting
{{vm_k8s}}

Available troubleshooting tools and recommended steps to take when debugging the behavior of your Charmed PostgreSQL deployment:

```{toctree}
Overview <overview>
```

## Advanced troubleshooting

Learn about manual and automated switchover/failover procedures when there is an issue with the primary unit:

```{toctree}
Switchover/failover <switchover-failover>
```

Examples of how to use low-level, Patroni-based CLI tools for advanced troubleshooting:

```{toctree}
CLI helpers <cli-helpers>
```

Details about using the SoS report tool with PostgreQL for data collection:

```{toctree}
SOS report <sos-report>
```