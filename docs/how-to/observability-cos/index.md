---
myst:
  html_meta:
    description: "Observability guides for Charmed PostgreSQL using the Canonical Observability Stack (COS), including monitoring, alert rules, tracing, and profiling."
---

(observability-cos)=
# Observability (COS)

The Canonical Observability Stack (COS) can be integrated with Charmed PostgreSQL to facilitate collection and visualization of telemetry data.

{{seealso}} [Observability documentation | What is COS?](https://documentation.ubuntu.com/observability/latest/explanation/overview/what-is-cos/)

## Guides

Charmed PostgreSQL supports the following COS applications:

| Function                                | Application                  |
|-----------------------------------------|------------------------------|
| {ref}`Monitoring <enable-monitoring>`   | Grafana                      |
| {ref}`Alert rules <enable-alert-rules>` | Prometheus, Loki             |
| {ref}`Tracing <enable-tracing>`         | Grafana Tempo                |
| {ref}`Profiling <enable-profiling>`     | Parca or Polar Signals Cloud |

```{toctree}
:titlesonly:
:hidden:

Enable monitoring <enable-monitoring>
Enable alert rules <enable-alert-rules>
Enable tracing <enable-tracing>
Enable profiling <enable-profiling>
```