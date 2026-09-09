---
myst:
  html_meta:
    description: "Refresh and upgrade Charmed PostgreSQL K8s to a new revision using juju refresh, with rollback guidance and recommended upgrade paths."
---

(refresh-k8s)=
# Refresh (upgrade)
{{k8s}}

````{dropdown} Emergency stop button
:open:
:class-container: dropdown-important
:icon: no-entry-fill

Halt an in-progress refresh with

```shell
juju config <app name> pause-after-unit-refresh=all
```

Then, consider {ref}`rolling back <roll-back>`.
````
