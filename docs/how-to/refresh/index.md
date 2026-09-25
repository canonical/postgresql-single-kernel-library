---
myst:
  html_meta:
    description: "Refresh and upgrade Charmed PostgreSQL to a new revision using juju refresh, with rollback guidance and recommended upgrade paths."
---

(refresh)=
# Refresh (upgrade)

````{dropdown} Emergency stop button
:open:
:class-container: dropdown-important
:icon: no-entry-fill

Halt an in-progress refresh with

```shell
juju config <app name> pause-after-unit-refresh=all
```

Then, consider rolling back.
````

```{toctree}
:titlesonly:

VM charm <vm>
K8s charm <k8s>
```
