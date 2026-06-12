---
myst:
  html_meta:
    description: "Refresh and upgrade Charmed PostgreSQL to a new revision using juju refresh, with rollback guidance and recommended upgrade paths."
---

(refresh)=
# Refresh (upgrade)
{{vm_k8s}}

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

Charmed PostgreSQL supports minor version in-place refresh via the [`juju refresh`](https://documentation.ubuntu.com/juju/3.6/reference/juju-cli/list-of-juju-cli-commands/refresh/#details).

## Unsupported refreshes

These are examples of refreshes that are **not** supported in-place.

In some of these cases, it may be possible to perform an out-of-place upgrade or downgrade.

* Minor in-place downgrade from PostgreSQL 14.13 to 14.10
* Major in-place upgrade from PostgreSQL 14 to 16
* Major in-place downgrade from PostgreSQL 16 to 14
* Any refresh from or to a non-stable version (e.g. 14/edge)

## Guides

```{toctree}
:titlesonly:
:maxdepth: 2

minor-upgrade
minor-rollback
```