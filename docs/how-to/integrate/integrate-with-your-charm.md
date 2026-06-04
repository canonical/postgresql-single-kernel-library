(integrate-with-your-charm)=
# How to integrate PostgreSQL with your charm
{{vm}}{{k8s}}

Charmed PostgreSQL can be integrated with any charmed application that supports its interfaces. This page provides some guidance and resources for charm developers to develop, integrate, and troubleshoot their charm so that it may connect with PostgreSQL.

(check-supported-interfaces)=
## Check supported interfaces

First, we recommend that you check {ref}`the supported interfaces <interfaces-and-endpoints>` of the current charm.

Most existing charms currently use [ops-lib-pgsql](https://github.com/canonical/ops-lib-pgsql) interface (legacy). For new charms, **Canonical recommends using [data-platform-libs](https://github.com/canonical/data-platform-libs).** <!--TODO: clarify interface compatibility in PG 16 (no legacy at all?) -->

For more information, see {ref}`charm-versions`.

## Integrate your charm with PostgreSQL

**For an introduction** to the concepts of Juju integrations, see [Juju | Integration](https://documentation.ubuntu.com/juju/3.6/reference/relation/).

**For some practical examples**, take a look at the following:
* {{vm}}{{k8s}} [postgresql-test-app](https://github.com/canonical/postgresql-test-app)
* {{vm}} [Discourse | How to migrate Nextcloud to new PostgreSQL](https://discourse.charmhub.io/t/nextcloud-postgresql-how-to-migrate-nextcloud-to-new-postgresql-vm-charms/10969)
* {{k8s}} [Ops | Integrate your charm with PostgreSQL](https://ops.readthedocs.io/en/latest/tutorial/from-zero-to-hero-write-your-first-kubernetes-charm/integrate-your-charm-with-postgresql.html)

## Troubleshooting & testing

* To learn the basics of charm debugging, start with [Juju | How to debug a charm](https://juju.is/docs/sdk/debug-a-charm)
* To troubleshoot PostgreSQL, check the {ref}`troubleshooting` reference
* To test PostgreSQL and other charms, check the {ref}`troubleshooting` reference

## FAQ

**Does the requirer need to set anything in relation data?**

It depends on the interface. Check the `postgresql_client` [interface requirements](https://github.com/canonical/charm-relation-interfaces/blob/main/interfaces/postgresql_client/v0/README.md).

**Is there a charm library available, or does my charm need to compile the postgresql relation data on its own?**

Yes, a library is available: [data-platform-libs](https://github.com/canonical/data-platform-libs). The integration is trivial: [example](https://github.com/nextcloud-charmers/nextcloud-charms/pull/78).

**How do I obtain the database URL/URI?**

This feature is [planned](https://warthogs.atlassian.net/browse/DPE-2278) but currently missing. <!--TODO: update! this is done!-->

Meanwhile, use [this](https://github.com/nextcloud-charmers/nextcloud-charms/blob/91f9eebb4d40eaaff9c2f7513f66980df75c2a3b/operator-nextcloud/src/charm.py#L610-L631) example or refer to the function below.

```python
def _db_connection_string(self) -> str:
    """Report database connection string using info from relation databag."""
    relation = self.model.get_relation("database")
    if not relation:
        return ""

    data = self._database.fetch_relation_data()[relation.id]
    username = data.get("username")
    password = data.get("password")
    endpoints = data.get("endpoints")

    return f"postgres://{username}:{password}@{endpoints}/ratings"
 ```
