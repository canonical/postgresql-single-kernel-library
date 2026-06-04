(migrate-data-via-backup-restore)=
# How to migrate data via backup/restore

Charmed PostgreSQL can restore backups of itself stored on S3-compatible storage (See: {ref}`restore-a-backup`). A similar restore approach is applicable for backups made by a different Charmed PostgreSQL installation or even another PostgreSQL charm. (See: {ref}`migrate-a-cluster`).

This is a guide for migrating data from modern charms. To migrate legacy charm data, refer to the guide {ref}`migrate-data-via-pg-dump`.

<!--TODO: clarify difference between:
* back-up-and-restore/restore-a-backup
* back-up-and-restore/migrate-a-cluster
* data-migration/migrate-data-via-backup-restore
* data-migration/migrate-data-via-pg-dump
-->

```{caution}
The Canonical Data Team describes here the general approach and does NOT support nor guarantee restoration results.

Always test a migration in a test environment before performing it in production!
```

## Prerequisites

* **Check [your application's compatibility](charm-versions)** with Charmed PostgreSQL before migrating production data from legacy charm
* Make sure **PostgreSQL versions are identical** before the migration

---

## Migrate database data

Below is the *general approach* to the migration:

1. Retrieve root/admin level credentials from legacy charm. <!--TODO: is "legacy" the right term here? -->

   See examples in {ref}`migrate-data-via-pg-dump`.

2. Install [pgBackRest](https://pgbackrest.org/) inside the old charm OR nearby.<!--TODO: clarify "nearby"-->

    Ensure the version is compatible with pgBackRest in the new Charmed PostgreSQL revision you are going to deploy! See examples in the [pgBackRest user guide](https://pgbackrest.org/user-guide.html#installation).

   ```{note}
   You can use the `charmed-postgresql` [snap](https://snapcraft.io/charmed-postgresql) (VM) or [rock](https://github.com/canonical/charmed-postgresql-rock) (K8s) directly.
   ```

3. Configure storage for database backup (local or remote, S3-based is recommended).

4. Create a first full logical backup during the off-peak

   See an example of a backup command [here](https://github.com/canonical/postgresql-k8s-operator/commit/f39caaa4c5c85afdb157bd53df54a24a1b9687ac#diff-cc5993b9da2438ecff27897b3ab9d2f9bc445cbf5b4f6369a1a0c2f404fe6a4fR186-R212).

5. {ref}`Restore the remote backup <migrate-a-cluster> to the Charmed PostgreSQL installation in your test environment.
6. Perform all the necessary tests to make sure your application accepted the new database.
7. Schedule and perform the final production migration, re-using the last steps above.