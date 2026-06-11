---
myst:
  html_meta:
    description: "Data migration guides for Charmed PostgreSQL, including upgrading from PostgreSQL 14 to 16, using pg_dump, and backup-based migration methods."
---

(data-migration)=
# Data migration

For guidance about moving data from a Charmed PostgreSQL 14 database to Charmed PostgreSQL 16, start here:

```{toctree}
:titlesonly:

Migrate data from PostgreSQL 14 to 16 <migrate-data-from-14-to-16>
```

Data migration between two PostgreSQL 14 clusters:

```{toctree}
:titlesonly:

Migrate data via `pg_dump` <migrate-data-via-pg-dump>
Migrate data via backup/restore <migrate-data-via-backup-restore>
```