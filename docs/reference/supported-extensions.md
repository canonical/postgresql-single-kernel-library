(supported-extensions)=
# Supported extensions

List of PostgreSQL extensions (aka plugins) supported by the charm.

```{seealso}
{ref}`enable-extension`
```

<!--TODO: K8s & revisions -->

| Extension                    | Config parameter name                       | Min. charm revision |
|------------------------------|---------------------------------------------|---------------------|
| address_standardizer_data_us | `plugin-address-standardizer-data-us-enable`|                     |
| address_standardizer         | `plugin-address-standardizer-enable`        |                     |
| pgAudit                      | `plugin-audit-enable`                       |                     |
| bloom                        | `plugin-bloom-enable`                       |                     |
| bool_plperl                  | `plugin-bool-plperl-enable`                 |                     |
| btree_gin                    | `plugin-btree-gin-enable`                   |                     |
| btree_gist                   | `plugin-btree-gist-enable`                  |                     |
| citext                       | `plugin-citext-enable`                      |                     |
| cube                         | `plugin-cube-enable`                        |                     |
| debversion                   | `plugin-debversion-enable`                  |                     |
| dict_int                     | `plugin-dict-int-enable`                    |                     |
| dict_xsyn                    | `plugin-dict-xsyn-enable`                   |                     |
| earthdistance                | `plugin-earthdistance-enable`               |                     |
| fuzzystrmatch                | `plugin-fuzzystrmatch-enable`               |                     |
| hll                          | `plugin-hll-enable`                         |                     |
| hstore                       | `plugin-hstore-enable`                      |                     |
| hypopg                       | `plugin-hypopg-enable`                      |                     |
| icu_ext                      | `plugin-icu-ext-enable`                     |                     |
| intarray                     | `plugin-intarray-enable`                    |                     |
| ip4r                         | `plugin-ip4r-enable`                        |                     |
| isn                          | `plugin-isn-enable`                         |                     |
| jsonb_plperl                 | `plugin-jsonb-plperl-enable`                |                     |
| lo                           | `plugin-lo-enable`                          |                     |
| ltree                        | `plugin-ltree-enable`                       |                     |
| old_snapshot                 | `plugin-old-snapshot-enable`                |                     |
| orafce                       | `plugin-orafce-enable`                      |                     |
| pg_freespacemap              | `plugin-pg-freespacemap-enable`             |                     |
| pg_similarity                | `plugin-pg-similarity-enable`               |                     |
| pg_trgm                      | `plugin-pg-trgm-enable`                     |                     |
| pg_visibility                | `plugin-pg-visibility-enable`               |                     |
| pgrowlocks                   | `plugin-pgrowlocks-enable`                  |                     |
| pgstattuple                  | `plugin-pgstattuple-enable`                 |                     |
| pg_stat_statements           | `plugin-pg-stat-statements-enable`          |                     |
| plperl                       | `plugin-plperl-enable`                      |                     |
| PL/Python                    | `plugin-plpython3u-enable`                  |                     |
| pltcl                        | `plugin-pltcl-enable`                       |                     |
| postgis                      | `plugin-postgis-enable`                     |                     |
| postgis_raster               | `plugin-postgis-raster-enable`              |                     |
| postgis_tiger_geocoder       | `plugin-postgis-tiger-geocoder-enable`      |                     |
| postgis_topology             | `plugin-postgis-topology-enable`            |                     |
| prefix                       | `plugin-prefix-enable`                      |                     |
| rdkit                        | `plugin-rdkit-enable`                       |                     |
| seg                          | `plugin-seg-enable`                         |                     |
| spi                          | `plugin-spi-enable`                         |                     |
| tablefunc                    | `plugin-tablefunc-enable`                   |                     |
| tcn                          | `plugin-tcn-enable`                         |                     |
| tds_fdw                      | `plugin-tds-fdw-enable`                     |                     |
| timescaledb                  | `plugin-timescaledb-enable`                 |                     |
| tsm_system_rows              | `plugin-tsm-system-rows-enable`             |                     |
| tsm_system_time              | `plugin-tsm-system-time-enable`             |                     |
| unaccent                     | `plugin-unaccent-enable`                    |                     |
| uuid_ossp                    | `plugin-uuid-ossp-enable`                   |                     |
| pgvector                     | `plugin-vector-enable`                      |                     |