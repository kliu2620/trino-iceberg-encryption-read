# trino-iceberg-encryption-read

Production-ready port of [trinodb/trino#28389](https://github.com/trinodb/trino/pull/28389)
"Add read support for Iceberg Parquet encryption" on top of an
**Iceberg 1.11.0-SNAPSHOT** baseline (the upstream PR's catalog wiring,
e.g. `apache/iceberg#13066`, only fully works with 1.11.0+).

End-to-end exercised against:

* Trino HEAD on this branch (482-SNAPSHOT)
* Iceberg 1.11.0-SNAPSHOT built from `apache/iceberg`
  `apache-iceberg-1.11.0-rc4`
* **Spark 3.5.6** (writer)
* Apache Hive 4.0.0 metastore (catalog)
* LocalStack 3.7 (KMS for the `AwsKeyManagementClient`)

## Why this branch exists

Trino's PR #28389 wires Iceberg's encryption read path into the Iceberg
connector, but at the time it was opened (Feb 2026) Iceberg 1.10.x did
not yet wire `StandardEncryptionManager` into HiveCatalog (only landed
in `apache/iceberg#13066`, Oct 2025). The PR ships a 2 KLOC change but
its end-to-end product test never converged because Iceberg 1.11.0 has
not been released yet.

This branch:

1. Brings PR #28389 forward.
2. Brings PR #26640 forward (Iceberg 1.11.0 upgrade).
3. Adapts the encryption code in trino-iceberg to the 1.11.0 API
   (`EncryptionUtil.createEncryptionManager` now takes the catalog-managed
   `List<EncryptedKey>`; `TableMetadata` carries `encryptionKeys()`;
   commit time must persist any newly generated manifest list keys back
   into table metadata).
4. Fills in the upstream PR's missing `newInputFile(ManifestListFile)`
   override so Trino can decrypt encrypted Avro manifest lists.
5. Adds an end-to-end test (`TestIcebergSparkEncryptionRead`) and a
   reproducible local harness under `iceberg-encryption-e2e/`.

## Verified scenarios

| Test class                                           | Pass | Notes                                              |
|------------------------------------------------------|------|----------------------------------------------------|
| `TestIcebergParquetEncryption`                       | 10/10 | unit tests bundled with PR #28389 (uses Iceberg API path) |
| `TestIcebergEncryptionConfig`                         | 2/2  | catalog property mapping                           |
| `TestKmsClientInstantiation`                          | 2/2  | `AwsKeyManagementClient` / `GcpKeyManagementClient` discovery |
| `TestParquetPredicates`                               | 5/5  | parquet predicate / decryption properties          |
| `TestIcebergSplitSource`                              | 9/9  | per-file decryption-data plumbing                  |
| `TestIcebergPageSourceProvider`                       | 3/3  | factory wiring                                     |
| `TestIcebergV2`                                       | 57/57 | regression on V2/MoR delete file path              |
| `TestIcebergMergeAppend`                              | 4/4  | merge / append commit path                         |
| **`TestIcebergSparkEncryptionRead` (new, this branch)** | **8/8** | **Spark 3.5.6 writer ↔ Trino reader, via HMS + LocalStack KMS** |

End-to-end coverage in `TestIcebergSparkEncryptionRead`:

* basic encrypted V3 table SELECT (with predicate pushdown)
* partitioned encrypted V3 table read (per-partition aggregation, partition prune)
* encrypted table with mixed types (int / long / string / double / decimal /
  date / timestamp / boolean / array<int> / map<string,int>)
* plaintext sibling table read (catalog-level encryption setup must not
  affect plaintext reads)
* `$files` metadata table assertion: every encrypted data file has
  non-null `key_metadata`; plaintext sibling has none
* INSERT and CTAS to encrypted tables fail with the expected error
* V3 default copy-on-write `DELETE` (100 rows in / 6 deleted), Trino
  returns 94 with predicate pruning; `$files` shows no DV/Puffin entry
  -- confirming we are on the supported (currently bug-free) write path

## Repository layout

```
.                                            # standard Trino monorepo (482-SNAPSHOT)
├── plugin/trino-iceberg/...                 # PR #28389 + 1.11 adaptations
│   └── src/test/java/io/trino/plugin/iceberg/TestIcebergSparkEncryptionRead.java
├── iceberg-encryption-e2e/                  # standalone harness; see its README
│   ├── docker/docker-compose.yml
│   ├── spark-app/{spark-defaults.conf, write-encrypted.sql, run-spark-write.sh}
│   ├── trino-conf/iceberg.properties
│   └── README.md
└── README.md  / SUMMARY.md
```

## Branch / commit overview

```
9df10b16f8a  Iceberg encryption: end-to-end Spark 3.5.6 -> Trino read test
05584715a14  Iceberg encryption: adapt to 1.11.0 API and persist manifest list keys
6423b424499  fixup! Update Iceberg to 1.11.0           (cherry-picked from #26640)
fbd4a5ddc64  Use MetricsConfig.forPositionDelete()     (cherry-picked from #26640)
adc6e4b0419  Test incorrect result by required field   (cherry-picked from #26640)
ab404863d38  Test time travel with schema evolution    (cherry-picked from #26640)
d333b8bd31c  Update Iceberg to 1.11.0                  (cherry-picked from #26640)
e5c08a3705f  WIP fix Spark <-> Trino PT                (cherry-picked from #28389)
56168868135  Add read support for encrypted Iceberg Parquet tables. (cherry-picked from #28389)
```

## Known upstream limitation

V3 + `merge-on-read` DELETE on encrypted tables hits an upstream bug:
`BaseDVFileWriter.createDV(...)` in Iceberg 1.11.0-rc4 does **not** set
`encryption_key_metadata` on the resulting `DeleteFile`, so the manifest
entry's `key_metadata` is null while the on-disk Puffin is encrypted.

* Issue: <https://github.com/apache/iceberg/issues/16157>
* Fix:   <https://github.com/apache/iceberg/pull/16158> (open, in review)

Workaround: stay on the V3 default `copy-on-write` mode, which we cover
in `testReadEncryptedTableWithCopyOnWriteDeletes`. Once #16158 lands
into 1.11 GA, the MoR/DV path will work transparently with no Trino
changes (read code already consumes `DeleteFile.keyMetadata()` correctly).

## Quickstart

See `iceberg-encryption-e2e/README.md` for the bootstrap commands
(building Iceberg 1.11.0-SNAPSHOT locally, starting Postgres + HMS +
LocalStack, creating the KMS key, building Trino, running the Spark
writer, then the Trino reader test).

## Docker image

The Trino server with this branch is published as
`docker.io/kliu2620/trino-iceberg-encryption:482-SNAPSHOT`.

```bash
docker pull kliu2620/trino-iceberg-encryption:482-SNAPSHOT
docker run --rm -p 8080:8080 kliu2620/trino-iceberg-encryption:482-SNAPSHOT
```

The image is built with the standard Trino server-rpm tarball plus the
iceberg connector from this branch and JDK 25.

## License

Trino code remains under Apache License 2.0, same as upstream
`trinodb/trino`.
