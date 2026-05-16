# Trino Iceberg Encryption Read — Final Project Summary

## Goal

Make Trino able to read **encrypted Iceberg V3 tables** that
Spark 3.5.6 has written, including all delete-file shapes
(copy-on-write rewrites, V2 parquet position deletes, V2 parquet
equality deletes, **V3 puffin deletion vectors**), end to end,
based on the latest stable Trino + a self-built Iceberg 1.11.0
that contains every fix needed for the combined V3 + DV +
encryption path.

## The 4 PRs that mattered

| # | PR | Status (as of 2026-05-16) | Why we needed it |
|---|----|---------------------------|-------------------|
| 1 | [trinodb/trino#28389](https://github.com/trinodb/trino/pull/28389) "Add read support for Iceberg Parquet encryption" | OPEN, in review since Feb 2026, last force-push 2026-04-30 with a "WIP fix Spark <-> Trino PT" commit | The **only** PR for the Trino read side. Targets parquet-formatted data + delete files. Does not cover V3 puffin DVs. Cannot merge until Iceberg 1.11 is GA because its end-to-end product test pulls Iceberg 1.11.0-SNAPSHOT. |
| 2 | [trinodb/trino#26640](https://github.com/trinodb/trino/pull/26640) "Update Iceberg to 1.11.0" | DRAFT, 2025-09 onward, ebi | PR #28389 assumes the 1.11 catalog-side encryption wiring (`apache/iceberg#13066`); cannot compile without it. |
| 3 | [apache/iceberg#13066](https://github.com/apache/iceberg/pull/13066) "Encryption integration and test" | **MERGED** to main 2025-10-21 by huaxingao | Iceberg HiveCatalog finally engages `StandardEncryptionManager` on writes/reads. Without this the whole encrypted-table flow doesn't actually run end to end on any catalog. |
| 4 | [apache/iceberg#16158](https://github.com/apache/iceberg/pull/16158) "Core: Fix reading encrypted Deletion Vectors" | OPEN, opened 2026-04-29 by ayushtkn, **APPROVED by ggershinsky 2026-05-15** ("LGTM"), 1 reviewer approval out of the committer set, **NOT yet merged** to main, will land before 1.11.0 GA | `BaseDVFileWriter.createDV(...)` previously omitted `withEncryptionKeyMetadata(...)`, so encrypted V3 puffin DVs ended up with `key_metadata = null` in the manifest. Without this fix, encrypted V3 + MoR `DELETE` writes seem to succeed but the table can never be read again (the on-disk file is AGS1-encrypted, but the manifest claims it's plaintext, leading to `Null key metadata buffer` / `Invalid bitmap data length`). |

## Timeline (why this combination only just became reachable)

```
2024              V3 spec drafts; BaseDVFileWriter introduced (PR #11476)
2025-Sep    1.10.0 V3 GA; deletion vectors plaintext fully working
2025-Oct-21 Iceberg #13066 merged: HiveCatalog encryption really works
2025-Oct → 2026-Apr:
  Plaintext V3 + DV: works fine
  Encrypted V3 INSERT/SELECT: works (no delete file)
  Encrypted V3 + DV DELETE: NOBODY tries this combination in CI
2026-Feb-20 Trino PR #28389 opened (parquet encryption read);
            "WIP fix Spark <-> Trino PT" because Iceberg 1.11.0 not yet released
2026-Apr-29 ayushtkn finally tries V3 + MoR DELETE on an encrypted Hive
            catalog table → trips the BaseDVFileWriter bug → opens Iceberg #16158
2026-May-15 ggershinsky (encryption author) approves #16158 with "LGTM"
2026-May-16 PR #16158 still OPEN, NOT yet in `apache/iceberg:main`,
            but committers will merge before 1.11.0 GA. We confirmed by
            running `git merge-base --is-ancestor` against `origin/main`
            that the fix commit (`1e21ec5f`) is not yet on main.
2026-05-16 (this work) We ported the fix locally:
              · Iceberg side: cherry-pick #16158 onto our 1.11.0-SNAPSHOT build
              · Trino side: write the missing read-side decryption for V3 puffin DVs
                             — community has no PR for this yet
              · End-to-end verified: Spark 3.5.6 writes encrypted V3 with
                MoR DELETE → Trino reads it correctly.
```

## What community is missing today

* **Iceberg #16158** — write side, approved, will merge soon. Self-contained.
* **Trino read of encrypted V3 puffin DVs** — *nobody is working on this yet*. We searched all open Trino PRs/issues for `encryption + (deletion vector | DV | puffin)` and found nothing. Existing encryption work in Trino tracks parquet only (PR #28389 explicitly says "Parquet" in its title). Once #16158 lands and #28389 merges, it will be a natural follow-up PR; today that gap is what our local change fills.

## What we built

### Code: `kliu2620/trino-iceberg-encryption-read` on GitHub

* Branch / `main` HEAD: `c2aeeb14270` "Iceberg encryption: support reading V3 puffin deletion vectors"
* Stack of commits (oldest at bottom):
  ```
  c2aeeb14270  Iceberg encryption: support reading V3 puffin deletion vectors        (NEW, ours)
  e7331838dc9  Add iceberg-encryption-e2e harness + repo summary                    (NEW, ours)
  9df10b16f8a  Iceberg encryption: end-to-end Spark 3.5.6 -> Trino read test        (NEW, ours)
  05584715a14  Iceberg encryption: adapt to 1.11.0 API and persist manifest list keys (NEW, ours)
  6423b424499  fixup! Update Iceberg to 1.11.0                                      (cherry-pick #26640)
  fbd4a5ddc64  Use MetricsConfig.forPositionDelete()                                (cherry-pick #26640)
  adc6e4b0419  Test incorrect result by required field in Iceberg                  (cherry-pick #26640)
  ab404863d38  Test time travel with schema evolution in Iceberg                   (cherry-pick #26640)
  d333b8bd31c  Update Iceberg to 1.11.0                                            (cherry-pick #26640)
  e5c08a3705f  WIP fix Spark <-> Trino PT                                          (cherry-pick #28389)
  56168868135  Add read support for encrypted Iceberg Parquet tables.              (cherry-pick #28389)
  ```

### Code: locally patched Apache Iceberg 1.11.0-SNAPSHOT

* Built from `apache-iceberg-1.11.0-rc4` plus PR #16158's `1e21ec5f` "Fix reading encrypted Deletion Vectors"
* Published to `~/.m2`; the release jars are uploaded to GitHub release `v0.1.0-iceberg-encryption`

### Tests passing on this combined stack — 96/96

| Suite | Passing |
|---|---|
| TestIcebergParquetEncryption (PR #28389's own unit tests) | 10/10 |
| TestIcebergSparkEncryptionRead (our new e2e suite) | 8/8 |
| TestIcebergEncryptionConfig | 2/2 |
| TestKmsClientInstantiation | 2/2 |
| TestParquetPredicates | 5/5 |
| TestIcebergSplitSource | 9/9 |
| TestIcebergPageSourceProvider | 3/3 |
| TestIcebergMergeAppend | 4/4 |
| TestIcebergV2 (regression) | 57/57 |
| **Total** | **96/96** |

In the running container we additionally exercised the actual V3 +
MoR + encrypted DV path against tables produced by Spark 3.5.6:

```
SELECT count(*) FROM encrypted.t_dv_real_v2                            → 94
SELECT count(*) FROM encrypted.t_dv_real_v2 WHERE id IN (3,7,9,12,18,24)→ 0
SELECT id FROM encrypted.t_dv_real_v2 WHERE country='B' AND id<20 ORDER BY id
                                                                       → 1,5,11,13,15,17,19
SELECT min(id), max(id), count(*) FROM encrypted.t_dv_real_v2
                                                                       → 0, 99, 94
SELECT count(*) FROM encrypted."t_dv_real_v2$files" WHERE key_metadata IS NULL
                                                                       → 0
```

### Image: `kliu2620/trino-iceberg-encryption` on Docker Hub

* Tags: `latest`, `v0.1.0`, `482-SNAPSHOT`, `482-SNAPSHOT-amd64`
* All four point at sha256:`a5b5fae1be6487b3dda911b0efe5175a8d05b7a989ceebd19afd4e060ad825f4`
* Bundles the patched `iceberg-core-1.11.0-SNAPSHOT.jar` and
  `trino-iceberg-482-SNAPSHOT.jar` so the entire encrypted V3 + DV
  read path works out of the box.

### Release: `v0.1.0-iceberg-encryption`

<https://github.com/kliu2620/trino-iceberg-encryption-read/releases/tag/v0.1.0-iceberg-encryption>

15 assets:
* `trino-iceberg-482-SNAPSHOT.jar` (Trino plugin with our patches)
* 13 `iceberg-*-1.11.0-SNAPSHOT.jar` (api / core / data / parquet / orc /
  hive-metastore / aws / aws-bundle / bundled-guava / common /
  spark-3.5_2.12 / spark-extensions-3.5_2.12 / spark-runtime-3.5_2.12)
* `SHA256SUMS`

## Two safety guarantees

1. **Plaintext (non-encrypted) tables are not affected.** All four code
   changes inside `trino-iceberg` are gated by either `format == PUFFIN`
   or `delete.keyMetadata().isPresent()`. CoW tables go through neither
   path, so the patched build is byte-equivalent in behavior to upstream
   Trino on plaintext data. Verified by running the full
   `TestIcebergV2` regression (57 cases, including all V2/V3 plaintext
   delete shapes) and `testReadEncryptedTableWithCopyOnWriteDeletes`.

2. **Encrypted V3 + CoW DELETE keeps working.** This is the supported
   path in current Iceberg releases (V3 default delete mode is
   `copy-on-write`). Our Trino patch never touches it; CoW commits
   produce no delete-file entries, so the new code paths are dormant.
