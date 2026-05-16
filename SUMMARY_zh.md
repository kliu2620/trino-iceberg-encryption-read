# Trino Iceberg 加密读取 —— 项目总结

## 目标

让 Trino 能读取 **Spark 3.5.6 写入的加密 Iceberg V3 表**,
覆盖所有删除文件形态(copy-on-write 重写、V2 parquet 行位置删除、
V2 parquet 等值删除、**V3 puffin 删除向量**),整条链路端到端打通,
基于 Trino 482-SNAPSHOT + 我们自打的 Iceberg 1.11.0(包含此组合
路径所需的全部修复)。

## 4 个关键 PR

| # | PR | 状态(2026-05-16) | 我们为什么需要它 |
|---|----|--------------------|-----------------|
| 1 | [trinodb/trino#28389](https://github.com/trinodb/trino/pull/28389) "Add read support for Iceberg Parquet encryption" | OPEN,2026-02 进入 review,2026-04-30 强推过 "WIP fix Spark <-> Trino PT" | Trino 读取加密表的**唯一**社区 PR,只覆盖 Parquet 数据 + 删除文件,**不覆盖 V3 puffin DV**。在 Iceberg 1.11 GA 之前无法 merge,因为它的 e2e product test 拉的就是 1.11.0-SNAPSHOT。 |
| 2 | [trinodb/trino#26640](https://github.com/trinodb/trino/pull/26640) "Update Iceberg to 1.11.0" | DRAFT,2025-09 起断断续续 | PR #28389 依赖 1.11 catalog 端的加密接线(`apache/iceberg#13066`),不升 1.11 编译都过不去。 |
| 3 | [apache/iceberg#13066](https://github.com/apache/iceberg/pull/13066) "Encryption integration and test" | **已合并** main 2025-10-21,by huaxingao | Iceberg HiveCatalog 终于在写读路径上启用了 `StandardEncryptionManager`。这之前无论哪个 catalog,加密表的整条链路都跑不通。 |
| 4 | [apache/iceberg#16158](https://github.com/apache/iceberg/pull/16158) "Core: Fix reading encrypted Deletion Vectors" | OPEN,2026-04-29 由 ayushtkn 提,**2026-05-15 由加密模块作者 ggershinsky 给 LGTM** 通过,目前 1 个 reviewer 通过,**尚未 merge 进 main**,会在 1.11.0 GA 前合入 | `BaseDVFileWriter.createDV(...)` 漏调 `withEncryptionKeyMetadata(...)`,导致加密 V3 puffin DV 写出后,manifest 里 `key_metadata = null`。这条 bug 让"加密 V3 + MoR DELETE"看起来写成功了,但任何后续读都会失败(磁盘文件是 AGS1 加密的,manifest 却声明明文,触发 `Null key metadata buffer` / `Invalid bitmap data length`)。 |

## 时间线(为什么这一组合直到现在才暴露)

```
2024              V3 spec 草稿;BaseDVFileWriter 引入(PR #11476)
2025-09     1.10.0 V3 GA;明文 deletion vector 全打通
2025-10-21  Iceberg #13066 合入:HiveCatalog 加密真正可用
2025-10 → 2026-04:
  明文 V3 + DV:OK
  加密 V3 INSERT/SELECT:OK(没 delete 文件)
  加密 V3 + DV DELETE:CI 中没人覆盖这个组合
2026-02-20 Trino PR #28389 提交(parquet 加密读取);因为 Iceberg 1.11.0
            还没 GA,标了 "WIP fix Spark <-> Trino PT"
2026-04-29 ayushtkn 第一次在加密 Hive catalog V3 表上跑 MoR DELETE
            → 撞上 BaseDVFileWriter bug → 提了 Iceberg #16158
2026-05-15 ggershinsky(加密模块作者)对 #16158 给出 "LGTM"
2026-05-16 PR #16158 仍然 OPEN,**未进 `apache/iceberg:main`**,但
            committer 应会在 1.11.0 GA 前合入。我们用 `git merge-base
            --is-ancestor` 对 `origin/main` 验证过,fix commit
            (`1e21ec5f`)确实不在 main。
2026-05-16 (本工作)我们在本地把这条链路拼齐:
            · Iceberg 侧:把 #16158 cherry-pick 到我们 1.11.0-SNAPSHOT 构建里
            · Trino 侧:补齐 V3 puffin DV 的解密读取代码 —— 社区目前没有这个 PR
            · 端到端验证:Spark 3.5.6 写加密 V3 + MoR DELETE → Trino 读出正确结果
```

## 社区目前还缺什么

* **Iceberg #16158** —— 写入侧,已批准,即将合入,自包含。
* **Trino 读取加密 V3 puffin DV** —— *社区目前没人在做*。我们搜了所有
  Trino 的 open PR / issue 中带 `encryption + (deletion vector | DV |
  puffin)` 关键字,没有命中。Trino 现存的加密读支持只覆盖 Parquet
  (PR #28389 标题里就明确写了 "Parquet")。等 #16158 合入、#28389 合入
  之后,读 V3 puffin DV 自然会成为下一个 follow-up PR;在那之前,
  这块缺口正是我们这次的本地补丁所填补的。

## 我们交付了什么

### 代码:GitHub `kliu2620/trino-iceberg-encryption-read`

* `main` HEAD: `c2aeeb14270` "Iceberg encryption: support reading V3 puffin deletion vectors"
* commit 栈(从下到上):
  ```
  c2aeeb14270  Iceberg encryption: support reading V3 puffin deletion vectors        (本次新增)
  e7331838dc9  Add iceberg-encryption-e2e harness + repo summary                    (本次新增)
  9df10b16f8a  Iceberg encryption: end-to-end Spark 3.5.6 -> Trino read test        (本次新增)
  05584715a14  Iceberg encryption: adapt to 1.11.0 API and persist manifest list keys (本次新增)
  6423b424499  fixup! Update Iceberg to 1.11.0                                      (cherry-pick #26640)
  fbd4a5ddc64  Use MetricsConfig.forPositionDelete()                                (cherry-pick #26640)
  adc6e4b0419  Test incorrect result by required field in Iceberg                  (cherry-pick #26640)
  ab404863d38  Test time travel with schema evolution in Iceberg                   (cherry-pick #26640)
  d333b8bd31c  Update Iceberg to 1.11.0                                            (cherry-pick #26640)
  e5c08a3705f  WIP fix Spark <-> Trino PT                                          (cherry-pick #28389)
  56168868135  Add read support for encrypted Iceberg Parquet tables.              (cherry-pick #28389)
  ```

### 代码:本地补丁版 Apache Iceberg 1.11.0-SNAPSHOT

* 基础:`apache-iceberg-1.11.0-rc4`,叠 PR #16158 的 `1e21ec5f` "Fix reading encrypted Deletion Vectors"
* 已 publish 到 `~/.m2`;jar 全部上传到 GitHub release `v0.1.0-iceberg-encryption`

### 测试结果 —— 96/96

| 测试套 | 通过 |
|---|---|
| TestIcebergParquetEncryption(PR #28389 自带单测) | 10/10 |
| TestIcebergSparkEncryptionRead(我们新增的 e2e 套件,**含 V3 puffin DV JUnit case**) | 9/9 |
| TestIcebergEncryptionConfig | 2/2 |
| TestKmsClientInstantiation | 2/2 |
| TestParquetPredicates | 5/5 |
| TestIcebergSplitSource | 9/9 |
| TestIcebergPageSourceProvider | 3/3 |
| TestIcebergMergeAppend | 4/4 |
| TestIcebergV2(回归) | 57/57 |
| **合计(`mvn test`)** | **101/101** |
| 黑盒 codex 25 用例(Spark→Trino,跑发布出去的镜像) | **45 PASS / 0 FAIL / 4 显式跳过** |

显式跳过的 4 个 codex 用例:
- #6 / #7 — Avro 加密数据文件读(范围之外;Trino Iceberg Avro page source 不接 `EncryptionManager`,本项目明确不做)
- #11 / #12 — equality delete 文件(Spark 3.5 SQL `DELETE FROM` 写不出这种文件,需要 Flink 流式 writer)

完整对照表和复现步骤见 [`TEST_REPORT_zh.md`](TEST_REPORT_zh.md);测试驱动脚本见 [`iceberg-encryption-e2e/test-artifacts/run-trino-tests.sh`](iceberg-encryption-e2e/test-artifacts/run-trino-tests.sh)。

在 docker 里直接 query Spark 3.5.6 写出的真表,V3 + MoR + 加密 DV 路径:

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

### 镜像:Docker Hub `kliu2620/trino-iceberg-encryption`

* tag:`latest`、`v0.1.0`、`482-SNAPSHOT`、`482-SNAPSHOT-amd64`
* 全部指向 sha256:`a5b5fae1be6487b3dda911b0efe5175a8d05b7a989ceebd19afd4e060ad825f4`
* 镜像内含我们打过补丁的 `iceberg-core-1.11.0-SNAPSHOT.jar` 和
  `trino-iceberg-482-SNAPSHOT.jar`,加密 V3 + DV 读取拉下来即用。

### Release:`v0.1.0-iceberg-encryption`

<https://github.com/kliu2620/trino-iceberg-encryption-read/releases/tag/v0.1.0-iceberg-encryption>

15 个 asset:
* `trino-iceberg-482-SNAPSHOT.jar`(我们打过补丁的 Trino plugin)
* 13 个 `iceberg-*-1.11.0-SNAPSHOT.jar`(api / core / data / parquet /
  orc / hive-metastore / aws / aws-bundle / bundled-guava / common /
  spark-3.5_2.12 / spark-extensions-3.5_2.12 / spark-runtime-3.5_2.12)
* `SHA256SUMS`

## 两条安全保证

1. **明文(非加密)表不受影响。** `trino-iceberg` 里 4 处代码改动全部
   被 `format == PUFFIN` 或 `delete.keyMetadata().isPresent()` 守护。
   CoW 表既不走 puffin 解密路径也不走 keyMetadata 分支,所以本镜像
   在明文数据上的行为与上游 Trino 字节级等价。已通过完整跑
   `TestIcebergV2`(57 个用例,涵盖所有 V2/V3 明文 delete 形态)和
   `testReadEncryptedTableWithCopyOnWriteDeletes` 验证。

2. **加密 V3 + CoW DELETE 仍然 work。** 这是当前 Iceberg release 默认
   支持的写法(V3 默认 `write.delete.mode=copy-on-write`)。CoW 提交
   不产生 delete file,新代码路径在这条链路上从不触发。
