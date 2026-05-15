# Iceberg encryption end-to-end harness

Local scripts that go with this repo to verify, end-to-end, that:

> A Spark 3.5.6 writer running on Iceberg 1.11.0-SNAPSHOT can produce an
> encrypted Iceberg V3 table, and the patched Trino in this repo can
> read it correctly.

The repository is the sibling of this directory; build Trino from
`/plugin/trino-iceberg` first.

## Status snapshot

* Trino side
  * `feature/iceberg-encryption-read` carries:
    * `trinodb/trino#28389` cherry-picked (Iceberg Parquet encryption read)
    * `trinodb/trino#26640` cherry-picked (Iceberg 1.10.1 → 1.11.0)
    * 1.11.0 API adaptations (see `Iceberg encryption: adapt to 1.11.0 API` commit)
  * 88 unit tests + 8 e2e tests pass (see top-level SUMMARY).
* Spark side
  * `iceberg-spark-runtime-3.5_2.12:1.11.0-SNAPSHOT`,
    `iceberg-spark-extensions-3.5_2.12:1.11.0-SNAPSHOT`,
    `iceberg-aws-bundle:1.11.0-SNAPSHOT` published locally from
    `apache/iceberg` `apache-iceberg-1.11.0-rc4`.
  * SparkSQL `CREATE TABLE … TBLPROPERTIES('encryption.key-id'=…)` plus
    `INSERT` and copy-on-write `DELETE` produce **PARE**-encrypted Parquet
    data files, **AGS1**-encrypted Avro manifests / manifest lists, and
    persist `encryption-keys` into the metadata.
* End-to-end
  * `TestIcebergSparkEncryptionRead`: 8/8 cases pass (basic, partitioned,
    typed, plaintext sibling, `$files` metadata, write-blocked, CoW DELETE).

### Known upstream limitation: V3 + MoR DELETE + encryption

V3 SparkSQL `DELETE` with `write.delete.mode=merge-on-read` writes a
deletion vector in a Puffin file. In Iceberg 1.11.0-rc4,
`BaseDVFileWriter.createDV(...)` does **not** call
`.withEncryptionKeyMetadata(...)` on the resulting `DeleteFile`, so the
manifest entry's `key_metadata` is null even though the on-disk Puffin
is actually encrypted with `AGS1`. Reading the table fails with one of:

```
Null key metadata buffer
   at org.apache.iceberg.encryption.StandardKeyMetadata.castOrParse
```
or
```
Invalid bitmap data length: 4096, expected 36
   at org.apache.iceberg.deletes.BitmapPositionDeleteIndex.deserialize
```

The bug is tracked upstream:

* Issue: <https://github.com/apache/iceberg/issues/16157>
* Fix: <https://github.com/apache/iceberg/pull/16158> (open, in review)

The fix mirrors the path already in `BaseFileWriterFactory` for Parquet
position/equality deletes: hold onto the `EncryptedOutputFile`, extract
its `keyMetadata` once the Puffin is closed, and feed it into both the
manifest entry and (via `EncryptionUtil.setFileLength`) the buffer that
the reader will use to know the encrypted file's full length.

### Workaround until #16158 lands

Use the V3 default copy-on-write mode for any DELETE/UPDATE/MERGE on an
encrypted Iceberg table. The default of `write.delete.mode` is
`copy-on-write`, so unless you have explicitly switched it to
`merge-on-read` you are already fine. Locking the modes explicitly:

```sql
CREATE TABLE … TBLPROPERTIES (
  'format-version' = '3',                  -- required by encryption
  'encryption.key-id' = '<kms-key-id>',
  'write.delete.mode' = 'copy-on-write',
  'write.update.mode' = 'copy-on-write',
  'write.merge.mode'  = 'copy-on-write'
);
```

`testReadEncryptedTableWithCopyOnWriteDeletes` in
`TestIcebergSparkEncryptionRead.java` exercises this path explicitly so
that any future regression on the read side surfaces in CI.

## Prerequisites

```
JDK 25.0.1+    builds & runs Trino                 ~/Library/Java/JavaVirtualMachines/jdk-25.0.3+9
JDK 17         runs Spark 3.5.6                    /Library/Java/JavaVirtualMachines/zulu-17.jdk
Spark 3.5.6    https://archive.apache.org/dist/spark/spark-3.5.6/
Docker         postgres + apache/hive metastore + localstack
Iceberg src    apache-iceberg-1.11.0-rc4 checkout under /tmp/iceberg-enc-work/iceberg-main
```

## One-time bootstrap

### 1. Publish Iceberg 1.11.0-SNAPSHOT into the local maven repo

```bash
cd /tmp/iceberg-enc-work/iceberg-main
echo "1.11.0-SNAPSHOT" > version.txt
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  ./gradlew --no-daemon -x test -x integrationTest \
    :iceberg-bom:publishToMavenLocal :iceberg-api:publishToMavenLocal \
    :iceberg-bundled-guava:publishToMavenLocal :iceberg-common:publishToMavenLocal \
    :iceberg-core:publishToMavenLocal :iceberg-data:publishToMavenLocal \
    :iceberg-parquet:publishToMavenLocal :iceberg-orc:publishToMavenLocal \
    :iceberg-hive-metastore:publishToMavenLocal \
    :iceberg-aws:publishToMavenLocal :iceberg-aws-bundle:publishToMavenLocal \
    :iceberg-gcp:publishToMavenLocal :iceberg-gcp-bundle:publishToMavenLocal \
    :iceberg-azure:publishToMavenLocal :iceberg-azure-bundle:publishToMavenLocal \
    :iceberg-snowflake:publishToMavenLocal :iceberg-nessie:publishToMavenLocal

JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  ./gradlew --no-daemon -x test -x integrationTest \
    -DsparkVersions=3.5 -DscalaVersion=2.12 \
    :iceberg-spark:iceberg-spark-3.5_2.12:publishToMavenLocal \
    :iceberg-spark:iceberg-spark-extensions-3.5_2.12:publishToMavenLocal \
    :iceberg-spark:iceberg-spark-runtime-3.5_2.12:publishToMavenLocal
```

### 2. Bring up the docker stack

```bash
cd iceberg-encryption-e2e/docker
docker compose up -d
docker compose ps     # postgres, metastore (apache/hive:4.0.0), localstack all healthy
```

### 3. Create a KMS master key in LocalStack

```bash
KEY_JSON=$(curl -s -X POST 'http://localhost:4566/' \
  -H 'X-Amz-Target: TrentService.CreateKey' \
  -H 'Content-Type: application/x-amz-json-1.1' \
  -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/...' \
  -d '{"Description":"iceberg encryption test key"}')
echo "$KEY_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['KeyMetadata']['KeyId'])" \
  > .kms-key-id
```

### 4. Build Trino with this branch

```bash
JAVA_HOME=$HOME/Library/Java/JavaVirtualMachines/jdk-25.0.3+9/Contents/Home \
  ./mvnw -pl plugin/trino-iceberg -am -DskipTests -Dair.check.skip-all \
    -Daether.remoteRepositoryFilter.groupId=false \
    -Daether.remoteRepositoryFilter.prefixes=false install
```

## Run write + read

### Write encrypted tables with Spark 3.5.6

```bash
iceberg-encryption-e2e/spark-app/run-spark-write.sh
```

Final stdout should print:

```
basic         7
partitioned   6
plaintext     2
types         2
```

### Read with Trino's e2e test

```bash
ICEBERG_ENC_E2E=1 \
AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_ENDPOINT_URL_KMS=http://localhost:4566 \
JAVA_HOME=$HOME/Library/Java/JavaVirtualMachines/jdk-25.0.3+9/Contents/Home \
  ./mvnw -pl plugin/trino-iceberg test \
    -Dair.check.skip-all \
    -Daether.remoteRepositoryFilter.groupId=false \
    -Daether.remoteRepositoryFilter.prefixes=false \
    -Dtest=TestIcebergSparkEncryptionRead
```

8 cases must pass.

## Trino catalog snippet

```properties
connector.name=iceberg
iceberg.catalog.type=HIVE_METASTORE
hive.metastore.uri=thrift://<host>:9083
iceberg.encryption.kms-type=AWS                 # AWS | GCP | AZURE
# iceberg.encryption.plaintext-files-allowed-for-encrypted-tables=false
# AWS_ENDPOINT_URL_KMS / AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# travel via standard SDK env vars; with IAM Roles in production these are
# not needed.
```

## Cleanup

```bash
cd iceberg-encryption-e2e/docker
docker compose down -v
rm -rf iceberg-encryption-e2e/warehouse/*
```

## Layout

```
iceberg-encryption-e2e/
├── README.md                       # this file
├── docker/
│   ├── docker-compose.yml          # postgres + apache/hive:4.0.0 metastore + localstack KMS
│   └── hms-jars/postgresql.jar     # JDO driver pushed into HMS at startup
├── spark-app/
│   ├── spark-defaults.conf         # SparkCatalog (Hive) + AWS KMS wiring
│   ├── write-encrypted.sql         # CREATE/INSERT t_basic / t_partitioned / t_types / t_plaintext / t_cow_delete
│   └── run-spark-write.sh          # JDK17 + 1.11.0-SNAPSHOT jars wrapper
├── trino-conf/
│   └── iceberg.properties          # sample Trino catalog config
└── warehouse/                      # host path mounted at the same path inside HMS container
```
