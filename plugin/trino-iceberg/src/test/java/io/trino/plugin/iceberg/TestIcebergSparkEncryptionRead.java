/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Map;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test that reads encrypted Iceberg tables written by Spark 3.5.6 +
 * Iceberg 1.11.0-SNAPSHOT.
 *
 * Prerequisites (the helper script under iceberg-encryption-e2e/ sets these up):
 *  - Hive Metastore on localhost:9083
 *  - LocalStack KMS on localhost:4566 with alias/iceberg-test
 *  - {@code iceberg.encrypted.t_basic / t_partitioned / t_types / t_plaintext}
 *    pre-populated by the Spark write script.
 *
 * Run with:
 *   mvn -pl plugin/trino-iceberg test \
 *       -Dtest=TestIcebergSparkEncryptionRead \
 *       -DICEBERG_ENC_E2E=1
 */
@EnabledIfEnvironmentVariable(named = "ICEBERG_ENC_E2E", matches = "1")
public class TestIcebergSparkEncryptionRead
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // AwsKeyManagementClient picks up the LocalStack endpoint via env vars set by the
        // shell wrapper (AWS_ENDPOINT_URL_KMS, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
        // TestingIcebergConnectorFactory installs a local-filesystem binder by default,
        // so we must NOT also enable the production fs.local.enabled (would duplicate-bind).
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "HIVE_METASTORE")
                .put("hive.metastore.uri", "thrift://localhost:9083")
                .put("iceberg.encryption.kms-type", "AWS")
                .put("iceberg.register-table-procedure.enabled", "true")
                .buildOrThrow();

        return IcebergQueryRunner.builder("encrypted")
                .setIcebergProperties(properties)
                .disableSchemaInitializer()
                .build();
    }

    @Test
    public void testReadBasicEncryptedTable()
    {
        // 7 rows written by Spark INSERT (5 + 2 separate inserts).
        assertThat(computeActual("SELECT id, name, amount FROM encrypted.t_basic ORDER BY id"))
                .isEqualTo(resultBuilder(getSession(), INTEGER, VARCHAR, DOUBLE)
                        .row(1, "alice", 100.5)
                        .row(2, "bob", 200.25)
                        .row(3, "charlie", 300.75)
                        .row(4, "dave", 400.0)
                        .row(5, "ellen", 500.5)
                        .row(6, "frank", 600.0)
                        .row(7, "grace", 700.0)
                        .build());

        assertThat(query("SELECT count(*) FROM encrypted.t_basic"))
                .matches("VALUES BIGINT '7'");

        // Predicate pushdown over Parquet
        assertThat(query("SELECT name FROM encrypted.t_basic WHERE id = 3"))
                .matches("VALUES VARCHAR 'charlie'");

        assertThat(query("SELECT name FROM encrypted.t_basic WHERE amount > 350"))
                .matches("VALUES VARCHAR 'dave', VARCHAR 'ellen', VARCHAR 'frank', VARCHAR 'grace'");
    }

    @Test
    public void testReadPartitionedEncryptedTable()
    {
        assertThat(query("SELECT count(*) FROM encrypted.t_partitioned"))
                .matches("VALUES BIGINT '6'");

        // Per-partition aggregate exercises split planning + decryption per partition.
        assertThat(computeActual("SELECT country, count(*) FROM encrypted.t_partitioned GROUP BY country ORDER BY country"))
                .isEqualTo(resultBuilder(getSession(), VARCHAR, BIGINT)
                        .row("JP", 1L)
                        .row("UK", 3L)
                        .row("US", 2L)
                        .build());

        // Partition pruning
        assertThat(query("SELECT name FROM encrypted.t_partitioned WHERE country = 'JP'"))
                .matches("VALUES VARCHAR 'frank'");
    }

    @Test
    public void testReadEncryptedTableWithVariousTypes()
    {
        assertThat(query("SELECT count(*) FROM encrypted.t_types"))
                .matches("VALUES BIGINT '2'");

        // Touch every column type so the entire row group is decrypted.
        MaterializedResult rows = computeActual("SELECT c_int, c_long, c_str, c_double, c_dec, c_date, c_bool, c_arr, c_map FROM encrypted.t_types ORDER BY c_int");
        assertThat(rows.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testReadPlaintextSiblingTableUnaffected()
    {
        assertThat(query("SELECT id FROM encrypted.t_plaintext ORDER BY id"))
                .matches("VALUES INTEGER '1', INTEGER '2'");
    }

    @Test
    public void testFilesMetadataTableShowsEncryption()
    {
        // Every data file in the encrypted tables must have non-null key_metadata,
        // mirroring the assertion ebi requested in PR #28389 reviews.
        assertThat(query("SELECT count(*) FROM encrypted.\"t_basic$files\" WHERE key_metadata IS NULL"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM encrypted.\"t_partitioned$files\" WHERE key_metadata IS NULL"))
                .matches("VALUES BIGINT '0'");

        // Plaintext sibling: no key metadata anywhere.
        assertThat(query("SELECT count(*) FROM encrypted.\"t_plaintext$files\" WHERE key_metadata IS NOT NULL"))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testWriteToEncryptedTableIsBlocked()
    {
        String table = "encrypted.t_basic";
        assertQueryFails(
                "INSERT INTO " + table + " VALUES (99, 'eve', 999.0)",
                ".*Writing to encrypted Iceberg tables is not supported.*");

        // CTAS to a new encrypted table should also be blocked (we don't have a Trino-side
        // KMS write path yet, only read).
        String name = "encrypted.ctas_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + name + " WITH (encryption_key_id = 'whatever') AS SELECT * FROM " + table,
                ".*(Writing to encrypted Iceberg tables is not supported|encryption_key_id).*");
    }

    /**
     * Confirms that a V3 encrypted table with copy-on-write DELETE (the V3 default) is
     * correctly read by Trino — i.e. the DV/Puffin write path is bypassed. This is the
     * recommended workaround until apache/iceberg fixes BaseDVFileWriter to set
     * key_metadata on Puffin DV manifest entries.
     */
    @Test
    public void testReadEncryptedTableWithCopyOnWriteDeletes()
    {
        // 100 rows inserted, 6 rows deleted via SparkSQL DELETE FROM (CoW).
        assertThat(query("SELECT count(*) FROM encrypted.t_cow_delete"))
                .matches("VALUES BIGINT '94'");

        // The deleted rows must be gone.
        assertThat(query("SELECT count(*) FROM encrypted.t_cow_delete WHERE id IN (3, 7, 9, 12, 18, 24)"))
                .matches("VALUES BIGINT '0'");

        // Partition pruning + decryption mix, must agree with Spark's view of the same data.
        assertThat(query("SELECT id FROM encrypted.t_cow_delete WHERE country = 'B' AND id < 20 ORDER BY id"))
                .matches("VALUES INTEGER '1', INTEGER '5', INTEGER '11', INTEGER '13', INTEGER '15', INTEGER '17', INTEGER '19'");

        // No DV or position-delete files should be present (CoW path).
        assertThat(query("SELECT count(*) FROM encrypted.\"t_cow_delete$files\" WHERE content <> 0"))
                .matches("VALUES BIGINT '0'");

        // Every remaining data file must still carry encryption key_metadata.
        assertThat(query("SELECT count(*) FROM encrypted.\"t_cow_delete$files\" WHERE key_metadata IS NULL"))
                .matches("VALUES BIGINT '0'");
    }
}
