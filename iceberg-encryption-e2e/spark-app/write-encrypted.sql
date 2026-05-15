-- Spark 3.5.6 + Iceberg 1.11.0-SNAPSHOT end-to-end encryption write script.
-- Iceberg 1.11 requires format-version=3 for encryption.
-- Note: V3 DELETE writes deletion vectors as Puffin files; in 1.11.0-rc4
-- DVUtil.writeDVs does not engage the encryption manager, so MoR delete tests
-- via SparkSQL DELETE are skipped here. Basic + partitioned table reads,
-- which exercise data-file + manifest + manifest-list decryption, are the
-- critical path for the Trino read PR.

CREATE NAMESPACE IF NOT EXISTS iceberg.encrypted;

-- ---------- 1. Basic encrypted table ----------
DROP TABLE IF EXISTS iceberg.encrypted.t_basic;
CREATE TABLE iceberg.encrypted.t_basic (id INT, name STRING, amount DOUBLE)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_basic VALUES
  (1, 'alice', 100.5),
  (2, 'bob',   200.25),
  (3, 'charlie', 300.75),
  (4, 'dave',  400.0),
  (5, 'ellen', 500.5);
INSERT INTO iceberg.encrypted.t_basic VALUES
  (6, 'frank', 600.0),
  (7, 'grace', 700.0);

-- ---------- 2. Partitioned encrypted table ----------
DROP TABLE IF EXISTS iceberg.encrypted.t_partitioned;
CREATE TABLE iceberg.encrypted.t_partitioned (id INT, name STRING, country STRING, ts DATE)
USING ICEBERG
PARTITIONED BY (country)
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_partitioned VALUES
  (1, 'alice',   'US', DATE'2026-05-15'),
  (2, 'bob',     'US', DATE'2026-05-16'),
  (3, 'charlie', 'UK', DATE'2026-05-15'),
  (4, 'dave',    'UK', DATE'2026-05-17'),
  (5, 'ellen',   'UK', DATE'2026-05-18'),
  (6, 'frank',   'JP', DATE'2026-05-19');

-- ---------- 3. Encrypted table with various column types ----------
DROP TABLE IF EXISTS iceberg.encrypted.t_types;
CREATE TABLE iceberg.encrypted.t_types (
  c_int INT, c_long BIGINT, c_str STRING, c_double DOUBLE, c_dec DECIMAL(10,3),
  c_date DATE, c_ts TIMESTAMP, c_bool BOOLEAN, c_arr ARRAY<INT>, c_map MAP<STRING,INT>)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_types VALUES
  (1, CAST(100000000000 AS BIGINT), 'alice', 1.5, 12.345,
   DATE'2026-05-15', TIMESTAMP'2026-05-15 10:30:00', true,
   ARRAY(1,2,3), MAP('a',1,'b',2)),
  (2, CAST(200000000000 AS BIGINT), 'bob',   2.5, 67.890,
   DATE'2026-05-16', TIMESTAMP'2026-05-16 11:30:00', false,
   ARRAY(4,5,6), MAP('c',3));

-- ---------- 4. Plaintext baseline table for differential testing ----------
DROP TABLE IF EXISTS iceberg.encrypted.t_plaintext;
CREATE TABLE iceberg.encrypted.t_plaintext (id INT, name STRING, amount DOUBLE)
USING ICEBERG
TBLPROPERTIES ('format-version'='2', 'write.format.default'='PARQUET');
INSERT INTO iceberg.encrypted.t_plaintext VALUES (1, 'alice', 100.5), (2, 'bob', 200.25);

-- Spark sanity reads (must succeed for the writer side to be considered correct)
SELECT 'basic'       AS label, count(*) AS n FROM iceberg.encrypted.t_basic UNION ALL
SELECT 'partitioned',           count(*)    FROM iceberg.encrypted.t_partitioned UNION ALL
SELECT 'types',                 count(*)    FROM iceberg.encrypted.t_types UNION ALL
SELECT 'plaintext',             count(*)    FROM iceberg.encrypted.t_plaintext
ORDER BY label;
