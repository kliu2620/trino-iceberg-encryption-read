-- Spark 3.5.6 + Iceberg 1.11.0-SNAPSHOT end-to-end encryption write script.
-- Iceberg 1.11 requires format-version=3 for encryption.
-- key_id is the primary KMS key alias; key_id_b is a rotation key used for
-- the KEK-rotation test table.

CREATE NAMESPACE IF NOT EXISTS iceberg.encrypted;

-- ===================================================================
-- 1. Basic encrypted table (case 1, 8, 9, 22, 24)
-- ===================================================================
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
-- Second snapshot, second manifest -> covers cases 8, 9, 22.
INSERT INTO iceberg.encrypted.t_basic VALUES
  (6, 'frank', 600.0),
  (7, 'grace', 700.0);

-- ===================================================================
-- 2. Partitioned encrypted table (case 18)
-- ===================================================================
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

-- ===================================================================
-- 3. All Parquet types (case 2) -- now including BINARY and FLOAT
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_types;
CREATE TABLE iceberg.encrypted.t_types (
  c_int    INT,
  c_long   BIGINT,
  c_str    STRING,
  c_float  FLOAT,
  c_double DOUBLE,
  c_dec    DECIMAL(10,3),
  c_date   DATE,
  c_ts     TIMESTAMP,
  c_bool   BOOLEAN,
  c_bin    BINARY,
  c_arr    ARRAY<INT>,
  c_map    MAP<STRING,INT>)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_types VALUES
  (1, CAST(100000000000 AS BIGINT), 'alice',
   CAST(1.25 AS FLOAT), 1.5, 12.345,
   DATE'2026-05-15', TIMESTAMP'2026-05-15 10:30:00', true,
   CAST('hello' AS BINARY),
   ARRAY(1,2,3), MAP('a',1,'b',2)),
  (2, CAST(200000000000 AS BIGINT), 'bob',
   CAST(2.75 AS FLOAT), 2.5, 67.890,
   DATE'2026-05-16', TIMESTAMP'2026-05-16 11:30:00', false,
   CAST('world' AS BINARY),
   ARRAY(4,5,6), MAP('c',3));

-- ===================================================================
-- 4. NULL values across all encrypted column types (case 3)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_nulls;
CREATE TABLE iceberg.encrypted.t_nulls (
  id INT, name STRING, amount DOUBLE, score FLOAT, raw BINARY, country STRING)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_nulls VALUES
  (1, 'alice', 100.0, CAST(1.5 AS FLOAT), CAST('a' AS BINARY), 'US'),
  (2,  NULL,   NULL,  NULL,                NULL,                NULL),
  (3, 'bob',   NULL,  CAST(3.5 AS FLOAT), CAST('c' AS BINARY), 'JP'),
  (4,  NULL,   400.0, NULL,                NULL,                'UK');

-- ===================================================================
-- 5. Empty encrypted table (case 4) -- created but never INSERTed.
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_empty;
CREATE TABLE iceberg.encrypted.t_empty (id INT, name STRING)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');

-- ===================================================================
-- 6. Big file with multiple row groups (case 5)
-- 50_000 rows; row-group target size set small to force multiple groups.
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_bigfile;
CREATE TABLE iceberg.encrypted.t_bigfile (id BIGINT, name STRING, val DOUBLE)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}',
               'write.parquet.row-group-size-bytes'='65536',
               'write.parquet.page-size-bytes'='8192',
               'write.target-file-size-bytes'='1073741824');
INSERT INTO iceberg.encrypted.t_bigfile
  SELECT id, concat('row-', cast(id AS string)) AS name, id * 1.5 AS val
  FROM range(0, 50000);

-- ===================================================================
-- 7. Multiple KEKs across different tables (case 14 + 15)
--    Iceberg 1.11 forbids ALTER ... 'encryption.key-id', so true KEK rotation
--    is owned by KMS itself (CMK rotation is transparent to Iceberg). To prove
--    that the connector handles multiple distinct KEKs, we create two tables,
--    one with key A and one with key B, and verify both are readable from the
--    same Trino instance with one KMS-client config.
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_kek_a;
CREATE TABLE iceberg.encrypted.t_kek_a (id INT, name STRING, batch INT)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_kek_a VALUES
  (1, 'a', 1), (2, 'b', 1), (3, 'c', 1);

DROP TABLE IF EXISTS iceberg.encrypted.t_kek_b;
CREATE TABLE iceberg.encrypted.t_kek_b (id INT, name STRING, batch INT)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id_b}');
INSERT INTO iceberg.encrypted.t_kek_b VALUES
  (4, 'd', 2), (5, 'e', 2), (6, 'f', 2);

-- ===================================================================
-- 8. V3 puffin Deletion Vector (cases 10, 18 partition prune over DV)
-- This is the table that exercises our own Trino patch + #16158 fix.
-- 100 rows, partitioned by country, MoR DELETE 6 rows -> Puffin DV.
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_dv_real_v2;
CREATE TABLE iceberg.encrypted.t_dv_real_v2 (id INT, name STRING, country STRING)
USING ICEBERG
PARTITIONED BY (country)
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'write.delete.mode'='merge-on-read',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_dv_real_v2
  SELECT id,
         concat('row-', cast(id AS string)) AS name,
         CASE WHEN id % 2 = 0 THEN 'A' ELSE 'B' END AS country
  FROM range(0, 100);
DELETE FROM iceberg.encrypted.t_dv_real_v2 WHERE id IN (3, 7, 9, 12, 18, 24);

-- ===================================================================
-- 9. CoW DELETE encrypted table (regression for non-DV path)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_cow_delete;
CREATE TABLE iceberg.encrypted.t_cow_delete (id INT, name STRING, country STRING)
USING ICEBERG
PARTITIONED BY (country)
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'write.delete.mode'='copy-on-write',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_cow_delete
  SELECT id,
         concat('row-', cast(id AS string)) AS name,
         CASE WHEN id % 2 = 0 THEN 'A' ELSE 'B' END AS country
  FROM range(0, 100);
DELETE FROM iceberg.encrypted.t_cow_delete WHERE id IN (3, 7, 9, 12, 18, 24);

-- ===================================================================
-- 10. Partition evolution (case 19)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_part_evo;
CREATE TABLE iceberg.encrypted.t_part_evo (id INT, name STRING, country STRING)
USING ICEBERG
PARTITIONED BY (country)
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_part_evo VALUES
  (1, 'a', 'US'), (2, 'b', 'UK'), (3, 'c', 'US');
-- Add a second partition field on top.
ALTER TABLE iceberg.encrypted.t_part_evo
  ADD PARTITION FIELD bucket(4, id);
INSERT INTO iceberg.encrypted.t_part_evo VALUES
  (4, 'd', 'US'), (5, 'e', 'UK'), (6, 'f', 'JP'),
  (7, 'g', 'US'), (8, 'h', 'UK'), (9, 'i', 'JP');

-- ===================================================================
-- 11. Schema evolution: ADD COLUMN (case 20)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_add_col;
CREATE TABLE iceberg.encrypted.t_add_col (id INT, name STRING)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_add_col VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE iceberg.encrypted.t_add_col ADD COLUMN amount DOUBLE;
INSERT INTO iceberg.encrypted.t_add_col VALUES (3, 'charlie', 300.0), (4, 'dave', 400.0);

-- ===================================================================
-- 12. Schema evolution: DROP COLUMN (case 21)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_drop_col;
CREATE TABLE iceberg.encrypted.t_drop_col (id INT, name STRING, extra STRING)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_drop_col VALUES
  (1, 'alice', 'x'), (2, 'bob', 'y'), (3, 'charlie', 'z');
ALTER TABLE iceberg.encrypted.t_drop_col DROP COLUMN extra;

-- ===================================================================
-- 13. Wide table for column projection (case 23)
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_wide;
CREATE TABLE iceberg.encrypted.t_wide (
  c01 INT, c02 STRING, c03 DOUBLE, c04 INT, c05 STRING,
  c06 INT, c07 STRING, c08 DOUBLE, c09 INT, c10 STRING,
  c11 INT, c12 STRING, c13 DOUBLE, c14 INT, c15 STRING,
  c16 INT, c17 STRING, c18 DOUBLE, c19 INT, c20 STRING)
USING ICEBERG
TBLPROPERTIES ('format-version'='3', 'write.format.default'='PARQUET',
               'encryption.key-id'='${key_id}');
INSERT INTO iceberg.encrypted.t_wide VALUES
  (1, 'a', 1.5, 11, 'aa', 21, 'aaa', 11.5, 31, 'aaaa',
   41, 'b',  2.5, 12, 'bb', 22, 'bbb', 12.5, 32, 'bbbb'),
  (2, 'c', 3.5, 13, 'cc', 23, 'ccc', 13.5, 33, 'cccc',
   42, 'd',  4.5, 14, 'dd', 24, 'ddd', 14.5, 34, 'dddd'),
  (3, 'e', 5.5, 15, 'ee', 25, 'eee', 15.5, 35, 'eeee',
   43, 'f',  6.5, 16, 'ff', 26, 'fff', 16.5, 36, 'ffff');

-- ===================================================================
-- 14. Plaintext baseline for differential testing
-- ===================================================================
DROP TABLE IF EXISTS iceberg.encrypted.t_plaintext;
CREATE TABLE iceberg.encrypted.t_plaintext (id INT, name STRING, amount DOUBLE)
USING ICEBERG
TBLPROPERTIES ('format-version'='2', 'write.format.default'='PARQUET');
INSERT INTO iceberg.encrypted.t_plaintext VALUES (1, 'alice', 100.5), (2, 'bob', 200.25);

-- Spark sanity reads.
SELECT 't_basic'        AS label, count(*) AS n FROM iceberg.encrypted.t_basic UNION ALL
SELECT 't_partitioned',           count(*)    FROM iceberg.encrypted.t_partitioned UNION ALL
SELECT 't_types',                 count(*)    FROM iceberg.encrypted.t_types UNION ALL
SELECT 't_nulls',                 count(*)    FROM iceberg.encrypted.t_nulls UNION ALL
SELECT 't_empty',                 count(*)    FROM iceberg.encrypted.t_empty UNION ALL
SELECT 't_bigfile',               count(*)    FROM iceberg.encrypted.t_bigfile UNION ALL
SELECT 't_kek_a',                  count(*)    FROM iceberg.encrypted.t_kek_a UNION ALL
SELECT 't_kek_b',                  count(*)    FROM iceberg.encrypted.t_kek_b UNION ALL
SELECT 't_dv_real_v2',            count(*)    FROM iceberg.encrypted.t_dv_real_v2 UNION ALL
SELECT 't_cow_delete',            count(*)    FROM iceberg.encrypted.t_cow_delete UNION ALL
SELECT 't_part_evo',              count(*)    FROM iceberg.encrypted.t_part_evo UNION ALL
SELECT 't_add_col',               count(*)    FROM iceberg.encrypted.t_add_col UNION ALL
SELECT 't_drop_col',              count(*)    FROM iceberg.encrypted.t_drop_col UNION ALL
SELECT 't_wide',                  count(*)    FROM iceberg.encrypted.t_wide UNION ALL
SELECT 't_plaintext',             count(*)    FROM iceberg.encrypted.t_plaintext
ORDER BY label;
