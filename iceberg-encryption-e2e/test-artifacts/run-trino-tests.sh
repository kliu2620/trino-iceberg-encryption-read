#!/usr/bin/env bash
# Drives all encrypted-read test cases (codex 1..25) against the running
# trino-enc-test container. Exits with non-zero if any case fails.
set -uo pipefail

CONTAINER="${CONTAINER:-trino-enc-test}"
TRINO="docker exec $CONTAINER /usr/bin/trino --catalog iceberg --schema encrypted --output-format CSV --no-progress"

# Pre-fetch snapshot ids for time-travel tests
SNAPS=$($TRINO --execute 'SELECT snapshot_id FROM "t_basic$history" ORDER BY made_current_at' 2>/dev/null | grep -v JAVA_OPTIONS | tr -d '"')
SNAP_FIRST=$(echo "$SNAPS" | head -1)
SNAP_LAST=$(echo "$SNAPS" | tail -1)
echo "DEBUG: t_basic snapshots: first=$SNAP_FIRST last=$SNAP_LAST" >&2
PASS=0
FAIL=0
declare -a RESULTS

run() {
  local id="$1" desc="$2" sql="$3" expect="$4"
  local out
  out="$($TRINO --execute "$sql" 2>&1 | grep -v JAVA_OPTIONS | grep -v "^WARNING" | grep -v "Picked up" | tr -d '\r')"
  if [[ "$out" == "$expect" ]]; then
    RESULTS+=("PASS  #${id}  ${desc}")
    PASS=$((PASS+1))
  else
    RESULTS+=("FAIL  #${id}  ${desc}")
    RESULTS+=("      sql:    ${sql}")
    RESULTS+=("      expect: ${expect}")
    RESULTS+=("      got:    $(echo "$out" | tr '\n' '|')")
    FAIL=$((FAIL+1))
  fi
}

# Helper to compare regex / contains
run_contains() {
  local id="$1" desc="$2" sql="$3" needle="$4"
  local out
  out="$($TRINO --execute "$sql" 2>&1 | grep -v JAVA_OPTIONS | grep -v "^WARNING" | grep -v "Picked up" | tr -d '\r')"
  if echo "$out" | grep -qE "$needle"; then
    RESULTS+=("PASS  #${id}  ${desc}")
    PASS=$((PASS+1))
  else
    RESULTS+=("FAIL  #${id}  ${desc}")
    RESULTS+=("      sql:    ${sql}")
    RESULTS+=("      regex:  ${needle}")
    RESULTS+=("      got:    $(echo "$out" | tr '\n' '|')")
    FAIL=$((FAIL+1))
  fi
}

# ============================================================
# Encrypted Parquet data file reads
# ============================================================
run "01" "Parquet basic encrypted read"                 \
  "SELECT count(*) FROM t_basic"                        '"7"'

run "02a" "Parquet types: int/long/string/double"       \
  "SELECT c_int, c_long, c_str, c_double FROM t_types ORDER BY c_int"  \
  '"1","100000000000","alice","1.5"
"2","200000000000","bob","2.5"'

run "02b" "Parquet types: float/decimal/binary"        \
  "SELECT c_float, c_dec, to_hex(c_bin) FROM t_types ORDER BY c_int"  \
  '"1.25","12.345","68656C6C6F"
"2.75","67.890","776F726C64"'

run "02c" "Parquet types: date/timestamp/boolean"       \
  "SELECT cast(c_date AS varchar), cast(c_ts AS varchar), c_bool FROM t_types ORDER BY c_int"  \
  '"2026-05-15","2026-05-15 02:30:00.000000 UTC","true"
"2026-05-16","2026-05-16 03:30:00.000000 UTC","false"'

run "02d" "Parquet types: array/map"                    \
  "SELECT c_arr, c_map FROM t_types ORDER BY c_int"  \
  '"[1, 2, 3]","{a=1, b=2}"
"[4, 5, 6]","{c=3}"'

run "03"  "NULL values"                                 \
  "SELECT count(*) FROM t_nulls WHERE name IS NULL"     '"2"'
run "03b" "NULL values count(amount)"                   \
  "SELECT count(amount) FROM t_nulls"                   '"2"'

run "04"  "Empty encrypted table"                       \
  "SELECT count(*) FROM t_empty"                        '"0"'
run "04b" "Empty encrypted table star query"            \
  "SELECT * FROM t_empty"                                ''

run "05"  "Big file multi row group"                    \
  "SELECT count(*) FROM t_bigfile"                      '"50000"'
run "05b" "Big file aggregate"                          \
  "SELECT sum(id), max(val) FROM t_bigfile"             '"1249975000","74998.5"'
run "05c" "Big file predicate"                          \
  "SELECT count(*) FROM t_bigfile WHERE id BETWEEN 1000 AND 2000"  '"1001"'

# ============================================================
# Encrypted manifest / manifest list
# ============================================================
run "08"  "Multiple manifests across appends"           \
  "SELECT count(DISTINCT path) FROM \"t_basic\$manifests\"" '"2"'
run "09"  "Multiple snapshots, all readable"            \
  "SELECT count(DISTINCT snapshot_id) FROM \"t_basic\$history\"" '"2"'
run "09b" "Each snapshot has parents in chain"          \
  "SELECT count(*) FROM \"t_basic\$history\" WHERE is_current_ancestor"  '"2"'

# ============================================================
# Delete file encryption
# ============================================================
run "10a" "V3 Puffin DV: count after MoR delete"        \
  "SELECT count(*) FROM t_dv_real_v2"                   '"94"'
run "10b" "V3 Puffin DV: deleted rows are gone"         \
  "SELECT count(*) FROM t_dv_real_v2 WHERE id IN (3,7,9,12,18,24)"  '"0"'
run "10c" "V3 Puffin DV: surviving rows correct"         \
  "SELECT id FROM t_dv_real_v2 WHERE country='B' AND id<20 ORDER BY id"  \
  '"1"
"5"
"11"
"13"
"15"
"17"
"19"'
run "10d" "V3 Puffin DV: aggregate min/max"             \
  "SELECT min(id), max(id), count(*) FROM t_dv_real_v2"  '"0","99","94"'
run "10e" "V3 Puffin DV: delete-file metadata is encrypted (key_metadata not null)"  \
  "SELECT count(*) FROM \"t_dv_real_v2\$files\" WHERE content=2 AND key_metadata IS NULL"  '"0"'

# CoW DELETE regression
run "10f" "CoW delete encrypted table"                  \
  "SELECT count(*) FROM t_cow_delete"                   '"94"'
run "10g" "CoW: no delete files"                        \
  "SELECT count(*) FROM \"t_cow_delete\$files\" WHERE content<>0"  '"0"'

# ============================================================
# Key management
# ============================================================
run "13"  "Single KEK table read"                       \
  "SELECT count(*) FROM t_kek_a"                        '"3"'
run "15a" "Multiple KEK tables in same trino"           \
  "SELECT count(*) FROM t_kek_b"                        '"3"'
run "15b" "Cross-KEK union read"                        \
  "SELECT sum(c) FROM (SELECT count(*) c FROM t_kek_a UNION ALL SELECT count(*) FROM t_kek_b)"  '"6"'

# ============================================================
# Partitioned tables / partition evolution
# ============================================================
run "18a" "Partition pruning"                           \
  "SELECT name FROM t_partitioned WHERE country='JP'"   '"frank"'
run "18b" "Partition group-by counts"                    \
  "SELECT country, count(*) FROM t_partitioned GROUP BY country ORDER BY country"  \
  '"JP","1"
"UK","3"
"US","2"'
run "19a" "Partition evolution: total count"            \
  "SELECT count(*) FROM t_part_evo"                     '"9"'
run "19b" "Partition evolution: country=US covers both old and new partitions"  \
  "SELECT count(*) FROM t_part_evo WHERE country='US'"  '"4"'

# ============================================================
# Schema evolution
# ============================================================
run "20a" "Add column: total rows after evolution"      \
  "SELECT count(*) FROM t_add_col"                      '"4"'
run "20b" "Add column: old rows have NULL in new column" \
  "SELECT count(*) FROM t_add_col WHERE amount IS NULL"  '"2"'
run "20c" "Add column: new rows have values"            \
  "SELECT id, amount FROM t_add_col WHERE amount IS NOT NULL ORDER BY id"  \
  '"3","300.0"
"4","400.0"'
run "21"  "Drop column"                                 \
  "SELECT count(*) FROM t_drop_col"                     '"3"'
run "21b" "Drop column: dropped column not in schema"    \
  "SELECT id, name FROM t_drop_col ORDER BY id"          \
  '"1","alice"
"2","bob"
"3","charlie"'

# ============================================================
# Time travel
# ============================================================
run "22a" "Time travel: read original snapshot of t_basic (5 rows)"  \
  "SELECT count(*) FROM t_basic FOR VERSION AS OF $SNAP_FIRST"  \
  '"5"'
run "22b" "Time travel: read latest snapshot (7 rows)"  \
  "SELECT count(*) FROM t_basic FOR VERSION AS OF $SNAP_LAST"  \
  '"7"'

# ============================================================
# Column projection / predicate pushdown
# ============================================================
run "23a" "Wide table: project 2 of 20 columns"         \
  "SELECT c01, c05 FROM t_wide ORDER BY c01"            \
  '"1","aa"
"2","cc"
"3","ee"'
run "23b" "Wide table: project 5 columns"               \
  "SELECT c02, c08, c14, c20, c19 FROM t_wide ORDER BY c01"  \
  '"a","11.5","12","bbbb","32"
"c","13.5","14","dddd","34"
"e","15.5","16","ffff","36"'
run "24a" "Predicate pushdown: id=3"                    \
  "SELECT name FROM t_basic WHERE id=3"                 '"charlie"'
run "24b" "Predicate pushdown: amount>350"              \
  "SELECT name FROM t_basic WHERE amount>350 ORDER BY name"  \
  '"dave"
"ellen"
"frank"
"grace"'

# ============================================================
# Plaintext sibling — must remain unaffected
# ============================================================
run "P1"  "Plaintext sibling read"                      \
  "SELECT count(*) FROM t_plaintext"                    '"2"'
run "P2"  "Plaintext sibling: key_metadata is NULL"     \
  "SELECT count(*) FROM \"t_plaintext\$files\" WHERE key_metadata IS NOT NULL"  '"0"'

# ============================================================
# Output report
# ============================================================
echo "==============================="
echo " Trino encryption-read e2e"
echo "==============================="
for line in "${RESULTS[@]}"; do echo "$line"; done
echo "-------------------------------"
echo "Pass: $PASS"
echo "Fail: $FAIL"
exit $FAIL
