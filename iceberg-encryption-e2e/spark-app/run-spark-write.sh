#!/usr/bin/env bash
# Run Spark 3.5.6 with locally-built Iceberg 1.11.0-SNAPSHOT and execute the
# encrypted-table write script against the running HMS + LocalStack KMS stack.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
SPARK_HOME="${SPARK_HOME:-$HOME/spark/spark-3.5.6-bin-hadoop3}"
M2="${M2:-$HOME/.m2/repository}"
KEY_ID="$(cat "$ROOT/.kms-key-id")"

export JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
# AWS SDK v2 reads these for KMS endpoint (LocalStack) + creds + region.
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT_URL_KMS=http://localhost:4566

# Iceberg 1.11.0-SNAPSHOT artifacts we built and published with `gradlew
# publishToMavenLocal`. Pulling them via --jars (and not --packages) so we don't
# have to teach Spark about a SNAPSHOT Maven repo.
JARS=$(printf "%s," \
  "$M2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0-SNAPSHOT/iceberg-spark-runtime-3.5_2.12-1.11.0-SNAPSHOT.jar" \
  "$M2/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.11.0-SNAPSHOT/iceberg-spark-extensions-3.5_2.12-1.11.0-SNAPSHOT.jar" \
  "$M2/org/apache/iceberg/iceberg-aws-bundle/1.11.0-SNAPSHOT/iceberg-aws-bundle-1.11.0-SNAPSHOT.jar")
JARS="${JARS%,}"

echo "Using KMS key id: $KEY_ID"
echo "Using Spark jars:"
printf '  %s\n' ${JARS//,/ }

exec "$SPARK_HOME/bin/spark-sql" \
  --properties-file "$HERE/spark-defaults.conf" \
  --jars "$JARS" \
  --conf "spark.sql.catalog.iceberg.warehouse=file:///Users/keyiliu/trino/iceberg-encryption-e2e/warehouse" \
  --conf "spark.driver.extraJavaOptions=-Daws.region=us-east-1" \
  -f "$HERE/write-encrypted.sql" \
  --hivevar key_id="$KEY_ID"
