#!/usr/bin/env bash
# provision_aurora_pgvector.sh
# Idempotent-ish: reuses cluster if already present, reuses master secret if created by RDS.
set -euo pipefail
IFS=$'\n\t'

# ----------------- Assumptions / invariants -----------------
# - You have AWS credentials configured (aws CLI v2, with permission to create RDS/Aurora clusters,
#   create DB instances, and to call rds-data:ExecuteStatement).
# - You have a DB subnet group and VPC security group already created.
# - This script targets Aurora PostgreSQL Serverless v2 (engine: aurora-postgresql).
# - It uses RDS-managed master password (--manage-master-user-password); the created DBCluster
#   response will include MasterUserSecret (SecretArn).
#
# Read the top docs referenced in the script comments before running:
# - Create Serverless v2 cluster / ServerlessV2ScalingConfiguration: AWS Aurora User Guide.
# - Data API: enable via --enable-http-endpoint, then use aws rds-data execute-statement.

# ----------------- User-editable configuration (minimal changes) -----------------
AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-aurora-pgvector-cluster}"
DB_INSTANCE_ID="${DB_INSTANCE_ID:-${CLUSTER_ID}-writer}"
ENGINE="${ENGINE:-aurora-postgresql}"
# Pick a serverless-v2-compatible engine minor version available in your region.
ENGINE_VERSION="${ENGINE_VERSION:-15.12}"    # change if unavailable in your region
MASTER_USERNAME="${MASTER_USERNAME:-pgadmin}"

# Network / infra -- replace with your real values before running
DB_SUBNET_GROUP_NAME="${DB_SUBNET_GROUP_NAME:-my-db-subnet-group}"
VPC_SECURITY_GROUP_IDS="${VPC_SECURITY_GROUP_IDS:-sg-0123456789abcdef0}"  # space or comma separated

# Free-plan safe defaults: reduce to <=4 ACUs to be compatible with many free-plan accounts
MIN_ACU="${MIN_ACU:-0.5}"
MAX_ACU="${MAX_ACU:-4}"

# Optional: number of seconds to auto-pause (set to 0 or omit if you don't want auto-pause)
SECONDS_UNTIL_AUTO_PAUSE="${SECONDS_UNTIL_AUTO_PAUSE:-600}"

# Timeouts
WAIT_TIMEOUT_CLUSTER_SECONDS="${WAIT_TIMEOUT_CLUSTER_SECONDS:-1200}"  # 20 minutes

# ----------------- Quick validations (fail fast) -----------------
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI v2 is required in PATH"; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required in PATH"; exit 2; }

# Check aws CLI version roughly (need a recent CLI that knows serverless-v2 flags).
AWS_CLI_VER_RAW="$(aws --version 2>&1 | awk '{print $1}')"
# Example output: aws-cli/2.9.6 Python/3.9.11 ...
AWS_CLI_VER="$(echo "$AWS_CLI_VER_RAW" | sed -E 's/^aws-cli\/([0-9]+)\..*$/\1/')"
if [[ -z "$AWS_CLI_VER_RAW" ]]; then
  echo "ERROR: can't parse aws --version output: ${AWS_CLI_VER_RAW}"
  exit 2
fi
# best-effort: require CLI major version 2 and reasonably new (2.9+ recommended)
if ! echo "$AWS_CLI_VER_RAW" | grep -q '^aws-cli/2'; then
  echo "ERROR: aws CLI v2 is required (found: ${AWS_CLI_VER_RAW}). Upgrade: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 2
fi

# Serverless ACU sanity
# ensure MAX_ACU is numeric and >= MIN_ACU
if ! awk "BEGIN{exit !($MAX_ACU+0 >= $MIN_ACU+0)}"; then
  echo "ERROR: MAX_ACU must be >= MIN_ACU"
  exit 2
fi

# For free accounts, enforce a conservative ceiling (avoid predictable FreeTierRestrictionError)
if awk "BEGIN{exit !($MAX_ACU > 4)}"; then
  echo "WARNING: MAX_ACU > 4; free-plan accounts may block this. Setting MAX_ACU=4 for free-plan compatibility."
  MAX_ACU=4
fi

# ----------------- Helper functions -----------------
aws_rds_create_cluster() {
  aws rds create-db-cluster \
    --db-cluster-identifier "$CLUSTER_ID" \
    --engine "$ENGINE" \
    --engine-version "$ENGINE_VERSION" \
    --db-subnet-group-name "$DB_SUBNET_GROUP_NAME" \
    --vpc-security-group-ids $(echo "$VPC_SECURITY_GROUP_IDS" | tr ',' ' ') \
    --serverless-v2-scaling-configuration "MinCapacity=${MIN_ACU},MaxCapacity=${MAX_ACU},SecondsUntilAutoPause=${SECONDS_UNTIL_AUTO_PAUSE}" \
    --enable-http-endpoint \
    --manage-master-user-password \
    --region "$AWS_REGION" \
    --output json
}

# ----------------- Main flow -----------------
echo "Region: $AWS_REGION"
echo "Cluster: $CLUSTER_ID (engine=${ENGINE} ${ENGINE_VERSION}), Writer instance: ${DB_INSTANCE_ID}"
echo "ACU range: ${MIN_ACU} - ${MAX_ACU} (SecondsUntilAutoPause=${SECONDS_UNTIL_AUTO_PAUSE})"

# If cluster already exists, reuse and extract metadata
set +e
EXISTING_CLUSTER_JSON="$(aws rds describe-db-clusters --db-cluster-identifier "${CLUSTER_ID}" --region "${AWS_REGION}" 2>/dev/null || true)"
set -e

if [[ -n "${EXISTING_CLUSTER_JSON}" && "$(echo "${EXISTING_CLUSTER_JSON}" | jq -r '.DBClusters | length')" -gt 0 ]]; then
  echo "Found existing cluster '${CLUSTER_ID}', reusing it."
  DB_CLUSTER_ARN="$(echo "${EXISTING_CLUSTER_JSON}" | jq -r '.DBClusters[0].DBClusterArn')"
  MASTER_SECRET_ARN="$(echo "${EXISTING_CLUSTER_JSON}" | jq -r '.DBClusters[0].MasterUserSecret.SecretArn // empty')"
else
  echo "Creating DB cluster ${CLUSTER_ID}..."
  set +e
  CREATE_OUT="$(aws_rds_create_cluster 2>&1)"
  CREATE_EXIT=$?
  set -e

  if [ $CREATE_EXIT -ne 0 ]; then
    echo "ERROR: create-db-cluster failed."
    echo "$CREATE_OUT"
    # If AWS indicates a FreeTierRestrictionError that mentions WithExpressConfiguration,
    # we can't set an unsupported CLI flag. Recommend console creation or account upgrade.
    if echo "$CREATE_OUT" | grep -qi "FreeTierRestrictionError"; then
      echo
      echo "AWS returned FreeTierRestrictionError. Two practical remediation options:"
      echo "  1) Run this script after upgrading the AWS account out of Free Plan limits."
      echo "  2) Create the cluster from the RDS Console using the Free-tier / Easy-create workflow (console sometimes sets required 'express' bits automatically)."
      echo
      exit 2
    fi
    exit $CREATE_EXIT
  fi

  # Parse response for ARNs
  DB_CLUSTER_ARN="$(echo "$CREATE_OUT" | jq -r '.DBCluster.DBClusterArn // empty')"
  MASTER_SECRET_ARN="$(echo "$CREATE_OUT" | jq -r '.DBCluster.MasterUserSecret.SecretArn // empty')"

  if [[ -z "$DB_CLUSTER_ARN" ]]; then
    echo "WARNING: create-db-cluster returned no DBClusterArn in the response. Attempting to describe the cluster..."
    DB_CLUSTER_ARN="$(aws rds describe-db-clusters --db-cluster-identifier "${CLUSTER_ID}" --region "${AWS_REGION}" --query 'DBClusters[0].DBClusterArn' --output text)"
  fi

  echo "Created cluster ARN: ${DB_CLUSTER_ARN}"
  if [[ -n "$MASTER_SECRET_ARN" ]]; then
    echo "Master user secret ARN (managed by RDS): ${MASTER_SECRET_ARN}"
  else
    echo "Master user secret ARN not present in create response; it may be available after the cluster becomes available."
  fi
fi

# ----------------- Create Serverless writer instance (db.serverless) -----------------
# If writer instance already exists skip creation
set +e
EXISTING_INSTANCE="$(aws rds describe-db-instances --db-instance-identifier "${DB_INSTANCE_ID}" --region "${AWS_REGION}" 2>/dev/null || true)"
set -e
if [[ -n "${EXISTING_INSTANCE}" && "$(echo "${EXISTING_INSTANCE}" | jq -r '.DBInstances | length')" -gt 0 ]]; then
  echo "Found existing DB instance ${DB_INSTANCE_ID}, skipping create-db-instance."
else
  echo "Creating Serverless v2 writer instance ${DB_INSTANCE_ID}..."
  aws rds create-db-instance \
    --db-cluster-identifier "${CLUSTER_ID}" \
    --db-instance-identifier "${DB_INSTANCE_ID}" \
    --db-instance-class db.serverless \
    --engine "${ENGINE}" \
    --region "${AWS_REGION}" \
    --output json
fi

# ----------------- Wait until available -----------------
echo "Waiting for DB cluster to become available (timeout ${WAIT_TIMEOUT_CLUSTER_SECONDS}s)..."
aws rds wait db-cluster-available --db-cluster-identifier "${CLUSTER_ID}" --region "${AWS_REGION}"
echo "Cluster available."

echo "Waiting for DB instance to become available..."
aws rds wait db-instance-available --db-instance-identifier "${DB_INSTANCE_ID}" --region "${AWS_REGION}"
echo "Instance available."

# Re-fetch cluster metadata to obtain any missing SecretArn
if [[ -z "${MASTER_SECRET_ARN:-}" ]]; then
  MASTER_SECRET_ARN="$(aws rds describe-db-clusters --db-cluster-identifier "${CLUSTER_ID}" --region "${AWS_REGION}" --query 'DBClusters[0].MasterUserSecret.SecretArn' --output text || true)"
fi
DB_CLUSTER_ARN="$(aws rds describe-db-clusters --db-cluster-identifier "${CLUSTER_ID}" --region "${AWS_REGION}" --query 'DBClusters[0].DBClusterArn' --output text)"

echo "Cluster ARN: ${DB_CLUSTER_ARN}"
if [[ -n "${MASTER_SECRET_ARN}" && "${MASTER_SECRET_ARN}" != "None" ]]; then
  echo "Master Secret ARN: ${MASTER_SECRET_ARN}"
else
  echo "WARNING: Master secret ARN not found. If you used --manage-master-user-password the secret should appear in the cluster response. If it's not present, check Console > RDS > Databases > ${CLUSTER_ID}."
fi

# ----------------- Install pgvector via Data API -----------------
# Data API requires cluster ARN and a Secrets Manager secret ARN with DB credentials
if [[ -z "${DB_CLUSTER_ARN}" || -z "${MASTER_SECRET_ARN}" || "${MASTER_SECRET_ARN}" == "None" ]]; then
  echo "Skipping pgvector install because required ARNs are missing."
  echo "Check cluster and secret in the RDS console and then run:"
  echo "  aws rds-data execute-statement --resource-arn <cluster-arn> --secret-arn <secret-arn> --sql \"CREATE EXTENSION IF NOT EXISTS vector;\" --database postgres --region ${AWS_REGION}"
  exit 0
fi

echo "Installing pgvector extension (CREATE EXTENSION IF NOT EXISTS vector;) via Data API..."
set +e
RDSDATA_OUT="$(aws rds-data execute-statement \
  --resource-arn "${DB_CLUSTER_ARN}" \
  --secret-arn "${MASTER_SECRET_ARN}" \
  --sql "CREATE EXTENSION IF NOT EXISTS vector;" \
  --database "postgres" \
  --region "${AWS_REGION}" 2>&1)"
RDSDATA_EXIT=$?
set -e

if [ $RDSDATA_EXIT -ne 0 ]; then
  echo "ERROR: rds-data execute-statement failed."
  echo "${RDSDATA_OUT}"
  echo "If the Data API call fails with permission issues, ensure the IAM principal running this script has 'rds-data:ExecuteStatement' and the SecretsManager secret policy allows access."
  exit $RDSDATA_EXIT
fi

echo "pgvector installation command executed. Verify with:"
echo "  aws rds-data execute-statement --resource-arn ${DB_CLUSTER_ARN} --secret-arn ${MASTER_SECRET_ARN} --sql \"SELECT extname, extversion FROM pg_extension WHERE extname='vector';\" --database postgres --region ${AWS_REGION}"

echo "Done."
