#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Non-interactive run.sh for Pulumi (Option: env-only config)
# - DOES NOT use `pulumi config set` or any interactive prompts
# - All required runtime settings must be provided as environment variables
# - Fails fast and deterministically if any required value is missing
#
# Usage:
#   export PROJECT_PREFIX=civic-index
#   export AWS_REGION=ap-south-1
#   export ECR_REPO_NAME=civic-index-repo
#   export IMAGE_TAG=sha-abcdef1234        # Must NOT be "latest"
#   export UI_S3_BUCKET=civic-index-ui
#   export CREATE_CLOUDFRONT=false         # "true" or "false"
#   ./run.sh create
#
# Optional envs:
#   PULUMI_STACK (default: prod)
#   PULUMI_STATE_BUCKET (default: pulumi-backend-670)
#   PULUMI_STATE_PREFIX (default: pulumi)
#   DDB_TABLE (default: pulumi-state-locks)
#   IMAGE_URI (legacy, optional) - not used for infra config, only preserved for backward compatibility
#   FORCE_DELETE (default: 0)
#   REQ_FILE, VENV_DIR, PROJECT_DIR can be used to override paths

usage(){ cat <<USAGE
usage: $0 create|delete

This script is strictly non-interactive. All required values must be set
in environment variables before invoking. The script will error out if
any required env var is missing or invalid.
USAGE
}

MODE="${1:-}"
if [ "$MODE" != "create" ] && [ "$MODE" != "delete" ]; then
  usage
  exit 2
fi

# -------------------------
# Non-interactive defaults
# -------------------------
export AWS_REGION="${AWS_REGION:-ap-south-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export PULUMI_STACK="${PULUMI_STACK:-prod}"
STACK_SHORT="$PULUMI_STACK"
export PULUMI_STATE_BUCKET="${PULUMI_STATE_BUCKET:-pulumi-backend-670}"
export PULUMI_STATE_PREFIX="${PULUMI_STATE_PREFIX:-pulumi}"
export DDB_TABLE="${DDB_TABLE:-pulumi-state-locks}"
export IMAGE_URI="${IMAGE_URI:-athithya5354/civic-indexing:latest}"
export FORCE_DELETE="${FORCE_DELETE:-0}"
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-password}"
export PULUMI_LOGIN_URL="s3://${PULUMI_STATE_BUCKET}/${PULUMI_STATE_PREFIX}"

LOG(){ printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }
DIE(){ echo "ERROR: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$SCRIPT_DIR}"
REQ_FILE="${REQ_FILE:-$PROJECT_DIR/requirements.txt}"
VENV_DIR="${VENV_DIR:-$PROJECT_DIR/venv}"

# -------------------------
# Required envs (non-interactive)
# -------------------------
REQUIRED_ENVS=( PROJECT_PREFIX AWS_REGION ECR_REPO_NAME IMAGE_TAG UI_S3_BUCKET CREATE_CLOUDFRONT )

# helper to check required envs
missing_envs=()
for k in "${REQUIRED_ENVS[@]}"; do
  val="${!k:-}"
  if [ -z "$val" ]; then
    missing_envs+=("$k")
  fi
done

if [ "${#missing_envs[@]}" -ne 0 ]; then
  LOG "Missing required environment variables (non-interactive mode):"
  for m in "${missing_envs[@]}"; do
    LOG "  - $m"
  done
  DIE "Set the above env vars and re-run. Example: export PROJECT_PREFIX=civic-index"
fi

# IMAGE_TAG validation
if [ "${IMAGE_TAG:-}" = "latest" ] || [ -z "${IMAGE_TAG:-}" ]; then
  DIE "IMAGE_TAG must be provided and must NOT be 'latest' (use immutable tag, e.g. git-sha)"
fi

# CREATE_CLOUDFRONT validation
case "${CREATE_CLOUDFRONT,,}" in
  "true"|"1"|"yes") CREATE_CLOUDFRONT="true" ;;
  "false"|"0"|"no") CREATE_CLOUDFRONT="false" ;;
  *) DIE "CREATE_CLOUDFRONT must be 'true' or 'false' (case-insensitive)" ;;
esac

# -------------------------
# preflight CLIs + AWS creds
# -------------------------
for bin in aws pulumi python3 jq; do
  command -v "$bin" >/dev/null 2>&1 || DIE "missing required CLI: $bin"
done
PY_BIN="$(command -v python3)"
[ -n "$PY_BIN" ] || DIE "python3 not found"

aws sts get-caller-identity >/dev/null 2>&1 || DIE "AWS credentials not working (aws sts get-caller-identity failed)"

# -------------------------
# helper functions
# -------------------------
ensure_bucket(){
  LOG "s3: verify state bucket ${PULUMI_STATE_BUCKET}"
  if aws s3api head-bucket --bucket "$PULUMI_STATE_BUCKET" >/dev/null 2>&1; then
    LOG "s3: state bucket exists: ${PULUMI_STATE_BUCKET}"
    aws s3api put-bucket-versioning --bucket "$PULUMI_STATE_BUCKET" --versioning-configuration Status=Enabled >/dev/null 2>&1 || true
  else
    DIE "S3 state bucket ${PULUMI_STATE_BUCKET} does not exist. Create it before running this script."
  fi
}

ensure_ddb(){
  LOG "ddb: ensure table ${DDB_TABLE}"
  if aws dynamodb describe-table --table-name "$DDB_TABLE" >/dev/null 2>&1; then
    LOG "ddb: table exists"
    return
  fi
  LOG "ddb: creating table ${DDB_TABLE}"
  aws dynamodb create-table --table-name "$DDB_TABLE" --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST --region "$AWS_REGION" >/dev/null
  aws dynamodb wait table-exists --table-name "$DDB_TABLE" --region "$AWS_REGION"
  LOG "ddb: table ready"
}

create_venv_if_missing(){
  if [ ! -d "$VENV_DIR" ]; then
    LOG "venv: creating at $VENV_DIR"
    "$PY_BIN" -m venv "$VENV_DIR"
    "$VENV_DIR/bin/python" -m pip install --upgrade "pip<26" >/dev/null
  fi
  if [ -f "$REQ_FILE" ]; then
    LOG "venv: installing from $REQ_FILE"
    "$VENV_DIR/bin/python" -m pip install -q -r "$REQ_FILE"
  else
    LOG "venv: no requirements.txt found; installing minimal runtime"
    "$VENV_DIR/bin/python" -m pip install -q pulumi pulumi-aws boto3
  fi
  VENV_PY="$VENV_DIR/bin/python"
  if [ ! -x "$VENV_PY" ]; then
    DIE "venv python not found or not executable at $VENV_PY"
  fi
  export PULUMI_PYTHON_CMD="${PULUMI_PYTHON_CMD:-$VENV_PY}"
  LOG "env: PULUMI_PYTHON_CMD set to $PULUMI_PYTHON_CMD"
}

write_outputs_json(){
  if [ -d "$PROJECT_DIR" ]; then
    cd "$PROJECT_DIR"
    LOG "pulumi: writing outputs to $PROJECT_DIR/outputs.json"
    pulumi stack output --stack "$STACK_SHORT" --json > outputs.json || LOG "pulumi stack output failed"
  else
    LOG "project dir $PROJECT_DIR not present; skipping outputs.json write"
  fi
}

remove_outputs_json(){
  if [ -d "$PROJECT_DIR" ]; then
    cd "$PROJECT_DIR"
    if [ -f outputs.json ]; then
      LOG "removing $PROJECT_DIR/outputs.json"
      rm -f outputs.json
    else
      LOG "no outputs.json present"
    fi
  else
    LOG "project dir $PROJECT_DIR not present; skipping outputs.json removal"
  fi
}

# -------------------------
# CREATE flow (non-interactive)
# -------------------------
if [ "$MODE" = "create" ]; then
  LOG "mode=create (non-interactive)"
  ensure_bucket
  ensure_ddb
  create_venv_if_missing
  export PULUMI_PYTHON_CMD

  LOG "pulumi: logging into backend $PULUMI_LOGIN_URL"
  pulumi login "$PULUMI_LOGIN_URL" >/dev/null

  LOG "pulumi: preparing project in $PROJECT_DIR"
  cd "$PROJECT_DIR"

  if [ ! -f Pulumi.yaml ]; then
    cat >Pulumi.yaml <<YAML
name: pulumi-infra
runtime: python
YAML
    LOG "created minimal Pulumi.yaml (name: pulumi-infra)"
  fi

  # select or init short stack (non-interactive)
  if pulumi stack select "$STACK_SHORT" >/dev/null 2>&1; then
    LOG "pulumi: selected existing stack $STACK_SHORT"
  else
    LOG "pulumi: creating stack $STACK_SHORT"
    pulumi stack init "$STACK_SHORT" >/dev/null
  fi

  # Export required envs into current process for Pulumi program to read (no pulumi config usage)
  # These are already present in env; reiterate here just for clarity and logs
  LOG "using environment values (non-interactive):"
  LOG "  PROJECT_PREFIX=${PROJECT_PREFIX}"
  LOG "  AWS_REGION=${AWS_REGION}"
  LOG "  ECR_REPO_NAME=${ECR_REPO_NAME}"
  LOG "  IMAGE_TAG=${IMAGE_TAG}"
  LOG "  UI_S3_BUCKET=${UI_S3_BUCKET}"
  LOG "  CREATE_CLOUDFRONT=${CREATE_CLOUDFRONT}"

  # Run preview and up (non-interactive). Pulumi program MUST read config from environment.
  LOG "pulumi: running preview (non-interactive)"
  if pulumi preview --stack "$STACK_SHORT" --non-interactive; then
    LOG "pulumi: preview succeeded; running up"
    pulumi up --stack "$STACK_SHORT" --yes
    write_outputs_json
    LOG "CREATE complete"
    exit 0
  else
    DIE "pulumi preview failed; aborting create"
  fi
fi

# -------------------------
# DELETE flow (non-interactive)
# -------------------------
if [ "$MODE" = "delete" ]; then
  LOG "mode=delete (non-interactive)"
  create_venv_if_missing
  export PULUMI_PYTHON_CMD

  LOG "pulumi: logging into backend $PULUMI_LOGIN_URL"
  pulumi login "$PULUMI_LOGIN_URL" >/dev/null

  cd "$PROJECT_DIR" || LOG "project dir $PROJECT_DIR not present; attempting remote stack operations"

  if pulumi stack select "$STACK_SHORT" >/dev/null 2>&1; then
    LOG "pulumi: selected stack $STACK_SHORT"
    LOG "pulumi: destroying selected stack (non-interactive)"
    pulumi destroy --stack "$STACK_SHORT" --yes --non-interactive || LOG "pulumi destroy returned non-zero"
    LOG "pulumi: removing selected stack metadata"
    pulumi stack rm --stack "$STACK_SHORT" --yes --non-interactive || LOG "pulumi stack rm returned non-zero"
  else
    LOG "pulumi: short stack $STACK_SHORT not selectable locally; trying explicit destroy"
    if pulumi destroy --stack "$STACK_SHORT" --yes --non-interactive 2>/dev/null; then
      LOG "pulumi: destroy --stack $STACK_SHORT succeeded"
      pulumi stack rm --stack "$STACK_SHORT" --yes --non-interactive || LOG "pulumi stack rm returned non-zero"
    else
      LOG "pulumi: explicit short-stack destroy failed; skipping (stack not present or permission denied)"
    fi
  fi

  if [ "$FORCE_DELETE" = "1" ]; then
    safe_delete_s3_prefix
    delete_ddb_table
  else
    LOG "FORCE_DELETE != 1; skipping backend S3 and DynamoDB deletion to preserve state"
  fi

  remove_outputs_json
  LOG "DELETE complete"
  exit 0
fi

DIE "internal error: unreachable"
