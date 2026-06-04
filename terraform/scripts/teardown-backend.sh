#!/usr/bin/env bash
set -euo pipefail

# teardown-backend.sh
#
# Tears down the remote Terraform backend FOR ONE ENVIRONMENT.
#
# What it does:
#   1. Resets the env's backend.tf to a placeholder (so the next person who
#      tries `terraform init` for that env is forced to bootstrap again).
#   2. If --delete-bucket is passed AND no OTHER envs are still using the
#      bucket, empties and deletes the shared state bucket too. Without
#      the flag we leave the bucket alone — other envs may still need it.
#
# DESTRUCTIVE: --delete-bucket permanently wipes ALL state in the bucket,
# regardless of which env it belongs to. Only use when you're tearing the
# entire project down.
#
# Usage:
#   ./teardown-backend.sh                                   # reset develop backend.tf only
#   ./teardown-backend.sh --env=prod
#   ./teardown-backend.sh --env=develop --delete-bucket     # also wipe the shared bucket

ENV="develop"
REGION="eu-west-1"
DELETE_BUCKET=false

for arg in "$@"; do
  case $arg in
    --env=*)         ENV="${arg#*=}" ;;
    --region=*)      REGION="${arg#*=}" ;;
    --delete-bucket) DELETE_BUCKET=true ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_TF="${SCRIPT_DIR}/../envs/${ENV}/backend.tf"

if [ ! -d "${SCRIPT_DIR}/../envs/${ENV}" ]; then
  echo "ERROR: env folder not found: ${SCRIPT_DIR}/../envs/${ENV}"
  exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="music-streaming-tfstate-${ACCOUNT_ID}"

echo "========================================================"
echo "  Music Streaming ETL — Backend Teardown"
echo "  Account       : ${ACCOUNT_ID}"
echo "  Env           : ${ENV}"
echo "  Bucket        : ${BUCKET}"
echo "  Region        : ${REGION}"
echo "  Delete bucket : ${DELETE_BUCKET}"
echo "========================================================"

# ── (Optional) empty and delete the shared S3 bucket ─────────────────────────
# Versioned buckets cannot be deleted while they hold any object versions
# or delete markers — we enumerate everything and bulk-delete in chunks
# of up to 1000 (the API limit), which is why we use python below.
if [ "${DELETE_BUCKET}" = "true" ]; then
  if aws s3api head-bucket --bucket "${BUCKET}" --region "${REGION}" 2>/dev/null; then
    echo "[DELETE] Emptying bucket ${BUCKET}..."

    # Delete all object versions.
    aws s3api list-object-versions \
      --bucket "${BUCKET}" \
      --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
      --output json | \
    python3 -c "
import json, sys, subprocess
data = json.load(sys.stdin)
objects = data.get('Objects') or []
if objects:
    payload = json.dumps({'Objects': objects, 'Quiet': True})
    subprocess.run(['aws', 's3api', 'delete-objects',
                    '--bucket', '${BUCKET}',
                    '--delete', payload], check=True)
    print(f'  Deleted {len(objects)} object version(s).')
else:
    print('  No object versions found.')
"

    # Delete all delete markers (tombstones left by previous deletes).
    aws s3api list-object-versions \
      --bucket "${BUCKET}" \
      --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
      --output json | \
    python3 -c "
import json, sys, subprocess
data = json.load(sys.stdin)
objects = data.get('Objects') or []
if objects:
    payload = json.dumps({'Objects': objects, 'Quiet': True})
    subprocess.run(['aws', 's3api', 'delete-objects',
                    '--bucket', '${BUCKET}',
                    '--delete', payload], check=True)
    print(f'  Deleted {len(objects)} delete marker(s).')
else:
    print('  No delete markers found.')
"

    aws s3api delete-bucket --bucket "${BUCKET}" --region "${REGION}"
    echo "[OK] Bucket ${BUCKET} deleted."
  else
    echo "[SKIP] Bucket ${BUCKET} does not exist."
  fi
else
  echo "[KEEP] Shared state bucket left intact. Use --delete-bucket to remove it (DESTRUCTIVE)."
fi

# ── Reset envs/<env>/backend.tf to placeholder ───────────────────────────────
# We don't delete the file because its presence signals to anyone reading
# the repo that the env uses remote state and needs bootstrapping. The
# <account-id> placeholder is invalid as-is, so `terraform init` will fail
# loudly here until setup-backend.sh is re-run — that's the intended
# guardrail.
echo "[RESET] Restoring ${BACKEND_TF} to placeholder state..."

cat > "${BACKEND_TF}" <<EOF
###############################################################################
# backend.tf — Remote state configuration (PLACEHOLDER, ${ENV})
#
# Run scripts/setup-backend.sh --env=${ENV} to provision the S3 state
# bucket and rewrite this file with the real values. The placeholder below
# is intentionally invalid so \`terraform init\` fails until the bootstrap
# script has run.
###############################################################################

terraform {
  backend "s3" {
    bucket       = "music-streaming-tfstate-<account-id>"
    key          = "etl/music-streaming/${ENV}/terraform.tfstate"
    region       = "eu-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
EOF

echo "[OK] ${BACKEND_TF} reset to placeholder."

echo ""
echo "========================================================"
echo "  Teardown complete for env=${ENV}."
echo "  To set up again run:"
echo "    ./scripts/setup-backend.sh --env=${ENV}"
echo "========================================================"
