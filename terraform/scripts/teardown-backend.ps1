# teardown-backend.ps1
#
# PowerShell equivalent of teardown-backend.sh — tears down the remote
# Terraform backend FOR ONE ENVIRONMENT on Windows.
#
# What it does:
#   1. Resets envs\<env>\backend.tf to a placeholder (so the next person
#      who tries `terraform init` for that env must bootstrap again).
#   2. If -DeleteBucket is passed, also empties and deletes the SHARED
#      state bucket. Use only when tearing the whole project down — other
#      envs may still need it.
#
# DESTRUCTIVE with -DeleteBucket: every state file in the bucket is gone
# permanently, regardless of which env it belonged to.
#
# Usage:
#   .\teardown-backend.ps1                                # reset develop backend.tf only
#   .\teardown-backend.ps1 -Env prod
#   .\teardown-backend.ps1 -Env develop -DeleteBucket    # also wipe shared bucket

param(
    [string]$Env    = "develop",
    [string]$Region = "eu-west-1",
    [switch]$DeleteBucket
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$EnvDir    = Join-Path $ScriptDir "..\envs\$Env"
$BackendTf = Join-Path $EnvDir "backend.tf"

if (-not (Test-Path $EnvDir)) {
    Write-Host "ERROR: env folder not found: $EnvDir"
    exit 1
}

$AccountId = (aws sts get-caller-identity --query Account --output text)
$Bucket    = "music-streaming-tfstate-$AccountId"

Write-Host "========================================================"
Write-Host "  Music Streaming ETL — Backend Teardown"
Write-Host "  Account       : $AccountId"
Write-Host "  Env           : $Env"
Write-Host "  Bucket        : $Bucket"
Write-Host "  Region        : $Region"
Write-Host "  Delete bucket : $DeleteBucket"
Write-Host "========================================================"

# ── (Optional) empty and delete the shared S3 bucket ─────────────────────────
# Versioned buckets reject delete until every version + delete marker is
# gone. We list both and bulk-delete in chunks via inline Python (the
# AWS CLI has no native equivalent that takes JSON via stdin).
if ($DeleteBucket) {
    $bucketExists = $false
    try {
        aws s3api head-bucket --bucket $Bucket --region $Region 2>$null
        if ($LASTEXITCODE -eq 0) { $bucketExists = $true }
    } catch {
        $bucketExists = $false
    }

    if ($bucketExists) {
        Write-Host "[DELETE] Emptying bucket $Bucket..."

        # Delete all object versions.
        $versionsJson = aws s3api list-object-versions `
            --bucket $Bucket `
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' `
            --output json
        $versionsJson | python -c @"
import json, sys, subprocess
data = json.load(sys.stdin)
objects = data.get('Objects') or []
if objects:
    payload = json.dumps({'Objects': objects, 'Quiet': True})
    subprocess.run(['aws', 's3api', 'delete-objects',
                    '--bucket', '$Bucket',
                    '--delete', payload], check=True)
    print(f'  Deleted {len(objects)} object version(s).')
else:
    print('  No object versions found.')
"@

        # Delete all delete markers.
        $markersJson = aws s3api list-object-versions `
            --bucket $Bucket `
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' `
            --output json
        $markersJson | python -c @"
import json, sys, subprocess
data = json.load(sys.stdin)
objects = data.get('Objects') or []
if objects:
    payload = json.dumps({'Objects': objects, 'Quiet': True})
    subprocess.run(['aws', 's3api', 'delete-objects',
                    '--bucket', '$Bucket',
                    '--delete', payload], check=True)
    print(f'  Deleted {len(objects)} delete marker(s).')
else:
    print('  No delete markers found.')
"@

        aws s3api delete-bucket --bucket $Bucket --region $Region
        Write-Host "[OK] Bucket $Bucket deleted."
    } else {
        Write-Host "[SKIP] Bucket $Bucket does not exist."
    }
} else {
    Write-Host "[KEEP] Shared state bucket left intact. Use -DeleteBucket to remove it (DESTRUCTIVE)."
}

# ── Reset envs\<env>\backend.tf to placeholder ───────────────────────────────
# Keep the file in version control as a placeholder so anyone looking at
# the repo can see the env uses remote state and needs bootstrapping. The
# <account-id> placeholder is invalid as-is, so `terraform init` will fail
# loudly until setup-backend.ps1 is re-run — that's the intended guardrail.
Write-Host "[RESET] Restoring $BackendTf to placeholder state..."

$PlaceholderContent = @"
###############################################################################
# backend.tf — Remote state configuration (PLACEHOLDER, $Env)
#
# Run scripts/setup-backend.ps1 -Env $Env to provision the S3 state bucket
# and rewrite this file with the real values. The placeholder below is
# intentionally invalid so ``terraform init`` fails until the bootstrap
# script has run.
###############################################################################

terraform {
  backend "s3" {
    bucket       = "music-streaming-tfstate-<account-id>"
    key          = "etl/music-streaming/$Env/terraform.tfstate"
    region       = "eu-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
"@

Set-Content -Path $BackendTf -Value $PlaceholderContent -Encoding UTF8

Write-Host "[OK] $BackendTf reset to placeholder."
Write-Host ""
Write-Host "========================================================"
Write-Host "  Teardown complete for env=$Env."
Write-Host "  To set up again run:"
Write-Host "    .\scripts\setup-backend.ps1 -Env $Env"
Write-Host "========================================================"
