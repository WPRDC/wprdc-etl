#!/usr/bin/env bash
# Shared helpers for the bin/ scripts. Source it, don't run it:
#
#     . "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
#
# The .env loader delegates to python-dotenv via the same function Dagster uses
# (dagster._core.secrets.env_file.get_env_var_dict). That costs an interpreter
# start, and buys exact parity: `${POSTGIS_PORT}` interpolation, quoting and
# comment handling resolve the way `dg dev` resolves them. A hand-rolled
# `set -a; . .env` drifts on exactly those cases.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

if [ -t 1 ]; then
  C_OK=$'\033[32m'; C_WARN=$'\033[33m'; C_ERR=$'\033[31m'
  C_DIM=$'\033[2m'; C_B=$'\033[1m'; C_0=$'\033[0m'
else
  C_OK=''; C_WARN=''; C_ERR=''; C_DIM=''; C_B=''; C_0=''
fi

say()  { printf '%s\n' "$*"; }
head1(){ printf '%s%s%s\n' "$C_B" "$*" "$C_0"; }
ok()   { printf '  %s✓%s %s\n' "$C_OK" "$C_0" "$*"; }
warn() { printf '  %s!%s %s\n' "$C_WARN" "$C_0" "$*"; }
bad()  { printf '  %s✗%s %s\n' "$C_ERR" "$C_0" "$*"; }
dim()  { printf '  %s%s%s\n' "$C_DIM" "$*" "$C_0"; }
die()  { printf '%s%s%s\n' "$C_ERR" "$*" "$C_0" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

# Populate the environment from .env, without clobbering anything already
# exported — an explicit `FOO=bar bin/doctor` should still win.
load_env() {
  [ -f "$REPO/.env" ] || return 0
  local dumped
  dumped="$(uv run python -c '
import os, shlex
from dagster._core.secrets.env_file import get_env_var_dict
for k, v in get_env_var_dict(os.getcwd()).items():
    if k not in os.environ:
        print(f"export {k}={shlex.quote(v)}")
' 2>/dev/null)" || return 0
  eval "$dumped"
}

# Defaults matching .env.example, applied after load_env so .env wins.
env_defaults() {
  : "${S3_ENDPOINT_URL:=http://localhost:4566}"
  : "${AWS_ACCESS_KEY_ID:=test}"
  : "${AWS_SECRET_ACCESS_KEY:=test}"
  : "${AWS_REGION:=us-east-1}"
  : "${AWS_DEFAULT_REGION:=$AWS_REGION}"
  : "${LANDING_BUCKET:=wprdc-etl-landing}"
  : "${POSTGIS_PORT:=5434}"
  : "${SPATIAL_DSN:=postgresql://dagster:dagster@localhost:${POSTGIS_PORT}/etl_spatial}"
  : "${DAGSTER_PG_URL:=postgresql://dagster:dagster@localhost:5432/dagster}"
  : "${CKAN_URL:=http://localhost:5001}"
  export S3_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION \
         AWS_DEFAULT_REGION LANDING_BUCKET POSTGIS_PORT SPATIAL_DSN \
         DAGSTER_PG_URL CKAN_URL
}

# awscli pointed at LocalStack instead of real AWS.
awslocal() { aws --endpoint-url "$S3_ENDPOINT_URL" "$@"; }

load_env
env_defaults
