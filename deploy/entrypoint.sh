#!/usr/bin/env bash
# Shared entrypoint for all Dagster services (webserver / daemon / code-server).
#
# Runs `dagster instance migrate` first — idempotent, a no-op when the Postgres
# schema is already current, required on first boot and after every `dagster`
# version bump. Then exec's the service command passed as CMD.
set -euo pipefail

: "${DAGSTER_HOME:?DAGSTER_HOME must be set}"

if [ "${DAGSTER_SKIP_MIGRATE:-}" != "1" ]; then
  echo "==> dagster instance migrate"
  dagster instance migrate
fi

echo "==> exec: $*"
exec "$@"
