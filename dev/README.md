# Local development

Local stand-ins for the three external things a pipeline touches — a source, the
S3 landing zone, and CKAN — plus a no-CKAN fast path.

## Bring up the deps

```
docker compose up -d
aws --endpoint-url http://localhost:4566 s3 mb s3://wprdc-etl-landing   # once
```

This starts:

| Service     | What it stands in for      | Where                     |
|-------------|----------------------------|---------------------------|
| localstack  | S3 landing zone            | http://localhost:4566     |
| postgres    | Dagster run/event storage  | localhost:5432            |
| sftp        | an SFTP source             | localhost:2222 (wprdc/wprdc) |

Put test files in `dev/sftp/` — they show up at `/outbound/` on the server.

## Two ways to run a pipeline locally

**1. No CKAN (fastest — iterate on extract/transform/validate).**
Set a sink dir and the loaders write output to local files instead of calling
CKAN. The full extract → land (LocalStack) → transform → validate path runs;
only the publish is redirected.

```
export WPRDC_ETL_SINK_DIR=./_dryrun
dg dev
```

Output lands in `_dryrun/<publisher>__<department>__<dataset>.csv` (and
`.geojson`/`.zip` for geo representations). `_dryrun/` is gitignored.

**2. Against CKAN (test the real publish).**
CKAN is a heavy multi-service stack (CKAN + Postgres + Solr + Redis +
xloader/DataPusher), and you already have a dockerized deployment — run that
from its own repo rather than a from-scratch stack here. Then point the ETL at
it and leave `WPRDC_ETL_SINK_DIR` unset:

```
# in your ckan-docker checkout
docker compose up -d          # CKAN on http://localhost:5000

# then, for wprdc-etl
export CKAN_URL=http://localhost:5000
export CKAN_API_KEY=<a dev sysadmin token>
```

(The `CkanResource.base_url` defaults to production; override it via the
top-level Definitions / env for local runs.)

## .env for local dev

```
S3_ENDPOINT_URL=http://localhost:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=us-east-1
LANDING_BUCKET=wprdc-etl-landing
WPRDC_LOCAL_SFTP=wprdc:wprdc
# WPRDC_ETL_SINK_DIR=./_dryrun     # uncomment for the no-CKAN path
# CKAN_URL / CKAN_API_KEY          # only when testing the real publish
```

## Exercising the production S3 IO manager locally (optional)

By default dev uses the filesystem IO manager for the `validated -> loaded`
asset handoff. To run the same `S3PickleIOManager` path production uses, against
LocalStack:

```
aws --endpoint-url http://localhost:4566 s3 mb s3://wprdc-dagster-runtime   # once

export WPRDC_IO_MANAGER=s3
export AWS_S3_ADDRESSING_STYLE=path        # S3Resource has no path-style flag
export DAGSTER_RUNTIME_BUCKET=wprdc-dagster-runtime
dg dev
```

This does **not** set `ENVIRONMENT`, so publishing still dry-runs to `_dryrun/`
— only the intermediate DataFrame now round-trips through S3.

## A test source component.yaml

```yaml
source:
  type: sftp
  host: localhost
  port: 2222
  path: /outbound/*.csv
  secret_ref: WPRDC_LOCAL_SFTP
```