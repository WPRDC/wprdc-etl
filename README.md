# wprdc-etl

Civic-data ETL for the [WPRDC](https://www.wprdc.org/) open data portal, built on
[Dagster](https://dagster.io/) components. Each dataset is a small YAML file; the
framework handles acquisition, an immutable S3 landing zone, transformation,
validation, and publishing to CKAN.

## Overview

A pipeline job is one **component instance** — a `defs.yaml` under `src/wprdc_etl/defs/`.
Two component types cover everything:

- **`TabularPipeline`** — data that is parsed, validated, and published to CKAN's
  DataStore (CSV/JSON/geospatial sources). Shape: `landed → validated → loaded`,
  with a `schema_ok` check.
- **`FilePipeline`** — blobs published as-is (PDFs, GeoTIFFs, images). Shape:
  `landed → published`. No parsing or validation.

Both share the same substrate: extract strategies, the S3 landing zone, the
taxonomy/asset-key scheme, scheduling, and the safety guards below.

### Pipeline stages (tabular)

| Stage | What it does |
|-------|--------------|
| **extract** | Pulls the source (SFTP today; HTTP/API stubbed) and lands raw bytes + a `manifest.json` in S3, checksum-idempotent. |
| **read** | Parses the landed file by format — CSV, JSON, or geospatial (GeoJSON / zipped Shapefile → GeoDataFrame). |
| **transform** | Declarative steps from the YAML (shared primitives) then an optional co-located `transform.py` for bespoke logic. |
| **validate** | `schema_ok` check — the file must be a **superset** of the declared columns; missing a declared column is a hard error. |
| **load** | `snapshot` → replace the CKAN resource via DataPusher+; `incremental` → upsert deltas by primary key. |
| **emit** | Optional extra representations (GeoJSON, Shapefile) published as their own CKAN file resources. |

### Key concepts

- **Immutable landing zone.** Every run lands the raw source in S3 under
  `publisher/[department/]dataset/<partition>/`, with a manifest (sha256, size,
  source mtime). Replay and audit come from here, not from re-fetching the source.
- **Snapshot vs incremental** (`ingest`): `snapshot` = full state each run (replace);
  `incremental` = cursor/watermark delta pull (upsert). The watermark is stored in S3.
- **Shared-vs-bespoke everywhere.** Common transforms/schemas are a shared library;
  a dataset drops a co-located `transform.py` / `schema.py` / `fetch.py` only when it
  needs something the shared vocabulary can't express.

## Project layout

```
wprdc-etl/
├── compose.yaml                 # dev deps: LocalStack (S3), Postgres, SFTP
├── dagster.yaml                 # optional: Postgres run storage (prod: managed PG)
├── .env.example                 # all env vars, dev + prod notes
├── pyproject.toml               # deps + [tool.dg.project] registry_modules
├── scripts/
│   ├── scaffold_pipeline.py     # interactive `defs.yaml` generator
│   └── _prompt.py               # shared questionary prompts
├── dev/
│   ├── README.md                # local loop details
│   └── sftp/                    # files here are served by the local SFTP server
└── src/wprdc_etl/
    ├── definitions.py           # top-level Definitions; wires resources once
    ├── runtime.py               # ENVIRONMENT gate: dry-run default + S3 write guard
    ├── resources.py             # LandingZone (S3) / SFTP / Ckan / Geocoder
    ├── dataset_modules.py       # loads co-located transform/schema/fetch modules
    ├── components/              # the component TYPES (registered in pyproject)
    │   ├── tabular_pipeline.py
    │   ├── file_pipeline.py
    │   ├── models.py            # shared config models
    │   └── _common.py           # taxonomy / partitions / schedule helpers
    ├── strategies/              # pluggable stage logic
    │   ├── extract.py  read.py  transform.py  load.py  emit.py  schema.py
    └── defs/                    # component INSTANCES (the jobs)
        └── allegheny_county/real_estate/assessments/
            ├── defs.yaml        # the instance config
            ├── schema.py        # full column contract (optional)
            └── transform.py     # bespoke transform (optional)
```

> **`defs/` is walked by Dagster.** Everything under it must be a component
> (`defs.yaml`) or a directory leading to one. Don't put tests, stray YAML, or
> non-component files here — they break the tree walker. Co-located `.py`
> (transform/schema/fetch) are fine; they're imported, not walked.

## Getting started

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                          # install deps into .venv
cp .env.example .env             # then edit as needed
uv run pre-commit install        # black runs on git commit (see .pre-commit-config.yaml)
```

## Local development

Everything defaults to a **dry run** (see Safety below), so local runs never touch
real S3 or CKAN unless you explicitly opt in.

```bash
# 1. start dev dependencies
docker compose up -d

# 2. create the landing bucket in LocalStack (once per fresh LocalStack)
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:4566 s3 mb s3://wprdc-etl-landing

# 3. load your .env and start Dagster
set -a && . ./.env && set +a
uv run dg dev                    # UI at http://localhost:3000
```

In the UI, open **Assets**, select a dataset's assets, and **Materialize** — for a
partitioned dataset, pick a partition (e.g. `2026-07-01`). In dry-run the `loaded`
step writes `_dryrun/<publisher>__<department>__<dataset>.csv` instead of calling CKAN.

Test sources: drop files in `dev/sftp/` and point a source at the local server
(`host: localhost`, `port: 2222`, `secret_ref: WPRDC_LOCAL_SFTP`). See `dev/README.md`.

## Adding a dataset

Use the scaffolder — it generates a correctly-named `defs.yaml`, the `__init__.py`
files, and (optionally) drafts `schema.py` from an existing CKAN resource:

```bash
uv run python scripts/scaffold_pipeline.py
```

Or write `defs.yaml` by hand. A tabular instance:

```yaml
type: wprdc_etl.components.tabular_pipeline.TabularPipeline

attributes:
  publisher: allegheny_county
  department: real_estate            # optional middle tier
  dataset: assessments
  source:
    type: sftp                       # sftp | http | api_bulk | api_incremental
    host: sftp.example.gov
    port: 22                         # optional (defaults to 22)
    path: /outbound/*.csv
    secret_ref: ALLEGHENY_SFTP       # env var holding "user:password"
  schedule: "0 6 1 * *"              # cron; omit for a placeholder sensor
  partition: monthly                 # none | daily | weekly | monthly
  ingest: snapshot                   # snapshot (replace) | incremental (upsert)
  transforms:                        # declarative steps, run in order
    - op: iso_date
      columns: [SALEDATE]
      fmt: "%m/%d/%Y"
    - op: coerce_numeric
      columns: [FAIRMARKETTOTAL]
    - op: strip
  ckan:
    resource_id: "REPLACE-WITH-CKAN-RESOURCE-UUID"
    # primary_key: [PARID]           # required when ingest: incremental
  # representations:                 # optional extra geo outputs
  #   - { format: geojson, resource_id: "...", lat: LAT, lng: LON }
```

**Co-located files** (all optional, next to `defs.yaml`):

- `schema.py` — a full column contract (`SCHEMA = frame({...})`) using the builders
  in `strategies/schema.py` (`key`, `txt`, `num`, `ge0`, `ranged`, `coded`, `year`).
  All declared columns are required; the file must be a superset.
- `transform.py` — `def transform(df, cfg): ...`, run after the declarative steps.
- `fetch.py` — `def fetch(source, since): ...` returning `(records, new_watermark)`;
  **required** for `api_incremental` sources.

**Transform primitives:** `iso_date`, `strip`, `rename`, `coerce_numeric`,
`drop_nulls`, `fill_na`, `select`, `drop_columns`, `snake_case_columns`, `to_crs`.

## Safety model

Two guards, both keyed off `ENVIRONMENT`. **Dry-run is the default** — you must set
`ENVIRONMENT=production` to write anywhere real.

| Guard | Non-production (default) | Production |
|-------|--------------------------|------------|
| **CKAN publish** | writes to `_dryrun/`, never calls CKAN | replaces/upserts the CKAN resource |
| **S3 landing write** | refused if targeting real AWS S3 (no `S3_ENDPOINT_URL`), unless `WPRDC_ALLOW_REAL_S3=1` | writes to real S3 |

Plus, independent of environment:

- **Schema superset check** — a landed file missing any declared column fails the
  `schema_ok` check (ERROR); value/type issues are warnings.
- **CKAN compatibility check** — before writing, the output schema is compared to the
  live CKAN resource. Upsert **blocks** on a column type change; replace **warns**.
- **Checksum idempotency** — an unchanged source file is not re-landed.

## Production

Target: a single VM running `docker compose` with three long-lived services —
`dagster-webserver`, `dagster-daemon`, and the code-location gRPC server — plus
an authenticating reverse proxy in front of the UI. Managed Postgres for the
Dagster instance; real AWS S3 for the landing zone and Dagster runtime state;
secrets from AWS Secrets Manager / SSM injected as container env.

Environment (injected at deploy time — never a committed `.env`):

```bash
ENVIRONMENT=production            # REQUIRED — without it, everything dry-runs
AWS_REGION=us-east-1              # no S3_ENDPOINT_URL / AWS keys — use the IAM role
LANDING_BUCKET=wprdc-etl-landing
DAGSTER_RUNTIME_BUCKET=wprdc-dagster-runtime   # S3 IO-manager pickles + compute logs
CKAN_URL=https://data.wprdc.org
CKAN_API_KEY=...
# per-source secrets, e.g. ALLEGHENY_SFTP=user:password

# Dagster instance (config: deploy/prod/dagster.yaml):
DAGSTER_HOME=/opt/dagster/home
DAGSTER_PG_URL=postgresql://user:pass@managed-host:5432/dagster?sslmode=require
DAGSTER_MAX_CONCURRENT_RUNS=4     # run-queue cap; tune without a redeploy

# Run-failure alerting (Slack):
DAGSTER_SLACK_BOT_TOKEN=xoxb-...
WPRDC_ALERT_SLACK_CHANNEL=#wprdc-etl-alerts
DAGSTER_WEBSERVER_URL=https://dagster.example.org
```

Notes:
- Credentials for S3 come from the instance/task/IRSA **IAM role**, not env keys.
- `ENVIRONMENT=production` must be set or the deploy silently publishes nothing.
  It also switches the asset IO manager to S3 (`S3PickleIOManager`) and enables
  the run-failure sensor.
- The **production** instance config is `deploy/prod/dagster.yaml` — it must be
  at `$DAGSTER_HOME/dagster.yaml` (the container copies it there). The repo-root
  `dagster.yaml` is the *dev* one that `dg dev` / `dg check` read; it is not used
  in prod.
- The **daemon is required**: schedules, sensors, the run queue, run monitoring,
  and alerting all need `dagster-daemon run`.
- Run `dagster instance migrate` on first boot and after every `dagster` version
  bump, before starting the webserver / daemon.
- SFTP host keys are verified in production (`SFTPResource.known_hosts`, or the
  container user's `~/.ssh/known_hosts`); an unknown key is rejected, not
  auto-added.

Infra still to build (see `deploy/prod/dagster.yaml`, and the deploy plan):
the `Dockerfile`, `deploy/compose.prod.yaml`, the two S3 buckets + IAM role +
bucket policies, the managed Postgres, and the reverse-proxy auth.

## Status / known gaps

- Extractors: **SFTP** and **API-incremental** implemented; **HTTP** and **bulk-API**
  are stubs.
- The `GeocoderResource` is a stub (only used when a dataset sets `geocode: true`).
- Geo + `incremental` isn't supported (geometry isn't JSON-serializable for upsert).
- Column names / date formats in the assessments example are drawn from the WPRDC
  data dictionary and should be verified against a real extract.