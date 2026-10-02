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
| postgis     | the admin-region store     | localhost:`$POSTGIS_PORT` (default 5434, db `spatial`) |
| sftp        | an SFTP source             | localhost:2222 (wprdc/wprdc) |

`postgis` is a second server, not a second database on the first — the official
`postgres:17` image is Debian trixie while `postgis/postgis:17-3.5` is bullseye,
and pointing the latter at the existing `dagster` volume trips a collation
version mismatch. In production `spatial` is just another database on the
managed instance with the extension enabled; the split is dev-only.

To use reverse geocoding locally, point the app at it and materialize the
boundary layers once:

```
export SPATIAL_DSN=postgresql://dagster:dagster@localhost:5434/spatial   # or $POSTGIS_PORT
dg launch --job city_of_pittsburgh__boundaries__neighborhood__job --partition <month>
```

Check what's loaded:

```
docker compose exec postgis psql -U dagster -d spatial \
  -c "SELECT layer, count(*) FROM admin_region GROUP BY layer;"
```

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

**1b. Load the boundary layers first.**
`reverse_geocode` reads the PostGIS admin-region store, so a dataset using it
(`city_of_pittsburgh/water_features`) fails until its layers are loaded:

```
uv run python scripts/refresh_boundaries.py --list   # see what's there
uv run python scripts/refresh_boundaries.py          # load them
```

Runs in process, needs no daemon, and touches no CKAN — the boundary pipelines
have no `ckan:` block. Layers still carrying a stub `source.url` are skipped
with a note rather than failing the batch.

**2. Against CKAN (test the real publish).**
CKAN is a heavy multi-service stack (CKAN + Postgres + Solr + Redis +
xloader/DataPusher), and you already have a dockerized deployment — run that
from its own repo rather than a from-scratch stack here. Then point the ETL at
it and opt in to real CKAN calls:

```
# in your ckan-docker checkout
docker compose up -d          # CKAN on http://localhost:5001
```

then in `wprdc-etl/.env` (gitignored; see the block below):

```
CKAN_URL=http://localhost:5001
CKAN_API_TOKEN=<a dev sysadmin token>
WPRDC_CKAN_WRITE=1            # publish for real instead of writing _dryrun/
```

`WPRDC_CKAN_WRITE=1` is what makes the publish steps actually call CKAN. Do NOT
reach for `ENVIRONMENT=production` — that would also unlock real AWS S3 writes,
unguard the spatial store, and switch the IO manager and alert sensor. Clearing
`WPRDC_ETL_SINK_DIR` does nothing on its own: unset is what produces `_dryrun/`.

`guard_ckan_write` refuses a non-local `CKAN_URL`, so this can't reach
data.wprdc.org by accident (the host is what's checked — any port is fine). The
landing step still writes to S3, so LocalStack has to be up as well.

(The `CkanResource.base_url` defaults to production; override it via the
top-level Definitions / env for local runs.)

## .env for local dev

Dagster loads `.env` from the working directory on every `dg` / `dagster` CLI
command (`_inject_local_env_file`), so put dev settings here rather than
exporting them each shell. Run workers are subprocesses of the `dg dev` process
and inherit them. `.env` is gitignored; `.env.example` is the committed template.

```
S3_ENDPOINT_URL=http://localhost:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=us-east-1
LANDING_BUCKET=wprdc-etl-landing
WPRDC_LOCAL_SFTP=wprdc:wprdc

# publishing to the local CKAN (leave commented for the _dryrun/ path)
# CKAN_URL=http://localhost:5001
# CKAN_API_TOKEN=<a dev sysadmin token>
# WPRDC_CKAN_WRITE=1

# pulling from a real source instead of the compose sftp service
# ALLEGHENY_SFTP_HOST=... / ALLEGHENY_SFTP_PORT=... / ALLEGHENY_SFTP=user:pass
```

One caveat: only the CLI loads `.env`. A bare `uv run python some_script.py`
does not, so a one-off script needs the variables exported (or has to parse
`.env` itself).

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