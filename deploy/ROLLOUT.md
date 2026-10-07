# Production rollout

The step-by-step path from an empty AWS account to every schedule running.
`README.md` in this directory is the architecture reference; this is the order
of operations. Tick boxes as you go.

Each phase ends in a state that is safe to leave overnight.

## 0. Repo (done)

- [x] CI builds and pushes `ghcr.io/wprdc/wprdc-etl` on every green `main`,
      and prints `WPRDC_ETL_IMAGE=…@sha256:…` in the job summary.
- [x] Nothing host-specific is committed: domain, UI credentials, SFTP host
      keys and secrets all come from files on the VM (§2).
- [x] `WPRDC_SCHEDULES_PAUSED=1` holds production schedules stopped (§4).

## 1. Provision

- [ ] **Postgres** (managed, 17 to match dev), two databases:
      ```sql
      CREATE DATABASE dagster;
      CREATE DATABASE etl_spatial;
      \connect etl_spatial
      CREATE EXTENSION postgis;
      ```
      Turn on automated backups. Dagster's run/event tables grow without
      bound; plan a retention job (deploy/prod/dagster.yaml only purges ticks).
- [ ] **S3**, us-east-1, Block Public Access on:
      - `wprdc-etl-landing` — then
        `aws s3api put-bucket-lifecycle-configuration --bucket wprdc-etl-landing --lifecycle-configuration file://deploy/s3-lifecycle.json`
      - `wprdc-dagster-runtime`
- [ ] **VM** — EC2, amd64 (CI builds amd64 only), ~16 GB RAM
      (`m6i.xlarge`/`t3.xlarge`: code-location is capped at 6 GB, plus
      webserver, daemon, OS), **50 GB gp3** root volume (peak ~25 GB: OS,
      2.2 GB images kept for rollback, run scratch; data lives in S3/RDS, so
      it doesn't grow with the data). Docker + compose plugin, the DB
      reachable from it. Ports 80/443 open; nothing else public.
- [ ] **IAM instance role** on the VM:
      - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, `s3:DeleteObject` on
        both buckets
      - `secretsmanager:GetSecretValue` on the secret(s) below
- [ ] **DNS** — an A record for the UI hostname (`DAGSTER_DOMAIN`) at the VM.
      Caddy fetches the certificate itself once the name resolves.
- [ ] **GHCR pull access** — the package is private by default. Either make
      `wprdc-etl` public in the org's package settings (the image holds code,
      no secrets), or `docker login ghcr.io` on the VM with a `read:packages`
      token.

## 2. Files on the VM

All under `/opt/wprdc-etl/`, root-owned. Check out the repo (or copy `deploy/`)
next to them — compose mounts `deploy/Caddyfile` by relative path.

- [ ] `deploy.env` — compose interpolation, not secret:
      ```sh
      WPRDC_ETL_IMAGE=ghcr.io/wprdc/wprdc-etl@sha256:...   # from the CI summary
      DAGSTER_DOMAIN=etl.example.org
      ENVIRONMENT=                                          # empty = dry run
      WPRDC_SCHEDULES_PAUSED=1
      ```
- [ ] `prod.env` (0600) — from Secrets Manager:
      ```sh
      DAGSTER_PG_URL=postgresql://dagster:...@host:5432/dagster
      SPATIAL_DSN=postgresql://dagster:...@host:5432/etl_spatial
      CKAN_API_TOKEN=...            # data.wprdc.org token with write access
      ALLEGHENY_SFTP=user:password
      ALLEGHENY_SFTP_HOST=...
      ALLEGHENY_SFTP_PORT=22
      DAGSTER_SLACK_BOT_TOKEN=xoxb-...
      ```
      e.g. `aws secretsmanager get-secret-value --secret-id wprdc-etl/prod --query SecretString --output text > prod.env`
      with the secret stored in env-file form.
- [ ] `proxy.env` (0600) — UI basic auth. Hash with
      `docker run --rm caddy:2 caddy hash-password`, and **single-quote** it:
      the hash is full of `$`, which compose would otherwise interpolate.
      ```sh
      DAGSTER_UI_USER=wprdc
      DAGSTER_UI_PASSWORD_HASH='$2a$14$...'
      ```
- [ ] `known_hosts` — SFTP host keys. `ssh-keyscan -p <port> <host> > known_hosts`,
      then **confirm the fingerprint** (`ssh-keygen -lf known_hosts`) with the
      publisher out of band. A key that isn't in this file fails the run at
      once, with no retries; a changed key does the same.

## 3. Boot, dry run

`ENVIRONMENT` empty. Nothing can publish, and nothing can land either: the S3
guard refuses real-AWS writes outside production. **Don't set
`WPRDC_ALLOW_REAL_S3` to get past it** — this phase proves the stack, not the
pipelines.

```sh
cd /opt/wprdc-etl/repo
docker compose --env-file /opt/wprdc-etl/deploy.env -f deploy/compose.prod.yaml pull
docker compose --env-file /opt/wprdc-etl/deploy.env -f deploy/compose.prod.yaml up -d
docker compose --env-file /opt/wprdc-etl/deploy.env -f deploy/compose.prod.yaml ps
```

- [ ] All four services healthy; `dagster instance migrate` ran in each log.
- [ ] `https://$DAGSTER_DOMAIN` gives a certificate and asks for the password.
- [ ] Code location loaded, 109 jobs and 108 schedules; Deployment → Daemons
      all green.
- [ ] Every schedule shows **Stopped**.

## 4. Production, schedules held

Set `ENVIRONMENT=production` in `deploy.env`, keep `WPRDC_SCHEDULES_PAUSED=1`,
re-run `up -d`. Writes are now real; schedules still don't fire. Everything
in this phase is launched by hand from the UI.

- [ ] **Schedules still Stopped** after the restart — check before anything else.
- [ ] **Region layers first** — the 15 datasets with a `region_layer:` block
      (`grep -rl region_layer: src/wprdc_etl/defs`): the county's zip codes,
      school/voting/council/senate/magisterial/DPW districts, municipal
      boundaries and census tracts; the city's police and fire zones, wards,
      neighborhoods, council districts and DPW street divisions. Until they've
      run, the 17 datasets with a `reverse_geocode` step fail, naming the
      missing layers. `gis/address_points` (the `key_layer`) belongs here too.
- [ ] **One dataset per source type** — an SFTP, an HTTP, an ArcGIS (mirror),
      a PASDA (`heavy`) and an `api_incremental` one. For each, on
      data.wprdc.org:
      - the rows and the data dictionary's type overrides are there
      - the resource carries `etl_sha256`
      - re-running it at once publishes nothing (it says the fingerprint matched)
- [ ] **Slack** — the failure sensor is on by default in production. Force a
      failure (e.g. launch an SFTP dataset for a partition old enough that its
      file is gone; it fails after the ~7 min of retries) and see it reach
      `#wprdc-etl-alerts` with a working link back to the UI.
- [ ] **Column-guard blocks** — expect some on datasets whose live table
      DataPusher+ retyped (`bool->text` and the like). Collect them, set
      `ckan.rebuild: true` on all of them in one commit, deploy, run each
      once, then revert the flag in the next commit. Batch these: every
      round is a commit, an image and a deploy.

## 5. Switch schedules on

Schedules are switched on in the UI, in batches. A schedule's UI state is
stored in Postgres and outlives redeploys; `WPRDC_SCHEDULES_PAUSED` only
decides the default for one nobody has touched.

- [ ] Turn on a first batch (e.g. the region layers from §4) **midweek**, so the Sunday
      03:00-08:59 ET window is the first run and you're around for it.
      Monthly datasets first run on the 1st, 03:00-07:59.
- [ ] Turn on `maintenance__catalogue_check__job` (Saturday 06:00). A quiet
      Saturday is the normal one; resolve any NEW / DRIFT before Sunday.
- [ ] Remaining batches, a weekend at a time.
- [ ] Once every schedule is on, remove `WPRDC_SCHEDULES_PAUSED` from
      `deploy.env` so new datasets start running when deployed.

## 6. Dataset follow-ups

- [ ] **Landmarks** — run `gis/address_points` before `addressing_landmarks`
      (the join reads `keyed_geometry`). Once the new package checks out,
      delete the old faulty `cd24b8f3`.
- [ ] Update the "Landmarks … DEV-ONLY" note in CLAUDE.md once it's live.

## Day-two operations

**Deploy a new version** — put the digest from the CI summary into
`deploy.env`, then `pull` + `up -d` as in §3. The entrypoint migrates on every
start. Then reclaim disk — each image is 2.2 GB:

```sh
docker image prune -af --filter "until=720h"
```

It removes only images no container uses and older than 30 days, so recent
rollback targets stay. **Roll back** the same way, with the previous digest;
keep a note of it.

**Dagster version bump** — a deliberate PR (CLAUDE.md); the migration runs on
the next deploy, and rolling back past it is not supported, so take a DB
snapshot first.

**SFTP host key rotated** — verify the new fingerprint with the publisher,
update `known_hosts`, `up -d` (no rebuild).
