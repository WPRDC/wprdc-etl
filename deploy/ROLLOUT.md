# Production rollout

The step-by-step path from an empty AWS account to every schedule running.
`README.md` in this directory is the architecture reference; this is the order
of operations. Tick boxes as you go.

Each phase ends in a state that is safe to leave overnight.

## 0. Repo (done)

- [x] CI builds and pushes `ghcr.io/wprdc/wprdc-etl` on every green `main`,
      tagged with the full commit sha (and `latest`); `deploy/release`
      deploys by that tag.
- [x] Nothing host-specific is committed: domain, UI credentials, SFTP host
      keys and secrets all come from files on the VM (§2).
- [x] `WPRDC_SCHEDULES_PAUSED=1` holds production schedules stopped (§4).

## 1. Provision

- [x] **Postgres** (managed, 17 to match dev), two databases:
      ```sql
      CREATE DATABASE dagster;
      CREATE DATABASE etl_spatial;
      \connect etl_spatial
      CREATE EXTENSION postgis;
      ```
      Turn on automated backups. Dagster's run/event tables grow without
      bound; plan a retention job (deploy/prod/dagster.yaml only purges ticks).
- [x] **S3**, us-east-1, Block Public Access on:
      - `wprdc-etl-landing` — then
        `aws s3api put-bucket-lifecycle-configuration --bucket wprdc-etl-landing --lifecycle-configuration file://deploy/s3-lifecycle.json`
      - `wprdc-dagster-runtime`
- [x] **VM** — EC2, Ubuntu, amd64 (CI builds amd64 only), ~16 GB RAM
      (`m6i.xlarge`/`t3.xlarge`: code-location is capped at 6 GB, plus
      webserver, daemon, OS), **50 GB gp3** root volume (peak ~25 GB: OS,
      2.2 GB images kept for rollback, run scratch; data lives in S3/RDS, so
      it doesn't grow with the data). Docker + compose plugin, `jq`, the DB
      reachable from it. Ports 80/443 open; nothing else public.
- [x] **IAM instance role** on the VM:
      - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, `s3:DeleteObject` on
        both buckets
      - `secretsmanager:GetSecretValue` on the `wprdc/etl/prod` secret (§2)
- [x] **DNS** — an A record for the UI hostname (`DAGSTER_DOMAIN`) at the VM.
      Caddy fetches the certificate itself once the name resolves.
- [x] **GHCR pull access** — the package is private by default. **Make it
      public** (recommended): the repo is already public and the image holds
      only that code, no secrets, so it reveals nothing new — and the VM then
      needs no credential that can expire or leave with the person who made it.
- 
1. If the org restricts it: github.com/organizations/WPRDC/settings →
   **Packages** → allow **Public** under package creation/visibility
   (needs an org owner).
2. github.com/orgs/WPRDC/packages → `wprdc-etl` → **Package settings** →
   **Danger Zone** → **Change visibility** → Public, type the name to
   confirm. Needs admin on the package (an org owner has it).
3. On the VM, logged out of GHCR, as `ubuntu`:
   ```sh
   docker logout ghcr.io
   docker pull ghcr.io/wprdc/wprdc-etl:latest


## 2. Files on the VM

All under `/opt/wprdc-etl/`, owned by the instance's default `ubuntu` user,
which runs `docker compose` without `sudo`. Root ownership would buy nothing:
`ubuntu` has passwordless sudo, and `docker` group membership is
root-equivalent anyway. The modes are what matter — the secret files stay
0600. Check out the repo (or copy `deploy/`) next to them — compose mounts
`deploy/Caddyfile` by relative path.

```sh
sudo usermod -aG docker ubuntu        # then log out and back in
sudo install -d -o ubuntu -g ubuntu -m 0700 /opt/wprdc-etl
git clone https://github.com/WPRDC/wprdc-etl.git /opt/wprdc-etl/repo
# ...write the files below, then:
chmod 600 /opt/wprdc-etl/{prod.env,proxy.env}
chmod 644 /opt/wprdc-etl/{deploy.env,known_hosts}
```

- [x] `deploy.env` — compose interpolation, not secret:
```sh
WPRDC_ETL_IMAGE=ghcr.io/wprdc/wprdc-etl:<sha>         # written by deploy/release
DAGSTER_DOMAIN=etl.example.org
ENVIRONMENT=                                          # empty = dry run
WPRDC_SCHEDULES_PAUSED=1
```
- [x] `prod.env` (0600) — from Secrets Manager. Store **one** secret,
`wprdc/etl/prod`, as key/value JSON (the console's default editor),
not a secret per value — one fetch, one ARN in the IAM policy:
```json
{
  "DAGSTER_PG_URL": "postgresql://dagster:...@host:5432/dagster?sslmode=require",
  "SPATIAL_DSN": "postgresql://dagster:...@host:5432/etl_spatial?sslmode=require",
  "CKAN_API_TOKEN": "...",
  "ALLEGHENY_SFTP": "user:password",
  "ALLEGHENY_SFTP_HOST": "...",
  "ALLEGHENY_SFTP_PORT": "22",
  "DAGSTER_SLACK_BOT_TOKEN": "xoxb-..."
}
```
`CKAN_API_TOKEN` is a data.wprdc.org token with write access. Render it
to env-file form on the VM (needs `jq`):

```sh
aws secretsmanager get-secret-value --secret-id wprdc/etl/prod \
  --query SecretString --output text \
  | jq -r "to_entries[] | \"\(.key)='\(.value)'\"" > /opt/wprdc-etl/prod.env
chmod 600 /opt/wprdc-etl/prod.env
```
Values are single-quoted because compose expands an unquoted `$` in an
`env_file`, and passwords/DSNs often contain one — so a value must not
itself contain `'`. Re-run this after rotating a value, then `deploy/dc up -d`.
- [x] `proxy.env` (0600) — UI basic auth. Hash with
`docker run --rm -it caddy:2 caddy hash-password` and **single-quote** it:
the hash is full of `$`, which compose would otherwise interpolate.
```sh
DAGSTER_UI_USER=wprdc                       # full UI: launch, terminate, toggle
DAGSTER_UI_PASSWORD_HASH='$2a$14$...'
DAGSTER_RO_USER=viewer                      # read-only UI (--read-only webserver)
DAGSTER_RO_PASSWORD_HASH='$2a$14$...'
```
- [x] `known_hosts` — SFTP host keys. `ssh-keyscan -p <port> <host> > known_hosts`,
then **confirm the fingerprint** (`ssh-keygen -lf known_hosts`) with the
publisher out of band. A key that isn't in this file fails the run at
once, with no retries; a changed key does the same.

## 3. Boot, dry run

`ENVIRONMENT` empty. Nothing can publish, and nothing can land either: the S3
guard refuses real-AWS writes outside production. **Don't set
`WPRDC_ALLOW_REAL_S3` to get past it** — this phase proves the stack, not the
pipelines.

Every compose command goes through `deploy/dc`, which supplies
`--env-file /opt/wprdc-etl/deploy.env -f deploy/compose.prod.yaml` and passes
the rest through, from any directory (`deploy/dc logs -f proxy`,
`deploy/dc restart daemon`, …). Optionally
`sudo ln -s /opt/wprdc-etl/repo/deploy/dc /usr/local/bin/dc` to type `dc`.

Bring it up with `deploy/release`: it checks out the latest `main`, pins
`WPRDC_ETL_IMAGE` in `deploy.env` to the image CI built from that commit
(adding the line if it's missing), pulls, runs `up -d` and prints `ps`:

```sh
/opt/wprdc-etl/repo/deploy/release
```

- [ ] All five services healthy; `dagster instance migrate` ran in each Dagster log except `webserver-ro`, which skips it.
- [x] `https://$DAGSTER_DOMAIN` gives a certificate and asks for the password.
- [x] Code location loaded, 109 jobs and 108 schedules; Deployment → Daemons
      all green.
- [x] Every schedule shows **Stopped**.

## 4. Production, schedules held

Set `ENVIRONMENT=production` in `deploy.env`, keep `WPRDC_SCHEDULES_PAUSED=1`,
re-run `deploy/dc up -d`. Writes are now real; schedules still don't fire. Everything
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
      `#etl-alerts` with a working link back to the UI.
- [ ] **Column-guard blocks** — expect some on datasets whose live table
      DataPusher+ retyped (`bool->text` and the like). For each, once you've
      checked the change is the expected one: open the job's Launchpad,
      **Edit tags**, add `wprdc/rebuild=true`, launch. Only that run drops and
      re-creates the table (keeping the dictionary's labels and notes); no
      commit, no deploy, nothing to revert.

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

**Deploy a new version** — once CI is green for the commit:

```sh
/opt/wprdc-etl/repo/deploy/release          # latest main
/opt/wprdc-etl/repo/deploy/release <sha>    # a specific commit
```

It checks out the commit, pins `WPRDC_ETL_IMAGE` to `:<sha>`, pulls, `up -d`,
and prunes images unused and older than 30 days (each is 2.2 GB; recent
rollback targets stay). The entrypoint migrates on every start. If CI hasn't
built that commit, the pull fails, the checkout is put back and the running
stack is untouched. A change to `deploy/` alone deploys the same way — it
just re-pulls the image it already has. It refuses a dirty checkout and a
commit older than the script itself.

**Roll back** — `deploy/release <previous sha>`. Every release is a line in
`/opt/wprdc-etl/releases.log`, with the image it replaced; the next plain
`deploy/release` returns to `main`.

**Dagster version bump** — a deliberate PR (CLAUDE.md); the migration runs on
the next deploy, and rolling back past it is not supported, so take a DB
snapshot first.

**SFTP host key rotated** — verify the new fingerprint with the publisher,
update `known_hosts`, `deploy/dc up -d` (no rebuild).
