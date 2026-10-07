# CLAUDE.md

Guidance for AI agents (Claude Code) working in this repo. Human-facing overview
lives in `README.md`, with the full reference split into `docs/pipelines.md`
(component types) and `docs/strategies.md` (stage logic); this file is the
operational cheat sheet plus the gotchas that will otherwise waste your time.

## What this is

Civic-data ETL on Dagster **components**. Each dataset is a `defs.yaml` under
`src/wprdc_etl/defs/`. Two component types: `TabularPipeline` (parsed → validated →
published to CKAN DataStore) and `FilePipeline` (blobs published as-is). See README
for the architecture.

## Commands

```bash
uv sync                        # install deps
uv run pre-commit install      # once: black + pytest run on git commit
uv run dg dev                  # load + run the UI (http://localhost:3000)
uv run dg check defs           # validate all defs.yaml (fast; run after YAML edits)
uv run dg list components      # confirm component types are registered
uv run pytest                  # run the test suite (tests/ — NOT under defs/)
uv run black src tests scripts # format (line-length 88); CI runs black --check
python -m py_compile <file>    # quick syntax check without a full load

uv run python scripts/refresh_boundaries.py --list   # what boundary layers exist
uv run python scripts/refresh_boundaries.py          # load them all into PostGIS
```

`bin/` wraps the dev loop: `bin/doctor` (health check — publish mode, services,
bucket, region-store layer counts), `bin/up` / `bin/down`, `bin/check` (black +
pytest + dg check), `bin/boundaries`, `bin/arcgis` (regenerate the ArcGIS pipelines), `bin/pasda`
(same for the PASDA-sourced ones),
`bin/run <dataset>`, `bin/s3`, `bin/psql`, `bin/seed-ckan` (stand up a dev
CKAN's packages/resources), `bin/editor-schema` (IDE autocomplete for
defs.yaml).
They load `.env` through Dagster's own parser, so they agree with `dg dev`. See
`bin/README.md`.

`refresh_boundaries.py` populates the admin-region store. Anything with a
`reverse_geocode` step fails against an empty PostGIS, so run it once on a fresh
database before materializing those datasets. It discovers layers by walking
defs/ for `region_layer:`, skips stubs whose `source.url` is still empty, and
runs each pipeline in process (no `dg dev`, no daemon).

Verify your changes with `uv run dg check defs` **and** a `uv run dg dev` load — the
YAML validator passing does NOT mean the code-location builds (they're separate
passes; `load_defs` is stricter). Run `uv run pytest` and `uv run black` too.

Local dev loop (LocalStack S3 + Postgres + SFTP) is in `dev/README.md`. The landing
bucket must be created once per fresh LocalStack:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:4566 s3 mb s3://wprdc-etl-landing
```

## Where things go

- **Component types** (the pipeline logic): `src/wprdc_etl/components/`. Must be listed
  in `pyproject.toml` `[tool.dg.project] registry_modules` or dg won't discover them.
- **Stage logic** (swappable strategies): `src/wprdc_etl/strategies/`
  (`extract`, `read`, `transform`, `load`, `emit`, `schema`).
- **Shared config models**: `src/wprdc_etl/components/models.py`.
- **Dataset instances** (the jobs): `src/wprdc_etl/defs/<publisher>/[department/]<dataset>/defs.yaml`
  plus optional co-located `schema.py`, `transform.py`, `fetch.py`.
- **Boundary layers** (feed the admin-region store): ordinary `gis/` pipelines
  with a `region_layer:` block (15 of them; `grep -rl region_layer: defs/`).
  There is no `boundaries/` folder any more.
- **Resources** (S3/SFTP/CKAN/PostGIS clients): `src/wprdc_etl/resources.py`.
- **Env gate + safety guards**: `src/wprdc_etl/runtime.py`.
- **Top-level Definitions** (resources, IO manager, alert sensor wired once):
  `src/wprdc_etl/definitions.py`.
- **Tests**: top-level `tests/` — NEVER under `defs/` (see gotchas).
- **Production deploy** (Dockerfile at repo root; prod instance config, compose
  stack, entrypoint): `deploy/` — architecture in `deploy/README.md`, the
  step-by-step rollout and day-two ops in `deploy/ROLLOUT.md`.

Design pattern: shared vocabulary + per-dataset composition. Add reusable ops to the
strategy libraries; use a co-located `transform.py`/`schema.py`/`fetch.py` only for
genuinely dataset-specific logic.

## Gotchas (these cost real time — do not relearn them)

- **Instance files are `defs.yaml`, not `component.yaml`.** The old name routes to a
  backcompat parser that fails with "No components found in YAML file."
- **Nothing but components under `defs/`.** `load_defs` walks every directory; a stray
  `tests/` dir, a bare `.yaml` with no `type:`, or a leftover example folder makes the
  walker fail. Co-located `.py` files are fine (imported, not walked). Directories are
  the hazard.
- **`__init__.py` must exist at every level of `defs/`** (and the package root). A
  missing one makes it a namespace package with no `__file__`, and `load_defs` fails.
- **Component/model classes must NOT use `from __future__ import annotations`.** Dagster's
  `Resolvable` reads raw annotations; stringized ones break derivation
  (`'str' object has no attribute '__name__'`). This applies to
  `components/tabular_pipeline.py`, `components/file_pipeline.py`, `components/models.py`.
- **Config models nested under a component must be `Resolvable`, not just `Model`.**
  `{{ env('VAR', 'default') }}` in `defs.yaml` is only applied to the fields of a
  `Resolvable`, and only while they're still strings — the recursion doesn't
  descend into an already-built pydantic model. A bare `Model` nested under the
  component therefore renders NOTHING, and fails silently: a templated
  `source.host` passes `dg check` and reaches paramiko as the literal string
  `{{ env(...) }}`. Everything in `components/models.py` is `(Model, Resolvable)`
  for this reason. Keep it that way when adding a config model.
- **Do NOT decorate component classes with `@dataclass`.** They're pydantic `Model`s
  (`class X(Component, Model, Resolvable)`); `@dataclass` breaks default handling so
  optional fields get reported as required.
- **Don't name a field `schema`** — it shadows a reserved `Model` attribute (warns and
  misbehaves). Use `required_columns` etc.
- **Don't add a method named `create_resource` to a `ConfigurableResource`.** That
  is Dagster's own hook for building the resource value. Overriding it with a
  different signature breaks resource init for EVERY asset that requests the
  resource, and surfaces as `RESOURCE_INIT_FAILURE: create_resource() missing 2
  required positional arguments` — nowhere near the code you wrote.
  `CkanResource.add_resource` is named that way for this reason.
- **A model referenced by another model must be defined ABOVE it** in
  `components/models.py`. `Resolvable` reads raw annotations and the module has no
  `from __future__ import annotations`, so `list[MirrorModel]` on `CkanModel`
  is a NameError unless `MirrorModel` already exists.
- **`load_defs` takes the defs MODULE, not a path:** `load_defs(wprdc_etl.defs)`.
- **Only one `Definitions` object at module scope** in `definitions.py` — don't bind the
  `load_defs(...)` result to a module-level name; inline it into the `merge`. The IO
  manager (`_io_manager()`) and alert sensor (`_alert_sensors()`) fold into that same
  `merge`, not new module-level objects.
- The dg components layer is version-sensitive. If something here looks wrong for the
  installed version, verify with `dg scaffold defs <type> <path>` (generates a canonical
  instance) rather than guessing.
- **Transform primitives can't reach resources unless they ask.** The contract is
  `fn(df, **params)`. A primitive needing the spatial store or the run context
  declares `@needs("spatial", "context")` and `apply_declarative` injects them;
  those names are NOT part of the YAML surface. `validate_steps` now also checks
  param names via `inspect.signature`, so a typo'd key fails at `dg check`.
- **Two Postgres servers in dev, one in prod.** `compose.yaml` runs `postgres`
  (5432, Dagster storage) and `postgis` (`$POSTGIS_PORT`, default 5434, db
  `etl_spatial`, the admin-region store — the host port is an env var because 5433
  collides with other local Postgres containers; it drives both the compose port
  mapping and `SPATIAL_DSN`, which expands `${POSTGIS_PORT}` from `.env`).
  They're separate only because `postgres:17` is Debian trixie and
  `postgis/postgis:17-3.5` is bullseye — pointing the PostGIS image at the
  `dagster` volume trips a collation version mismatch. In prod `etl_spatial` is just
  another database on the managed instance. Don't "simplify" dev back to one
  service without checking glibc.
- **`postgis/postgis` is amd64-only** — the compose service pins
  `platform: linux/amd64`, so it runs emulated on Apple Silicon. Fine for
  boundary layers (hundreds of rows); don't use it for bulk data.
- **Production's DataPusher+ re-creates the DataStore table on EVERY load.**
  Verified on data.wprdc.org (2026-10-02): a reload re-typed an established
  table from qsv inference (text ids/wards -> numeric, a bool -> text,
  float8 -> numeric), and an explicit `float8` override still came back
  `numeric` — a column only changes type when the table is re-created. So:
  - **The data dictionary pins the types.** DataPusher+ honours
    `info.type_override` when it re-creates, so `CkanResource.replace` writes
    one per text/numeric/timestamp column before every load — into the new
    table on a first load or rebuild, in place (merged into curator
    labels/notes, live types kept) on an existing one. Without it text ids
    came back numeric every time; with it they stayed text. It also throws
    the dictionary away when it re-creates (seen locally), which is why the
    overrides are rewritten whenever a load finds them missing.
  - **`bool` can't be pinned**, so the replace loader converts booleans first
    (`ckan.bool_format`: `text` -> True/False, the default, or `int` -> 1/0
    for older datasets). Title case because most of production's text
    booleans already use it. A dataset whose live table has a real `bool` column is
    blocked once by the column guard (`bool->text`); clear it with one
    `ckan.rebuild: true` run.
  - **Anything else added to the table is lost on every load** —
    ckanext-spatialdata's `dataspatial_wkb` included. `replace()` still
    truncates (`filters: {}`) rather than drops on a normal load, but that no
    longer protects the geometry column. No dataset sets `ckan.spatial` yet;
    the first one must restore it after every load, not only after a rebuild.
  - **We create the table, before the upload,** on a first load and on a
    rebuild (drop, then create): the upload sets off a DataPusher+ job of its
    own, which must find the new table, not none and not the old one.
  - **Local oddity (DataPusher+ v3.0.0a0), now moot:** one long-lived local
    resource stored `False`/`True` while its file said `false`/`true`; fresh
    resources store exactly what is sent. Publishing `True`/`False` keeps the
    file and the DataStore in agreement either way.
  The column guard still blocks an added/removed/retyped column until
  `ckan.rebuild: true` — not to protect the table any more, but so a schema
  change reaches consumers on purpose rather than silently.
- **A publish that wouldn't change CKAN writes nothing.** Each CKAN write
  first compares what it would send with what CKAN holds: `replace` and
  `publish_file` against the `etl_sha256` field they record on the resource
  (CKAN keeps unknown resource fields as extras), `publish_link` against the
  resource's `url`, `patch_package` against the live notes/tags. `replace`
  also needs the DataStore row count to match — DataPusher+ runs after the
  upload, so a failed push would otherwise hide behind a matching
  fingerprint. It compares OUTPUT, not the landed file: a config change,
  reloaded boundary layer or edited description changes the output from
  identical source bytes. Representations fingerprint the frame
  (`frame_fingerprint`) because a shapefile's DBF header stamps its write
  date. `ckan.rebuild` always publishes; `WPRDC_FORCE_PUBLISH=1` overrides the
  check for a resource someone changed by hand. The PostGIS layers do the
  same: `replace_layer` / `replace_key_layer` fingerprint the exact rows (and
  value/label/key fields) into `content_sha256` on the layer's metadata row,
  written in the same transaction as the rows, and return a `LayerLoad` whose
  `changed` is False when nothing was rewritten. The saving is the write, not
  the run: download and decode still dominate. Landing still snapshots every
  partition — at ~28 GB/yr across all 107 datasets, dedupe isn't worth it.
- **Network steps retry in production; permanent failures must say so.**
  `network_retry_policy()` (`_common.py`) puts a 3-retry, ~1/2/4-min jittered
  backoff on every step that talks to the outside world — landing,
  `loaded`, representations, mirrors, metadata, PostGIS layers — and on
  nothing in dev, where a failure should surface in seconds. ArcGIS is why:
  `arcgis_ready_url` waits 95s for an export to build, and a cold KML or
  Feature Collection can take longer. A failure no retry can fix (missing
  config, a title gone from the catalogue, the replace/upsert column guard)
  is raised as `dg.Failure(..., allow_retries=False)` so it alerts at once.
  **New code raising a permanent error must do the same**, or it burns ~7
  minutes of backoff before anyone hears about it.
- **`ingest` and `publish` are separate axes.** `ingest` is how data arrives
  (`snapshot` | `incremental`), `publish` is how it reaches CKAN (`replace` |
  `upsert`); unset, `publish` derives the old pairing. `incremental` + `replace`
  forces `accumulate`, which maintains
  `_state/<publisher>/[<dept>/]<dataset>/current.parquet` and publishes that
  whole table. Parquet, not CSV — a CSV round trip turns nullable ints into
  floats and eats leading zeros on the identifier columns the merge keys on.
- **`department:` drives the asset key, not the folder.** Moving a dataset's
  directory without editing the field leaves the files in one place and the
  data in another — the asset key, the S3 landing prefix and the asset group
  all come from `department:`, so nothing fails to announce the drift. Four
  PASDA layers moved `pasda/` -> `gis/` and kept landing under
  `allegheny_county/pasda/`. `test_department_matches_its_folder` now catches
  it, and a generator's `DEPARTMENT` constant has to agree with where it
  writes.
- **A numeric scalar reaches a `Resolvable` field as an `int`,** quoted or not.
  `dataset_id: "1224"` in the YAML arrives as `1224`; the resolve pass coerces
  it before pydantic sees it, so a `str`-only field rejects its own generated
  value. `SourceModel.dataset_id` is `str | int | None` for this reason, and
  `PasdaExtractor` does the `str()` itself. Don't "tighten" it back to `str`.
- **`ckan.sync_metadata` does not imply a catalogue.** `mirror` does — it
  copies the distributions `data.json` lists — but a sync only needs text and
  tags, and a `pasda` source supplies the text itself via `ckan.description`.
  The two invariants in `build_defs` are therefore separate: `mirror` requires
  `source.type: arcgis`, `sync_metadata` requires arcgis **or** an explicit
  `ckan.description`.
- **An empty catalogue entry must not sync tags.** `package_tags({})` is `[]`,
  and `package_patch` with `tags: []` CLEARS what a curator set — so
  `sync_package_metadata` sends `tags=None` when `entry` is empty and patches
  the description alone. `patch_package` skips any field that is `None`.
- **A dataset needs `ckan` or `region_layer` (or both).** `ckan` is optional now:
  a boundary layer we consume but don't republish has no CKAN target and builds
  no `loaded` asset. The invariant is enforced in `build_defs`.
- **Geometry looked up BY KEY lives in `keyed_geometry`, not `admin_region`.**
  `admin_region` dissolves by id and answers `ST_Contains`; a point contains
  nothing, and 660k address points there would slow every reverse geocode. A
  `key_layer:` block (on `gis/address_points`) loads it, `join_geometry` reads
  it. Keys go through `key_text`, never `_region_text`: the latter formats
  with `:g`, which turns 1234567 into `1.23457e+06`.
- **Reverse-geocoded columns must NOT go in `schema.py`.** That contract
  validates the RAW landed file, pre-transform, so a derived column declared
  there is reported missing and fails `schema_ok` as ERROR.
- **Two `dagster.yaml` files.** The repo-root one is DEV — `dg dev` / `dg check` read
  `$CWD/dagster.yaml`. Production is `deploy/prod/dagster.yaml`, active only when
  `DAGSTER_HOME` points at it. Don't add prod-only blocks (QueuedRunCoordinator,
  S3ComputeLogManager, …) to the root file — they'd load under `dg dev`.
- **`dg utils generate-component-schema` omits this project's components.** In
  1.13 it calls `list_all_components_schema(entry_points=True,
  extra_modules=())`, so `registry_modules` is never read and the schema has no
  TabularPipeline/FilePipeline. `scripts/editor_schema.py` (`bin/editor-schema`)
  passes our modules itself and injects the value sets from the code
  registries (EXTRACTORS, LOADERS, MIRROR_KINDS, PRIMITIVES, declared
  region/key layer names). `bin/check` regenerates it; `test_editor_schema`
  keeps every real defs.yaml valid against it — a schema that underlines good
  files teaches people to ignore the underlines.
- **Format with black, not ruff.** `[tool.black] line-length = 88`; the pre-commit
  hook and CI both enforce it. Don't add ruff / isort / flake8.
- **Dagster is pinned exactly** (`dagster==1.13.14`, paired libs `==0.29.14`). Bumping
  is a deliberate PR that refreshes `uv.lock` AND runs `dagster instance migrate` (the
  Postgres schema is release-coupled).
- **A partitioned job's schedule must say which partition.** A plain
  `ScheduleDefinition` requests none, so every scheduled run of a partitioned
  job would have failed in production (`bin/run` hid it by choosing the
  partition itself). `schedule_or_sensor` uses
  `build_schedule_from_partitioned_job`, which runs the latest COMPLETE
  partition; `partitioned_schedule_fields` makes `dg check` reject a cron that
  doesn't fit the cadence (weekly `M H * * DOW`, monthly `M H DOM * *`). Its
  timezone comes from the PARTITIONS definition — Dagster refuses one on the
  schedule — so `partitions_for` builds them in America/New_York.
- **Schedules start RUNNING in production, STOPPED elsewhere.** Dagster
  creates every schedule stopped, so a deploy would run nothing until ~110
  were switched on by hand. `default_schedule_status()` keys off
  `is_production()`, like the Slack sensor; dev stays stopped so `dg dev` with
  a daemon never starts pulling and publishing on its own.
  `WPRDC_SCHEDULES_PAUSED=1` holds them stopped in production too — the
  first-boot brake (deploy/ROLLOUT.md §4-5). It only sets the DEFAULT: a
  schedule switched on or off in the UI keeps that state in Postgres.
- **Schedules are slots, not hand-picked crons — outside business hours.**
  Weekly datasets run on Sunday 03:00-08:59 ET (a Sunday run lands the week
  that just ended), monthly ones on the 1st 03:00-07:59 (the 1st can be a
  weekday), on a 3-minute grid, starting after the 02:00 DST change.
  `scripts/schedule_slots.py` spaced the first layout evenly (weekly runs
  3-6 minutes apart); the generators give a new dataset a free hashed slot and
  keep an existing one's. `test_schedules` holds the rules.
- **Job run tags ↔ prod concurrency limits.** `_common.py:run_tags()` stamps
  `wprdc/{publisher,dataset,source,ingest,heavy}` on every asset job;
  `deploy/prod/dagster.yaml` `tag_concurrency_limits` keys off them — change a key in
  one place, change the other.

## Safety — do not violate

The system fails safe: **dry-run is the default; real writes require
`ENVIRONMENT=production`.** When working here:

- **Never set `ENVIRONMENT=production`** in dev, tests, or committed config. Leave it
  unset so loaders write to `_dryrun/` and CKAN is never called. It also selects the
  S3 pickle IO manager and enables the Slack run-failure sensor.
- **Never point tests or dev runs at real S3 or real CKAN.** The S3 write guard refuses
  real-AWS writes outside production; do not set `WPRDC_ALLOW_REAL_S3` to get around it.
  To exercise the S3 IO-manager path in dev, set `WPRDC_IO_MANAGER=s3` (+
  `AWS_S3_ADDRESSING_STYLE=path`) — it uses LocalStack and does NOT touch
  `ENVIRONMENT`, so publishing still dry-runs.
- **To test a job against a LOCAL CKAN, set `WPRDC_CKAN_WRITE=1` — never
  `ENVIRONMENT=production`.** It makes the publish steps really call CKAN and
  unlocks nothing else: `guard_real_s3_write`, `guard_spatial_write`,
  `_io_manager()` and `_alert_sensors()` all still key off `ENVIRONMENT`.
  `guard_ckan_write` refuses a non-local `CKAN_URL`, so this can't reach
  data.wprdc.org on its own; `WPRDC_ALLOW_REMOTE_CKAN` is the deliberate opt-out
  for a staging portal. Landing still needs LocalStack.
- **`bin/seed-ckan` reproduces the PRODUCTION ids on a dev CKAN.** A sysadmin
  token may set `id` explicitly on `package_create` AND `resource_create`
  (verified on CKAN 2.12), so a local run exercises the same lookup path as
  production rather than a dev-only fallback. The script verifies each id came
  back as asked and fails if not.
- **`package_patch` with a `resources` list REPLACES the resource list.** It
  does honour explicit resource ids, but anything omitted is dropped — a probe
  lost a resource created moments earlier. `CkanResource.patch_package` only
  ever sends `notes`/`tags`, which is why metadata sync is safe; keep it that
  way.
- **Never point a dev run's `SPATIAL_DSN` at the production PostGIS.**
  `guard_spatial_write` refuses boundary-layer WRITES to a non-local host outside
  production; don't set `WPRDC_ALLOW_REMOTE_SPATIAL` to get around it. Reads are
  deliberately unguarded so `reverse_geocode` works in dry-run.
- **SFTP host keys** are auto-added in dev, rejected-if-unknown in production
  (`SFTPResource._connect`). Don't make dev use `RejectPolicy`. Production
  reads them from `SFTP_KNOWN_HOSTS` (the file the deploy mounts); an unknown
  or changed key is `allow_retries=False`, so it alerts at once.
- **Never commit secrets or `.env`.** Credentials are referenced by env-var name
  (`secret_ref`), never inlined in `defs.yaml`.
- Don't weaken the guards in `runtime.py`, the schema superset check, or the CKAN
  compatibility check to make something pass. They are intentional.

## ArcGIS Hub sources (`source.type: arcgis`)

Allegheny County and the City of Pittsburgh both publish their GIS layers on an
ArcGIS Hub site with a DCAT catalogue at `<site>/data.json`. Those pipelines
address a layer by **catalogue title, not URL** — a Hub download URL embeds the
ArcGIS item id and changes whenever the layer is republished, so a hardcoded
URL rots. `ArcGisExtractor` resolves title -> download URL per run.

`bin/arcgis` generates and refreshes those pipelines from the catalogue:

```bash
bin/arcgis --list                     # catalogue vs what's wired; flags drifted titles
bin/arcgis                            # dry run — what a sync would change
bin/arcgis --write                    # create the missing defs.yaml + schema.py
bin/arcgis --write --force            # also regenerate existing ones
bin/arcgis --only zip_codes --write
```

Things that will bite you here:

- **Generated schemas are sampled from the real CSV export**, not from the
  legacy marshmallow classes. The legacy engine lowercased every header before
  matching, so a legacy `load_from` name is NOT evidence of the real header
  casing. Don't "restore" the legacy spellings.
- **The catalogue's link is in `accessURL`, not `downloadURL`** — every entry in
  both sites, as of this writing.
- **`format: ZIP` is ambiguous**: it's both the Shapefile and the File
  Geodatabase. The distribution *title* disambiguates.
- **A title can appear twice** (the county publishes two "DPW Maintenance
  Districts"). The resolver takes the most recently modified and records
  `arcgis_ambiguous_titles` in the manifest; `bin/arcgis` says so too.
- **The CKAN target comes from the legacy payload's `package_id`**, resolved to
  a resource via `package_show`. A layer with no legacy seed has no known
  target, and `bin/arcgis` skips it rather than writing a `defs.yaml` that
  `build_defs` would reject for having no destination.
- The county catalogue contains unrendered template rows (`{{name}}`); they're
  filtered out.
- **A catalogue title can carry edge whitespace** (the city publishes
  `"Neighborhoods "`). A plain YAML scalar silently drops it, so generated
  titles are quoted and the resolver falls back to a stripped comparison.
- **Some layers are plain TABLES** (no geometry) — `addressing_landmarks`, and
  Street Aliases (on hold). The Hub still offers GeoJSON / Shapefile / KML
  for them, but every feature is null-geometry (verified: 0 of 39,986). The
  generator asks the REST endpoint (`?f=json` -> `type: "Table"`), sources
  the CSV, sets the csv mirror `datastore: true` (it is the only form of the
  data) and skips the geometry formats (`TABLE_MIRRORS`).
- **A table that keys into geometry is joined, not copied.** `GEOMETRY_JOINS`
  makes the generator render `coerce_text` + `join_geometry` steps and publish
  the joined frame to `ckan.resource_id` (minted for a new package; `bin/seed-ckan`
  creates it with that id) instead of mirroring the csv. Landmarks store
  `ADDRESS_ID` 450843 where the address points store `SSAP450843`, hence
  `key_format: "SSAP{}"`; 98.9% join, and the ~425 misses reference address
  ids that exist nowhere, not even in the county's live layer.
- **The legacy title -> package pairing can be wrong.** `PACKAGE_OVERRIDES`
  corrects it: "" = look up by title / mint, `MINT` = always a NEW package (a
  faulty one is being retired — Landmarks' old `cd24b8f3`). The claim guard
  applies to legacy ids too, so two layers can't share a package silently.
  Stubbed descriptions (new package or empty notes) come from the catalogue
  minus the county's harvest boilerplate, and the defs say STUB — review
  before publishing.
- **`source.title` is a lookup key, not a display title.** The extractor
  finds the layer by that exact catalogue title, so "tidying" it breaks
  landing (`bin/arcgis --list` reports DRIFT). A nicer CKAN title or URL name
  for a NEW package goes in `PACKAGE_TITLES` / `PACKAGE_NAMES`, which render
  `ckan.package_title` / `ckan.package_name` — read only when the package is
  created (`bin/seed-ckan`, the catch-up script). The name defaults to a slug
  of the title.
- **Type inference refuses `num()` for identifiers.** A FIPS code, GEOID, zip or
  zero-padded coordinate parses as a number but is a label; `num()` eats the
  leading zeros and, on a blank-bearing column, fails `schema_ok` outright.

### Mirroring the distributions

A WPRDC GIS package carries six resources, all of them from the publisher's
`data.json` — see `allegheny-county-boundary`. `ckan.mirror` declares them
and `strategies/mirror.py` publishes them:

| mirror `format` | CKAN resource | how |
|---|---|---|
| `geojson` | GeoJSON | uploaded file; **the source** the pipeline reads |
| `csv` | CSV | uploaded file |
| `shapefile` | ZIP "Shapefile" | uploaded file |
| `kml` | KML | uploaded file |
| `hub_page` | HTML "ArcGIS Hub Dataset" | link only |
| `rest_api` | HTML "Esri Rest API" | link only |

**GeoJSON is the source** (`source.format: geojson`), so the validated frame
is a GeoDataFrame with real geometry — which is what `region_layer` needs. The
CSV export of a *polygon* layer has none, only `Shape__Area` /
`Shape__Length`. The two are not the same data either: the GeoJSON of
`municipal_boundaries` has 19 properties against the CSV's 21 columns, and
`county_boundary` spells the same field `COUNTYFIPS` where the CSV says
`County FIPS`. Generated schemas are therefore sampled from **GeoJSON
properties**; a CSV-derived schema fails `schema_ok` on both counts.

**These resources are copied, not derived.** That is the difference between
`MirrorModel` and `RepresentationModel` — a representation re-serialises the
frame, which needs geometry in it and produces our bytes, not the
publisher's. Don't merge them.

**No `ckan.resource_id`.** It means "publish the validated frame to this
DataStore resource as CSV" (`CkanResource.replace` always uploads
`data.csv`), so pointing it at the GeoJSON resource would push a CSV body
into a GeoJSON resource. It is optional now; a mirror dataset leaves it
unset, which also means no `loaded` asset — so `schema_ok` is the only
pre-publish check on those datasets.

**`mirror[].datastore` makes an uploaded file queryable**, by the route its
format needs. An uploaded file and a DataStore table coexist fine (the
existing GeoJSON resource is both `url_type=upload` and `datastore_active`).
A csv goes through `datapusher_submit`. A **geojson must go through the
spatial load endpoint** — `CkanResource.spatial_load_action`, which loads the
file and builds the geometry column in one step. That endpoint **does not
exist yet**, so `load_geojson_to_datastore` raises, and deliberately does NOT
fall back to DataPusher+: that would build a table with the properties as
columns and no geometry, and nothing downstream would notice. Generated defs
therefore set `datastore: false` everywhere; flip the geojson's and re-sync
once the endpoint lands.

**`ckan.description` replaces the publisher's description**; `description_suffix`
appends to whatever the body ends up being, so the two compose. The county's
ArcGIS descriptions are harvest boilerplate, so `bin/arcgis` writes the curated
text already on data.wprdc.org into every `allegheny_county/gis/*` defs.yaml
(the `portal_description` flag in `PUBLISHERS`). City datasets keep the source
text — their own descriptions are good.

It comes from the `notes` in the same `package_show` the generator already
calls for resource ids, so it costs no extra requests. Two traps:

- **CKAN tokens are scoped by host.** `CKAN_API_TOKEN` is the token for
  `CKAN_URL` — the portal we write to — and is sent ONLY to that host.
  `bin/arcgis --ckan` reads from production by default, so an unscoped read
  shipped a localhost credential to data.wprdc.org on every lookup. A
  different portal needs `CKAN_READ_TOKEN`, which is only required for a
  PRIVATE target package.
- **`bin/arcgis --ckan` defaults to data.wprdc.org on purpose.** Pointing it at a
  dev CKAN would read back whatever was last published there — including the
  harvested text these overrides exist to replace, or `bin/seed-ckan`'s
  placeholder notes.
- **Don't combine `description_suffix` with a `portal_description` publisher.**
  The sync writes `description + suffix` to `notes`, and the next generator run
  reads `notes` back as `description` — so the suffix would be folded in and
  appended again, accumulating on every pass. The generator never emits a
  suffix for those publishers for this reason.

`ckan.sync_metadata` pushes the catalogue's description and keywords onto the
package. The description is HTML pasted out of Word, so
`strategies/metadata.py` converts it to Markdown (CKAN renders `notes` as
Markdown, and a surviving `style=` attribute shows as literal text). Anything
of ours goes in `ckan.description_suffix` as a YAML block scalar — appended
after a blank line, so an upstream edit can't overwrite it. It is NOT
generated; add it per dataset when there is something to say:

```yaml
  ckan:
    sync_metadata: true
    description_suffix: |
      ## About this copy

      Mirrored weekly from the county's ArcGIS Hub by the WPRDC ETL.
```

Tags are lower-cased because one catalogue carries both `Environment` and
`environment`. Both `mirror` and `sync_metadata` need `ckan.package_id` and
`source.type: arcgis`; `build_defs` enforces that. Metadata writes go through
`package_patch`, never `package_update` — the latter treats an omitted field
as cleared and would blank the groups, license and extras a curator set.

## PASDA sources (`source.type: pasda`)

Four Allegheny County layers come from Penn State's PASDA archive instead of
the county's Hub. They live under `gis/` beside the Hub-sourced ones —
`gis/{parcels,address_points,street_centerlines,building_footprints}` — because
PASDA is the archive that distributes them, NOT a county department; the layers
are ordinary county GIS / Addressing data and `source.type: pasda` is what
records where the bytes come from.
The county's `data.json` does list them, but with an "ArcGIS GeoServices REST
API" distribution whose URL is actually a PASDA landing page, and with no
downloadable file at all — so there is nothing for the Hub generator to
mirror.

`CATALOGUE_EXCLUSIONS` in `src/wprdc_etl/catalogue_check.py` is the table
that keeps them out of the Hub sync. It is keyed by publisher and catalogue
title and carries a reason, which `bin/arcgis --list` prints as `ELSEWHERE`.
Add to it rather than special-casing a title inline — that is what makes it
extensible. It lives in the package, not `scripts/`, because the weekly
catalogue check reads it in production.

**The weekly catalogue check** (`maintenance__catalogue_check__job`,
Saturday 06:00 ET) does `bin/arcgis --list`'s job unattended: it reports a
catalogue layer no pipeline uses and the exclusions don't explain (NEW), a
wired title gone from its catalogue (DRIFT — that run will fail Sunday), and a
PASDA dataset id that no longer resolves. It only reports — run metadata in
the UI, and a Slack message in production when there is something to act on.
Silence is the normal week, so a layer that will never be wired (a web page
with no file) belongs in the exclusions, or it is reported every Saturday.

`bin/pasda` generates these the way `bin/arcgis` generates the others (dry-run
by default, `--write` to apply, `--force` to regenerate). The layers are a
pinned registry, `PASDA_DATASETS` in `scripts/sync_pasda.py`: folder ->
dataset id -> CKAN package, plus an `expect_fields` sanity count.

Things that will bite you here:

- **Addressed by dataset id, not URL.** The download filename carries a
  release date (`AlleghenyCounty_Parcels20260928.zip`), so a stored URL rots
  every republish. `resolve_pasda_download` scrapes the current link off
  `DataSummary.aspx?dataset=<id>` per run.
- **`expect_fields` is a rough check, not an assertion.** A shapefile's DBF
  truncates field NAMES to 10 characters, so the sampled names will not match
  what the layer's REST metadata reported; only the count is comparable, and
  only approximately.
- **These are big files** (19–121 MB zipped, several times that decoded), so
  all four set `heavy: true`. Sampling a schema downloads the whole zip — a
  shapefile cannot be usefully range-read — which is why `SAMPLE_ROWS` caps
  what gets decoded from it.
- **No mirror, no upstream tags.** There is no catalogue entry, so these use
  `ckan.description` (the curated text from data.wprdc.org) with
  `sync_metadata: true`, and the sync leaves the package's tags alone. The
  GeoJSON resource is a `representation` built from the validated frame, not
  a mirror: PASDA ships only a shapefile, so there is no upstream GeoJSON to
  copy.
- **`building_footprints` is shaped differently** — its CKAN package is
  county-hosted, with ArcGIS-style resource names and no PASDA landing-page
  resource, and its REST resource URL is dead
  (`gisdata.alleghenycounty.us` returns "Service not found"). PASDA is its
  only live source.

## Adding a dataset

Prefer the scaffolder (generates a correct `defs.yaml` + `__init__.py` files, optionally
drafts `schema.py` from CKAN):

```bash
uv run python scripts/scaffold_pipeline.py
```

`defs.yaml` anatomy and the transform-primitive / schema-builder vocabularies are in the
README ("Adding a dataset"). `api_incremental` sources require a co-located `fetch.py`.
Set `heavy: true` on a dataset whose source file is hundreds of MB — it tags the run so
the prod coordinator serialises heavy decodes.

## Known gaps / current work

- Extractors: SFTP, HTTP, ArcGIS, PASDA and API-incremental implemented; **bulk-API** is a
  stub in `strategies/extract.py`. HTTP reads `source.url` (falls back to
  `source.path`); optional HTTP Basic auth via `secret_ref` ("user:password").
- `GeocoderResource` (address -> lat/lon) is still a stub. The REVERSE direction is
  implemented: `reverse_geocode` resolves coordinates to admin regions against the
  PostGIS store (`SpatialResource`), with layers loaded by the `region_layer`
  pipelines. Data that references geometry by identifier uses `join_geometry`
  against a `key_layer` instead (landmarks -> address points); parcels by PIN
  would be the same pattern, not address geocoding.
- **Don't try to push spatial SQL at WPRDC's CKAN DataStore.** PostGIS is there and
  `ST_Contains`/`ST_Intersects` are allowlisted, but the CDN WAF 403s any
  `datastore_search_sql` containing the substring `wkb` — and the geometry column is
  `dataspatial_wkb`. `::` casts are blocked too and CKAN denies `CAST`. Boundary
  layers are read from their GeoJSON *file* resources instead (the DataStore's
  non-spatial `geometry` column is unprojected WKT with no SRID).
- **Landmarks <- address points: CKAN is ready, the ETL isn't live yet.**
  As of 2026-10-06 production CKAN is at parity with dev, so the new
  `addressing_landmarks` package (`5a07d365…`, CSV resource `3b2cf234…`)
  exists there. What remains (deploy/ROLLOUT.md §6): production
  `keyed_geometry` is empty until `gis/address_points` runs, so it must run
  before `addressing_landmarks`; and the old faulty `cd24b8f3` is still live
  until the user deletes it. Remove this note once both are done.
- **Street Aliases is on hold** — excluded in `CATALOGUE_EXCLUSIONS`, not
  wired. When it's picked back up: the local CKAN still holds a stale
  `cd24b8f3` shell NAMED `allegheny-county-addressing-street-aliases` (from the
  old wrong wiring), so seeding it will 409 until that shell is purged.
- `pli_division` has no boundary layer on WPRDC, so it can't be reverse geocoded.
  It equals the ward on all 232 live `water_features` rows, so that dataset's
  `transform.py` copies it from `ward` instead.
- Geo + `publish: upsert` is unsupported (geometry isn't JSON-serializable for
  the upsert payload). An incremental geo dataset can still publish via
  `publish: replace`, which accumulates and ships a file.
- The assessments example's column names / date formats are drawn from the WPRDC data
  dictionary and unverified against a real extract — confirm before trusting.
- Deploy: code, image build (CI -> `ghcr.io/wprdc/wprdc-etl`) and `deploy/`
  are done; the infra isn't — managed Postgres, the two S3 buckets + IAM role,
  Secrets Manager wiring, the VM and DNS still need provisioning. Track it in
  `deploy/ROLLOUT.md`, which is the checklist.