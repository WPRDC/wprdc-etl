# Pipelines (component types)

Reference for the two component types in `src/wprdc_etl/components/`. A
**pipeline job is one component instance** — a `defs.yaml` under
`src/wprdc_etl/defs/`. This document covers what each type accepts, what it
builds, and when it fails. The stage logic those pipelines call into is
documented separately in [strategies.md](strategies.md); the project overview is
in the top-level [README](../README.md).

- [How an instance becomes assets](#how-an-instance-becomes-assets)
- [Naming: keys, groups, job names](#naming-keys-groups-job-names)
- [TabularPipeline](#tabularpipeline)
- [FilePipeline](#filepipeline)
- [Partitioning](#partitioning)
- [Scheduling, jobs, run tags](#scheduling-jobs-run-tags)
- [Resources](#resources)
- [Dry-run behaviour](#dry-run-behaviour)
- [Co-located escape hatches](#co-located-escape-hatches)
- [Adding or changing a component type](#adding-or-changing-a-component-type)
- [Known gaps](#known-gaps)

## How an instance becomes assets

```
defs/<publisher>/[<department>/]<dataset>/defs.yaml
        │
        │  load_defs(wprdc_etl.defs)          ← definitions.py
        │    walks the defs/ tree
        │    matches `type:` to a registered component class
        │    validates `attributes:` against that class's Model fields
        ▼
   TabularPipeline(...)  /  FilePipeline(...)      one instance per defs.yaml
        │
        │  .build_defs(context) -> dg.Definitions
        ▼
   assets + asset checks + one job + (a schedule or a sensor)
```

Everything happens at **load** time (`dg check defs`, `dg dev`, the production
code server), so a malformed instance fails there rather than at 3am. The
component types are discovered because `pyproject.toml` lists them:

```toml
[tool.dg.project]
registry_modules = ["wprdc_etl.components.*"]
```

`uv run dg list components` confirms both types are registered;
`uv run dg check defs` validates every instance.

## Naming: keys, groups, job names

`components/_common.py:taxonomy()` derives three names from
publisher / department / dataset. Everything else is built from them.

| Name | Value | Used for |
|------|-------|----------|
| `key_prefix` | `[publisher, department?, dataset]` | the asset key prefix — `allegheny_county/real_estate/assessments/landed` |
| `group` | `publisher_department` (or just `publisher`) | the UI group; no slashes allowed, so it is underscore-joined |
| `stem` | `publisher__department__dataset` | job / schedule / sensor names, and the `wprdc/dataset` run tag |

`department` is the optional middle tier. Omit it and both the asset key and the
group collapse to two levels.

## TabularPipeline

`wprdc_etl.components.tabular_pipeline.TabularPipeline` — data that is parsed,
validated and published to CKAN's DataStore, and/or loaded into the PostGIS
admin-region store.

### Asset graph

```
landed ──► validated ──┬──► loaded                 (only when `ckan:` is set)
              │        ├──► published/<format>     (one per `representations:`)
              │        └──► region_layer           (only when `region_layer:` is set)
              │
              └──► schema_ok                       (asset check — re-reads the LANDED file)
```

| Asset key suffix | Returns | What it does |
|------------------|---------|--------------|
| `landed` | `dict` (the manifest) | Runs the extractor for `source.type`; lands raw bytes + `manifest.json` in S3. Checksum-idempotent. |
| `validated` | `DataFrame` / `GeoDataFrame` | Reads the landed file, optionally geocodes, then applies the declarative `transforms:` followed by any co-located `transform.py`. |
| `accumulated` | `DataFrame` / `GeoDataFrame` | Folds `validated` into the cumulative canonical table in `_state/`, writes it back, returns the whole thing. Built **only** when the dataset accumulates. |
| `loaded` | nothing | Runs the loader for `publish` (replace or upsert), against `accumulated` when there is one, else `validated`. Built **only** when `ckan:` is set. |
| `published/<format>` | nothing | One asset per entry in `representations:`; serialises the validated frame to GeoJSON / Shapefile and uploads it to its own CKAN file resource. |
| `region_layer` | nothing | Replaces this dataset's layer in the PostGIS admin-region store. Built **only** when `region_layer:` is set. |
| `key_layer` | nothing | Replaces this dataset's layer in the PostGIS keyed-geometry store, which `join_geometry` reads. Built **only** when `key_layer:` is set. |

`validated` is the only asset whose value is *handed to* a downstream asset
through the IO manager. `landed` returns the manifest dict, which the IO manager
stores but nothing reads back — downstream assets declare `deps=[landed]` and
re-read from S3. `loaded`, `published/<format>` and `region_layer` are typed
`-> None`, so Dagster types their output `Nothing` and the IO manager is never
asked to store anything for them.

**`schema_ok` validates the RAW landed file, not the validated frame.** It
re-reads the landed object and checks it is a *superset* of the declared
columns. That is why a column your transforms *derive* (anything
`reverse_geocode` attaches, a computed `full_address`) must **not** be declared
in `schema.py` — it isn't in the raw file, so it would be reported missing.

| `schema_ok` outcome | Severity |
|---------------------|----------|
| a declared column is missing from the file | **ERROR** (`missing_columns`) |
| columns all present, pandera value/type checks fail | **WARN** |
| columns all present, everything passes | pass |

It also emits a **drift signal** — informational, never a failure. The check
fingerprints the file's column set (order-insensitive), compares it to the last
set this dataset presented, and records the new one as the next baseline:

| Metadata | Meaning |
|----------|---------|
| `missing_columns` | declared columns the file doesn't have — this is what fails the check |
| `schema_drifted` | the column set changed since the previous check |
| `columns_added` / `columns_removed` | *which* columns, when it drifted |
| `column_hash` | the fingerprint itself |

The baseline is **dataset-scoped, not partition-scoped** — it lives at
`_state/<publisher>/[<department>/]<dataset>/schema.json` in the landing bucket,
alongside the incremental watermark. It can't live in a landing manifest: those
are per-partition and immutable, and they're written by `landed`, which never
parses the file and so never sees a column list. A dataset's first check has no
baseline, so nothing drifts — that run only establishes one.

### Fields

Required: `publisher`, `dataset`, `source`, and at least one of `ckan` /
`region_layer` / `key_layer`.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `publisher` | str | — | Top level of the asset key, e.g. `allegheny_county`. |
| `dataset` | str | — | Leaf of the asset key, e.g. `assessments`. |
| `department` | str \| null | `null` | Optional middle tier, e.g. `real_estate`. |
| `source` | [SourceModel](#sourcemodel) | — | How the data is acquired. |
| `ckan` | [CkanModel](#ckanmodel) \| null | `null` | The CKAN DataStore target. Optional — a boundary layer we consume but don't republish has none. |
| `region_layer` | [RegionLayerModel](#regionlayermodel) \| null | `null` | Marks this dataset as a source of administrative boundaries. |
| `key_layer` | [KeyLayerModel](#keylayermodel) \| null | `null` | Marks this dataset as a geometry lookup by key (address points by `ADDRESS_ID`), for `join_geometry`. |
| `geometry` | [GeometryModel](#geometrymodel) \| null | `null` | How to build geometry from the validated frame. Declared **once** and read by every geospatial output. Unnecessary when the source is already geospatial. |
| `representations` | list[[RepresentationModel](#representationmodel)] \| null | `null` | Alternative representations of the dataset, published as their own CKAN file resources. |
| `schedule` | str \| null | `null` | Cron. When omitted a placeholder arrival sensor is created instead. |
| `partition` | str | `daily` | `none` \| `daily` \| `weekly` \| `monthly`. Ignored (forced to unpartitioned) when `ingest: incremental`. |
| `ingest` | str | `snapshot` | How the data ARRIVES: `snapshot` (whole state each run) \| `incremental` (watermark delta). |
| `publish` | str \| null | derived | How it REACHES CKAN: `replace` \| `upsert`. Unset derives the pairing `ingest` used to imply — snapshot → replace, incremental → upsert. |
| `accumulate` | bool \| null | derived | Keep a cumulative canonical table and publish *that*. Forced on for `incremental` + `replace`; opt in by hand for a snapshot source with a cumulative target. Needs `ckan.primary_key`. |
| `transforms` | list[dict] \| null | `null` | Declarative steps, run in order. Each is `{op: <primitive>, ...params}`. Validated at load. |
| `required_columns` | list[str] \| null | `null` | Lightweight presence-only contract, used only when the dataset ships no `schema.py`. (Named this, not `schema`, because `schema` is reserved on the base `Model`.) |
| `geocode` | bool | `false` | Forward geocoding (address → lat/lon) before transforms. `GeocoderResource` is still a stub, so this currently raises if set. |
| `heavy` | bool | `false` | Stamps the `wprdc/heavy` run tag for datasets whose source is hundreds of MB. |

Any field in these blocks accepts `{{ env('VAR', 'default') }}`. Use it for
values that are **deployment config rather than dataset config** — above all
hostnames, which shouldn't be committed:

```yaml
  source:
    type: sftp
    host: "{{ env('ALLEGHENY_SFTP_HOST', '') }}"   # publisher-level
    port: "{{ env('ALLEGHENY_SFTP_PORT', '22') }}"
    path: /outbound/assessments/*.csv              # dataset-level, stays here
    secret_ref: ALLEGHENY_SFTP                     # var NAME, not the secret
```

Scope these to the **publisher**, not the dataset: one SFTP server serves all of
a publisher's feeds, so `ALLEGHENY_SFTP_HOST` is shared and only the per-dataset
`path` lives in each `defs.yaml`.

The empty default matters. A template with no default raises at load, which
would break `dg check defs` and CI for anyone without the var set; `''` keeps
the config structurally valid and lets the SFTP extractor fail loudly on the
first run with a message naming what's missing. Non-string fields (`port`) are
coerced after rendering. This works only because every model here is
`Resolvable` — see the note in `components/models.py`.

#### SourceModel

| Field | Type | Notes |
|-------|------|-------|
| `type` | str | `sftp` \| `http` \| `arcgis` \| `pasda` \| `api_bulk` \| `api_incremental`. Selects the extractor. |
| `host` | str \| null | SFTP host. |
| `port` | int \| null | SFTP port; defaults to 22 when unset. |
| `path` | str \| null | SFTP glob (`/outbound/*.csv`). Also accepted as an HTTP URL fallback. |
| `url` | str \| null | Full URL for `http` sources. |
| `secret_ref` | str \| null | **Name of an env var** holding `user:password` — never the secret itself. Required for `sftp`; optional HTTP Basic auth for `http`. |
| `catalog` | str \| null | `arcgis`: the Hub site's `data.json`. |
| `title` | str \| null | `arcgis`: the layer's catalogue title, matched exactly. |
| `dataset_id` | str \| int \| null | `pasda`: the numeric id in the layer's `DataSummary.aspx?dataset=` landing page. `int` as well as `str` because dagster's resolve pass coerces a numeric-looking scalar, so a quoted `"1224"` still arrives as an int. |
| `format` | str \| null | `arcgis`: `csv` (default) \| `geojson` \| `shapefile` \| `kml` \| `file_geodatabase` \| `feature_collection` \| `xlsx` \| `geopackage` \| `sqlite`. `pasda`: `shapefile` (default) \| `csv` \| `kmz`. |

#### MirrorModel

One upstream distribution copied onto its own CKAN resource. Copied, not
derived — an ArcGIS CSV export of a polygon layer carries no geometry, so a
`RepresentationModel` built from the validated frame would be shapeless.

| Field | Type | Notes |
|-------|------|-------|
| `format` | str | `geojson` \| `shapefile` \| `kml` (uploaded) \| `hub_page` \| `rest_api` (link only). `csv` is fine here; it's refused only when `ckan.resource_id` is also set. |
| `resource_id` | str \| null | Target resource. Created (needs `ckan.package_id`) when absent. |
| `name` | str \| null | CKAN resource name; defaults per format to what existing packages use. |
| `datastore` | bool | Also ingest the uploaded file so it's queryable. A csv goes via DataPusher+; a **geojson** needs `CkanResource.spatial_load_action`, which doesn't exist yet and raises rather than falling back. |

#### CkanModel

| Field | Type | Notes |
|-------|------|-------|
| `resource_id` | str | The CKAN resource (UUID) that receives the tabular data. |
| `primary_key` | list[str] \| null | Required for `publish: upsert` (the upsert key) and for `accumulate` (the merge key). |
| `rebuild` | bool | `false` | Make a `replace` DROP the DataStore table instead of truncating it. Needed for a genuine schema change; discards anything else added to the table, including `ckanext-spatialdata`'s geometry column and indexes. |

#### GeometryModel

Where every geospatial output of the dataset gets its geometry — the exports
below and `ckan.spatial` both read it. It's dataset-level rather than per-output
because they all want the same answer, and two copies could drift. Omit it when
the source is already geospatial (geojson/shapefile); the frame carries its own.

| Field | Type | Notes |
|-------|------|-------|
| `wkt` | str \| null | Column holding WKT geometry. Dropped from the output after conversion, so it isn't published twice. |
| `lat`, `lng` | str \| null | Or a coordinate pair, for point data with no WKT column. |

`wkt` wins when both are set — it carries shapes other than points, so a dataset
declaring one means it.

#### RepresentationModel

The same canonical frame, serialised differently, published as its own resource.
The name is broader than what's implemented: only the geospatial formats exist
today, but the shape fits Parquet, XLSX, or a fixed-width extract equally well —
adding one is a branch in `publish_representation`.

| Field | Type | Notes |
|-------|------|-------|
| `format` | str | `geojson` \| `shapefile` (`shp` is accepted too). |
| `resource_id` | str | The CKAN **file** resource for this representation — not the DataStore one. |

Geometry is not configured here; the geospatial formats read the dataset's
`geometry:` block. Format dispatch happens before that step, so a future
non-geospatial representation never asks for geometry it doesn't need.

#### RegionLayerModel

| Field | Type | Notes |
|-------|------|-------|
| `name` | str | Layer name in the store, e.g. `council_district`. This is what `reverse_geocode` asks for. |
| `value_field` | str | Source column holding the stable region code. Features are dissolved by this. |
| `label_field` | str \| null | Source column holding the human label, if the layer has one. |

#### KeyLayerModel

| Field | Type | Notes |
|-------|------|-------|
| `name` | str | Layer name in the keyed-geometry store, e.g. `address_point`. This is what `join_geometry` asks for. |
| `key_field` | str | Source column holding the key. A repeated key keeps its first row. |

Not a region layer: nothing is dissolved and nothing answers point-in-polygon.
The source must read as geometry (geojson / shapefile).

### Load-time invariants

These raise at `dg check` / `dg dev`, not during a run:

| Condition | Error |
|-----------|-------|
| `transforms:` names an unknown op, misspells a param, or omits a required one | `transform step N: unknown op ...` / `... accepted params: ...` |
| a `reverse_geocode` step has a malformed `regions:` entry | `reverse_geocode: regions[N] ...` |
| none of `ckan:` / `region_layer:` / `key_layer:` is set | `<stem>: needs a destination ...` |
| a `join_geometry` step's `key_format` lacks exactly one `{}` | `join_geometry: key_format ... must contain exactly one '{}'` |
| `publish: upsert` without `ckan.primary_key` | `<stem>: publish 'upsert' requires ckan.primary_key (the upsert key)` |
| `accumulate` without `ckan.primary_key` | `<stem>: accumulate requires ckan.primary_key (the merge key)` |
| `accumulate` together with `publish: upsert` | `<stem>: accumulate with publish 'upsert' would accumulate twice ...` |
| an unrecognised `publish` value | `<stem>: unknown publish mode ...` |
| `ckan.spatial` without a `geometry:` block | `<stem>: `ckan.spatial` needs a `geometry:` block ...` |

### Example

`defs/city_of_pittsburgh/water_features/defs.yaml` — HTTP source, weekly
snapshot, declarative transforms including reverse geocoding, one CSV resource
plus a GeoJSON representation:

```yaml
type: wprdc_etl.components.tabular_pipeline.TabularPipeline

attributes:
  publisher: city_of_pittsburgh
  dataset: water_features
  source:
    type: http
    url: https://storage.googleapis.com/pghpa_open_data/cartegraph/water_features.csv
  schedule: "0 6 * * 1"          # 06:00 America/New_York every Monday
  partition: weekly
  ingest: snapshot
  transforms:
    - op: coerce_text
      columns: [id, council_district, ward, police_zone]
    - op: strip
    - op: reverse_geocode
      lat: latitude
      lng: longitude
      regions:
        - neighborhood
        - layer: council_district
          value: id
        - layer: census_tract           # published as `tract`
          column: tract
          value: id
        - fire_zone
        - public_works_division
  ckan:
    resource_id: "513290a6-2bac-4e41-8029-354cbda6a7b7"
  geometry:
    lat: latitude
    lng: longitude
  representations:
    - format: geojson
      resource_id: "f7c252a5-28be-43ab-95b5-f3eb0f1eef67"
```

Builds: `city_of_pittsburgh/water_features/{landed,validated,loaded,published/geojson}`,
the `schema_ok` check, job `city_of_pittsburgh__water_features__job`, and schedule
`city_of_pittsburgh__water_features__schedule`.

A boundary layer is the same component with `region_layer:` and (here) no
`ckan:` at all — see `defs/city_of_pittsburgh/boundaries/neighborhood/defs.yaml`:

```yaml
  region_layer:
    name: neighborhood
    value_field: hood_no         # the stable code
    label_field: hood            # the human label
```

That instance builds `landed`, `validated`, `region_layer` and `schema_ok` —
no `loaded`, because there is no CKAN target.

## FilePipeline

`wprdc_etl.components.file_pipeline.FilePipeline` — blobs (PDFs, GeoTIFFs,
images) published as-is. No parsing, no transform, no validation, no DataStore.

### Asset graph

```
landed ──► published
```

| Asset key suffix | Returns | What it does |
|------------------|---------|--------------|
| `landed` | `dict` (the manifest) | Same extractors as the tabular type; lands the blob under its real extension (`data.tif`, `data.pdf`). |
| `published` | nothing | Streams the landed object S3 → temp file → CKAN via a normal resource upload (`publish_file`). Never touches the DataStore/DataPusher+, and never buffers the blob in memory. |

### Fields

Required: `publisher`, `dataset`, `source`, `ckan`. (Unlike the tabular type,
`ckan` is **not** optional here — publishing the blob is the entire point.)

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `publisher`, `dataset`, `department` | str / str \| null | — | As above. |
| `source` | [SourceModel](#sourcemodel) | — | Same extractor vocabulary. |
| `ckan` | [CkanModel](#ckanmodel) | — | `resource_id` is the single file resource that gets re-uploaded. |
| `schedule` | str \| null | `null` | Cron, else a placeholder sensor. |
| `partition` | str | `none` | See below — `none` is the default here, unlike the tabular type. |
| `heavy` | bool | `false` | Same run tag. |

`partition` reuses the tabular vocabulary but means something slightly
different for a blob:

- `daily` / `weekly` / `monthly` — a **snapshot series**, e.g. a daily weather
  GeoTIFF. Every run's file is kept in the immutable landing bucket; CKAN
  carries a single "latest" resource that re-points at the newest partition.
- `none` (default) — a **single evolving object**, e.g. a report PDF. Landed at
  `.../current/`, replace-in-place.

## Partitioning

`components/_common.py:partitions_for()`:

| `partition` | Partitions definition | Partition key example |
|-------------|----------------------|-----------------------|
| `monthly` | `MonthlyPartitionsDefinition(start_date="2024-01-01")` | `2026-08-01` |
| `weekly` | `WeeklyPartitionsDefinition(start_date="2024-01-01")` | `2026-08-30` |
| `none` / unset | none — the asset is unpartitioned | — (the code uses the literal `current`) |
| anything else (incl. `daily`) | `DailyPartitionsDefinition(start_date="2024-01-01")` | `2026-08-30` |

Two things worth knowing:

- **An unrecognised cadence silently becomes daily.** `partition: dialy` is a
  daily pipeline, not an error.
- **`ingest: incremental` forces the assets unpartitioned**, whatever
  `partition:` says — the watermark drives increments, not the calendar.

Partition boundaries are UTC even though schedules fire on Eastern time
(see below); aligning them is a separate, larger change.

## Scheduling, jobs, run tags

Every instance gets exactly one job, `<stem>__job`, selecting all of its assets.
Then:

- `schedule:` set → a `ScheduleDefinition` named `<stem>__schedule`, with
  `execution_timezone = America/New_York` (every publisher is a Pittsburgh-area
  civic agency, so schedules are Eastern wall-clock, not UTC).
- `schedule:` omitted → a placeholder sensor named `<stem>__sensor` that yields
  a `SkipReason`. It's a seam for arrival-triggered datasets; it does not
  trigger runs yet, so materialize those manually.

`run_tags()` stamps every job:

| Tag | Value | Set for |
|-----|-------|---------|
| `wprdc/publisher` | `cfg.publisher` | both types |
| `wprdc/dataset` | the `stem` | both types |
| `wprdc/source` | `cfg.source.type` | both types |
| `wprdc/ingest` | `cfg.ingest` | tabular only (FilePipeline has no `ingest`) |
| `wprdc/heavy` | `"true"` | when `heavy: true` |

**These tag keys are load-bearing in production.** `deploy/prod/dagster.yaml`
keys its `tag_concurrency_limits` off them: one run per `wprdc/dataset`, two per
`wprdc/publisher`, and one `wprdc/heavy` run at a time. Rename a tag in
`_common.py` and you must rename it there too.

## Resources

Assets bind resources **by parameter name**. The resources themselves are
instantiated once in `definitions.py` and are deliberately *not* declared in any
component's `Definitions`:

| Parameter | Resource | Used by |
|-----------|----------|---------|
| `landing` | `LandingZoneResource` (S3) | `landed`, `validated`, `schema_ok`, file `published` |
| `sftp` | `SFTPResource` | `landed` (SFTP sources) |
| `ckan` | `CkanResource` | `loaded`, `published/<format>`, file `published` |
| `geocoder` | `GeocoderResource` (stub) | `validated`, when `geocode: true` |
| `spatial` | `SpatialResource` (PostGIS) | `validated` (for `reverse_geocode`), `region_layer` |

## Dry-run behaviour

Dry-run is the **default**; real writes require `ENVIRONMENT=production`. What
each asset does when dry-running:

| Asset | Dry-run (default) | Production |
|-------|-------------------|------------|
| `landed` | writes to the configured S3 endpoint — LocalStack in dev. Writing to *real* AWS S3 outside production is refused (`WPRDC_ALLOW_REAL_S3=1` is the deliberate opt-out) | writes to real S3 |
| `validated` | unchanged (pure compute) | unchanged |
| `loaded` | writes `_dryrun/<stem>.csv`, never calls CKAN | replace / upsert against CKAN |
| `published/<format>` | writes `_dryrun/<stem>.geojson` / `.zip` | uploads to the CKAN file resource |
| `region_layer` | writes to the local PostGIS; a **non-local** DSN is refused (`WPRDC_ALLOW_REMOTE_SPATIAL=1` is the deliberate opt-out) | writes to the production PostGIS |
| file `published` | copies the blob to `_dryrun/<stem><ext>` | uploads to the CKAN file resource |

The sink directory is `WPRDC_ETL_SINK_DIR` if set, else `_dryrun/`. Reverse
geocoding *reads* the spatial store in dry-run — only writes are guarded.

## Co-located escape hatches

Optional Python files next to a `defs.yaml`, loaded by
`dataset_modules.load_dataset_module()`. They are imported, not walked, so they
don't upset the `defs/` tree walker.

| File | Contract | When |
|------|----------|------|
| `schema.py` | `SCHEMA = frame({...})` — a pandera `DataFrameSchema` | the dataset needs a full column contract. Every declared column is required; the raw file must be a superset |
| `transform.py` | `def transform(df, cfg=None) -> df` | bespoke logic the shared primitives can't express. Runs **after** the declarative steps |
| `fetch.py` | `def fetch(source, since) -> (records, new_watermark)` | **required** for `api_incremental` sources |

`scripts/scaffold_pipeline.py` generates typed stubs for these.

## Adding or changing a component type

The framework has sharp edges. In order of how much time they cost:

1. **Instance files are `defs.yaml`**, never `component.yaml` — the old name
   routes to a backcompat parser that fails with "No components found in YAML file".
2. **Nothing but components under `defs/`.** `load_defs` walks every directory;
   a stray `tests/` dir or a bare `.yaml` with no `type:` breaks the walk.
   Co-located `.py` files are fine.
3. **`__init__.py` at every level of `defs/`**, or the tree becomes a namespace
   package with no `__file__` and `load_defs` fails.
4. **No `from __future__ import annotations`** in a module defining a component
   or a config `Model`. Dagster's `Resolvable` reads raw annotations; stringized
   ones break field derivation. (This is why `tabular_pipeline.py` imports
   pandas eagerly — its asset annotations must resolve at runtime.)
5. **Don't decorate a component class with `@dataclass`.** They are pydantic
   `Model`s; `@dataclass` breaks default handling, so optional fields start
   being reported as required.
6. **Don't name a field `schema`** — it shadows a reserved attribute on `Model`.
7. Register the module in `pyproject.toml` `registry_modules`, then confirm with
   `uv run dg list components`.

Verify with **both** `uv run dg check defs` *and* a `uv run dg dev` load — they
are separate passes and `load_defs` is the stricter of the two.

## Known gaps

- `geocode: true` raises — `GeocoderResource` is a stub. Parcel-style data
  should resolve geometry with a spatial join on a parcel id anyway, not address
  geocoding.
- Geo + `publish: upsert` is unsupported: geometry isn't JSON-serialisable for
  the DataStore upsert payload. An incremental geo dataset can still be
  published — set `publish: replace`, which accumulates and ships a file, and
  the geometry travels as a column rather than as JSON.
- `pli_division` has no boundary layer on WPRDC, so it can't be recovered by
  reverse geocoding. For `water_features` it equals the ward on every live row,
  so that dataset's `transform.py` copies it from `ward`.
- The placeholder arrival sensor never requests a run.

## Accumulating datasets

`publish: replace` hands DataPusher+ a COMPLETE file. An `ingest: incremental`
run only holds a delta, so the full table has to live somewhere we own — that's
what `accumulate` does, and why `incremental` + `replace` turns it on for you.

The motivation is the download path. `publish: upsert` writes to the DataStore
and **never touches the resource's file**, so the file is stale or absent and
CKAN has to serialise the whole table on every download request — slow on a long
lived dataset and prone to timing out behind the CDN. Accumulating and replacing
gives the resource a real static CSV, and leaves the DataStore for API queries.

The canonical table lives beside the watermark and drift baseline, as a parquet
plus a small sidecar:

```
_state/<publisher>/[<department>/]<dataset>/current.parquet
_state/<publisher>/[<department>/]<dataset>/current.json    {rows, columns, updated_at}
```

Parquet, not CSV — reading a CSV back turns nullable ints into floats and eats
leading zeros on the very identifier columns the merge keys on.

The merge (`strategies/accumulate.py`) is:

- **first run** — no stored table, so the delta *is* the table;
- **delta wins** on a primary-key collision, so a corrected row replaces the
  stored one;
- **columns are unioned**, prior order first, so the published header only ever
  grows rightward and DataPusher+ keeps seeing a table it recognises;
- **rows are never deleted.** A delta can't express a deletion; neither can an
  upsert. Removing rows means editing the stored parquet by hand.

A null in the primary key is fatal, not a warning: nulls never compare equal, so
such a row would be re-appended on every single run and the table would grow
without bound. Fix it with a `drop_nulls` or `fill_na` transform step.

### The state-loss guard

The sidecar exists for one job. If the stored parquet goes missing or comes back
short, the merge would quietly treat this run's delta as the entire dataset,
`loaded` would upload that as the complete file, and DataPusher+ would reload
CKAN down to it — the published dataset loses its history and **nothing errors**.

So `accumulated` compares what it read against what the sidecar says was last
written, and raises before the publish if the table shrank. This is a hard
failure rather than a check: an asset check runs after the materialisation, far
too late to stop the upload.

A genuine first run has no sidecar and nothing to compare. To reset an
accumulator on purpose, delete **both** `current.parquet` and `current.json`.
