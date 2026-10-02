# Strategies (stage logic)

Reference for `src/wprdc_etl/strategies/`. The component types own the pipeline
*shape* — extract → land → read → transform → validate → load/emit. This package
owns the swappable *how* of each stage, selected by a field in the instance's
`defs.yaml`. Adding a source type or a transform is a change here, not a new
pipeline.

The component types that call into these are documented in
[pipelines.md](pipelines.md).

| Stage | Module | Selected by | Registry |
|-------|--------|-------------|----------|
| [extract](#extract) | `extract.py` | `source.type` | `EXTRACTORS` → `get_extractor()` |
| metadata | `metadata.py` | — | catalogue HTML → CKAN Markdown notes + tags (pure) |
| mirror | `mirror.py` | `ckan.mirror` | copies upstream distributions onto CKAN resources |
| [read](#read) | `read.py` | the landed file's extension | `READERS` → `get_reader()` |
| [transform](#transform) | `transform.py` | each step's `op:` | `PRIMITIVES` |
| [load](#load) | `load.py` | `ingest` | `LOADERS` → `get_loader()` |
| [emit](#emit) | `emit.py` | each entry's `format:` | (dispatch inside `publish_representation`) |
| [schema](#schema) | `schema.py` | imported by a dataset's `schema.py` | — |

## Extract

**Contract.** One method, resources passed by keyword so each extractor takes
only what it needs:

```python
class Extractor:
    def extract(self, context, cfg, *, landing, sftp=None) -> dict[str, Any]:
        ...  # lands the raw artifact + manifest, returns the manifest
```

`get_extractor(source.type)` raises `NotImplementedError` for an unregistered
type.

| `source.type` | Class | Status |
|---------------|-------|--------|
| `sftp` | `SftpFileExtractor` | implemented |
| `http` | `HttpFileExtractor` | implemented |
| `arcgis` | `ArcGisExtractor` | implemented — resolves the download URL from an ArcGIS Hub `data.json` by layer title |
| `api_bulk` | `ApiBulkExtractor` | **stub** — registered so the dispatch seam exists; calling it hits the base `NotImplementedError` |
| `api_incremental` | `ApiIncrementalExtractor` | implemented (needs a co-located `fetch.py`) |

### The landing zone

Every extractor ends at `landing.land_file(...)`, which writes two objects:

```
s3://{bucket}/{publisher}/[{department}/]{dataset}/{partition}/
    data.<ext>          the raw bytes, under the source's real extension
    manifest.json       the sidecar below
```

```json
{
  "publisher": "...", "department": null, "dataset": "...",
  "partition": "2026-08-30",
  "filename": "data.csv",
  "sha256": "...", "size": 65481,
  "source": { "...": "extractor-specific provenance" },
  "skipped": false
}
```

**Idempotency is by checksum.** If a manifest already exists for this partition
with a matching `sha256`, nothing is uploaded and the existing manifest comes
back with `skipped: true`. The unpartitioned case uses the literal partition key
`current`.

`filename` is what makes the rest of the pipeline format-agnostic: [read](#read)
dispatches off it, so a landed `data.geojson` reaches the geo reader without
anything else being configured.

### SftpFileExtractor

1. Resolves `source.secret_ref` from the environment and splits it on the first
   `:` into user / password. A missing `secret_ref` or unset env var is a
   `dg.Failure`.
2. Lists `source.path` (a glob) on `source.host`, port `source.port or 22`.
   No matches → `dg.Failure`.
3. Picks the **newest match by mtime**, lands it under `data<ext>` where `ext`
   comes from the remote filename (falling back to `.csv`).
4. Streams remote → temp file → S3, so a multi-hundred-MB extract is never held
   in memory.

Provenance recorded in `source`: the remote path, mtime and size.
Output metadata: `sha256`, `size`, `skipped`, `remote_path`, `filename`.

Host keys: auto-added in dev, **rejected if unknown in production**
(`SFTPResource.known_hosts`).

### HttpFileExtractor

1. URL is `source.url`, falling back to `source.path`. Neither → `dg.Failure`.
2. Optional HTTP Basic auth if `source.secret_ref` is set (same `user:password`
   env-var convention).
3. Streams the response body to a temp file in 1 MiB chunks, then lands it.
   Extension comes from the URL path with any query string stripped, so
   `.../features.geojson?token=x` lands as `data.geojson`.

Provenance recorded in `source`: `url`, `etag`, `last_modified`,
`content_length`, `content_type` — the HTTP analogue of SFTP's mtime/size.
Idempotency still keys off the content checksum, not these headers.

### ApiIncrementalExtractor

Cursor/watermark delta pull; pairs with `ingest: incremental` (upsert). The API
shape varies per source, so the actual pull is a co-located hook:

```python
# defs/<publisher>/[<department>/]<dataset>/fetch.py
def fetch(source, since) -> tuple[list[dict], Any]:
    """Return (records newer than `since`, new_watermark).

    `since` is the stored watermark (None on first run); `new_watermark` is the
    highest cursor value seen — a datetime string, an id, a page token. It is
    opaque to the framework."""
```

Flow: read the watermark → `fetch(source, since)` → land the records as
`delta.json` at the `current` key → advance the watermark. Missing `fetch.py`
is a `dg.Failure`. An empty delta is a clean no-op, and **the watermark only
advances when records came back and `new_watermark` is not None**, so a failed
or empty pull can't skip data.

Watermark state lives outside the dataset's prefix, next to the schema-drift
baseline `schema_ok` keeps (see
[pipelines.md](pipelines.md#asset-graph)) — both are mutable running state,
which is exactly what the landing zone proper isn't:

```
s3://{bucket}/_state/{publisher}/[{department}/]{dataset}/watermark.json
s3://{bucket}/_state/{publisher}/[{department}/]{dataset}/schema.json
```

### Adding an extractor

Subclass `Extractor`, override `extract`, register it in `EXTRACTORS`. Land via
`landing.land_file()` (streaming) rather than `land()` (in-memory bytes) unless
the payload is small, and call `context.add_output_metadata()` with at least
`sha256` / `size` / `skipped` so a run's provenance shows in the UI.

## Read

`read_landed(landing, cfg, partition)` downloads the landed object to a temp
file and parses it with the format-appropriate reader:

1. Read the manifest; take `filename` (default `data.csv`).
2. Format = the extension, lowercased, minus the dot (empty → `csv`).
3. Download `{prefix}/{filename}` to a temp file.
4. `get_reader(fmt)(path)` — unknown format raises `NotImplementedError`.

| Format | Reader | Returns |
|--------|--------|---------|
| `csv` | `pd.read_csv` | `DataFrame` |
| `json` | `pd.read_json` | `DataFrame` |
| `geojson` | `gpd.read_file` | `GeoDataFrame` |
| `shp` | `gpd.read_file` | `GeoDataFrame` |
| `zip` | `gpd.read_file("zip://...")` | `GeoDataFrame` (assumed zipped shapefile bundle) |

**Readers take a local file path, not a buffer.** That's what geospatial formats
need — a shapefile is a multi-file bundle (`.shp`/`.shx`/`.dbf`/`.prj`) that
sources ship as a `.zip`, and geopandas reads it in place through the `zip://`
virtual filesystem. For CSV this costs one local disk write, which is cheap and
keeps the interface uniform.

`pd.read_json` is used with default orient; refine it when a real JSON source
appears.

## Transform

Two layers, and they compose:

1. **Declarative steps** in `defs.yaml` (`transforms:`), run in order by
   `apply_declarative`. This is the shared 80%.
2. **A co-located `transform.py`**, run *after* the declarative steps. It
   imports these same primitives, so it composes rather than reimplements.

### Primitive contract

```python
def my_primitive(df, **params) -> df
```

Every primitive is frame-in / frame-out and **guards missing columns** — a
schema hiccup degrades gracefully instead of raising mid-batch. Failing the run
is `schema_ok`'s job, not a transform's.

`apply_declarative` works on `df.copy()`, so the IO-manager input is never
mutated.

### The vocabulary

| `op` | Params | Effect |
|------|--------|--------|
| `iso_date` | `columns`, `fmt` (optional) | Parse and re-render as ISO 8601 `YYYY-MM-DD`. Unparseable values become NaT. |
| `strip` | `columns` (optional) | Trim leading/trailing whitespace. With no `columns`, every object-dtype column. |
| `rename` | `mapping` | Rename columns via `{old: new}`. |
| `coerce_numeric` | `columns` | To numeric; non-numeric becomes NaN. |
| `coerce_text` | `columns` | To nullable string — the inverse of `coerce_numeric`. `1011` renders `"1011"`, not `"1011.0"`; missing stays NA. For ids/codes a CSV parser reads as numbers (zip, FIPS, district, account id). |
| `drop_nulls` | `columns` | Drop rows null in any named column (e.g. a missing key). |
| `fill_na` | `value`, `columns` (optional) | Fill NA with a constant, on named columns or the whole frame. |
| `select` | `columns` | Keep only the named columns that exist, in order. |
| `drop_columns` | `columns` | Remove the named columns if present. |
| `snake_case_columns` | — | Normalise column names to snake_case. |
| `to_crs` | `epsg` | Reproject a GeoDataFrame. No-op on a frame with no geometry. |
| `reverse_geocode` | `lat`, `lng`, `regions` | Attach administrative region columns by point-in-polygon. See below. |

### Runtime dependencies: `@needs`

Most primitives are pure frame→frame. A few need something from the run — the
spatial store, the Dagster context — and say so explicitly:

```python
@needs("spatial", "context")
def reverse_geocode(df, lat, lng, regions, *, spatial, context=None):
    ...
```

`apply_declarative` injects those by name from its `deps` mapping (the
`validated` asset passes `{"spatial": spatial, "context": context}`) and raises
a clear error if a needed dep isn't available:

```
op 'reverse_geocode' needs 'spatial', which isn't available here
```

**Injected names are not part of the YAML surface.** `validate_steps` stands
them in before checking a step's params, so `spatial:` in a `defs.yaml` is an
error, not a hook.

### Load-time validation

`validate_steps(cfg.transforms)` runs in `build_defs`, so a bad step fails at
`dg check` / `dg dev` rather than at 3am:

- a step with no `op`;
- an unknown `op` (the error lists every known one);
- a misspelled or missing param — checked by binding the YAML params against
  the primitive's real signature with `inspect.signature`, and the error lists
  the accepted params;
- op-specific extra validation, registered in `_STEP_VALIDATORS` — today
  `reverse_geocode`, whose `regions:` entries are normalised (and rejected) up
  front.

### reverse_geocode

Derives region columns from a row's coordinates by point-in-polygon against
boundary layers in the PostGIS admin-region store. Use it for a column the
source stopped shipping, or never shipped.

```yaml
- op: reverse_geocode
  lat: latitude
  lng: longitude
  regions:
    - neighborhood                  # -> column "neighborhood", the label
    - layer: council_district       # -> column "district", the stable code
      column: district
      value: id
```

A `regions` entry is either a bare layer name or a mapping of
`layer` / `column` / `value` — any other key is rejected at load. `column`
defaults to the layer name; `value` defaults to `name` and must be `name` or
`id`. `name` yields the layer's human label and **falls back to the code** for
layers that have none (e.g. `public_works_division`).

Behaviour worth knowing:

- **Only distinct points are queried.** Many rows share a location and the
  lookup cost is per point, so the frame's coordinates are deduplicated before
  the round trip.
- **Misses are null, not fatal.** A point outside every region — a river
  coordinate, a broken extract — leaves the column null and logs a count.
- **Missing coordinate columns degrade.** The requested columns are added as
  all-null (dtype `string`) with a warning; the database is never touched.
- Output columns are always pandas `string` dtype.
- The matching `*/boundaries/*` pipeline must have been materialised at least
  once, or the lookup fails loud naming the layers that *are* loaded.

Layer loading, dissolving and the `ST_MakeValid` normalisation live in
`SpatialResource.replace_layer` — see [pipelines.md](pipelines.md#tabularpipeline)
for the `region_layer:` block that drives it.

### join_geometry

Adds coordinates to a table that references geometry by identifier instead
of carrying it, by looking each row's key up in a key layer of the PostGIS
keyed-geometry store.

```yaml
- op: join_geometry
  layer: address_point        # the key_layer name
  key: ADDRESS_ID             # this frame's key column
  key_format: "SSAP{}"        # optional str.format template; default "{}"
  lat: latitude               # optional output names (these are the defaults)
  lng: longitude
```

- **Keys are exact text.** `450843`, `450843.0` and `" 450843 "` all look up
  as `450843` (`key_text`) before `key_format` is applied — a key column with a
  blank reads as float. Precede it with `coerce_text` on the key so the
  published id loses the `.0` too.
- **Only distinct keys are queried**, in one round trip.
- **Misses are null, not fatal**, and counted in a warning, as are rows with no
  key at all. A missing key column degrades to all-null coordinates.
- A polygon layer yields `ST_PointOnSurface` — a point guaranteed inside it.
- `key_format` must contain exactly one `{}`; anything else fails at load.
- The layer must have been loaded by its `key_layer:` pipeline, or the lookup
  fails loud naming the key layers that are.

Loading lives in `SpatialResource.replace_key_layer` (COPY, one transaction,
duplicate keys keep the first row) — see the `key_layer:` block in
[pipelines.md](pipelines.md#tabularpipeline).

### Adding a primitive

Write `fn(df, **params) -> df`, guard missing columns, register it in
`PRIMITIVES` under its YAML name. Add `@needs(...)` only if it genuinely needs a
runtime object, and add an entry to `_STEP_VALIDATORS` if its params need more
than a name check. Because `validate_steps` binds against the real signature,
the param names you choose *are* the YAML surface — renaming one is a breaking
change to every `defs.yaml` that uses it.

## Load

How the transformed frame reaches CKAN. Derived from `ingest` — the two are
one-to-one, so the loader isn't separately configurable. `get_loader` raises
`ValueError` on an unknown mode.

| `ingest` | Loader | Mechanism |
|----------|--------|-----------|
| `snapshot` | `ReplaceLoader` | Upload the whole file onto the resource, drop the DataStore table, trigger DataPusher+ to reload from scratch. |
| `incremental` | `UpsertLoader` | Upsert changed rows by primary key straight into the DataStore API. No DataPusher+. |

Both follow the same order: **dry-run short-circuit → compatibility check →
write**.

### Dry-run short-circuit

`dump_local()` writes `_dryrun/<stem>.csv` and returns `True`, and the loader
returns immediately — CKAN is never called. It returns `False` only in
production. Note the sink file is always CSV, whatever the frame contains.

### The compatibility check

`compat_report()` compares the output's columns/types to what's *currently*
published, via `datastore_search` with `limit: 0` (an empty result means no
DataStore table yet — a fresh resource, so nothing to break):

```python
{"live_exists": bool, "removed": [...], "added": [...], "type_changes": {col: (live, out)}}
```

Types compare on **coarse buckets** so CKAN's postgres-ish types and our
inferred types are judged on like terms:

| Bucket | CKAN types folded into it |
|--------|---------------------------|
| `text` | `text`, `varchar`, `char`, `json`, `jsonb` |
| `number` | `int`, `int2/4/8`, `integer`, `bigint`, `smallint`, `numeric`, `float`, `float4/8`, `double precision`, `real` |
| `time` | `timestamp`, `timestamptz`, `timestamp without time zone`, `date` |
| `bool` | `bool`, `boolean` |

Anything unrecognised coarsens to `text`.

The policy differs by mode, deliberately:

| | `added` columns | `removed` columns | `type_changes` |
|---|---|---|---|
| **replace** | **BLOCKS** | **BLOCKS** | **BLOCKS** — the reload truncates rather than drops, so the table keeps its current columns and types and has nowhere to put the change. Set `ckan.rebuild: true` to drop and recreate |
| **replace** + `rebuild` | WARN | WARN — consumer-breaking changes should still be visible | WARN |
| **upsert** | — | WARN — those columns exist in CKAN but won't be updated | **BLOCKS** — upserting into an existing typed table can fail or corrupt. Reset the table and re-run if the change is intended |

A first load (no live table) never blocks: DataPusher+ creates the table from
the file.

### What each loader actually calls

`ReplaceLoader` → `CkanResource.replace()`:

1. `resource_patch` with the CSV as a file upload (patch preserves the
   resource's other metadata). The CSV is streamed from a temp file rather than
   buffered in memory — an accumulated table is the whole dataset, not a delta.
2. `datastore_delete` with **`filters: {}`**, which deletes the rows and leaves
   the table standing. Failures are ignored, since the first load has no table.
3. `datapusher_submit`. This is **async**: it returns once the job is queued,
   not once rows have landed.

**Truncate, not drop.** `datastore_delete` with no `filters` drops the whole
table, taking with it any column something else added — above all
`ckanext-spatialdata`'s `dataspatial_wkb` and its GiST index, which aren't part
of the resource's publicly viewable columns. `ckan.rebuild: true` restores the
drop for when the column set genuinely changed; the compatibility check above
blocks a structural change until you set it.

Submitting to DataPusher+ explicitly is deliberate — CKAN's auto-trigger on
resource change has a long history of not firing when only the file changes
(ckan/datapusher#151, ckan/ckan#5727).

> Unverified against a live deployment: that `filters: {}` truncates rather than
> drops on this CKAN, that DataPusher+ doesn't drop and recreate the table
> itself regardless, and whether `ckanext-spatialdata` repopulates
> `dataspatial_wkb` from a trigger or needs an explicit call after the reload.
> If it needs a call, `replace()` has a marked seam for it — but the call has to
> wait for the dpp job, which means polling, not fire-and-forget.

`UpsertLoader` → `CkanResource.upsert()`:

1. Returns early on an empty delta.
2. `datastore_create` with inferred fields, the primary key and `force: true` —
   safe to call every run.
3. `datastore_upsert` in batches of 10,000 records, with NaN coerced to `None`
   so the JSON payload is valid.

Dtype → CKAN field type inference (`_infer_fields`): bool → `bool`;
integer/float → `numeric`; datetime → `timestamp`; everything else → `text`.

## Accumulate

`strategies/accumulate.py` folds a run's frame into the dataset's cumulative
canonical table, kept at `_state/<publisher>/[<department>/]<dataset>/current.parquet`.
It exists because `publish: replace` needs a COMPLETE file and an
`ingest: incremental` run only has a delta.

`merge(prior, delta, primary_key)` is deliberately resource-free so it's
testable on plain frames. First run returns the delta; afterwards it unions the
columns (prior order first), concatenates, and drops duplicates on the primary
key keeping the delta's copy. Null primary keys raise — they never dedupe, so
the rows would be re-appended every run. Rows are never deleted.

## Emit

Additional geospatial exports of a dataset — files built from the same canonical
validated frame and published as their own CKAN **file** resources, one publish
asset per entry in `representations:`.

Geometry comes from the frame when it's already a GeoDataFrame, else from the
dataset's `geometry:` block via `ensure_geo`. That block is dataset-level, not
per-export: `ckan.spatial` reads the same one, and two copies could drift.

`publish_representation(cfg, rep, dataframe, *, ckan, context=None)`:

1. **Geometry.** If the frame is already a `GeoDataFrame` it's used as-is.
   Otherwise the representation must name `lat`/`lng` columns, and point
   geometry is built from them, assumed EPSG:4326. Neither → `dg.Failure`.
2. **Format.** `geojson` → a `.geojson` written with the GeoJSON driver,
   uploaded as `data.geojson`. `shapefile` (or `shp`) → the multi-file bundle is
   written to a temp dir and zipped, uploaded as `data.zip`. Anything else is a
   `dg.Failure`.
3. **Dry-run** writes `_dryrun/<stem>.geojson` / `.zip` and skips CKAN.

These publish as file resources, not DataStore tables, so they pair with
`snapshot` datasets rather than `incremental`.

## Schema

The shared column vocabulary a dataset's co-located `schema.py` imports. Each
dataset's full column list stays in its own file; the building blocks live here.

| Builder | Returns |
|---------|---------|
| `txt(**checks)` | Nullable text column. |
| `num(**checks)` | Nullable numeric column (coerced at the schema level). |
| `key()` | Required, non-null identifier — text, min length 1. |
| `ge0()` | Nullable numeric that must be ≥ 0 (prices, areas, counts). |
| `ranged(low, high)` | Nullable numeric constrained to `[low, high]`. |
| `coded(values)` | Nullable text constrained to a small set of codes. |
| `year(low=1700)` | Nullable year in `[low, current_year + 1]`. |
| `frame(columns, *, strict=False, coerce=True)` | The `DataFrameSchema` itself, with the project defaults. |
| `geo_point_columns(lat="lat", lng="lng")` | A lat/lng pair with valid coordinate ranges, to spread into a columns dict with `**`. |

```python
from wprdc_etl.strategies.schema import coded, frame, geo_point_columns, key, txt, year

SCHEMA = frame(
    {
        "id": key(),
        "name": txt(),
        "status": coded(["active", "retired"]),
        "year_built": year(),
        **geo_point_columns("latitude", "longitude"),
    }
)
```

Three rules:

- **Each builder returns a FRESH `Column`.** Never share one instance across
  keys — pandera assigns the column name during validation, so a reused instance
  can collide.
- **`frame()` defaults are superset-friendly.** `strict=False` allows extra
  columns; `coerce=True` coerces CSV values to the declared dtype. Columns are
  required by pandera default, so the landed file must be a *superset* of what
  you declare.
- **This validates the RAW landed file**, before transforms run. A column your
  transforms derive must not be declared here, or `schema_ok` reports it missing
  and fails as ERROR.

Missing declared columns are an ERROR; value/type check failures are a WARN —
see [pipelines.md](pipelines.md#asset-graph).

## Conventions across strategies

- **Config is duck-typed.** Helpers take any config exposing
  publisher / dataset / source / department / schedule / partition — the
  `PipelineConfig` protocol in `components/models.py`. A strategy that reaches
  past that surface (`load.py` needs `cfg.ckan`) annotates the concrete
  component type instead.
- **Heavy imports are lazy.** pandas, geopandas, pandera, boto3, paramiko and
  psycopg are imported inside functions or under `if TYPE_CHECKING:`, so a
  module loads cheaply and a checkout missing an optional dependency still
  imports.
- **Guards are not obstacles.** The S3 real-write guard, the spatial remote-write
  guard, the schema superset check and the CKAN compatibility check are
  intentional. Don't weaken one to make something pass.
