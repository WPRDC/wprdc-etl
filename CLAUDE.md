# CLAUDE.md

Guidance for AI agents (Claude Code) working in this repo. Human-facing overview
and full reference live in `README.md`; this file is the operational cheat sheet
plus the gotchas that will otherwise waste your time.

## What this is

Civic-data ETL on Dagster **components**. Each dataset is a `defs.yaml` under
`src/wprdc_etl/defs/`. Two component types: `TabularPipeline` (parsed → validated →
published to CKAN DataStore) and `FilePipeline` (blobs published as-is). See README
for the architecture.

## Commands

```bash
uv sync                       # install deps
uv run dg dev                 # load + run the UI (http://localhost:3000)
uv run dg check defs          # validate all defs.yaml (fast; run after YAML edits)
uv run dg list components     # confirm component types are registered
uv run pytest                 # run tests (tests live in top-level tests/)
python -m py_compile <file>   # quick syntax check without a full load
```

Verify your changes with `uv run dg check defs` **and** a `uv run dg dev` load — the
YAML validator passing does NOT mean the code-location builds (they're separate
passes; `load_defs` is stricter).

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
- **Resources** (S3/SFTP/CKAN clients): `src/wprdc_etl/resources.py`.
- **Env gate + safety guards**: `src/wprdc_etl/runtime.py`.
- **Tests**: top-level `tests/` — NEVER under `defs/` (see gotchas).

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
- **Do NOT decorate component classes with `@dataclass`.** They're pydantic `Model`s
  (`class X(Component, Model, Resolvable)`); `@dataclass` breaks default handling so
  optional fields get reported as required.
- **Don't name a field `schema`** — it shadows a reserved `Model` attribute (warns and
  misbehaves). Use `required_columns` etc.
- **`load_defs` takes the defs MODULE, not a path:** `load_defs(wprdc_etl.defs)`.
- **Only one `Definitions` object at module scope** in `definitions.py` — don't bind the
  `load_defs(...)` result to a module-level name; inline it into the `merge`.
- The dg components layer is version-sensitive. If something here looks wrong for the
  installed version, verify with `dg scaffold defs <type> <path>` (generates a canonical
  instance) rather than guessing.

## Safety — do not violate

The system fails safe: **dry-run is the default; real writes require
`ENVIRONMENT=production`.** When working here:

- **Never set `ENVIRONMENT=production`** in dev, tests, or committed config. Leave it
  unset so loaders write to `_dryrun/` and CKAN is never called.
- **Never point tests or dev runs at real S3 or real CKAN.** The S3 write guard refuses
  real-AWS writes outside production; do not set `WPRDC_ALLOW_REAL_S3` to get around it.
- **Never commit secrets or `.env`.** Credentials are referenced by env-var name
  (`secret_ref`), never inlined in `defs.yaml`.
- Don't weaken the guards in `runtime.py`, the schema superset check, or the CKAN
  compatibility check to make something pass. They are intentional.

## Adding a dataset

Prefer the scaffolder (generates a correct `defs.yaml` + `__init__.py` files, optionally
drafts `schema.py` from CKAN):

```bash
uv run python scripts/scaffold_pipeline.py
```

`defs.yaml` anatomy and the transform-primitive / schema-builder vocabularies are in the
README ("Adding a dataset"). `api_incremental` sources require a co-located `fetch.py`.

## Known gaps / current work

- Extractors: SFTP and API-incremental implemented; **HTTP** and **bulk-API** are stubs
  in `strategies/extract.py`.
- `GeocoderResource` is a stub. Note: parcel data usually resolves to geometry via a
  spatial join on a parcel ID, not address geocoding — prefer a join primitive where a
  spatial key exists.
- Geo + `incremental` is unsupported (geometry isn't JSON-serializable for upsert).
- The assessments example's column names / date formats are drawn from the WPRDC data
  dictionary and unverified against a real extract — confirm before trusting.