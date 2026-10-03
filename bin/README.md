# bin/

Quality-of-life wrappers for the local dev loop. Every script reads `.env`
through the same parser Dagster uses, so what they see is what `dg dev` sees —
including `${POSTGIS_PORT}` interpolation.

| | |
|---|---|
| `bin/doctor` | Health-check everything and say how to fix what's broken. **Start here.** |
| `bin/up` | Start the compose stack, wait for both Postgres servers, create the landing bucket. Idempotent. |
| `bin/down` | Stop it. `--volumes` also wipes run history, the region store and the bucket (asks first). |
| `bin/check` | `black --check` + `pytest` + `dg check defs`, all of them, then a summary. |
| `bin/boundaries` | Load the PostGIS boundary layers `reverse_geocode` reads. `--list` to look first. |
| `bin/seed-ckan` | Create the packages/resources the defs publish to, on the local CKAN. Dry-run by default. |
| `bin/arcgis` | Generate/refresh the ArcGIS pipelines from each publisher's `data.json`. Dry-run by default. |
| `bin/editor-schema` | Write `.dg/defs.schema.json` and map it onto every `defs.yaml` in PyCharm: autocomplete, value suggestions, inline errors. |
| `bin/pasda` | Generate/refresh the pipelines sourced from Penn State's PASDA archive. Dry-run by default. |
| `bin/run <dataset>` | Materialize one dataset, working out its partition key. `--list` for the options. |
| `bin/s3` | List the LocalStack landing bucket without the endpoint/credential flags. |
| `bin/psql [spatial\|dagster]` | psql into a dev database, following `POSTGIS_PORT`. |

## A fresh checkout

```bash
cp .env.example .env     # then edit it
bin/up
bin/boundaries           # region store — needed before anything reverse-geocodes
bin/doctor               # confirm
bin/run city_of_pittsburgh/water_features
```

## Regenerating the GIS pipelines

Allegheny County and the City of Pittsburgh publish their GIS layers through
ArcGIS Hub sites, each with a DCAT catalogue at `<site>/data.json`. `bin/arcgis`
turns that catalogue into pipelines — a `defs.yaml` per layer plus a
`schema.py` sampled from the layer's real GeoJSON properties.

```bash
bin/arcgis --list        # catalogue vs what's wired, and any title that drifted
bin/arcgis               # dry run: exactly what a sync would create or skip
bin/arcgis --write       # apply
```

It writes nothing without `--write`, and never touches a dataset that already
has a `defs.yaml` unless you add `--force` — so hand-tightened schemas survive
a re-sync.

The pipelines it writes address their layer by catalogue **title**, not URL: a
Hub download URL embeds the ArcGIS item id and changes when the layer is
republished. `bin/arcgis --list` reports a title that has fallen out of the
catalogue, which is the drift that actually happens.

## The PASDA pipelines

Four county layers are not sourced from the Hub at all. The county's
`data.json` lists them with an "ArcGIS GeoServices REST API" distribution
whose URL is in fact a PASDA landing page, and offers no downloadable file —
so the Hub generator excludes them (`CATALOGUE_EXCLUSIONS` in
`src/wprdc_etl/catalogue_check.py`, which `bin/arcgis --list` reports as
`ELSEWHERE`) and
`bin/pasda` writes them instead:

```bash
bin/pasda --list      # the registry vs what's wired
bin/pasda             # dry run
bin/pasda --write     # apply  (--force to regenerate existing ones)
```

These address their layer by **PASDA dataset id**, not URL: the download
filename carries a release date (`AlleghenyCounty_StreetCenterlines20260928.zip`),
so a stored URL rots on every republish. The extractor resolves the current
link off `DataSummary.aspx?dataset=<id>` per run and records the id, the
landing page and the dated filename in the landed manifest.

Unlike the Hub pipelines these have no catalogue behind them, so there is
nothing to `ckan.mirror` and no upstream tags. They set `ckan.description`
with the curated text from data.wprdc.org, and the metadata sync pushes that
description while leaving the package's tags alone.

Which layer lives where is a registry, not a guess: `PASDA_DATASETS` in
`scripts/sync_pasda.py` pins each folder to its dataset id and CKAN package.

They are written into `gis/` with the Hub-sourced layers, since PASDA is the
archive that distributes them rather than a county department — the layers
themselves are county GIS / Addressing data, and `source.type: pasda` is what
records the origin.

## Why doctor exists

The two failure modes that waste the most time are silent. A stale
`SPATIAL_DSN` port makes the region store simply unreachable, and forgetting
`WPRDC_CKAN_WRITE` means a "successful" run writes to `_dryrun/` while you watch
CKAN for something that is never coming. `doctor` reports the publish mode
first, for that reason.
