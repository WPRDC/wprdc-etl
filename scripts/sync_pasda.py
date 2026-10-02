#!/usr/bin/env python3
"""Generate the PASDA-sourced pipelines.

    bin/pasda --list              # what's registered and what it resolves to
    bin/pasda                     # dry run
    bin/pasda --write             # write defs.yaml + schema.py
    bin/pasda --only parcels --write --overwrite-defs

Penn State's PASDA archive hosts several Allegheny County layers that the
county's own ArcGIS Hub only LINKS to. The county's data.json is wrong about
them: each advertises an "ArcGIS GeoServices REST API" distribution whose URL
is in fact a PASDA HTML landing page, and none offers a downloadable file. So
`bin/arcgis` excludes them (CATALOGUE_EXCLUSIONS) and they live here instead.

PASDA publishes no catalogue — no DCAT, no API — so the registry below is
hand-maintained. Adding a layer means adding a row. The dataset id is the
stable handle: download filenames embed a release date
(AlleghenyCounty_Parcels20260928.zip), so a stored URL rots on every
republish, and the extractor resolves the current one per run.

These are large: 18-116MB zipped and several times that decoded, so every one
is tagged `heavy: true` for the prod run coordinator.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
DEFS = REPO / "src" / "wprdc_etl" / "defs"
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from schema_infer import builder_for  # noqa: E402  (scripts/ is on sys.path)

# "gis", not "pasda": PASDA is the ARCHIVE these are distributed from, not a
# county org, and the layers themselves are ordinary Allegheny County GIS /
# Addressing data that sit beside the Hub-sourced ones. `source.type: pasda`
# is what records where the bytes come from. The department also drives the
# asset key, the S3 landing prefix and the asset group, so it has to agree
# with the folder these are generated into.
DEPARTMENT = "gis"
# Sampling the schema means downloading the whole zip (a shapefile cannot be
# usefully range-read), so only the first rows are decoded from it.
SAMPLE_ROWS = 300


@dataclass(frozen=True)
class KeyLayerSpec:
    """A `key_layer:` block: this dataset's geometry, looked up by `key_field`,
    is what join_geometry reads under `name`."""

    name: str
    key_field: str


@dataclass(frozen=True)
class PasdaDataset:
    """One PASDA layer and where it publishes.

    `ckan_package` is the CKAN package id, not a name: these are existing,
    live WPRDC datasets, so the target is pinned rather than searched for.
    `expect_fields` is what the layer's REST metadata reported — a cheap
    sanity check that the sampled shapefile is the layer we think it is. The
    shapefile's DBF truncates field NAMES to 10 characters, so the names will
    differ from REST; only the count is comparable, and even that only
    roughly.
    """

    folder: str
    dataset_id: str
    title: str
    ckan_package: str
    expect_fields: int | None = None
    key_layer: KeyLayerSpec | None = None


PASDA_DATASETS: list[PasdaDataset] = [
    PasdaDataset(
        folder="parcels",
        dataset_id="1214",
        title="Allegheny County Parcel Boundaries",
        ckan_package="709e4e52-6f82-4cd0-a848-f3e2b3f5d22b",
        expect_fields=13,
    ),
    PasdaDataset(
        folder="address_points",
        dataset_id="1219",
        title="Allegheny County Addressing Address Points",
        ckan_package="4988ae5c-a677-4a7f-9bd0-e735c19a8ff3",
        expect_fields=36,
        # Landmarks carry an ADDRESS_ID and no geometry of their own;
        # addressing_landmarks gets its coordinates from here.
        key_layer=KeyLayerSpec(name="address_point", key_field="ADDRESS_ID"),
    ),
    PasdaDataset(
        folder="street_centerlines",
        dataset_id="1224",
        title="Allegheny County Addressing Street Centerlines",
        ckan_package="34f6668d-130d-4e10-b49b-598c43b83d27",
        expect_fields=61,
    ),
    PasdaDataset(
        folder="building_footprints",
        dataset_id="1195",
        title="Allegheny County Building Footprint Locations",
        # County-hosted rather than PASDA-hosted, which is why this package is
        # shaped differently from the other three (ArcGIS-style resource names,
        # no PASDA landing-page resource). Its REST resource URL is also dead —
        # gisdata.alleghenycounty.us returns "Service not found" — so PASDA is
        # its only live source.
        ckan_package="926d9afe-ea94-4211-9623-d9ad52fd0778",
    ),
]

PUBLISHER = "allegheny_county"
SCHEDULE = "0 8 1 * *"  # 08:00 America/New_York, 1st of the month


@dataclass
class Resolved:
    """What a sync pass worked out for one registered dataset."""

    spec: PasdaDataset
    url: str = ""
    filename: str = ""
    notes: str = ""
    # CKAN resource name -> id, for the package's existing resources
    resources: dict[str, str] = field(default_factory=dict)
    columns: list[tuple[str, str]] = field(default_factory=list)


def ckan_package(base_url: str, package_id: str) -> dict[str, Any]:
    """Read the target package: its notes and its existing resources."""
    import requests

    resp = requests.get(
        f"{base_url.rstrip('/')}/api/3/action/package_show",
        params={"id": package_id},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["result"]


def sample_shapefile(url: str, rows: int) -> list[tuple[str, str]]:
    """Field names and inferred builders from the shapefile's first rows.

    The whole zip is downloaded because a shapefile's DBF cannot be
    usefully range-read, but only `rows` features are decoded — parcels has
    586,985 of them.

    Sampled from the FILE, not from the layer's REST metadata: a DBF truncates
    field names to 10 characters, so REST names are not what lands, and
    schema_ok validates what lands.
    """
    import tempfile

    import geopandas as gpd
    import requests

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as fh:
        tmp = fh.name
        with requests.get(
            url,
            stream=True,
            timeout=600,
            headers={"User-Agent": "wprdc-etl (civic data ETL)"},
        ) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)
    try:
        gdf = gpd.read_file(f"zip://{tmp}", rows=rows)
    finally:
        Path(tmp).unlink(missing_ok=True)

    out = []
    for col in gdf.columns:
        if col == gdf.geometry.name:
            continue  # the GeoDataFrame's geometry, not a data column
        vals = [str(v) for v in gdf[col].dropna().tolist() if str(v).strip()]
        out.append((col, builder_for(vals, col)))
    return out


# --------------------------------------------------------------------------
# Renderers
# --------------------------------------------------------------------------
def render_defs_yaml(r: Resolved) -> str:
    """The defs.yaml for one PASDA dataset."""
    spec = r.spec
    csv_res = r.resources.get("csv", "")
    geojson_res = r.resources.get("geojson", "")
    reps = ""
    if geojson_res:
        reps = (
            "  # The GeoJSON resource is DERIVED from the validated frame, not\n"
            "  # copied: PASDA ships only a shapefile, so there is no upstream\n"
            "  # GeoJSON to mirror.\n"
            "  representations:\n"
            "    - format: geojson\n"
            f'      resource_id: "{geojson_res}"\n'
        )
    key_layer = ""
    if spec.key_layer:
        key_layer = (
            "  # Also feeds the PostGIS keyed-geometry store, where the\n"
            "  # join_geometry transform looks a row's coordinates up by key.\n"
            "  key_layer:\n"
            f"    name: {spec.key_layer.name}\n"
            f"    key_field: {spec.key_layer.key_field}\n"
        )
    desc = ""
    if r.notes:
        body = "\n".join(
            f"      {ln}" if ln.strip() else "" for ln in r.notes.split("\n")
        )
        desc = (
            "    # The curated text already on data.wprdc.org.\n"
            "    description: |-\n" + body + "\n"
        )
    return f"""type: wprdc_etl.components.tabular_pipeline.TabularPipeline

# {spec.title}
#
# GENERATED by scripts/sync_pasda.py. Sourced from Penn State's PASDA
# archive, NOT the county's ArcGIS Hub: the county's data.json lists this
# layer with an "ArcGIS GeoServices REST API" distribution whose URL is in
# fact a PASDA landing page, and offers no downloadable file at all. The Hub
# generator therefore excludes it (CATALOGUE_EXCLUSIONS in sync_arcgis.py).
#
# Addressed by PASDA dataset id, not URL: the download filename carries a
# release date ({r.filename or "…"}), so a stored URL rots on every
# republish. The extractor resolves the current link off the landing page.
#
# `heavy: true` because this is a large file — the zipped shapefile alone is
# tens to hundreds of MB, several times that decoded — and the prod run
# coordinator serialises heavy decodes.

attributes:
  publisher: {PUBLISHER}
  department: {DEPARTMENT}
  dataset: {spec.folder}
  source:
    type: pasda
    dataset_id: "{spec.dataset_id}"
    format: shapefile
  schedule: "{SCHEDULE}"
  partition: monthly
  ingest: snapshot
  heavy: true
  ckan:
    package_id: "{spec.ckan_package}"
    # The DataStore table, published from the validated frame as CSV.
    resource_id: "{csv_res}"
    sync_metadata: true
{desc}{reps}{key_layer}"""


def render_schema_py(r: Resolved) -> str:
    """The schema.py for one PASDA dataset."""
    spec = r.spec
    used = sorted({b for _, b in r.columns} | {"frame"})
    lines = [
        f'"""Pandera schema for {PUBLISHER} / {DEPARTMENT} / {spec.folder}.',
        "",
        spec.title,
        "",
        "GENERATED by scripts/sync_pasda.py from the first",
        f"{SAMPLE_ROWS} rows of the PASDA shapefile, so these are the ACTUAL",
        "field names of the landed file — which is what schema_ok validates.",
        "",
        "They are NOT the field names the layer's REST service reports: a",
        "shapefile's DBF truncates field names to 10 characters, so the two",
        "disagree. What lands is what matters here.",
        "",
        "Types are conservative — txt() unless every sampled value parsed as a",
        "number, and never for an identifier-shaped name. Tighten by hand:",
        "  * mark the identifier key()",
        "  * bound code domains with coded([...])",
        "Do NOT add reverse-geocoded or otherwise derived columns here.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        f"from wprdc_etl.strategies.schema import {', '.join(used)}",
        "",
        "SCHEMA = frame(",
        "    {",
    ]
    for name, builder in r.columns:
        lines.append(f'        "{name}": {builder}(),')
    lines += ["    }", ")", ""]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------
def resolve(spec: PasdaDataset, ckan_url: str) -> Resolved:
    """Everything needed to write one dataset's files."""
    from wprdc_etl.strategies.extract import resolve_pasda_download

    r = Resolved(spec=spec)
    r.url, prov = resolve_pasda_download(spec.dataset_id, "shapefile")
    r.filename = prov["pasda_filename"]

    pkg = ckan_package(ckan_url, spec.ckan_package)
    # CR dropped as sync_arcgis does: portal notes carry \r\n, and a stray CR
    # inside the YAML block scalar makes every re-sync look like a change.
    r.notes = (pkg.get("notes") or "").replace("\r\n", "\n").replace("\r", "\n")
    r.notes = r.notes.strip()
    for res in pkg.get("resources", []):
        fmt = (res.get("format") or "").upper()
        # Map by FORMAT: these packages name the same thing inconsistently
        # ("KML" for a KMZ on two of them, "KMZ" on another), so the format is
        # the more reliable key, and each appears once per package.
        key = {"CSV": "csv", "GEOJSON": "geojson", "ZIP": "shapefile"}.get(fmt)
        if key and key not in r.resources:
            r.resources[key] = res["id"]
    return r


def ensure_packages(folder: str) -> None:
    """__init__.py at every level, or load_defs fails on a namespace package."""
    path = DEFS / PUBLISHER / DEPARTMENT / folder
    for d in (path.parent.parent, path.parent, path):
        d.mkdir(parents=True, exist_ok=True)
        init = d / "__init__.py"
        if not init.exists():
            init.write_text("")


def main() -> int:
    """Resolve each registered dataset and write its files."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--list", action="store_true", help="show the registry only")
    ap.add_argument("--write", action="store_true", help="write files")
    ap.add_argument(
        "--overwrite-defs", action="store_true", help="replace an existing defs.yaml"
    )
    ap.add_argument(
        "--overwrite-schema", action="store_true", help="replace an existing schema.py"
    )
    ap.add_argument("--only", nargs="+", metavar="FOLDER")
    ap.add_argument("--ckan", default="https://data.wprdc.org")
    args = ap.parse_args()

    specs = PASDA_DATASETS
    if args.only:
        specs = [s for s in specs if s.folder in set(args.only)]

    if args.list:
        print(f"\n{len(specs)} PASDA dataset(s)")
        for s in specs:
            target = DEFS / PUBLISHER / DEPARTMENT / s.folder / "defs.yaml"
            print(
                f"  {'wired' if target.exists() else '-':7} {s.folder:22} "
                f"dataset={s.dataset_id:6} {s.title[:44]}"
            )
        return 0

    mode = "WRITING" if args.write else "dry run (--write to apply)"
    print(f"\n{len(specs)} PASDA dataset(s)  [{mode}]")
    failed = 0
    for spec in specs:
        target = DEFS / PUBLISHER / DEPARTMENT / spec.folder
        plan = []
        if not (target / "defs.yaml").exists() or args.overwrite_defs:
            plan.append("defs.yaml")
        if not (target / "schema.py").exists() or args.overwrite_schema:
            plan.append("schema.py")
        if not plan:
            print(f"  KEEP   {spec.folder:22} both files exist")
            continue
        try:
            r = resolve(spec, args.ckan)
            # Only pay the download when a schema is actually wanted.
            if "schema.py" in plan:
                r.columns = sample_shapefile(r.url, SAMPLE_ROWS)
        except Exception as exc:  # network, parse, CKAN
            print(f"  FAIL   {spec.folder:22} {str(exc)[:80]}")
            failed += 1
            continue
        if "schema.py" in plan and not r.columns:
            print(f"  FAIL   {spec.folder:22} shapefile had no fields")
            failed += 1
            continue
        if not r.resources.get("csv"):
            print(
                f"  FAIL   {spec.folder:22} package has no CSV resource to publish to"
            )
            failed += 1
            continue
        extra = ""
        if spec.expect_fields and r.columns:
            extra = f"  (REST reported {spec.expect_fields})"
        print(
            f"  WRITE  {spec.folder:22} {len(r.columns):3} fields{extra}  "
            f"[{'+'.join(f.split('.')[0] for f in plan)}]"
        )
        if args.write:
            ensure_packages(spec.folder)
            if "defs.yaml" in plan:
                (target / "defs.yaml").write_text(render_defs_yaml(r))
            if "schema.py" in plan:
                (target / "schema.py").write_text(render_schema_py(r))
    if not args.write:
        print("\nnothing written — re-run with --write")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
