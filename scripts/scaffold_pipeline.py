#!/usr/bin/env python3
"""Interactive scaffolder for wprdc-etl pipeline jobs.

Creates the defs/ folder + files for a new pipeline instance:
  - defs.yaml (TabularPipeline or FilePipeline)
  - __init__.py files down the tree
  - schema.py   (tabular; optionally generated from an existing CKAN resource)
  - transform.py stub (tabular, optional)
  - fetch.py stub (tabular incremental)

Run:  python scripts/scaffold_pipeline.py   (needs `uv add questionary`)

Migration aid: point it at an existing CKAN resource and it drafts schema.py
from the live DataStore fields, so the new pipeline matches what's already
published. The draft uses txt()/num() only — refine with key()/coded()/ranges
by hand, and note schema.py validates the RAW source while CKAN reflects the
POST-transform shape, so adjust for anything your transforms change.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from _prompt import confirm, select, text

ROOT = Path(__file__).resolve().parent.parent
DEFS = ROOT / "src" / "wprdc_etl" / "defs"

PIPELINE_TYPES = [
    ("tabular", "parsed & validated into the DataStore"),
    ("file", "blob (PDF / GeoTIFF / image), no parsing"),
]
SOURCE_TYPES = [
    ("sftp", "pull file(s) over SFTP"),
    ("http", "download a file over HTTP(S)"),
    ("api_bulk", "full dataset from an API"),
    ("api_incremental", "cursor/watermark deltas"),
]
PARTITIONS = [
    ("none", "single object, no history"),
    ("daily", "one snapshot per day"),
    ("weekly", "one snapshot per week"),
    ("monthly", "one snapshot per month"),
]
INGEST = [
    ("snapshot", "full-state replace via DataPusher+"),
    ("incremental", "delta upsert by primary key"),
]


def slug(s):
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")


# --------------------------------------------------------------------------
# CKAN schema pull (migration)
# --------------------------------------------------------------------------
_CKAN_TO_BUILDER = {
    "text": "txt",
    "int": "num",
    "integer": "num",
    "numeric": "num",
    "float": "num",
    "double precision": "num",
    "bigint": "num",
    "bool": "txt",
    "boolean": "txt",
    "timestamp": "txt",
    "timestamptz": "txt",
    "date": "txt",
    "json": "txt",
}


def fetch_ckan_fields(base_url, resource_id, api_key=None):
    import requests

    headers = {"Authorization": api_key} if api_key else {}
    resp = requests.post(
        f"{base_url.rstrip('/')}/api/3/action/datastore_search",
        headers=headers,
        json={"resource_id": resource_id, "limit": 0},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN error: {payload.get('error')}")
    return [f for f in payload["result"]["fields"] if not f["id"].startswith("_")]


def render_schema_py(fields):
    used = {_CKAN_TO_BUILDER.get(f["type"], "txt") for f in fields}
    imports = ", ".join(sorted({"frame", *used}))
    lines = [
        '"""Pandera schema — DRAFTED from the live CKAN resource.',
        "",
        "Starting point only. CKAN can't tell us primary keys or bounded code",
        "domains, and this reflects the POST-transform published shape while this",
        "schema validates the RAW source. Refine: mark the key column key(),",
        "constrain codes with coded([...]), add ge0()/ranged() where sensible,",
        "and adjust for anything your transforms add/rename/retype.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        f"from wprdc_etl.strategies.schema import {imports}",
        "",
        "SCHEMA = frame(",
        "    {",
    ]
    for f in fields:
        builder = _CKAN_TO_BUILDER.get(f["type"], "txt")
        lines.append(f'        "{f["id"]}": {builder}(),   # ckan: {f["type"]}')
    lines += ["    }", ")", ""]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Renderers
# --------------------------------------------------------------------------
def render_component_yaml(a):
    t = a["ptype"]
    type_path = (
        "wprdc_etl.components.tabular_pipeline.TabularPipeline"
        if t == "tabular"
        else "wprdc_etl.components.file_pipeline.FilePipeline"
    )
    lines = [f"type: {type_path}", "", "attributes:", f"  publisher: {a['publisher']}"]
    if a["department"]:
        lines.append(f"  department: {a['department']}")
    lines.append(f"  dataset: {a['dataset']}")

    lines += ["  source:", f"    type: {a['source_type']}"]
    if a["host"]:
        lines.append(f"    host: {a['host']}")
    if a["path"]:
        lines.append(f"    path: {a['path']}")
    if a["secret_ref"]:
        lines.append(f"    secret_ref: {a['secret_ref']}")

    if a["schedule"]:
        lines.append(f'  schedule: "{a["schedule"]}"')
    lines.append(f"  partition: {a['partition']}")

    if t == "tabular":
        lines.append(f"  ingest: {a['ingest']}")
        lines.append("  # transforms: []   # add declarative steps as needed")
        if not a["gen_schema"]:
            lines.append(
                "  required_columns: []   # or ship a co-located schema.py instead"
            )

    lines += ["  ckan:", f'    resource_id: "{a["resource_id"]}"']
    if t == "tabular" and a["ingest"] == "incremental":
        lines.append(f'    primary_key: [{", ".join(a["primary_key"])}]')

    return "\n".join(lines) + "\n"


TRANSFORM_STUB = '''"""Bespoke transforms for this dataset. Runs AFTER the declarative steps.
Return the modified frame. Delete this file if the declarative steps suffice.
"""

from __future__ import annotations

import pandas as pd


def transform(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    return df
'''

FETCH_STUB = '''"""Incremental pull for this dataset (API-specific).

Return (records newer than `since`, new_watermark). `since` is the stored
high-water mark (None on first run); new_watermark is the highest cursor value
seen — a datetime string, id, or page token.
"""

from __future__ import annotations


def fetch(source, since):
    records = []
    new_watermark = since
    raise NotImplementedError("implement the API pull for this dataset")
    return records, new_watermark
'''


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    print("\n=== wprdc-etl pipeline scaffolder ===")

    a = {}
    a["ptype"] = select("Pipeline type", PIPELINE_TYPES, default="tabular")
    a["publisher"] = slug(text("Publisher (e.g. allegheny_county)", required=True))
    dept = text("Department (optional, e.g. real_estate)")
    a["department"] = slug(dept) if dept else ""
    a["dataset"] = slug(text("Dataset (e.g. assessments)", required=True))

    a["source_type"] = select("Source type", SOURCE_TYPES, default="sftp")
    a["host"] = text("Source host (blank if n/a)")
    a["path"] = text("Source path/glob (blank if n/a)")
    a["secret_ref"] = text("Secret env var name (blank if none)")

    a["schedule"] = text('Cron schedule (blank for a sensor, e.g. "0 6 1 * *")')

    default_partition = "none" if a["ptype"] == "file" else "daily"
    a["partition"] = select("Partition cadence", PARTITIONS, default=default_partition)

    a["ingest"] = "snapshot"
    a["primary_key"] = []
    if a["ptype"] == "tabular":
        if a["source_type"] == "api_incremental":
            a["ingest"] = "incremental"
        else:
            a["ingest"] = select("Ingest mode", INGEST, default="snapshot")
        if a["ingest"] == "incremental":
            pk = text("Primary key column(s), comma-separated", required=True)
            a["primary_key"] = [c.strip() for c in pk.split(",") if c.strip()]

    a["resource_id"] = text("CKAN resource id (UUID)", required=True)

    a["gen_schema"] = False
    schema_py = None
    if a["ptype"] == "tabular" and confirm(
        "Draft schema.py from the existing CKAN resource?", default=True
    ):
        base = text("CKAN base URL", default="https://data.wprdc.org")
        key = text("CKAN API key (blank if public)")
        try:
            fields = fetch_ckan_fields(base, a["resource_id"], key or None)
            schema_py = render_schema_py(fields)
            a["gen_schema"] = True
            print(f"  pulled {len(fields)} fields from CKAN")
        except Exception as e:
            print(f"  ! could not fetch CKAN schema ({e}); skipping schema.py")

    # --- create files ---
    target = DEFS.joinpath(
        a["publisher"], *([a["department"]] if a["department"] else []), a["dataset"]
    )
    if target.exists() and any(target.iterdir()):
        if not confirm(f"{target} exists and is non-empty. Continue?", default=False):
            print("aborted.")
            sys.exit(1)
    target.mkdir(parents=True, exist_ok=True)

    node = DEFS
    for seg in target.relative_to(DEFS).parts:
        node = node / seg
        (node / "__init__.py").touch()

    written = []
    (target / "defs.yaml").write_text(render_component_yaml(a))
    written.append(target / "defs.yaml")

    if schema_py:
        (target / "schema.py").write_text(schema_py)
        written.append(target / "schema.py")

    if a["ptype"] == "tabular":
        if confirm("Add a transform.py stub?", default=False):
            (target / "transform.py").write_text(TRANSFORM_STUB)
            written.append(target / "transform.py")
        if a["ingest"] == "incremental":
            (target / "fetch.py").write_text(FETCH_STUB)
            written.append(target / "fetch.py")

    print("\nCreated:")
    for p in written:
        print(f"  {p.relative_to(ROOT)}")
    print("\nNext: review defs.yaml, refine schema.py, then `dg dev`.\n")


if __name__ == "__main__":
    main()
