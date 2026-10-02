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

Layout: `Answers` is everything gathered below; `prompt_answers()` asks the
questions (grouped by topic); `render_component_yaml()` / `render_schema_py()`
turn answers into file text; `write_pipeline_files()` does the actual I/O.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")


@dataclass
class Answers:
    """Everything the scaffolder asks for, plus the CKAN schema draft (if
    any). One instance flows from prompt_answers() into the renderers and
    write_pipeline_files()."""

    ptype: str  # "tabular" | "file"
    publisher: str
    department: str  # "" when there's no department tier
    dataset: str
    source_type: str  # "sftp" | "http" | "api_bulk" | "api_incremental"
    url: str  # http sources
    host: str  # sftp sources
    path: str  # sftp sources (glob)
    secret_ref: str
    schedule: str  # cron, or "" for a placeholder sensor
    partition: str
    ingest: str  # "snapshot" | "incremental" (tabular only)
    primary_key: list[str]  # required when ingest == "incremental"
    resource_id: str
    schema_py: str | None  # rendered schema.py source, or None


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


def fetch_ckan_fields(
    base_url: str, resource_id: str, api_key: str | None = None
) -> list[dict[str, str]]:
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


def render_schema_py(fields: list[dict[str, str]]) -> str:
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
def render_component_yaml(a: Answers) -> str:
    tabular = a.ptype == "tabular"
    type_path = (
        "wprdc_etl.components.tabular_pipeline.TabularPipeline"
        if tabular
        else "wprdc_etl.components.file_pipeline.FilePipeline"
    )
    lines = [f"type: {type_path}", "", "attributes:", f"  publisher: {a.publisher}"]
    if a.department:
        lines.append(f"  department: {a.department}")
    lines.append(f"  dataset: {a.dataset}")

    lines += ["  source:", f"    type: {a.source_type}"]
    if a.url:
        lines.append(f"    url: {a.url}")
    if a.host:
        lines.append(f"    host: {a.host}")
    if a.path:
        lines.append(f"    path: {a.path}")
    if a.secret_ref:
        lines.append(f"    secret_ref: {a.secret_ref}")

    if a.schedule:
        lines.append(f'  schedule: "{a.schedule}"')
    lines.append(f"  partition: {a.partition}")

    if tabular:
        lines.append(f"  ingest: {a.ingest}")
        lines.append("  # transforms: []   # add declarative steps as needed")
        if not a.schema_py:
            lines.append(
                "  required_columns: []   # or ship a co-located schema.py instead"
            )

    lines += ["  ckan:", f'    resource_id: "{a.resource_id}"']
    if tabular and a.ingest == "incremental":
        lines.append(f'    primary_key: [{", ".join(a.primary_key)}]')

    return "\n".join(lines) + "\n"


TRANSFORM_STUB = '''"""Bespoke transforms for this dataset. Runs AFTER the declarative steps.
Return the modified frame. Delete this file if the declarative steps suffice.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from wprdc_etl.components.tabular_pipeline import TabularPipeline


def transform(df: pd.DataFrame, cfg: TabularPipeline | None = None) -> pd.DataFrame:
    return df
'''

FETCH_STUB = '''"""Incremental pull for this dataset (API-specific).

Return (records newer than `since`, new_watermark). `since` is the stored
high-water mark (None on first run); new_watermark is the highest cursor value
seen — a datetime string, id, or page token.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from wprdc_etl.components.models import SourceModel


def fetch(source: SourceModel, since: Any) -> tuple[list[dict[str, Any]], Any]:
    records: list[dict[str, Any]] = []
    new_watermark = since
    raise NotImplementedError("implement the API pull for this dataset")
    return records, new_watermark
'''


# --------------------------------------------------------------------------
# Prompts
# --------------------------------------------------------------------------
def prompt_identity() -> tuple[str, str, str, str]:
    """Component type + the publisher/department/dataset path."""
    ptype = select("Pipeline type", PIPELINE_TYPES, default="tabular")
    publisher = slug(text("Publisher (e.g. allegheny_county)", required=True))
    department = slug(text("Department (optional, e.g. real_estate)"))
    dataset = slug(text("Dataset (e.g. assessments)", required=True))
    return ptype, publisher, department, dataset


def prompt_source() -> tuple[str, str, str, str, str]:
    """Source type + connection details. http takes a url; everything else
    (sftp, and the API types, which fill theirs in via fetch.py) takes
    host/path."""
    source_type = select("Source type", SOURCE_TYPES, default="sftp")
    url = host = path = ""
    if source_type == "http":
        url = text("Source URL (https://...)", required=True)
    else:
        host = text("Source host (blank if n/a)")
        path = text("Source path/glob (blank if n/a)")
    secret_ref = text("Secret env var name (blank if none)")
    return source_type, url, host, path, secret_ref


def prompt_ingest(ptype: str, source_type: str) -> tuple[str, list[str]]:
    """Ingest mode + primary key. FilePipeline has no ingest concept."""
    if ptype != "tabular":
        return "snapshot", []
    ingest = (
        "incremental"
        if source_type == "api_incremental"
        else select("Ingest mode", INGEST, default="snapshot")
    )
    primary_key = []
    if ingest == "incremental":
        pk = text("Primary key column(s), comma-separated", required=True)
        primary_key = [c.strip() for c in pk.split(",") if c.strip()]
    return ingest, primary_key


def prompt_schema(ptype: str, resource_id: str) -> str | None:
    """Optionally draft schema.py from the live CKAN resource. Returns the
    rendered source, or None (FilePipeline, declined, or the fetch failed)."""
    if ptype != "tabular":
        return None
    if not confirm("Draft schema.py from the existing CKAN resource?", default=True):
        return None
    base = text("CKAN base URL", default="https://data.wprdc.org")
    key = text("CKAN API key (blank if public)")
    try:
        fields = fetch_ckan_fields(base, resource_id, key or None)
        print(f"  pulled {len(fields)} fields from CKAN")
        return render_schema_py(fields)
    except Exception as e:
        print(f"  ! could not fetch CKAN schema ({e}); skipping schema.py")
        return None


def prompt_answers() -> Answers:
    ptype, publisher, department, dataset = prompt_identity()
    source_type, url, host, path, secret_ref = prompt_source()

    schedule = text('Cron schedule (blank for a sensor, e.g. "0 6 1 * *")')
    default_partition = "none" if ptype == "file" else "daily"
    partition = select("Partition cadence", PARTITIONS, default=default_partition)

    ingest, primary_key = prompt_ingest(ptype, source_type)
    resource_id = text("CKAN resource id (UUID)", required=True)
    schema_py = prompt_schema(ptype, resource_id)

    return Answers(
        ptype=ptype,
        publisher=publisher,
        department=department,
        dataset=dataset,
        source_type=source_type,
        url=url,
        host=host,
        path=path,
        secret_ref=secret_ref,
        schedule=schedule,
        partition=partition,
        ingest=ingest,
        primary_key=primary_key,
        resource_id=resource_id,
        schema_py=schema_py,
    )


# --------------------------------------------------------------------------
# Write files
# --------------------------------------------------------------------------
def write_pipeline_files(a: Answers) -> None:
    target = DEFS.joinpath(
        a.publisher, *([a.department] if a.department else []), a.dataset
    )
    if target.exists() and any(target.iterdir()):
        if not confirm(f"{target} exists and is non-empty. Continue?", default=False):
            print("aborted.")
            sys.exit(1)
    target.mkdir(parents=True, exist_ok=True)

    # __init__.py at every level, or `defs/` becomes a namespace package and
    # dg's load_defs can't find it (see CLAUDE.md gotchas).
    node = DEFS
    for seg in target.relative_to(DEFS).parts:
        node = node / seg
        (node / "__init__.py").touch()

    written = [target / "defs.yaml"]
    (target / "defs.yaml").write_text(render_component_yaml(a))

    if a.schema_py:
        (target / "schema.py").write_text(a.schema_py)
        written.append(target / "schema.py")

    if a.ptype == "tabular":
        if confirm("Add a transform.py stub?", default=False):
            (target / "transform.py").write_text(TRANSFORM_STUB)
            written.append(target / "transform.py")
        if a.ingest == "incremental":
            (target / "fetch.py").write_text(FETCH_STUB)
            written.append(target / "fetch.py")

    print("\nCreated:")
    for p in written:
        print(f"  {p.relative_to(ROOT)}")
    print("\nNext: review defs.yaml, refine schema.py, then `dg dev`.\n")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> None:
    print("\n=== wprdc-etl pipeline scaffolder ===")
    write_pipeline_files(prompt_answers())


if __name__ == "__main__":
    main()
