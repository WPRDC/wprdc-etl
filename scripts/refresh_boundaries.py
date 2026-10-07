#!/usr/bin/env python3
"""Refresh every boundary layer in the PostGIS admin-region store.

    uv run python scripts/refresh_boundaries.py            # refresh them all
    uv run python scripts/refresh_boundaries.py --list     # show what would run
    uv run python scripts/refresh_boundaries.py -l neighborhood fire_zone

Why you need this: `reverse_geocode` resolves coordinates against layers held in
PostGIS, and those layers only get there when the dataset carrying their
`region_layer:` block runs. A dataset that reverse-geocodes (city_of_pittsburgh/water_features, say)
fails on a fresh database until the layers it names have been materialized at
least once. Rather than hunting down which pipelines those are, run this.

It discovers layers by walking defs/ for a `region_layer:` block, so a boundary
pipeline added later is picked up with no change here. A `key_layer:` block
(address points, which join_geometry looks coordinates up in) is a layer too,
and is loaded the same way. Layers whose source is
still an empty stub are listed and skipped, not failed — for an `arcgis`
source that means no catalogue title, since those carry no URL.

Runs each pipeline IN PROCESS (`execute_in_process`), so no `dg dev` and no
Dagster daemon are needed — just the compose stack for S3 landing and PostGIS.
Publishing is unaffected: these pipelines have no `ckan:` block, and the region
store write is permitted in dry-run for a local host (see runtime.py).
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import yaml

REPO = Path(__file__).resolve().parent.parent
DEFS = REPO / "src" / "wprdc_etl" / "defs"


@dataclass
class Layer:
    """One boundary pipeline, read straight from its defs.yaml."""

    path: Path
    publisher: str
    department: str | None
    dataset: str
    layer: str
    url: str
    source_type: str = "http"
    title: str = ""
    dataset_id: str = ""

    @property
    def rel(self) -> str:
        return str(self.path.parent.relative_to(DEFS))

    @property
    def job(self) -> str:
        """The job name build_defs gave this dataset.

        Derived with the same `taxonomy()` the component uses, rather than
        re-implementing the naming rule — they must not drift.
        """
        from wprdc_etl.components._common import taxonomy

        _, _, stem = taxonomy(
            SimpleNamespace(
                publisher=self.publisher,
                department=self.department,
                dataset=self.dataset,
            )
        )
        return f"{stem}__job"

    @property
    def configured(self) -> bool:
        """False for a stub that can't actually fetch anything.

        What counts as configured depends on the source type. An `arcgis`
        source has no `url` at all — it addresses its layer by catalogue
        title, because a Hub download URL embeds the ArcGIS item id and
        changes when the layer is republished. Checking `url` alone reported
        every arcgis boundary layer as an unconfigured stub and skipped it,
        while the same pipelines materialized fine by hand.
        """
        if self.source_type == "arcgis":
            return bool(self.title.strip())
        if self.source_type == "pasda":
            return bool(self.dataset_id.strip())
        return bool(self.url.strip())

    @property
    def source_hint(self) -> str:
        """What is missing, for the STUB line."""
        return {
            "arcgis": "source.title is empty",
            "pasda": "source.dataset_id is empty",
        }.get(self.source_type, "source.url is empty")


def discover() -> list[Layer]:
    """Every defs.yaml carrying a `region_layer:` or `key_layer:` block."""
    found: list[Layer] = []
    for path in sorted(DEFS.rglob("defs.yaml")):
        try:
            doc = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as exc:
            print(f"  ! unparseable: {path} ({exc})", file=sys.stderr)
            continue
        attrs = (doc or {}).get("attributes") or {}
        region = attrs.get("region_layer") or attrs.get("key_layer")
        if not region:
            continue
        found.append(
            Layer(
                path=path,
                publisher=attrs.get("publisher", ""),
                department=attrs.get("department"),
                dataset=attrs.get("dataset", ""),
                layer=region.get("name", "?"),
                url=str((attrs.get("source") or {}).get("url") or ""),
                source_type=str((attrs.get("source") or {}).get("type") or "http"),
                title=str((attrs.get("source") or {}).get("title") or ""),
                dataset_id=str((attrs.get("source") or {}).get("dataset_id") or ""),
            )
        )
    return found


def preflight() -> list[str]:
    """Environment problems worth naming up front rather than as a stack trace."""
    problems = []
    if not os.getenv("SPATIAL_DSN"):
        problems.append(
            "SPATIAL_DSN is not set — the region store is where these layers go. "
            "See .env.example (it derives from POSTGIS_PORT)."
        )
    if not os.getenv("S3_ENDPOINT_URL"):
        problems.append(
            "S3_ENDPOINT_URL is not set — landing would target real AWS S3 and "
            "the write guard will refuse it. Point it at LocalStack."
        )
    return problems


def run_one(layer: Layer) -> bool:
    """Materialize one boundary pipeline in process. True if it succeeded."""
    import wprdc_etl.definitions as d

    try:
        job = d.defs.resolve_job_def(layer.job)
    except Exception as exc:  # noqa: BLE001 - report, don't abort the batch
        print(f"    job {layer.job!r} not found: {exc}")
        return False

    # Last COMPLETE partition: a monthly layer refreshed mid-month should load
    # the finished month, not a window that hasn't closed.
    key = job.partitions_def.get_last_partition_key() if job.partitions_def else None
    label = f" [{key}]" if key else ""
    print(f"    running {layer.job}{label}")
    try:
        result = job.execute_in_process(partition_key=key, raise_on_error=False)
    except Exception as exc:  # noqa: BLE001
        print(f"    FAILED: {exc}")
        return False
    if not result.success:
        for ev in result.get_step_failure_events():
            print(f"    FAILED at {ev.step_key}: {ev.step_failure_data.error.message}")
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Refresh the PostGIS boundary layers used by reverse_geocode."
    )
    ap.add_argument(
        "--list",
        "-n",
        action="store_true",
        help="show what would run and exit, touching nothing",
    )
    ap.add_argument(
        "--layer",
        "-l",
        nargs="+",
        metavar="NAME",
        help="only these layers (region_layer name, e.g. neighborhood)",
    )
    ap.add_argument(
        "--publisher",
        "-p",
        metavar="NAME",
        help="only this publisher (e.g. city_of_pittsburgh)",
    )
    args = ap.parse_args()

    layers = discover()
    if args.publisher:
        layers = [x for x in layers if x.publisher == args.publisher]
    if args.layer:
        wanted = set(args.layer)
        layers = [x for x in layers if x.layer in wanted]
        missing = wanted - {x.layer for x in layers}
        if missing:
            print(f"no such layer: {', '.join(sorted(missing))}", file=sys.stderr)
            return 2

    if not layers:
        print("no boundary layers matched")
        return 1

    ready = [x for x in layers if x.configured]
    stubs = [x for x in layers if not x.configured]

    print(
        f"{len(layers)} boundary layer(s) found: {len(ready)} ready, "
        f"{len(stubs)} not yet configured\n"
    )
    for x in ready:
        print(f"  ready  {x.layer:26s} {x.rel}")
    for x in stubs:
        print(f"  STUB   {x.layer:26s} {x.rel}  ({x.source_hint})")

    if args.list:
        return 0
    if not ready:
        print("\nnothing to run — every matching layer is still a stub.")
        return 1

    if problems := preflight():
        print("\nenvironment:")
        for p in problems:
            print(f"  ! {p}")
        print("\nrefusing to run. Fix the above, or use --list.")
        return 2

    print()
    failed = []
    for x in ready:
        print(f"  {x.layer}")
        if not run_one(x):
            failed.append(x.layer)

    print(f"\n{len(ready) - len(failed)}/{len(ready)} refreshed", end="")
    if failed:
        print(f"; FAILED: {', '.join(failed)}")
        return 1
    print(".")
    if stubs:
        print(f"({len(stubs)} stub(s) skipped — fill in the source to include them)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
