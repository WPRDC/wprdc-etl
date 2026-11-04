"""Seed an accumulating dataset's canonical table from its live CKAN resource.

    bin/seed-accumulator                         # dev: dry run, every accumulating dataset
    deploy/seed-accumulator ...                  # production: same flags, in the container
    bin/seed-accumulator --only air_quality_hourly allegheny_county/jail/jail_census
    bin/seed-accumulator --only requests_311 --write
    bin/seed-accumulator --only requests_311 --write --force   # replace existing state

An accumulating dataset (`accumulate: true`, or `ingest: incremental` with
`publish: replace`) publishes `_state/.../current.parquet` — the whole table
the ETL has built up — as one CSV. On its FIRST run there is no stored table,
so the incoming file IS the table, and DataPusher+ would reload CKAN down to
whatever that one file holds: years of air-quality readings replaced by the
last few days. This script closes that gap. It downloads the resource's
current DataStore contents (`/datastore/dump/<id>`) and writes them as the
canonical table, so the first run merges onto the history instead of
replacing it.

Run it once per dataset, after its defs.yaml exists and BEFORE its first
publishing run. It refuses to overwrite existing state without `--force`:
once the pipeline has run, the stored table is the source of truth and a
reseed would throw away everything merged since.

Types follow the DataStore's own column types, because the merge only
matches a key that is the same KIND on both sides (accumulate.py refuses a
mismatch rather than silently doubling the table):

    text / unknown       -> string          (leading zeros survive)
    int2 / int4 / int8   -> integer         (nullable Int64 if any blanks)
    numeric / float*     -> float
    bool                 -> boolean         (the dump writes t / f)
    timestamp / date     -> datetime        (= the to_datetime op)

So a pipeline whose merge key is a timestamp or date must run `to_datetime`
on it, and a text id must stay text (`coerce_text`).

Reads data.wprdc.org by default, anonymously — public resources need no
token. `CKAN_READ_TOKEN` is sent if set (a private resource); `CKAN_API_TOKEN`
is sent only when `--ckan` is the portal it belongs to (`CKAN_URL`), the
same host scoping `bin/arcgis --ckan` uses.

Writes go through LandingZoneResource, so the usual S3 guard applies: a dev
run writes to LocalStack (`S3_ENDPOINT_URL`), and real S3 only in production.
It lives in the package rather than scripts/ because production has to run
it, and only src/ ships in the image (deploy/seed-accumulator).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

DEFS = Path(__file__).resolve().parent / "defs"

DEFAULT_CKAN = "https://data.wprdc.org"
INT_TYPES = {"int2", "int4", "int8", "integer", "int", "bigint", "smallint"}
FLOAT_TYPES = {"numeric", "float4", "float8", "float", "double precision", "real"}


@dataclass
class Target:
    folder: str
    publisher: str
    department: str | None
    dataset: str
    resource_id: str
    primary_key: list[str]

    @property
    def key(self) -> str:
        return "/".join(p for p in (self.publisher, self.department, self.dataset) if p)


def accumulating_targets() -> list[Target]:
    """Every defs.yaml whose pipeline accumulates, with what the seed needs.

    Mirrors components/_common.py:resolve_modes — read from the raw YAML so
    this doesn't have to build the code location.
    """
    out = []
    for path in sorted(DEFS.rglob("defs.yaml")):
        attrs = (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}
        ingest = attrs.get("ingest", "snapshot")
        publish = attrs.get("publish") or (
            "upsert" if ingest == "incremental" else "replace"
        )
        accumulate = attrs.get("accumulate")
        if accumulate is None:
            accumulate = ingest == "incremental" and publish == "replace"
        ckan = attrs.get("ckan") or {}
        if not accumulate or not ckan.get("resource_id"):
            continue
        out.append(
            Target(
                folder=path.parent.name,
                publisher=attrs["publisher"],
                department=attrs.get("department"),
                dataset=attrs["dataset"],
                resource_id=str(ckan["resource_id"]),
                primary_key=list(ckan.get("primary_key") or []),
            )
        )
    return out


def _headers(ckan_url: str) -> dict[str, str]:
    token = os.getenv("CKAN_READ_TOKEN")
    if not token:
        own = urlsplit(os.getenv("CKAN_URL", "")).hostname
        if own and own == urlsplit(ckan_url).hostname:
            token = os.getenv("CKAN_API_TOKEN")
    headers = {"User-Agent": "wprdc-etl seed-accumulator"}
    if token:
        headers["Authorization"] = token
    return headers


def datastore_info(ckan_url: str, resource_id: str) -> tuple[dict[str, str], int]:
    """(column -> DataStore type, row count) for a resource, `_id` excluded."""
    import requests

    resp = requests.get(
        f"{ckan_url}/api/3/action/datastore_search",
        params={"resource_id": resource_id, "limit": 0},
        headers=_headers(ckan_url),
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()["result"]
    fields = {f["id"]: f["type"] for f in result["fields"] if f["id"] != "_id"}
    return fields, int(result.get("total") or 0)


def download_dump(ckan_url: str, resource_id: str, dest: str) -> None:
    """Stream the resource's full DataStore table as CSV to `dest`."""
    import requests

    with requests.get(
        f"{ckan_url}/datastore/dump/{resource_id}",
        params={"format": "csv"},
        headers=_headers(ckan_url),
        stream=True,
        timeout=600,
    ) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)


def typed_frame(csv_path: str, fields: dict[str, str]) -> Any:
    """Read a DataStore dump and give each column its DataStore type."""
    import pandas as pd

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[""])
    df = df.drop(columns=["_id"], errors="ignore")
    for col, kind in fields.items():
        if col not in df.columns:
            continue
        kind = kind.lower()
        s = df[col]
        if kind in INT_TYPES:
            n = pd.to_numeric(s, errors="coerce")
            df[col] = n.astype("Int64") if n.isna().any() else n.astype("int64")
        elif kind in FLOAT_TYPES:
            df[col] = pd.to_numeric(s, errors="coerce")
        elif kind == "bool":
            df[col] = s.map({"t": True, "f": False, "true": True, "false": False})
            df[col] = df[col].astype("boolean")
        elif kind.startswith("timestamp") or kind == "date":
            df[col] = pd.to_datetime(s, errors="coerce")
        else:
            df[col] = s.astype("string")
    return df


def landing_zone() -> Any:
    """The landing zone, configured exactly as definitions.py configures it."""
    from wprdc_etl.resources import LandingZoneResource

    kwargs: dict[str, Any] = {
        "bucket": os.getenv("LANDING_BUCKET", "wprdc-etl-landing"),
        "region": os.getenv("AWS_REGION", "us-east-1"),
    }
    if os.getenv("S3_ENDPOINT_URL"):
        kwargs.update(
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            force_path_style=True,
        )
    return LandingZoneResource(**kwargs)


def seed(t: Target, args: argparse.Namespace, landing: Any) -> bool:
    """Seed one dataset. Returns False on a problem worth a non-zero exit."""
    fields, total = datastore_info(args.ckan, t.resource_id)
    existing = landing.read_table_meta(t.publisher, t.dataset, t.department)
    missing_key = [c for c in t.primary_key if c not in fields]

    print(f"\n{t.key}  (resource {t.resource_id})")
    print(f"  CKAN rows: {total:,}   columns: {len(fields)}")
    print(f"  merge key: {t.primary_key}")
    if existing:
        print(
            f"  existing state: {existing.get('rows', '?'):,} rows, "
            f"written {existing.get('updated_at', '?')}"
        )
    if missing_key:
        print(f"  ERROR: merge key column(s) {missing_key} are not in the resource")
        return False
    if existing and not args.force:
        print("  skip: state already exists (pass --force to replace it)")
        return True
    if not args.write:
        print("  dry run: would download and seed (pass --write)")
        return True

    with tempfile.TemporaryDirectory() as tmp:
        dump = os.path.join(tmp, "dump.csv")
        print("  downloading ...", flush=True)
        download_dump(args.ckan, t.resource_id, dump)
        df = typed_frame(dump, fields)

    if len(df) != total:
        print(f"  ERROR: dump has {len(df):,} rows but datastore_search says {total:,}")
        return False
    null_keys = {
        c: int(df[c].isna().sum()) for c in t.primary_key if df[c].isna().any()
    }
    if null_keys:
        # Null keys never dedupe; the pipeline would refuse this table anyway.
        print(f"  ERROR: null merge-key values in CKAN: {null_keys}")
        return False
    dupes = int(df.duplicated(subset=t.primary_key).sum())
    if dupes:
        print(f"  note: {dupes:,} duplicate merge keys in CKAN; keeping the last")
        df = df.drop_duplicates(subset=t.primary_key, keep="last").reset_index(
            drop=True
        )

    landing.write_table_state(t.publisher, t.dataset, df, department=t.department)
    print(f"  seeded {len(df):,} rows -> _state/{t.key}/current.parquet")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", nargs="+", help="dataset folder names or keys")
    ap.add_argument("--write", action="store_true", help="actually seed")
    ap.add_argument("--force", action="store_true", help="replace existing state")
    ap.add_argument("--ckan", default=DEFAULT_CKAN, help="portal to read from")
    args = ap.parse_args()
    args.ckan = args.ckan.rstrip("/")

    targets = accumulating_targets()
    if args.only:
        wanted = set(args.only)
        targets = [t for t in targets if t.folder in wanted or t.key in wanted]
        unknown = wanted - {t.folder for t in targets} - {t.key for t in targets}
        if unknown:
            print(f"not an accumulating dataset: {sorted(unknown)}", file=sys.stderr)
            return 2
    if not targets:
        print("no accumulating datasets matched")
        return 0

    landing = landing_zone()
    ok = True
    for t in targets:
        try:
            ok = seed(t, args, landing) and ok
        except Exception as e:  # report and keep going; exit non-zero at the end
            print(f"  ERROR: {e}")
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
