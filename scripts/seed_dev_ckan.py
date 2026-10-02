#!/usr/bin/env python3
"""Create the packages and resources the defs publish to, on a LOCAL CKAN.

    bin/seed-ckan                 # dry run: what is missing
    bin/seed-ckan --write         # create it
    bin/seed-ckan --only greenways municipal_boundaries

Every `defs.yaml` names its destination by production CKAN id, and a local
CKAN has none of them, so a dev publish run 404s on the first write.

A sysadmin token may set `id` explicitly on both `package_create` and
`resource_create` (verified against CKAN 2.12), so this reproduces the
production ids exactly. That matters more than convenience: dev then
exercises the same lookup path as production, instead of diverging behind a
dev-only fallback that would mask a genuine id mismatch.

It VERIFIES rather than assumes — if CKAN hands back a different id than the
one asked for, every mismatch is reported and the run exits non-zero, because
silently renumbered ids would 404 at publish time and be far harder to trace.

Needs a LOCAL sysadmin token in CKAN_API_TOKEN. It refuses to touch a
non-local portal, the same rule `guard_ckan_write` applies on the publish
path. Seeded resources are empty shells with a placeholder URL; the pipelines
fill them in.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
import time
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
DEFS = REPO / "src" / "wprdc_etl" / "defs"
sys.path.insert(0, str(REPO / "src"))

# Resource name + format per mirror kind, matching what the mirror publisher
# looks for (strategies/mirror.py MIRROR_KINDS). A seeded resource has to be
# findable by that lookup, or the pipeline creates a duplicate beside it.
MIRROR_RESOURCES = {
    "csv": ("CSV", "CSV"),
    "geojson": ("GeoJSON", "GeoJSON"),
    "shapefile": ("ZIP", "Shapefile"),
    "kml": ("KML", "KML"),
    "hub_page": ("HTML", "ArcGIS Hub Dataset"),
    "rest_api": ("HTML", "Esri Rest API"),
}
PLACEHOLDER_URL = "https://example.org/seeded-by-dev-script"


# Which CKAN organization owns a seeded package, by the data.json it came
# from. Datasets are created PRIVATE: a seeded package is an empty shell until
# its pipeline runs, and an empty public dataset on a portal is worse than no
# dataset.
OWNER_ORGS = {
    "allegheny_county": "allegheny-county",
    "city_of_pittsburgh": "city-of-pittsburgh",
}


@dataclass
class Target:
    """One package and the resources a dataset publishes into it."""

    dataset: str
    package_id: str
    title: str
    publisher: str = ""
    notes: str = ""
    # ckan.package_name, when the defs choose the URL name.
    name: str = ""
    # (resource_id, ckan_format, resource_name)
    resources: list[tuple[str, str, str]] = field(default_factory=list)


def discover() -> list[Target]:
    out: list[Target] = []
    for path in sorted(DEFS.rglob("defs.yaml")):
        attrs = (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}
        ckan = attrs.get("ckan") or {}
        if not ckan:
            continue
        pid = ckan.get("package_id") or ""
        title = _title(path, attrs)
        target = Target(
            dataset=path.parent.name,
            package_id=pid,
            title=(ckan.get("package_title") or "").strip() or title,
            publisher=attrs.get("publisher") or "",
            notes=(ckan.get("description") or "").strip(),
            name=(ckan.get("package_name") or "").strip(),
        )
        # The frame's DataStore target, when the dataset has one.
        if ckan.get("resource_id"):
            target.resources.append((ckan["resource_id"], "CSV", "CSV"))
        for mirror in ckan.get("mirror") or []:
            rid = mirror.get("resource_id")
            fmt = (mirror.get("format") or "").lower()
            if not rid or fmt not in MIRROR_RESOURCES:
                continue
            ckan_fmt, default_name = MIRROR_RESOURCES[fmt]
            target.resources.append((rid, ckan_fmt, mirror.get("name") or default_name))
        # Files built FROM the frame (`representations:` — geojson /
        # shapefile) publish to a resource id that has to exist too; missing
        # it, the publish step 404s. They sit at the top level, not under ckan.
        for rep in attrs.get("representations") or []:
            rid = rep.get("resource_id")
            fmt = (rep.get("format") or "").lower()
            if not rid or fmt not in MIRROR_RESOURCES:
                continue
            target.resources.append((rid, *MIRROR_RESOURCES[fmt]))
        out.append(target)
    return out


def _title(path: Path, attrs: dict) -> str:
    """A human title: the catalogue title if there is one, else the folder."""
    src = attrs.get("source") or {}
    return src.get("title") or path.parent.name.replace("_", " ").title()


def _is_uuid(value: str) -> bool:
    import re

    return bool(re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", value, re.I))


def _ckan_name(target: Target) -> str:
    """A url-safe CKAN `name` for a seeded package."""
    import re

    if target.name:
        return target.name
    base = re.sub(r"[^a-z0-9]+", "-", target.title.strip().lower()).strip("-")
    return (base or target.dataset)[:90]


class Ckan:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def call(self, action: str, payload: dict) -> dict:
        """POST an action, retrying the failures a dev CKAN actually produces.

        Seeding is ~525 writes back to back and that is enough to tip a
        single-container stack over: CKAN indexes every package into Solr
        synchronously, and a Solr that falls behind (or restarts) answers
        `Search Index Error` / refuses the connection. Those are transient, so
        they are retried with backoff instead of aborting a half-finished run.
        A genuine rejection — bad payload, duplicate id, no permission — is
        not retried.
        """
        import time

        import requests

        last = ""
        for wait in (0, 3, 8, 20, 30):
            if wait:
                time.sleep(wait)
            try:
                resp = requests.post(
                    f"{self.base_url}/api/3/action/{action}",
                    headers={"Authorization": self.api_key},
                    json=payload,
                    timeout=120,
                )
            except requests.RequestException as exc:  # CKAN itself restarting
                last = f"connection: {str(exc)[:90]}"
                continue
            body = resp.json() if resp.content else {}
            if body.get("success"):
                return body["result"]
            err = str(body.get("error") or resp.text[:200])
            last = f"{resp.status_code}: {err[:160]}"
            transient = resp.status_code >= 500 or "Search Index Error" in err
            if not transient:
                raise RuntimeError(f"{action} failed ({last})")
        raise RuntimeError(f"{action} failed after retries ({last})")

    def exists(self, action: str, ident: str) -> dict | None:
        import requests

        resp = requests.get(
            f"{self.base_url}/api/3/action/{action}",
            headers={"Authorization": self.api_key},
            params={"id": ident},
            timeout=60,
        )
        if resp.status_code == 200 and resp.json().get("success"):
            return resp.json()["result"]
        return None


def ensure_org(ckan: Ckan, org: str) -> None:
    """Create `org` if the portal lacks it. Idempotent.

    A fresh dev CKAN has none of the publisher organizations — this one had
    `city-of-pittsburgh` but not `allegheny-county` — and package_create fails
    without an owner. Creating it is safe here because the script refuses any
    non-local portal.
    """
    if ckan.exists("organization_show", org):
        return
    ckan.call(
        "organization_create", {"name": org, "title": org.replace("-", " ").title()}
    )
    print(f"  ORG    created {org!r}")


def guard_local(base_url: str) -> None:
    """Refuse a non-local portal. Seeding writes packages; pointing this at a
    real portal would litter it with empty shells."""
    if not any(h in base_url for h in ("localhost", "127.0.0.1", "://ckan")):
        raise SystemExit(
            f"refusing to seed {base_url} — this creates packages, and it is "
            "only meant for a local dev CKAN. Set CKAN_URL to localhost."
        )


def seed(ckan: Ckan, targets: list[Target], owner_org: str | None, write: bool) -> int:
    created_pkg = created_res = had_pkg = had_res = 0
    mismatched: list[str] = []

    for t in targets:
        if not t.package_id or t.package_id == "REPLACE_ME":
            print(f"  SKIP   {t.dataset:34} no package_id")
            continue

        pkg = ckan.exists("package_show", t.package_id)
        if pkg:
            had_pkg += 1
        else:
            print(f"  PKG    {t.dataset:34} {t.package_id}  {t.title[:34]}")
            if write:
                org = owner_org or OWNER_ORGS.get(t.publisher) or "wprdc"
                ensure_org(ckan, org)
                payload = {
                    "name": _ckan_name(t),
                    "title": t.title,
                    "owner_org": org,
                    # The dataset's own curated text when it has some,
                    # otherwise say plainly where the shell came from.
                    "notes": t.notes
                    or "Seeded by scripts/seed_dev_ckan.py — no content yet.",
                    # PRIVATE: a seeded package holds nothing until its
                    # pipeline runs, and an empty public dataset is worse on a
                    # portal than no dataset. Publish it deliberately.
                    "private": True,
                }
                # A sysadmin token may choose the id, so the production id is
                # reproduced exactly and the defs need no translation. A
                # package_id that is already a NAME rather than a uuid
                # (greenways was, briefly) becomes the name instead.
                if _is_uuid(t.package_id):
                    payload["id"] = t.package_id
                else:
                    payload["name"] = t.package_id
                got = ckan.call("package_create", payload)
                ident = got["id"] if _is_uuid(t.package_id) else got["name"]
                if ident != t.package_id:
                    mismatched.append(
                        f"{t.dataset}: asked for {t.package_id}, CKAN gave {ident}"
                    )
                pkg = got
            created_pkg += 1
            # Breathe. CKAN reindexes synchronously and a burst of creates is
            # what knocked Solr over the first time this ran.
            time.sleep(0.2)

        existing_ids = {r["id"] for r in (pkg or {}).get("resources", [])}
        for rid, fmt, name in t.resources:
            if rid in existing_ids:
                had_res += 1
                continue
            print(f"  RES    {t.dataset:34} {fmt:8} {name[:22]:24} {rid}")
            if write and pkg:
                got = ckan.call(
                    "resource_create",
                    {
                        "package_id": pkg["id"],
                        "id": rid,
                        "name": name,
                        "format": fmt,
                        "url": PLACEHOLDER_URL,
                    },
                )
                if got["id"] != rid:
                    mismatched.append(
                        f"{t.dataset}/{name}: asked for {rid}, CKAN gave {got['id']}"
                    )
            created_res += 1

    verb = "created" if write else "would create"
    print(
        f"\n  {verb}: {created_pkg} package(s), {created_res} resource(s)"
        f"   already present: {had_pkg} package(s), {had_res} resource(s)"
    )
    if mismatched:
        print("\n  CKAN DID NOT HONOUR THE REQUESTED IDS:")
        for m in mismatched:
            print(f"    {m}")
        print(
            "\n  The defs point at the ids it refused, so publishing would still\n"
            "  404. Setting ids explicitly needs a sysadmin token."
        )
        return 1
    if not write:
        print("\n  nothing written — re-run with --write")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--write", action="store_true", help="actually create things")
    ap.add_argument("--only", nargs="+", metavar="DATASET")
    ap.add_argument(
        "--owner-org",
        default=os.environ.get("CKAN_DEV_ORG"),
        help="override the owner org; by default it follows the dataset's "
        "publisher (allegheny-county / city-of-pittsburgh)",
    )
    args = ap.parse_args()

    base_url = os.environ.get("CKAN_URL", "http://localhost:5001")
    guard_local(base_url)
    api_key = os.environ.get("CKAN_API_TOKEN", "").strip()
    if not api_key:
        raise SystemExit(
            "CKAN_API_TOKEN is not set. Seeding creates packages and resources, "
            "which needs a LOCAL dev sysadmin token in .env."
        )

    targets = discover()
    if args.only:
        wanted = set(args.only)
        targets = [t for t in targets if t.dataset in wanted]

    ckan = Ckan(base_url, api_key)

    mode = "WRITING" if args.write else "dry run"
    print(f"\n{base_url}  —  {len(targets)} dataset(s)  [{mode}]")
    return seed(ckan, targets, args.owner_org, args.write)


if __name__ == "__main__":
    raise SystemExit(main())
