#!/usr/bin/env python3
"""Generate/refresh the GIS pipelines from a publisher's ArcGIS Hub catalogue.

    bin/arcgis --list                      # catalogue vs what's wired
    bin/arcgis                             # dry run: what sync would change
    bin/arcgis --write                     # write files that don't exist yet
    bin/arcgis --write --overwrite-schema  # also regenerate existing schema.py
    bin/arcgis --write --overwrite-defs    # also regenerate existing defs.yaml
    bin/arcgis --write --force             # both overwrites
    bin/arcgis --only municipal_boundaries zip_codes
    bin/arcgis --publisher city_of_pittsburgh
    bin/arcgis --refresh                   # bypass the cached data.json

An ArcGIS Hub site publishes a DCAT catalogue at <site>/data.json describing
every layer it hosts: title, modified date, and a download URL per format. That
is the whole input here.

No URL is ever written into a defs.yaml. A Hub download URL embeds the ArcGIS
item id, which changes whenever the layer is republished, so a stored URL
breaks. What gets stored is the layer's TITLE (`source.type: arcgis`), and
`ArcGisExtractor` looks the URL up in the catalogue on every run — so a
republished layer keeps working untouched, and a RENAMED one fails loudly
instead of silently fetching nothing.

Two things are read from CKAN rather than the catalogue, both out of one
`package_show` per dataset: the resource ids each mirror publishes to, and —
for publishers flagged `portal_description` — the curated `notes` that becomes
`ckan.description`. That is why `--ckan` must stay pointed at production; a dev
portal would feed back whatever was last published to it.

Writing is per file and opt-in. Blocks the generator owns (REGION_LAYERS,
the mirror list, the description) are rebuilt from its own tables on every
`--overwrite-defs`, so they survive; anything hand-added to defs.yaml does not.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFS = REPO / "src" / "wprdc_etl" / "defs"
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from schema_infer import builder_for  # noqa: E402  (scripts/ is on sys.path)
from wprdc_etl.strategies.extract import (  # noqa: E402  (needs sys.path above)
    arcgis_ready_url,
    is_arcgis_pending,
)

# Each publisher's Hub site. `strip` are leading words removed when deriving a
# folder name, so "Allegheny County Municipal Boundaries" -> municipal_boundaries.
PUBLISHERS = {
    "allegheny_county": {
        "catalog": "https://openac-alcogis.opendata.arcgis.com/data.json",
        "strip": ("Allegheny County-Owned", "Allegheny County", "Allegheny"),
        "schedule": "0 7 * * 1",  # 07:00 America/New_York, Mondays
        # The county's ArcGIS descriptions are harvest boilerplate, so the
        # curated text already on data.wprdc.org is written into each
        # defs.yaml as `ckan.description`, replacing the publisher's.
        "portal_description": True,
        "owner_org": "allegheny-county",
    },
    "city_of_pittsburgh": {
        "catalog": "https://pghgishub-pittsburghpa.opendata.arcgis.com/data.json",
        "strip": ("City of Pittsburgh", "Pittsburgh City", "Pittsburgh"),
        "schedule": "30 7 * * 1",
        # The city's own descriptions are good; those datasets keep the
        # source text plus whatever `description_suffix` adds.
        "portal_description": False,
        "owner_org": "city-of-pittsburgh",
    },
}
DEPARTMENT = "gis"

# Catalogue titles this generator does NOT own, and who does. A layer listed
# here is reported and skipped instead of being wired, however its catalogue
# entry looks.
#
# The county's data.json is wrong about these four: each advertises an
# "ArcGIS GeoServices REST API" distribution whose URL is actually a PASDA
# HTML landing page, and none offers a downloadable file. The real data lives
# on PASDA, so the pipelines live under the `pasda` department with
# `source.type: pasda`. Without this table they fall out as "no GeoJSON
# distribution", which is the right outcome by accident — a future change to
# that check would silently wire them a second time.
#
# Add to this table whenever a layer's real source turns out to be somewhere
# other than the Hub.
CATALOGUE_EXCLUSIONS: dict[str, dict[str, str]] = {
    "allegheny_county": {
        "Allegheny County Parcel Boundaries": "pasda (dataset 1214)",
        "Allegheny County Addressing Address Points": "pasda (dataset 1219)",
        "Allegheny County Addressing Street Centerlines": "pasda (dataset 1224)",
        "Allegheny County Building Footprint Locations": "pasda (dataset 1195)",
        # On hold. A plain table with no WPRDC package of its own; its
        # FOLDER_ALIASES and PACKAGE_OVERRIDES entries are kept so removing
        # this line is all it takes to wire it as addressing_street_aliases.
        "Allegheny County Addressing Street Aliases": "nobody - on hold",
    },
    "city_of_pittsburgh": {},
}


@dataclass(frozen=True)
class RegionLayerSpec:
    """How a GIS folder maps onto an admin-region layer used for reverse geocoding.

    `name` is the string `reverse_geocode` calls the layer by, so it is
    fixed — renaming one silently breaks every dataset that resolves against
    it. The other two are PROPERTY names in the GeoJSON export, read off the
    live data rather than guessed.

    `value_field` is the region's stable identity, and it is the column
    `SpatialResource.replace_layer` DISSOLVES by: features sharing a value are
    unioned into one region. That is right when the value IS the region's
    identity (a zip split across two polygons), and badly wrong when it isn't.
    """

    name: str
    value_field: str
    label_field: str | None = None


# Folders that are also administrative-boundary layers.
REGION_LAYERS: dict[str, dict[str, RegionLayerSpec]] = {
    "allegheny_county": {
        "census_tracts_2020": RegionLayerSpec(
            name="census_tract",
            value_field="GEOID",  # 11-digit FIPS, matching the published column
            label_field="NAMELSAD",
        ),
        "council_districts": RegionLayerSpec(
            name="county_council_district",
            value_field="District",
            label_field="LABEL",
        ),
        "dpw_districts": RegionLayerSpec(
            name="county_dpw_district",
            value_field="District",
        ),
        "magisterial_districts": RegionLayerSpec(
            name="magisterial_district",
            value_field="Magisterial_District",
            # No label_field on purpose. This layer is MUNICIPALITY polygons
            # tagged with their district, so `LABEL` is the municipality name.
            # After the dissolve, aggfunc="first" would label a whole district
            # after one arbitrary municipality inside it. The value here
            # ("Magisterial District 05-2-01") already reads as a label, and
            # reverse_geocode's `value: name` default falls back to it.
        ),
        "municipal_boundaries": RegionLayerSpec(
            name="municipality",
            value_field="MUNICODE",
            label_field="LABEL",
        ),
        "school_districts": RegionLayerSpec(
            name="school_district",
            value_field="SCHOOLD",  # the district name doubles as the code
        ),
        "senate_districts": RegionLayerSpec(
            name="state_senate_district",
            value_field="LEG_DISTRI",
        ),
        "voting_districts": RegionLayerSpec(
            name="voting_district",
            # NOT `DISTRICT_1`, which the older hand-written stub used: that
            # is the district's ordinal WITHIN its municipality, and it takes
            # only 39 distinct values across 400 features. Dissolving by it
            # would union unrelated districts into 39 sprawling multipolygons,
            # and ST_Contains would then answer confidently and wrongly.
            # MWD_PAD_1 is municipality + ward + district, unique county-wide.
            value_field="MWD_PAD_1",
            label_field="Muni_War_1",  # "West Deer Dist 8"
        ),
        "zip_codes": RegionLayerSpec(
            name="zip_code",
            value_field="ZIP",
            label_field="NAME",
        ),
    },
    "city_of_pittsburgh": {
        "council_districts_2022": RegionLayerSpec(
            name="council_district",
            value_field="DIST_ID",
            label_field="DIST_NAME",  # "D8"
        ),
        "fire_zones": RegionLayerSpec(
            name="fire_zone",
            # 101 regions from 102 features, and that is correct: zone 1-14 is
            # two disjoint polygons, which the dissolve unions. Do NOT "fix"
            # this by keying on `mapbook` — its 102/102 uniqueness is a single
            # stray trailing space ('1-14 ' vs '1-14'), so it would yield two
            # regions whose ids differ invisibly.
            value_field="dist_zone",
        ),
        "neighborhoods": RegionLayerSpec(
            name="neighborhood",
            value_field="hood_no",
            label_field="hood",
            # The census block-group columns on this layer (geoid10, tractce10,
            # namelsad10) are 1:1 attributes, not a join: hood is 90/90 unique.
        ),
        "department_of_public_works_street_divisions": RegionLayerSpec(
            name="public_works_division",
            value_field="division",
        ),
        "wards": RegionLayerSpec(
            name="ward",
            value_field="ward",  # "1".."32"; no label beyond the number
        ),
        "police_zones": RegionLayerSpec(
            name="police_zone",
            value_field="zone",  # "1".."6"
        ),
    },
}

# How much of the export to read before inferring types. Big enough to be
# representative, small enough that large files don't download 500MB.
SAMPLE_BYTES = 1 << 21  # 2 MiB
SAMPLE_ROWS = 2000
# GeoJSON features are far larger than csv rows (each carries its geometry),
# so fewer of them fit in the same byte budget. Enough to infer types from.
GEOJSON_SAMPLE_FEATURES = 200


# --------------------------------------------------------------------------
# Catalogue
# --------------------------------------------------------------------------
@dataclass
class Entry:
    """One catalogue layer, paired with wherever it already lives on disk."""

    title: str
    modified: str
    identifier: str
    formats: set[str]
    # Distribution TITLES as well as formats: ZIP is both the Shapefile and
    # the File Geodatabase, so presence of "ZIP" cannot tell you which of
    # them this layer actually offers.
    dist_titles: set[str] = field(default_factory=set)
    folder: str = ""
    existing: bool = False
    ambiguous: bool = False
    package_id: str = ""
    resource_id: str = ""
    # mirror format -> CKAN resource id ("" when the resource must be created)
    mirrors: dict = field(default_factory=dict)
    # Curated portal text, when this publisher overrides the catalogue's.
    description: str = ""
    # True when no package exists for this layer on the target portal, so the
    # id was minted and `bin/seed-ckan` has to create it.
    new_package: bool = False
    # True when `description` is a stub drawn from the catalogue rather than
    # curated portal text — the rendered comment says to review it.
    description_stub: bool = False
    # True when the REST layer is a plain table (no geometry): the CSV is the
    # source and only TABLE_MIRRORS are published.
    table: bool = False
    # Set for a folder in GEOMETRY_JOINS: the frame is joined to a key layer
    # and published to `resource_id` rather than copied.
    join: "GeometryJoinSpec | None" = None
    columns: list[tuple[str, str]] = field(default_factory=list)

    @property
    def has_csv(self) -> bool:
        """Whether the catalogue offers a CSV. Only a mirror needs one now —
        the source is the GeoJSON."""
        return "CSV" in self.formats

    @property
    def has_geojson(self) -> bool:
        """Whether this layer can be wired at all.

        The GeoJSON is the source, so a layer without one cannot be landed —
        `parcels` and `street_centerlines` publish only a REST endpoint.
        """
        return "GeoJSON" in self.formats


def slugify(title: str, strip: tuple[str, ...]) -> str:
    """Derive a folder name from a catalogue title.

    `strip` drops the publisher's own prefix, so "Allegheny County Municipal
    Boundaries" becomes `municipal_boundaries` rather than
    `allegheny_county_municipal_boundaries`. Parenthesised qualifiers go too
    ("(Current)"). This is only the fallback: a title already claimed by a
    wired dataset, an alias or the legacy bridge keeps that folder instead.

    Recurses once with no prefixes if stripping leaves nothing — "Allegheny
    County" alone would otherwise slug to the empty string.
    """
    name = title.strip()
    for prefix in strip:
        if name.lower().startswith(prefix.lower()):
            name = name[len(prefix) :]
            break
    name = re.sub(r"\(.*?\)", " ", name)  # "(Current)" and friends
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or slugify(
        title, ()
    )


def claimed_packages() -> dict[str, str]:
    """package_id -> dataset folder, across EVERY wired defs.yaml.

    CKAN ids are globally unique, so two datasets sharing a package also share
    its resources — and `resource_create` then 409s with "Resource id already
    exists", which is how this surfaced. Worse, both pipelines would publish
    over each other.

    Read from the whole tree, not just this publisher: a collision across
    publishers is just as broken.
    """
    import yaml

    out: dict[str, str] = {}
    for path in sorted(DEFS.rglob("defs.yaml")):
        try:
            doc = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError:
            continue
        ckan = (doc.get("attributes") or {}).get("ckan") or {}
        if ckan.get("package_id"):
            out.setdefault(ckan["package_id"], path.parent.name)
    return out


def wired_titles(publisher: str) -> dict[str, str]:
    """title -> folder, for pipelines already pointing at this catalogue.

    The generated defs.yaml carries `source.title`, so once a dataset is wired
    its own file is the authority on which folder owns which layer. That keeps
    a hand-renamed folder from being regenerated under a derived name.
    """
    import yaml

    out: dict[str, str] = {}
    root = DEFS / publisher / DEPARTMENT
    if not root.is_dir():
        return out
    for path in sorted(root.glob("*/defs.yaml")):
        try:
            doc = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError:
            continue
        src = (doc.get("attributes") or {}).get("source") or {}
        if src.get("type") == "arcgis" and src.get("title"):
            out[src["title"]] = path.parent.name
    return out


# Folders that predate this script and that the legacy bridge below cannot
# reach — no seed referenced their schema class, so there is nothing to match
# on. Hand-maintained: without an entry here a sync creates a near-duplicate
# folder beside each (`boundary/` next to `county_boundary/`, and so on).
FOLDER_ALIASES = {
    "allegheny_county": {
        "Allegheny County Boundary": "county_boundary",
        "Landslide Pomeroy Study": "landslide_pomeroy",
        "Allegheny County Parcel Boundaries": "parcels",
        "Allegheny County Addressing Address Points": "address_points",
        "Allegheny County Addressing Street Centerlines": "street_centerlines",
        "Allegheny County Addressing Landmarks": "addressing_landmarks",
        "Allegheny County Addressing Street Aliases": "addressing_street_aliases",
    },
    "city_of_pittsburgh": {},
}

# Corrections to the legacy payload's title -> package pairing.
#   ""   the legacy id is WRONG: look the layer up on the portal by title, and
#        mint an id if nothing is found.
#   MINT ignore the portal entirely and publish to a NEW package — for a
#        package that exists but is faulty and will be retired.
# The payload paired "Street Aliases" with cd24b8f3, the Addressing Landmarks
# package. That package is itself being replaced: its link resources point at
# Street Aliases and its GeoJSON is an empty placeholder.
MINT = "mint"
PACKAGE_OVERRIDES: dict[str, dict[str, str]] = {
    "allegheny_county": {
        "Allegheny County Addressing Street Aliases": "",
        "Allegheny County Addressing Landmarks": MINT,
    },
    "city_of_pittsburgh": {},
}


@dataclass(frozen=True)
class GeometryJoinSpec:
    """Coordinates for a table that references geometry by key.

    Renders a `join_geometry` transform step and publishes the joined frame
    as the CSV DataStore resource (`ckan.resource_id`) instead of copying the
    publisher's CSV — the copy would have no coordinates.
    """

    layer: str  # the key_layer name the geometry is loaded under
    key: str  # this table's column holding the key
    key_format: str = "{}"  # str.format template mapping it onto the layer's keys


GEOMETRY_JOINS: dict[str, dict[str, GeometryJoinSpec]] = {
    "allegheny_county": {
        # Landmarks store ADDRESS_ID 450843; the address points (PASDA,
        # gis/address_points) store SSAP450843. 98.9% join; the rest reference
        # address ids that exist nowhere, not even the county's live layer.
        "addressing_landmarks": GeometryJoinSpec(
            layer="address_point", key="ADDRESS_ID", key_format="SSAP{}"
        ),
    },
    "city_of_pittsburgh": {},
}

# The mirror kinds that mean anything for a TABLE. The Hub also offers
# GeoJSON / Shapefile / KML for a table, but exported from a layer with no
# geometry they carry no shapes — a null-geometry GeoJSON is worse than none.
TABLE_MIRRORS = ("csv", "xlsx", "hub_page", "rest_api")

# The legacy rocket-etl payloads. Two things are mined from them and nothing
# else: the CKAN package_id each layer publishes to (the Hub catalogue knows
# nothing about WPRDC), and the folder names already scaffolded under gis/ —
# each scaffolded schema.py names the marshmallow class it came from, and each
# seed pairs that class with a catalogue title, so the chain maps a title to
# the folder someone already chose for it. That is what makes a re-sync fill in
# `basins/` rather than creating `basin_outlines_map/` beside it.
LEGACY_PAYLOADS = {
    "allegheny_county": REPO / "old" / "payload" / "ac" / "gis_jobs.py",
    "city_of_pittsburgh": REPO / "old" / "payload" / "pgh" / "gis_jobs.py",
}


def _legacy_seeds(publisher: str) -> list[tuple[str, str | None, str | None]]:
    r"""(title, schema class, package_id) for every seed in the payload.

    Brace-balanced on purpose. A `seeds.append({...})` body contains nested
    dicts, and a non-greedy `\{(.*?)\}\)` regex stops at the first `})` — which
    can straddle a seed boundary and pair one dataset's title with another's
    package_id. That is not hypothetical: it put Basins' CKAN resource under
    the Address Points title.

    Commented-out seeds ARE included. They describe jobs that no longer run,
    but their title and package_id are still accurate — four of the wired
    datasets (zip_codes, neighborhoods, dpwparkmaintenance and the
    congressional districts) have no live seed and their targets check out
    against the portal. Brace balancing is what makes reading them safe.
    """
    payload = LEGACY_PAYLOADS.get(publisher)
    if not payload or not payload.is_file():
        return []
    text = payload.read_text()

    out = []
    for match in re.finditer(r"seeds\.append\(\{", text):
        open_brace = match.end() - 1
        depth = 0
        close_brace = None
        for i in range(open_brace, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    close_brace = i
                    break
        if close_brace is None:
            continue
        body = text[open_brace : close_brace + 1]
        title = re.search(r"'arcgis_dataset_title':\s*'([^']+)'", body)
        cls = re.search(r"'schema':\s*(\w+)", body)
        pkg = re.search(r"'package_id':\s*'([^']+)'", body)
        if title:
            out.append(
                (
                    title.group(1),
                    cls.group(1) if cls else None,
                    pkg.group(1) if pkg else None,
                )
            )
    return out


def legacy_packages(publisher: str) -> dict[str, str]:
    """title -> CKAN package id, from the legacy payload.

    This is the only record of which CKAN dataset each ArcGIS layer publishes
    to; the Hub catalogue knows nothing about WPRDC. Without it a generated
    pipeline has no destination, and `build_defs` rejects a dataset that has
    neither `ckan` nor `region_layer`.
    """
    return {t: p for t, _, p in _legacy_seeds(publisher) if p}


def legacy_folders(publisher: str) -> dict[str, str]:
    """title -> existing folder, bridged through the legacy schema class."""
    cls_to_title = {c: t for t, c, _ in _legacy_seeds(publisher) if c}

    out: dict[str, str] = {}
    root = DEFS / publisher / DEPARTMENT
    if not root.is_dir():
        return out
    for schema in sorted(root.glob("*/schema.py")):
        m = re.search(r"marshmallow schema `(\w+)`", schema.read_text())
        if m and m.group(1) in cls_to_title:
            out[cls_to_title[m.group(1)]] = schema.parent.name
    return out


def load_catalog(publisher: str, refresh: bool) -> list[Entry]:
    """Every usable layer in a publisher's catalogue, paired with its folder.

    Folder resolution is deliberately ordered: a title already in a wired
    defs.yaml wins, then FOLDER_ALIASES, then the legacy schema-class bridge,
    then a derived slug. Anything else would rename folders out from under
    existing datasets.

    Drops the county catalogue's unrendered template rows ("{{name}}"), and
    collapses duplicate titles to the most recently modified — a pipeline
    addresses its layer BY title, so two entries sharing one is ambiguous by
    construction and resolving it by catalogue order would be arbitrary.
    """
    from wprdc_etl.strategies.extract import fetch_catalog

    cfg = PUBLISHERS[publisher]
    wired = wired_titles(publisher)
    adopted = legacy_folders(publisher)
    aliases = FOLDER_ALIASES.get(publisher, {})
    packages = legacy_packages(publisher)
    overrides = PACKAGE_OVERRIDES.get(publisher, {})

    excluded = CATALOGUE_EXCLUSIONS.get(publisher, {})

    by_title: dict[str, Entry] = {}
    for ds in fetch_catalog(cfg["catalog"], refresh=refresh):
        title = ds.get("title")
        # The county catalogue carries unrendered template rows ("{{name}}").
        if not title or "{{" in title:
            continue
        if title in excluded:
            continue
        folder = (
            wired.get(title)
            or aliases.get(title)
            or adopted.get(title)
            or slugify(title, cfg["strip"])
        )
        entry = Entry(
            title=title,
            modified=(ds.get("modified") or "")[:10],
            identifier=ds.get("identifier", ""),
            formats={d.get("format") for d in ds.get("distribution", [])},
            dist_titles={
                d.get("title") for d in ds.get("distribution", []) if d.get("title")
            },
            folder=folder,
            existing=(DEFS / publisher / DEPARTMENT / folder / "defs.yaml").exists(),
            package_id=overrides.get(title, packages.get(title, "")),
        )
        # A title can appear twice (the county publishes two "DPW Maintenance
        # Districts" entries). Since a pipeline addresses its layer BY title,
        # that is ambiguous by construction — keep the most recently modified
        # and say so, rather than let the choice depend on catalogue order.
        prior = by_title.get(title)
        if prior is None:
            by_title[title] = entry
        else:
            keep, drop = sorted((prior, entry), key=lambda e: e.modified, reverse=True)
            keep.ambiguous = True
            by_title[title] = keep
            print(
                f"  NOTE   {title!r} appears twice in the catalogue; "
                f"keeping the {keep.modified} entry, ignoring {drop.modified}"
            )

    return sorted(by_title.values(), key=lambda e: e.folder)


def _read_token(base_url: str) -> str:
    """The token to read `base_url` with, or "" to read anonymously.

    Scoped by HOST on purpose. `CKAN_API_TOKEN` is the dev token used to WRITE
    to the local portal (`CKAN_URL`), and `--ckan` defaults to production — so
    reading the token unconditionally shipped a localhost credential to
    data.wprdc.org on every lookup. It could never authenticate there, but it
    should not leave the machine either.

    So: `CKAN_API_TOKEN` is sent only to the host `CKAN_URL` names, and any
    other portal needs `CKAN_READ_TOKEN`. Reading a private production
    package is then an explicit opt-in rather than an accident of which
    variable happened to be set.
    """
    import os
    from urllib.parse import urlsplit

    def host(url: str) -> str:
        return (urlsplit(url).hostname or "").lower()

    target = host(base_url)
    own = host(os.environ.get("CKAN_URL", ""))
    if target and target == own:
        return os.environ.get("CKAN_API_TOKEN", "").strip()
    return os.environ.get("CKAN_READ_TOKEN", "").strip()


class CkanLookupFailed(RuntimeError):
    """The portal could not be asked — distinct from "no such resource".

    Conflating the two is how a transient 502 becomes a permanent wrong
    answer: the dataset gets skipped as if it had no publish target, and the
    skip line reads identically to a genuine absence.
    """


def ckan_package(base_url: str, package_id: str) -> dict[str, Any]:
    """The whole package for `package_id`, with retries.

    The whole thing rather than just its resources, because the portal's
    curated `notes` is what `ckan.description` is populated from and it comes
    back in the same response — no second call.

    Read-only — this resolves publish TARGETS at generation time; nothing here
    writes to CKAN. Retried because a sync asks ~90 times in a row and the
    portal does return the occasional 502 under that; without the retry each
    one silently costs a dataset.

    A token is sent only when one is configured FOR THIS HOST — see
    `_read_token`. Some target packages are private, and an anonymous read of
    those comes back 403 "not authorized to read package", which looks like a
    dead id but is just a missing credential.
    """
    import time

    import requests

    api_key = _read_token(base_url)
    headers = {"Authorization": api_key} if api_key else {}

    last = ""
    for attempt, wait in enumerate((0, 2, 5, 10)):
        if wait:
            time.sleep(wait)
        try:
            resp = requests.get(
                f"{base_url.rstrip('/')}/api/3/action/package_show",
                params={"id": package_id},
                headers=headers,
                timeout=60,
            )
            if resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                continue
            resp.raise_for_status()
            return resp.json()["result"]
        except Exception as exc:
            last = str(exc)[:60]
    hint = ""
    if "403" in last and not api_key:
        hint = (
            f" (403 and no token configured for {base_url} — the package may be "
            "private; set CKAN_READ_TOKEN)"
        )
    raise CkanLookupFailed(f"{package_id}: {last}{hint}")


def find_package_by_title(base_url: str, title: str) -> str | None:
    """A package on `base_url` whose title matches `title` exactly, or None.

    The legacy payload is not a complete map of what WPRDC publishes — nine
    catalogue layers already have a package it never knew about. Searching by
    title finds those, so they get wired to the existing dataset instead of
    having a second one created beside it.

    Exact, case-insensitive title equality only. `package_search` is fuzzy and
    will happily return "Allegheny County Trails Locations" for a query of
    "Allegheny County Trails"; accepting a near match would publish one
    layer's data over a different dataset.
    """
    import requests

    api_key = _read_token(base_url)
    try:
        resp = requests.get(
            f"{base_url.rstrip('/')}/api/3/action/package_search",
            params={"q": f'title:"{title}"', "rows": 5},
            headers={"Authorization": api_key} if api_key else {},
            timeout=60,
        )
        resp.raise_for_status()
        results = resp.json()["result"]["results"]
    except Exception:
        return None
    wanted = title.strip().lower()
    for pkg in results:
        if (pkg.get("title") or "").strip().lower() == wanted:
            return pkg["id"]
    return None


# Fixed namespace for minting ids for packages that do not exist yet. uuid5 of
# (namespace, catalogue title) is DETERMINISTIC: re-running the generator
# produces the same id rather than a fresh one each time, so the defs.yaml is
# stable and `bin/seed-ckan` can create the package with that exact id on any
# portal — dev now, production later.
NEW_PACKAGE_NAMESPACE = uuid.UUID("6f9b1d2e-0c7a-5f3b-9e4d-8a1c2b3d4e5f")


def minted_package_id(title: str) -> str:
    """A stable package id for a layer with no package anywhere yet."""
    return str(uuid.uuid5(NEW_PACKAGE_NAMESPACE, title.strip()))


def minted_resource_id(package_id: str, kind: str) -> str:
    """A stable id for a resource a NEW package needs before its first publish.

    Only the frame's DataStore target needs one: `ckan.resource_id` must name
    an existing resource, whereas a mirror with no id is created on first
    publish. `bin/seed-ckan` creates the resource with exactly this id.
    """
    return str(uuid.uuid5(NEW_PACKAGE_NAMESPACE, f"{package_id}/{kind}"))


def pick_resource(resources: list[dict[str, Any]], fmt: str) -> str | None:
    """The resource in `resources` matching `fmt`, preferring a DataStore one."""
    want = fmt.upper()
    for require_datastore in (True, False):
        for res in resources:
            if (res.get("format") or "").upper() != want:
                continue
            if require_datastore and not res.get("datastore_active"):
                continue
            return res["id"]
    return None


# --------------------------------------------------------------------------
# Column sampling
# --------------------------------------------------------------------------
def _iter_geojson_features(
    url: str, max_features: int, max_bytes: int
) -> list[dict[str, Any]]:
    """Pull the first features out of a GeoJSON without downloading it all.

    Reading the whole file to learn its property names does not scale: a
    street-centerline layer is hundreds of MB and a full download stalled a
    bulk re-sync for many minutes on a single dataset. Only the first handful
    of features are needed, so this streams and stops.

    It scans for balanced `{...}` objects inside the `features` array rather
    than json.loads-ing a prefix (which is never valid JSON on its own),
    tracking string state so a brace inside a name can't unbalance the count.
    """
    import requests

    features: list[dict[str, Any]] = []
    buf = bytearray()
    with requests.get(url, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=1 << 16):
            if not chunk:
                continue
            if not buf and is_arcgis_pending(bytes(chunk[:4096])):
                raise RuntimeError("ArcGIS export still pending after waiting")
            buf.extend(chunk)
            if len(buf) >= max_bytes:
                break

    text = buf.decode("utf-8", "replace")
    anchor = text.find('"features"')
    if anchor == -1:
        return []
    start = text.find("[", anchor)
    if start == -1:
        return []

    depth = 0
    in_string = False
    escaped = False
    obj_start = -1
    for i in range(start + 1, len(text)):
        ch = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start != -1:
                try:
                    features.append(json.loads(text[obj_start : i + 1]))
                except ValueError:
                    pass  # truncated tail; stop looking
                obj_start = -1
                if len(features) >= max_features:
                    break
    return features


def sample_geojson_columns(url: str) -> list[tuple[str, str]]:
    """Infer a schema from a GeoJSON export's feature properties.

    The GeoJSON is NOT the CSV with geometry bolted on — the property set
    genuinely differs. `municipal_boundaries` drops `Shape__Area` and
    `Shape__Length` (21 csv columns, 19 properties), and `county_boundary`
    calls the same field `COUNTYFIPS` where the csv says `County FIPS`. A
    CSV-derived schema fails `schema_ok` on both counts, so this samples the
    file the pipeline actually lands.

    `geometry` never appears here: it is the GeoDataFrame's geometry column,
    not a property, and pandera would demand a dtype for it.
    """
    arcgis_ready_url(url)
    features = _iter_geojson_features(url, GEOJSON_SAMPLE_FEATURES, SAMPLE_BYTES)
    if not features:
        return []

    # Union the keys across the sample: a feature whose value is null must not
    # drop the column, and property order is stable within a file.
    columns: list[str] = []
    seen: dict[str, list[str]] = {}
    for feature in features:
        for key, value in (feature.get("properties") or {}).items():
            if key not in seen:
                columns.append(key)
                seen[key] = []
            if value is not None and str(value).strip():
                seen[key].append(str(value))
    return [(c, builder_for(seen[c], c)) for c in columns]


def sample_columns(url: str) -> list[tuple[str, str]]:
    """Read the head of the CSV export and infer a schema builder per column.

    Deliberately conservative: txt() or num(), nothing invented. Tightening to
    key()/coded()/ranged() is a human job against the data dictionary — see the
    banner the generated schema carries.
    """
    import requests

    from wprdc_etl.strategies.extract import arcgis_ready_url, is_arcgis_pending

    # ArcGIS answers 200 with a "still generating" JSON placeholder until the
    # export is built. Sampling that yields a schema whose "columns" are
    # fragments of the placeholder — which is exactly what happened the first
    # time this ran, and it produced schema.py files that would not even parse.
    arcgis_ready_url(url)

    buf = io.BytesIO()
    with requests.get(url, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=1 << 16):
            buf.write(chunk)
            if buf.tell() >= SAMPLE_BYTES:
                break

    if is_arcgis_pending(buf.getvalue()[:4096]):
        raise RuntimeError("ArcGIS export still pending after waiting")

    text = buf.getvalue().decode("utf-8-sig", errors="replace")
    # A truncated download almost certainly ends mid-row; drop the last line.
    if buf.tell() >= SAMPLE_BYTES:
        text = text[: text.rfind("\n") + 1]

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return []
    columns = [c for c in reader.fieldnames if c]

    seen: dict[str, list[str]] = {c: [] for c in columns}
    for i, row in enumerate(reader):
        if i >= SAMPLE_ROWS:
            break
        for c in columns:
            v = (row.get(c) or "").strip()
            if v:
                seen[c].append(v)

    return [(c, builder_for(seen[c], c)) for c in columns]


def raw_entry(raw: list[dict[str, Any]], title: str) -> dict[str, Any] | None:
    """The catalogue entry for `title`, most recently modified first.

    Exact match, then a stripped one — the same fallback the resolver uses
    for titles published with edge whitespace.
    """
    hits = [d for d in raw if d.get("title") == title] or [
        d for d in raw if (d.get("title") or "").strip() == title.strip()
    ]
    hits.sort(key=lambda d: d.get("modified") or "", reverse=True)
    return hits[0] if hits else None


def is_table(entry: dict[str, Any] | None) -> bool | None:
    """Whether the layer behind a catalogue entry is a plain TABLE.

    Asked of the REST endpoint (`?f=json` reports `type: "Table"`), because
    nothing in the DCAT entry says so — the Hub offers a GeoJSON for a table
    too, it just has null geometry. None when there is no REST link or it
    could not be read, which callers treat as a feature layer: the behaviour
    every dataset had before tables were recognised.
    """
    import requests

    url = next(
        (
            d.get("accessURL")
            for d in (entry or {}).get("distribution", [])
            if d.get("format") == _LINK_FORMATS["rest_api"]
        ),
        None,
    )
    if not url:
        return None
    try:
        resp = requests.get(url, params={"f": "json"}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("type") == "Table"
    except (requests.RequestException, ValueError):
        return None


# The county appends this to every description. It explains the OLD harvest
# ("harvested on a weekly basis … click the Explore button"), which is false
# for a dataset this pipeline publishes, so it is dropped from stubs.
_HARVEST_BOILERPLATE = re.compile(
    r"^If viewing this description on the Western Pennsylvania Regional Data Center"
)
# Word nests bold spans, which converts to `**A:****  **B` — an empty bold
# run, then a closing `**` after whitespace, which CommonMark won't close.
_EMPTY_BOLD = re.compile(r"\*\*\*\*")
_SPACE_BEFORE_CLOSE = re.compile(r"\*\*(\S[^*\n]*?)([ \t]+)\*\*")


def stub_description(entry: dict[str, Any] | None) -> str:
    """A reviewable `ckan.description` from the catalogue's own text.

    Used where there is no curated portal text to prefer — a new package,
    or one whose notes are empty. The harvest boilerplate is
    removed and the bold runs repaired; everything else is the publisher's.
    """
    from wprdc_etl.strategies.metadata import html_to_markdown

    text = html_to_markdown((entry or {}).get("description"))
    paragraphs = [
        p for p in text.split("\n\n") if not _HARVEST_BOILERPLATE.match(p.strip())
    ]
    # Word's non-breaking spaces would keep `**A:\xa0**` from closing.
    text = "\n\n".join(paragraphs).replace("\xa0", " ")
    text = _EMPTY_BOLD.sub("", text)
    return _SPACE_BEFORE_CLOSE.sub(r"**\1**\2", text).strip()


# --------------------------------------------------------------------------
# Renderers
# --------------------------------------------------------------------------
def render_defs_yaml(publisher: str, entry: Entry) -> str:
    """The complete defs.yaml for one dataset.

    Hand-built rather than yaml.dump'd because the comments carry most of the
    explanation, and a dumper would drop them. The title goes through
    json.dumps so a quoted scalar survives: the city publishes
    "Neighborhoods " with a trailing space, and a plain YAML scalar silently
    loses it, after which the resolver cannot find the layer.
    """
    cfg = PUBLISHERS[publisher]
    return f"""type: wprdc_etl.components.tabular_pipeline.TabularPipeline

# {entry.title}
#
# GENERATED by scripts/sync_arcgis.py from the publisher's ArcGIS Hub
# catalogue. Safe to edit by hand — a re-sync leaves an existing file alone
# unless you pass --overwrite-defs.
#
# The source is addressed by catalogue TITLE, not URL: a Hub download URL
# embeds the ArcGIS item id and changes whenever the layer is republished,
# while the title is stable. `bin/arcgis --list` reports a title that has
# drifted out of the catalogue.
#
{_render_source_note(entry)}#
# Catalogue last reported this layer modified {entry.modified}.

attributes:
  publisher: {publisher}
  department: {DEPARTMENT}
  dataset: {entry.folder}
  source:
    type: arcgis
    catalog: {cfg["catalog"]}
    title: {json.dumps(entry.title)}
    format: {"csv" if entry.table else "geojson"}
  schedule: "{cfg["schedule"]}"
  partition: weekly
  ingest: snapshot
  ckan:
{_render_new_package(entry)}    package_id: "{entry.package_id}"
{_render_resource_id(entry)}    # Push the publisher's description (HTML -> Markdown) and keywords onto
    # the package. Anything of ours goes in `description_suffix`, which is
    # appended and so survives an upstream edit.
    sync_metadata: true
{_render_description(entry)}{_render_mirrors(entry)}{_render_region_layer(publisher, entry.folder)}{_render_join(entry)}"""


# our mirror name -> (CKAN format, the resource name existing packages use)
MIRRORABLE = [
    ("geojson", "GeoJSON", "GeoJSON"),
    ("csv", "CSV", "CSV"),
    ("shapefile", "ZIP", "Shapefile"),
    ("kml", "KML", "KML"),
    ("file_geodatabase", "ZIP", "File Geodatabase"),
    ("feature_collection", "JSON", "Feature Collection"),
    ("xlsx", "XLSX", "Excel"),
    ("geopackage", "GPKG", "GeoPackage"),
    ("sqlite", "SQLITE", "SQLite Geodatabase"),
    ("hub_page", "HTML", "ArcGIS Hub Dataset"),
    ("rest_api", "HTML", "Esri Rest API"),
]
# the DCAT `format` each file mirror needs present in the catalogue entry
# the DCAT `format` each file mirror needs present in the catalogue entry.
# ZIP covers two of them, so presence alone can't tell Shapefile from File
# Geodatabase — _DCAT_TITLE pins the ones that share a format.
ARCGIS_DCAT = {
    "geojson": "GeoJSON",
    "csv": "CSV",
    "shapefile": "ZIP",
    "kml": "KML",
    "file_geodatabase": "ZIP",
    "feature_collection": "TXT",
    "xlsx": "XLSX",
    "geopackage": "GPKG",
    "sqlite": "GDB",
}
# CKAN formats that more than one mirror kind uses: ZIP is Shapefile and File
# Geodatabase, HTML is the Hub page and the Esri REST link. For these the
# resource must be matched by NAME only — a format fallback would hand one
# kind the other's resource, and publishing a geodatabase over the shapefile
# resource would look entirely successful.
_SHARED_CKAN_FORMATS = {"ZIP", "HTML"}
_DCAT_TITLE = {
    "shapefile": "Shapefile",
    "file_geodatabase": "File Geodatabase",
}
# Formats where a DataStore ingest is even meaningful — links are not.
_DATASTORE_CAPABLE = {"geojson", "csv"}
_LINK_FORMATS = {
    "hub_page": "Web Page",
    "rest_api": "ArcGIS GeoServices REST API",
}


def pick_named_resource(
    resources: list[dict[str, Any]], name: str, fmt: str
) -> str | None:
    """The resource to publish this mirror to, or None to let the pipeline
    create one.

    Name first, always. The format fallback is for a resource a curator
    renamed by hand, and it is skipped entirely when the format is shared by
    two mirror kinds (see _SHARED_CKAN_FORMATS) — otherwise File Geodatabase
    picks up the Shapefile's resource, which is exactly what happened the
    first time this ran.
    """
    wanted = name.strip().lower()
    for res in resources:
        if (res.get("name") or "").strip().lower() == wanted:
            return res["id"]
    want_fmt = fmt.strip().upper()
    if want_fmt in _SHARED_CKAN_FORMATS:
        return None
    for res in resources:
        if (res.get("format") or "").strip().upper() == want_fmt:
            return res["id"]
    return None


def _render_source_note(entry: Entry) -> str:
    """The banner paragraph explaining which distribution is the source."""
    if entry.table and entry.join:
        text = f"""\
A plain TABLE, not a feature layer: its REST endpoint reports type
"Table" and there is no geometry. Each row's {entry.join.key} keys into the
{entry.join.layer} key layer, so join_geometry adds latitude/longitude from
it and the JOINED frame is published as the CSV DataStore resource. The
publisher's own CSV is not copied (it has no coordinates), and the geometry
formats the Hub offers are left out — exported from a table they are empty."""
    elif entry.table:
        text = """\
A plain TABLE, not a feature layer: its REST endpoint reports type
"Table" and there is no geometry. So the CSV is the source, and the
geometry formats the Hub also offers (GeoJSON, Shapefile, KML) are left
out — exported from a table they carry no shapes. Every CKAN resource
below is a copy of the publisher's own distribution; the csv is also
loaded into the DataStore, since it is the only form of the data."""
    else:
        text = """\
GeoJSON is the source, so the validated frame is a GeoDataFrame carrying
real geometry — the CSV export of a polygon layer has none, only
Shape__Area / Shape__Length. Every CKAN resource below is a byte-for-byte
copy of the publisher's own distribution, nothing is derived from the
frame, and so there is no `ckan.resource_id`."""
    return "".join(f"# {line}\n" for line in text.split("\n"))


def _render_resource_id(entry: Entry) -> str:
    """`ckan.resource_id`, for a dataset that publishes its joined frame."""
    if not entry.resource_id:
        return ""
    return (
        "    # The DataStore table, published from the validated + joined frame.\n"
        f'    resource_id: "{entry.resource_id}"\n'
    )


def _render_join(entry: Entry) -> str:
    """The `transforms:` block that joins a table to its key layer.

    Rendered rather than hand-added so `--overwrite-defs` keeps it. The
    derived latitude/longitude must NOT go in schema.py: that contract
    validates the raw landed file, before this runs.
    """
    if not entry.join:
        return ""
    j = entry.join
    lines = [
        f"  # Coordinates from the {j.layer} key layer (loaded by its `key_layer:`",
        "  # pipeline). Unmatched keys publish with blank coordinates and are",
        "  # counted in the run log.",
        "  transforms:",
        "    # The key column reads as float when it has a blank (450843.0);",
        "    # publish it as the id it is.",
        "    - op: coerce_text",
        f"      columns: [{j.key}]",
        "    - op: join_geometry",
        f"      layer: {j.layer}",
        f"      key: {j.key}",
    ]
    if j.key_format != "{}":
        lines.append(f"      key_format: {json.dumps(j.key_format)}")
    return "\n".join(lines) + "\n"


def _render_region_layer(publisher: str, folder: str) -> str:
    """The `region_layer:` block, for a folder that is also a boundary layer.

    Rendered by the generator rather than hand-added, so `--overwrite-defs`
    keeps it instead of wiping it.
    """
    spec = REGION_LAYERS.get(publisher, {}).get(folder)
    if spec is None:
        return ""
    lines = [
        "  # Also feeds the PostGIS admin-region store, which is what",
        "  # reverse_geocode resolves points against. Needs the geojson source:",
        "  # replace_layer requires a GeoDataFrame.",
        "  region_layer:",
        f"    name: {spec.name}",
        f"    value_field: {spec.value_field}",
    ]
    if spec.label_field:
        lines.append(f"    label_field: {spec.label_field}")
    return "\n".join(lines) + "\n"


def _render_new_package(entry: Entry) -> str:
    """Flag a package_id that nothing has created yet.

    The id is minted (uuid5 of the catalogue title) and deterministic, so this
    file is stable — but publishing 404s until `bin/seed-ckan` creates the
    package with that id. Saying so in the file beats discovering it in a run.
    """
    if not entry.new_package:
        return ""
    return (
        "    # NOT YET CREATED. This id is minted from the catalogue title and\n"
        "    # is stable, but no package carries it: run `bin/seed-ckan --write`\n"
        "    # against the target portal before publishing.\n"
    )


def _render_description(entry: Entry) -> str:
    """`ckan.description` as a YAML literal block scalar.

    `|-` (strip chomping) because the portal text often ends in blank lines
    and a trailing newline inside the scalar would round-trip differently
    every sync, making every run look like a change. CR is dropped for the
    same reason; tabs are left alone but would be unusual.
    """
    text = (entry.description or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.strip()
    if not text:
        return ""
    body = "\n".join(
        f"      {line}" if line.strip() else "" for line in text.split("\n")
    )
    if entry.description_stub:
        why = (
            "    # STUB from the publisher's catalogue description (harvest\n"
            "    # boilerplate removed). REVIEW before publishing: it replaces\n"
            "    # the package notes on every run.\n"
        )
    else:
        why = (
            "    # Replaces the publisher's description. Sourced from this\n"
            "    # dataset's curated text on data.wprdc.org.\n"
        )
    return why + "    description: |-\n" + body + "\n"


def _render_mirrors(entry: Entry) -> str:
    """The `ckan.mirror` block — every distribution this layer publishes.

    `datastore` is written false throughout, the geojson included. Ingesting
    a geojson needs the spatial load endpoint, which does not exist yet
    (`CkanResource.spatial_load_action`), so setting it true today would give
    all 70-odd datasets a permanently failing asset. The files still reach
    CKAN either way; flip this and re-sync when the endpoint lands.
    """
    if not entry.mirrors:
        return ""
    lines = ["    mirror:"]
    for fmt, resource_id in entry.mirrors.items():
        lines.append(f"      - format: {fmt}")
        # An empty id means the package has no such resource yet; the pipeline
        # creates it on first publish, which is why package_id is required.
        if resource_id:
            lines.append(f'        resource_id: "{resource_id}"')
        if fmt == "csv" and entry.table:
            lines.append("        datastore: true    # the only form of a table's data")
        elif fmt in _DATASTORE_CAPABLE:
            why = (
                "needs the spatial load endpoint (not built yet)"
                if fmt == "geojson"
                else "flip to true if a queryable table is ever wanted"
            )
            lines.append(f"        datastore: false   # {why}")
    return "\n".join(lines) + "\n"


def _schema_provenance(entry: Entry) -> list[str]:
    """The schema banner lines saying which export the columns came from."""
    if entry.table:
        return [
            "GENERATED by scripts/sync_arcgis.py by sampling the real CSV",
            f"export (first {SAMPLE_ROWS} rows / {SAMPLE_BYTES // 1024}KiB). This layer is a",
            "plain TABLE with no geometry, so the CSV is the landed file and",
            "these are its ACTUAL headers — which is what schema_ok validates,",
            "since it runs pre-transform. Nor are these the legacy rocket-etl",
            "`load_from` names: that engine lowercased every header before",
            "matching, so its spellings are not evidence of the real casing.",
        ]
    return [
        "GENERATED by scripts/sync_arcgis.py by sampling the real GeoJSON",
        f"export (first {GEOJSON_SAMPLE_FEATURES} features / {SAMPLE_BYTES // 1024}KiB), so these are the ACTUAL",
        "feature properties of the landed file — which is what schema_ok",
        "validates, since it runs pre-transform.",
        "",
        "Do NOT reconcile these against the CSV export: it is a DIFFERENT",
        "column set, not the same data without geometry. The GeoJSON of",
        "municipal_boundaries has 19 properties against the CSV's 21 columns,",
        "and county_boundary spells the same field COUNTYFIPS where the CSV",
        "says `County FIPS`. Nor are these the legacy rocket-etl `load_from`",
        "names: that engine lowercased every header before matching, so its",
        "spellings are not evidence of the real casing.",
    ]


def render_schema_py(publisher: str, entry: Entry) -> str:
    """The complete schema.py for one dataset, from its sampled columns.

    The banner is load-bearing: it records WHICH export the columns came from.
    An earlier version of this function still claimed the CSV after the
    sampler had moved to GeoJSON, which would send anyone reconciling the two
    chasing phantom mismatches.
    """
    used = sorted({b for _, b in entry.columns} | {"frame"})
    lines = [
        f'"""Pandera schema for {publisher} / {DEPARTMENT} / {entry.folder}.',
        "",
        f"{entry.title}",
        "",
        *_schema_provenance(entry),
        "",
        "Types are conservative — txt() unless every sampled value parsed as a",
        "number. Tighten by hand against the data dictionary:",
        "  * mark the identifier key()",
        "  * bound code domains with coded([...])",
        "  * ge0() / ranged(lo, hi) where the range is known",
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
    for name, builder in entry.columns:
        lines.append(f'        "{name}": {builder}(),')
    lines += ["    }", ")", ""]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------
def ensure_packages(publisher: str, folder: str, write: bool) -> None:
    """__init__.py at every level, or load_defs fails on a namespace package."""
    path = DEFS / publisher / DEPARTMENT / folder
    for d in (path.parent.parent, path.parent, path):
        if write:
            d.mkdir(parents=True, exist_ok=True)
            init = d / "__init__.py"
            if not init.exists():
                init.write_text("")


def plan_files(
    publisher: str, folder: str, overwrite_defs: bool, overwrite_schema: bool
) -> list[str]:
    """Which of this dataset's files this run is allowed to write.

    Decided PER FILE, not per dataset. A dataset whose defs.yaml exists but
    whose schema.py is missing still gets the schema written by a plain
    `--write` — the missing one is new, whatever its neighbour's state.
    """
    target = DEFS / publisher / DEPARTMENT / folder
    plan = []
    if not (target / "defs.yaml").exists() or overwrite_defs:
        plan.append("defs.yaml")
    if not (target / "schema.py").exists() or overwrite_schema:
        plan.append("schema.py")
    return plan


def sync(
    publisher: str,
    entries: list[Entry],
    *,
    write: bool,
    overwrite_defs: bool,
    overwrite_schema: bool,
    ckan_url: str,
) -> int:
    """Generate one publisher's datasets. Returns the number that failed.

    Ordering matters for cost and for honesty:

    * The file plan is computed FIRST, so a dataset with nothing to write
      never downloads anything — sampling the export is the expensive step.
    * A CKAN lookup that could not complete is a FAILURE, not a skip. A skip
      reads as a settled answer ("no target exists"), and a transient 502 or a
      403 on a private package is not that.
    * The folder/dataset name is asserted before writing. A mismatch collides
      two datasets on one asset key and breaks the whole code location, so it
      is caught here rather than in `dg dev`.

    Expect "export still pending" failures on a cold run: ArcGIS builds each
    format on first request. The request is what warms it, so a re-run clears
    most of them.
    """
    from wprdc_etl.strategies.extract import fetch_catalog, resolve_arcgis_distribution

    raw = fetch_catalog(PUBLISHERS[publisher]["catalog"])

    claimed = claimed_packages()
    created = updated = kept = skipped = failed = 0
    for e in entries:
        target = DEFS / publisher / DEPARTMENT / e.folder
        plan = plan_files(publisher, e.folder, overwrite_defs, overwrite_schema)

        # Nothing to write means nothing to download. Sampling the export is
        # the expensive part of this loop, so the plan is decided first.
        if not plan:
            kept += 1
            continue
        # A feature layer is sourced from its GeoJSON, a plain table from its
        # CSV, so that is what a dataset needs to exist at all. A layer that
        # publishes only a REST endpoint can't be landed.
        ds = raw_entry(raw, e.title)
        e.table = bool(is_table(ds))
        need = "CSV" if e.table else "GeoJSON"
        if need not in e.formats:
            print(
                f"  SKIP   {e.folder:34} no {need} distribution "
                f"({sorted(f for f in e.formats if f)})"
            )
            skipped += 1
            continue
        if e.package_id == MINT:
            e.package_id = minted_package_id(e.title)
            e.new_package = True
        if not e.package_id:
            # The legacy payload does not know this layer. It may still exist
            # on the portal under a package the payload never recorded, so
            # look it up by title before concluding it needs creating.
            found = find_package_by_title(ckan_url, e.title)
            if found:
                e.package_id = found
            else:
                e.package_id = minted_package_id(e.title)
                e.new_package = True
        if claimed.get(e.package_id, e.folder) != e.folder:
            # Another dataset already publishes to that package. Keep the
            # incumbent — it is a live publish target — and refuse to wire
            # this one rather than silently repoint either. Checked for a
            # legacy id as well as a discovered one: the payload paired
            # "Street Aliases" with the Addressing Landmarks package, which
            # is exactly how two layers came to share it.
            print(
                f"  CONFLICT {e.folder:32} package {e.package_id} is already "
                f"claimed by {claimed[e.package_id]!r} — not wiring"
            )
            skipped += 1
            continue
        if e.new_package:
            # Nothing to read: the package does not exist yet. Every resource
            # id stays empty and the pipeline creates them on first publish.
            resources: list[dict[str, Any]] = []
        else:
            try:
                package = ckan_package(ckan_url, e.package_id)
                resources = package.get("resources", [])
                if PUBLISHERS[publisher].get("portal_description"):
                    e.description = (package.get("notes") or "").strip()
            except CkanLookupFailed as exc:
                # NOT a skip: we don't know whether a target exists, so
                # failing is the honest outcome. A skip here would look like a
                # settled answer.
                print(f"  FAIL   {e.folder:34} CKAN lookup failed — {exc}")
                failed += 1
                continue
        # No curated text to prefer — a new package or empty notes — so stub
        # it from the catalogue for review.
        if PUBLISHERS[publisher].get("portal_description") and not e.description:
            e.description = stub_description(ds)
            e.description_stub = bool(e.description)
        # Mirror every distribution the layer actually offers, matched to the
        # resource already in the package where there is one. Nothing is
        # invented: a format the catalogue doesn't publish is left out.
        for fmt, ckan_fmt, res_name in MIRRORABLE:
            if e.table and fmt not in TABLE_MIRRORS:
                continue
            if fmt in ("hub_page", "rest_api"):
                if _LINK_FORMATS[fmt] not in e.formats:
                    continue
            elif fmt in _DCAT_TITLE:
                # Shares its DCAT format with another kind, so match on the
                # distribution title instead of the format.
                if _DCAT_TITLE[fmt] not in e.dist_titles:
                    continue
            elif ARCGIS_DCAT[fmt] not in e.formats:
                continue
            e.mirrors[fmt] = pick_named_resource(resources, res_name, ckan_fmt) or ""
        source_fmt = "csv" if e.table else "geojson"
        if e.folder in GEOMETRY_JOINS.get(publisher, {}):
            # The joined frame IS the published CSV, so the publisher's own
            # csv is not copied — validate_mirrors refuses both. Its resource
            # (if the package has one) becomes the DataStore target.
            existing = e.mirrors.pop("csv", "")
            e.resource_id = existing or minted_resource_id(e.package_id, "csv")
            e.join = GEOMETRY_JOINS[publisher][e.folder]
        elif source_fmt not in e.mirrors:
            print(f"  SKIP   {e.folder:34} no {source_fmt} resource to publish to")
            skipped += 1
            continue

        try:
            url, _ = resolve_arcgis_distribution(raw, e.title, source_fmt)
            e.columns = (sample_columns if e.table else sample_geojson_columns)(url)
        except Exception as exc:  # network, parse, resolve
            print(f"  FAIL   {e.folder:34} {str(exc)[:70]}")
            failed += 1
            continue
        if not e.columns:
            print(f"  FAIL   {e.folder:34} export had no header row")
            failed += 1
            continue

        claimed.setdefault(e.package_id, e.folder)
        verb = "UPDATE" if e.existing else "CREATE"
        print(
            f"  {verb:6} {e.folder:34} {len(e.columns):3} cols  "
            f"[{'+'.join(f.split('.')[0] for f in plan)}]  {e.title[:32]}"
        )
        if write:
            # The folder IS the dataset name — render_defs_yaml writes
            # `dataset: {entry.folder}` and the pipeline's asset keys derive
            # from it. A mismatch means two datasets collide on one asset key
            # and the whole code location fails to build, so assert it here
            # rather than discover it in `dg dev`.
            if target.name != e.folder:
                print(f"  FAIL   {e.folder:34} would write into {target.name}/")
                failed += 1
                continue
            ensure_packages(publisher, e.folder, write)
            if "defs.yaml" in plan:
                (target / "defs.yaml").write_text(render_defs_yaml(publisher, e))
            if "schema.py" in plan:
                (target / "schema.py").write_text(render_schema_py(publisher, e))
        if e.existing:
            updated += 1
        else:
            created += 1

    print(
        f"\n  {created} created, {updated} updated, {kept} left alone, "
        f"{skipped} skipped, {failed} failed"
    )
    return failed


def show_list(publisher: str, entries: list[Entry]) -> None:
    """Print catalogue coverage, and the drift that actually happens.

    A wired dataset whose title has vanished from the catalogue is reported as
    DRIFT — the failure the legacy `counterpart.py` existed to catch, and the
    reason addressing by title needs a watchdog at all. Writes nothing.
    """
    wired = [e for e in entries if e.existing]
    print(f"\n{publisher}  —  {PUBLISHERS[publisher]['catalog']}")
    print(f"  {len(entries)} layers in catalogue, {len(wired)} wired\n")
    for e in entries:
        mark = "wired" if e.existing else ("-" if e.has_geojson else "no geo")
        flag = " (dup title)" if e.ambiguous else ""
        print(f"  {mark:7} {e.folder:34} {e.modified:12} {e.title[:46]}{flag}")

    for title, owner in sorted(CATALOGUE_EXCLUSIONS.get(publisher, {}).items()):
        print(f"  ELSEWHERE {title[:44]:46} owned by {owner}")

    # A folder we wired whose title has since vanished is the failure mode the
    # legacy 'counterpart.py' existed to catch.
    live = {e.title for e in entries}
    for title, folder in sorted(wired_titles(publisher).items()):
        if title not in live:
            print(f"\n  DRIFT  {folder}: title {title!r} is no longer in the catalogue")


def main() -> int:
    """Parse arguments and run each publisher. Exit code is failures, not skips.

    `--write` alone never replaces an existing file; the two `--overwrite-*`
    flags opt in per file and `--force` is shorthand for both. Default is a dry
    run, which still performs every lookup and sample so the report is real.
    """
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publisher", choices=sorted(PUBLISHERS), action="append")
    ap.add_argument("--list", action="store_true", help="show coverage, change nothing")
    ap.add_argument(
        "--write",
        action="store_true",
        help="write files that don't exist yet; never replaces one that does",
    )
    ap.add_argument(
        "--overwrite-schema",
        action="store_true",
        help="also regenerate schema.py for datasets that already have one",
    )
    ap.add_argument(
        "--overwrite-defs",
        action="store_true",
        help="also regenerate defs.yaml — DISCARDS hand edits (transforms, "
        "geometry, region_layer, a corrected resource_id)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="shorthand for --overwrite-defs --overwrite-schema",
    )
    ap.add_argument("--only", nargs="+", metavar="FOLDER")
    ap.add_argument("--refresh", action="store_true", help="bypass the catalogue cache")
    ap.add_argument(
        "--ckan",
        default="https://data.wprdc.org",
        help="portal to resolve publish targets against (read-only)",
    )
    args = ap.parse_args()

    publishers = args.publisher or sorted(PUBLISHERS)
    failed = 0
    for publisher in publishers:
        entries = load_catalog(publisher, args.refresh)
        if args.only:
            entries = [e for e in entries if e.folder in set(args.only)]
        if args.list:
            show_list(publisher, entries)
            continue
        overwriting = [
            what
            for what, on in (
                ("defs.yaml", args.overwrite_defs or args.force),
                ("schema.py", args.overwrite_schema or args.force),
            )
            if on
        ]
        mode = "WRITING" if args.write else "dry run (--write to apply)"
        if overwriting:
            mode += f", REPLACING existing {' + '.join(overwriting)}"
        print(f"\n{publisher}  —  {len(entries)} layers  [{mode}]")
        failed += sync(
            publisher,
            entries,
            write=args.write,
            overwrite_defs=args.overwrite_defs or args.force,
            overwrite_schema=args.overwrite_schema or args.force,
            ckan_url=args.ckan,
        )

    if not args.list and not args.write:
        print("\nnothing written — re-run with --write")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
