"""Invariants that must hold across every defs.yaml in the tree.

These exist because `bin/arcgis` writes 70+ files at a time, and each of the
failures below actually happened during that first bulk generation. A unit
test on the generator would not have caught them: they are properties of the
written tree, not of any one function.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

DEFS = pathlib.Path(__file__).resolve().parent.parent / "src" / "wprdc_etl" / "defs"
ALL_DEFS = sorted(DEFS.rglob("defs.yaml"))


def _attrs(path: pathlib.Path) -> dict:
    return (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}


def test_there_are_defs_to_check() -> None:
    assert ALL_DEFS, "no defs.yaml found — the glob or the tree moved"


@pytest.mark.parametrize("path", ALL_DEFS, ids=lambda p: str(p.parent.name))
def test_dataset_matches_its_folder(path: pathlib.Path) -> None:
    """`dataset:` names the folder it lives in.

    Asset keys derive from publisher/department/dataset, so a mismatch makes
    two datasets collide on one key and the whole code location fails to
    build. A stray generated file put `dataset: basins` inside
    `gis/address_points/`, which is exactly this.
    """
    assert _attrs(path).get("dataset") == path.parent.name


@pytest.mark.parametrize("path", ALL_DEFS, ids=lambda p: str(p.parent.name))
def test_department_matches_its_folder(path: pathlib.Path) -> None:
    """`department:` names the folder the dataset lives under.

    The field, not the folder, is what the asset key, the S3 landing prefix
    and the asset group derive from — so moving a dataset's directory without
    editing it leaves the files in one place and the data in another, with
    nothing failing to announce it. Four PASDA layers moved from `pasda/` to
    `gis/` and kept landing under `allegheny_county/pasda/`.
    """
    expected = path.parent.parent.name
    department = _attrs(path).get("department")
    if department is None:
        # No department at all means <publisher>/<dataset>/, so the parent of
        # the parent is the publisher rather than a department folder.
        return
    assert department == expected


def test_publisher_department_dataset_are_unique() -> None:
    """The triple is the asset-key prefix, so it has to be unique."""
    seen: dict[tuple, pathlib.Path] = {}
    clashes = []
    for path in ALL_DEFS:
        a = _attrs(path)
        key = (a.get("publisher"), a.get("department"), a.get("dataset"))
        if key in seen:
            clashes.append(f"{key} in both {seen[key]} and {path}")
        seen[key] = path
    assert not clashes, "duplicate asset-key prefixes: " + "; ".join(clashes)


@pytest.mark.parametrize(
    "path",
    [p for p in ALL_DEFS if (_attrs(p).get("source") or {}).get("type") == "arcgis"],
    ids=lambda p: str(p.parent.name),
)
def test_arcgis_sources_are_addressable(path: pathlib.Path) -> None:
    """An arcgis source needs a catalogue and a title to resolve at all."""
    src = _attrs(path)["source"]
    assert src.get("catalog"), "source.catalog is required for arcgis"
    assert src.get("title"), "source.title is required for arcgis"


@pytest.mark.parametrize(
    "path",
    [p for p in ALL_DEFS if (_attrs(p).get("source") or {}).get("type") == "arcgis"],
    ids=lambda p: str(p.parent.name),
)
def test_arcgis_titles_keep_their_whitespace(path: pathlib.Path) -> None:
    """A title with stray whitespace must be quoted in the YAML.

    The city catalogue publishes "Neighborhoods " with a trailing space. An
    unquoted plain scalar silently drops it, and the resolver then can't find
    the layer. Re-reading the raw text is the point — yaml.safe_load has
    already normalised it by the time we see the value.
    """
    title = _attrs(path)["source"]["title"]
    if title != title.strip():
        raw = [
            ln
            for ln in path.read_text().splitlines()
            if ln.strip().startswith("title:")
        ]
        assert raw and (
            '"' in raw[0] or "'" in raw[0]
        ), f"{title!r} has edge whitespace but is written unquoted"


def test_every_dataset_has_a_destination() -> None:
    """`build_defs` rejects a dataset with no ckan, region_layer or key_layer,
    and it does so at load time — which means one bad file breaks every job."""
    orphans = [
        str(p.relative_to(DEFS))
        for p in ALL_DEFS
        if not any(_attrs(p).get(k) for k in ("ckan", "region_layer", "key_layer"))
    ]
    assert not orphans, f"no ckan/region_layer/key_layer destination: {orphans}"


def test_key_layer_names_are_unique() -> None:
    """One dataset per key layer — `replace_key_layer` deletes the layer
    before inserting, so two declarers would overwrite each other."""
    seen: dict[str, str] = {}
    clashes = []
    for path in ALL_DEFS:
        name = (_attrs(path).get("key_layer") or {}).get("name")
        if not name:
            continue
        where = str(path.parent.relative_to(DEFS))
        if name in seen:
            clashes.append(f"{name!r}: {seen[name]} and {where}")
        seen[name] = where
    assert not clashes, "key_layer name declared twice — " + "; ".join(clashes)


def test_every_join_geometry_layer_is_loaded_by_some_dataset() -> None:
    """A join against a key layer nothing loads fails every run, and only at
    run time — `lookup_keys` can only say so once the store is queried."""
    provided = {
        (_attrs(p).get("key_layer") or {}).get("name")
        for p in ALL_DEFS
        if _attrs(p).get("key_layer")
    }
    dangling = [
        f"{p.parent.relative_to(DEFS)} -> {step.get('layer')!r}"
        for p in ALL_DEFS
        for step in _attrs(p).get("transforms") or []
        if step.get("op") == "join_geometry" and step.get("layer") not in provided
    ]
    assert not dangling, f"join_geometry on a layer no key_layer loads: {dangling}"


def test_region_layer_names_are_unique() -> None:
    """One dataset per admin-region layer.

    `SpatialResource.replace_layer` does `DELETE FROM admin_region WHERE
    layer = %s` before inserting, so two datasets declaring the same `name`
    overwrite each other and whichever job ran last silently wins. This
    actually happened: the nine `allegheny_county/boundaries/*` stubs each
    collided with their `gis/` replacement, and `municipality` had two live
    sources racing.
    """
    seen: dict[str, str] = {}
    clashes = []
    for path in ALL_DEFS:
        region = _attrs(path).get("region_layer") or {}
        name = region.get("name")
        if not name:
            continue
        where = str(path.parent.relative_to(DEFS))
        if name in seen:
            clashes.append(f"{name!r}: {seen[name]} and {where}")
        seen[name] = where
    assert not clashes, "region_layer name declared twice — " + "; ".join(clashes)


@pytest.mark.parametrize(
    "path",
    [p for p in ALL_DEFS if _attrs(p).get("region_layer")],
    ids=lambda p: str(p.parent.name),
)
def test_region_layers_read_a_geospatial_source(path: pathlib.Path) -> None:
    """`replace_layer` requires a GeoDataFrame and fails loudly on a plain
    frame, so a boundary layer's source has to be a geospatial format. A CSV
    export of a polygon layer carries no geometry at all."""
    source = _attrs(path).get("source") or {}
    fmt = (source.get("format") or "").lower()
    url = (source.get("url") or "").lower()
    geo = fmt in ("geojson", "shapefile") or url.endswith((".geojson", ".zip"))
    assert geo, f"region_layer source is not geospatial (format={fmt!r})"


def test_ckan_ids_are_claimed_by_one_dataset_each() -> None:
    """A CKAN package or resource id belongs to exactly one dataset.

    CKAN ids are globally unique, so two datasets sharing a package share its
    resources and publish over each other. It happened: the legacy payload
    maps "Addressing Street Aliases" to the package TITLED "Addressing
    Landmarks", so the legacy mapping and title-based discovery both landed on
    it. The symptom was a 409 "Resource id already exists" from the seeder,
    four resources deep — not where the cause was.
    """
    packages: dict[str, str] = {}
    resources: dict[str, str] = {}
    clashes = []
    for path in ALL_DEFS:
        ckan = _attrs(path).get("ckan") or {}
        here = str(path.parent.relative_to(DEFS))
        pid = ckan.get("package_id")
        if pid:
            if pid in packages:
                clashes.append(f"package {pid}: {packages[pid]} and {here}")
            packages[pid] = here
        wanted = [m.get("resource_id") for m in (ckan.get("mirror") or [])]
        wanted.append(ckan.get("resource_id"))
        for rid in [r for r in wanted if r]:
            if rid in resources:
                clashes.append(f"resource {rid}: {resources[rid]} and {here}")
            resources[rid] = here
    assert not clashes, "CKAN id claimed twice — " + "; ".join(clashes)
