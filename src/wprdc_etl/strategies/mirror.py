"""Mirror an ArcGIS Hub layer's distributions onto CKAN resources.

An existing WPRDC GIS package (allegheny-county-boundary, say) carries six
resources, and every one of them comes from the publisher's `data.json`:

    ArcGIS Hub Dataset   HTML      link   -> the Hub landing page
    Esri Rest API        HTML      link   -> the GeoServices endpoint
    CSV                  CSV       table  -> owned by the DataStore publish
    GeoJSON              GeoJSON   upload
    KML                  KML       upload
    Shapefile            ZIP       upload

This module reproduces the four non-CSV ones plus the package's description
and tags. The CSV stays with `strategies/load.py`, which puts it in the
DataStore.

Why copy rather than derive: the CSV export of a polygon layer has no
geometry, only `Shape__Area` / `Shape__Length`. A GeoJSON built from the
validated frame would therefore be geometry-less for most of these layers.
See MirrorModel.
"""

from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING, Any

import dagster as dg

from wprdc_etl.runtime import sink_dir
from wprdc_etl.strategies.extract import (
    ARCGIS_FORMATS,
    arcgis_ready_url,
    is_arcgis_pending,
    resolve_arcgis_distribution,
)
from wprdc_etl.strategies.metadata import package_description, package_tags

if TYPE_CHECKING:
    from wprdc_etl.components.models import MirrorModel
    from wprdc_etl.resources import CkanResource, LandingZoneResource

# our mirror format -> (CKAN format, default resource name, is a link only)
#
# The names match what the existing packages already use, so a mirror lands on
# the resource that is already there instead of creating a near-duplicate
# beside it.
MIRROR_KINDS: dict[str, tuple[str, str, bool]] = {
    "csv": ("CSV", "CSV", False),
    "geojson": ("GeoJSON", "GeoJSON", False),
    "shapefile": ("ZIP", "Shapefile", False),
    "kml": ("KML", "KML", False),
    # The rest of the Hub's formats. Resource names match the distribution
    # titles the publisher uses, which is also what an existing package would
    # already call them.
    "file_geodatabase": ("ZIP", "File Geodatabase", False),
    "feature_collection": ("JSON", "Feature Collection", False),
    "xlsx": ("XLSX", "Excel", False),
    "geopackage": ("GPKG", "GeoPackage", False),
    "sqlite": ("SQLITE", "SQLite Geodatabase", False),
    "hub_page": ("HTML", "ArcGIS Hub Dataset", True),
    "rest_api": ("HTML", "Esri Rest API", True),
}

# The catalogue distribution a link kind points at.
_LINK_DISTRIBUTION = {
    "hub_page": "Web Page",
    "rest_api": "ArcGIS GeoServices REST API",
}


def validate_mirrors(
    mirrors: list[MirrorModel] | None, *, has_datastore_target: bool = False
) -> None:
    """Fail at load time on an unusable mirror list."""
    for m in mirrors or []:
        fmt = (m.format or "").lower()
        if fmt == "csv" and has_datastore_target:
            raise ValueError(
                "ckan.mirror lists 'csv' while ckan.resource_id is also set — "
                "that publishes the same table twice, once as the DataStore "
                "load of the validated frame and once as a file copy. Use "
                "resource_id for a frame-derived table, or mirror the csv for "
                "a faithful copy of the publisher's own export, not both"
            )
        if fmt not in MIRROR_KINDS:
            raise ValueError(
                f"unknown ckan.mirror format {m.format!r} "
                f"(expected one of {sorted(MIRROR_KINDS)})"
            )


def _link_url(entry: dict[str, Any], kind: str) -> str:
    """The URL for a link-only mirror, from the catalogue entry."""
    wanted = _LINK_DISTRIBUTION[kind]
    for dist in entry.get("distribution", []):
        if dist.get("format") == wanted:
            url = dist.get("downloadURL") or dist.get("accessURL")
            if url:
                return url
    if kind == "hub_page" and entry.get("landingPage"):
        return entry["landingPage"]
    raise dg.Failure(
        f"catalogue entry {entry.get('title')!r} has no {wanted} distribution "
        f"to point the {kind} resource at"
    )


def _resource_id(
    cfg: Any, mirror: MirrorModel, kind: tuple[str, str, bool], ckan: CkanResource
) -> str:
    """The CKAN resource to write, resolving or creating it if need be."""
    ckan_fmt, default_name, _ = kind
    if mirror.resource_id:
        return mirror.resource_id
    name = mirror.name or default_name
    if not cfg.ckan.package_id:
        raise dg.Failure(
            f"mirror {mirror.format!r} has no resource_id, so it must be "
            "created — which needs ckan.package_id"
        )
    found = ckan.find_resource(cfg.ckan.package_id, name, ckan_fmt)
    if found:
        return found
    return ckan.add_resource(cfg.ckan.package_id, name, ckan_fmt)


def publish_mirror(
    cfg: Any,
    mirror: MirrorModel,
    entry: dict[str, Any],
    *,
    ckan: CkanResource,
    landing: LandingZoneResource | None = None,
    manifest: dict[str, Any] | None = None,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """Copy one catalogue distribution onto its CKAN resource.

    When the distribution is the one this dataset landed (the geojson, for an
    arcgis source), the landed bytes are reused rather than re-fetched — see
    `_landed_copy`.
    """
    fmt = (mirror.format or "").lower()
    kind = MIRROR_KINDS[fmt]
    _, default_name, is_link = kind
    name = mirror.name or default_name
    sink = sink_dir()

    if is_link:
        url = _link_url(entry, fmt)
        if sink:
            if context:
                context.log.info(
                    f"[dry-run] would point {name!r} at {url} (skipped CKAN)"
                )
            return
        changed = ckan.publish_link(_resource_id(cfg, mirror, kind, ckan), url)
        if context:
            context.log.info(
                f"pointed {name!r} at {url}"
                if changed
                else f"unchanged: {name!r} already points at {url}"
            )
        return

    # resolve_arcgis_distribution takes the catalogue LIST, not one entry —
    # wrap it so the distribution/format matching (and the ZIP
    # Shapefile-vs-Geodatabase disambiguation) is the same code the extractor
    # uses, rather than a second implementation that could drift.
    ext = ARCGIS_FORMATS[fmt][2]
    stem = "__".join(x for x in [cfg.publisher, cfg.department, cfg.dataset] if x)

    if sink:
        os.makedirs(sink, exist_ok=True)
        out = os.path.join(sink, f"{stem}{ext}")
        via = "landed" if _landed_copy(manifest, landing, ext, out) else "fetched"
        if via == "fetched":
            _download(_distribution(entry, fmt), out)
        if context:
            extra = " + DataStore ingest" if getattr(mirror, "datastore", False) else ""
            context.log.info(f"[dry-run] wrote {out} ({via}){extra} (skipped CKAN)")
        return

    fd, tmp = tempfile.mkstemp(suffix=ext)
    os.close(fd)
    try:
        via = "landed" if _landed_copy(manifest, landing, ext, tmp) else "fetched"
        if via == "fetched":
            _download(_distribution(entry, fmt), tmp)
        resource_id = _resource_id(cfg, mirror, kind, ckan)
        changed = ckan.publish_file(resource_id, tmp, f"data{ext}")
        if context:
            context.log.info(
                f"uploaded {name!r} ({os.path.getsize(tmp)} bytes, {via})"
                if changed
                else f"unchanged: {name!r} already holds these bytes — not uploaded"
            )
        # Upload first, then make it queryable: the ingest reads the file that
        # was just put there. An unchanged file is re-ingested only if it never
        # made it into the DataStore.
        if getattr(mirror, "datastore", False) and (
            changed or not ckan.resource(resource_id).get("datastore_active")
        ):
            _ingest(fmt, resource_id, ckan=ckan, context=context)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _ingest(
    fmt: str,
    resource_id: str,
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """Make an uploaded mirror queryable, by the route its format needs.

    A geojson has to go through the spatial load endpoint, which builds the
    geometry column as part of the ingest. DataPusher+ is NOT an acceptable
    substitute there: it would land the properties as columns with no
    geometry, and nothing downstream would notice.
    """
    if fmt == "geojson":
        ckan.load_geojson_to_datastore(resource_id)
        if context:
            context.log.info(f"submitted {resource_id} for spatial DataStore load")
        return
    ckan.submit_to_datastore(resource_id)
    if context:
        context.log.info(f"submitted {resource_id} to DataPusher+")


def _distribution(entry: dict[str, Any], fmt: str) -> str:
    """The download URL for a format, waiting out a cold export."""
    url, _ = resolve_arcgis_distribution([entry], entry["title"], fmt)
    arcgis_ready_url(url)
    return url


def _landed_copy(
    manifest: dict[str, Any] | None,
    landing: LandingZoneResource | None,
    ext: str,
    dest: str,
) -> bool:
    """Copy the landed artifact to `dest` when it already IS this format.

    The point is that the published file and the PostGIS region layer come
    from the SAME bytes. Re-fetching the geojson from ArcGIS would be a
    second, independent download of a file the publisher rebuilds without
    warning — and it demonstrably does: an export readable in one run can be
    regenerating an hour later. Two fetches means CKAN could end up with a
    different export than the one `region_layer` dissolved into PostGIS, with
    every step reporting success.

    Reading the landed copy also takes the geojson out of the cold-export
    lottery entirely, since S3 always has it.

    Returns True when the landed file was used, False to fall back to the
    network (the csv / kml / shapefile mirrors, which are never landed).
    """
    filename = (manifest or {}).get("filename") or ""
    if not (manifest and landing and filename.endswith(ext)):
        return False
    key = (
        landing.prefix(
            manifest["publisher"],
            manifest["dataset"],
            manifest["partition"],
            department=manifest.get("department"),
        )
        + "/"
        + filename
    )
    landing.download(key, dest)
    return True


def _download(url: str, path: str) -> None:
    """Stream a distribution to `path`, refusing the Pending placeholder."""
    import requests

    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        first = True
        with open(path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                if first and is_arcgis_pending(chunk[:4096]):
                    raise dg.Failure(
                        f"ArcGIS returned its 'export not ready' placeholder "
                        f"for {url} — refusing to mirror it"
                    )
                first = False
                fh.write(chunk)


def sync_package_metadata(
    cfg: Any,
    entry: dict[str, Any],
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """Push the catalogue's description and tags onto the CKAN package.

    The description is the publisher's, converted from HTML to Markdown, with
    this dataset's `ckan.description_suffix` appended — so our own note about
    the pipeline survives an upstream edit instead of being overwritten by it.
    A dataset that sets `ckan.description` replaces the publisher's text
    entirely; the suffix still applies on top.

    `entry` is empty for a source with no catalogue behind it (PASDA). Tags
    are then left as they are rather than synced: `package_tags({})` is `[]`,
    and patching a package with an empty tag list CLEARS whatever a curator
    put there. Only the description, which the defs.yaml supplies itself, is
    pushed.
    """
    if not cfg.ckan.package_id:
        raise dg.Failure("ckan.sync_metadata needs ckan.package_id")

    notes = package_description(
        entry,
        cfg.ckan.description_suffix,
        override=getattr(cfg.ckan, "description", None),
    )
    tags = package_tags(entry) if entry else None

    if sink_dir():
        if context:
            context.log.info(
                f"[dry-run] would patch package {cfg.ckan.package_id}: "
                f"{len(notes)} chars of notes, "
                f"tags={'unchanged' if tags is None else tags} (skipped CKAN)"
            )
        return

    changed = ckan.patch_package(cfg.ckan.package_id, notes=notes, tags=tags)
    if context:
        if not changed:
            context.log.info(
                "unchanged: the package already has this description and tags"
            )
            return
        shown = "tags unchanged" if tags is None else f"tags {tags}"
        context.log.info(f"patched description ({len(notes)} chars), {shown}")
