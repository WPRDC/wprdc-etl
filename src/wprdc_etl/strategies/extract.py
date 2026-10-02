"""Extract strategies: how raw data is acquired and written to the landing zone.

Selected by `source.type` in each component config. An extractor lands the raw
artifact(s) + manifest and returns the manifest dict. Adding a new source type
is a new subclass registered in EXTRACTORS — not a new pipeline.

SftpFileExtractor, HttpFileExtractor and ArcGisExtractor are implemented.
ApiBulkExtractor is
still a stub, registered so the dispatch seam exists and `dg check` sees the
full surface; it gets filled in a later build step.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from typing import TYPE_CHECKING, Any

import dagster as dg

from wprdc_etl.dataset_modules import load_dataset_module

if TYPE_CHECKING:
    from wprdc_etl.components.models import PipelineConfig
    from wprdc_etl.resources import LandingZoneResource, SFTPResource


class Extractor:
    """Base strategy. Concrete extractors override extract().

    Resources are passed by keyword so each extractor takes only what it needs
    (SFTP needs `sftp`; future HTTP/API extractors will take an http client).
    """

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError


class SftpFileExtractor(Extractor):
    """Pull the newest file matching a glob from an SFTP server and land it."""

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        partition = context.partition_key if context.has_partition_key else "current"
        src = cfg.source

        if not src.host:
            # Hostnames are deployment config, so a defs.yaml typically resolves
            # this from a publisher-level env var with an empty default rather
            # than committing the real server. Empty means that var isn't set —
            # catch it here instead of handing "" to paramiko.
            raise dg.Failure(
                "source.host is empty — it resolves from the environment; set "
                "the var named in this dataset's defs.yaml source block"
            )
        if not src.secret_ref:
            raise dg.Failure("source.secret_ref is required for sftp sources")
        secret = os.environ.get(src.secret_ref)
        if not secret:
            raise dg.Failure(f"env var {src.secret_ref} not set")
        user, _, password = secret.partition(":")  # expected "user:password"
        port = src.port or 22

        matches = sftp.list_matching(src.host, user, password, src.path, port=port)
        if not matches:
            raise dg.Failure(f"no files matched {src.path} on {src.host}")

        newest = max(matches, key=lambda m: m["mtime"])

        # Land under the source's real extension (data.csv, data.tif, ...) so
        # blobs keep their type and downstream can find the object via the
        # manifest rather than assuming .csv.
        ext = os.path.splitext(newest["path"])[1] or ".csv"
        filename = f"data{ext}"

        # Stream to a temp file and upload from disk, so the file (which can be
        # hundreds of MB) is never held fully in memory.
        fd, tmp = tempfile.mkstemp(suffix=ext)
        os.close(fd)
        try:
            sftp.fetch_to_file(src.host, user, password, newest["path"], tmp, port=port)
            manifest = landing.land_file(
                cfg.publisher,
                cfg.dataset,
                partition,
                tmp,
                source_meta=newest,
                department=cfg.department,
                filename=filename,
            )
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

        context.add_output_metadata(
            {
                "sha256": manifest["sha256"],
                "size": manifest["size"],
                "skipped": manifest["skipped"],
                "remote_path": newest["path"],
                "filename": filename,
            }
        )
        return manifest


class HttpFileExtractor(Extractor):
    """Download a file from an HTTP(S) URL and land it.

    Streams the response body straight to a temp file (never held fully in
    memory), then hands it to the landing zone, which is checksum-idempotent —
    an unchanged file is not re-landed. The URL comes from source.url (source.path
    is accepted as a fallback). Optional HTTP Basic auth: point the source's
    secret_ref at an env var holding "user:password".
    """

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        import requests

        partition = context.partition_key if context.has_partition_key else "current"
        src = cfg.source

        url = src.url or src.path
        if not url:
            raise dg.Failure("source.url is required for http sources")

        auth = _basic_auth(src.secret_ref)
        return _download_and_land(
            context, cfg, landing=landing, url=url, partition=partition, auth=auth
        )


def _basic_auth(secret_ref: str | None) -> tuple[str, str] | None:
    """Resolve an optional HTTP Basic credential from an env var."""
    if not secret_ref:
        return None
    secret = os.environ.get(secret_ref)
    if not secret:
        raise dg.Failure(f"env var {secret_ref} not set")
    user, _, password = secret.partition(":")  # expected "user:password"
    return (user, password)


def _download_and_land(
    context: dg.AssetExecutionContext,
    cfg: PipelineConfig,
    *,
    landing: LandingZoneResource,
    url: str,
    partition: str,
    auth: tuple[str, str] | None = None,
    ext: str | None = None,
    extra_meta: dict[str, Any] | None = None,
    reject_pending: bool = False,
) -> dict[str, Any]:
    """Stream a URL to the landing zone and return its manifest.

    Shared by the http and arcgis extractors. The body is streamed to a temp
    file, never held in memory, and the landing zone is checksum-idempotent —
    an unchanged file is not re-landed.

    `ext` overrides the extension guessed from the URL, which matters for
    arcgis: its download endpoints end in `/geojson?layers=0`, so there is no
    extension in the path to infer the reader from.
    """
    import requests

    # Land under the source's real extension so blobs keep their type and
    # downstream dispatches the reader off the manifest filename (not .csv).
    ext = ext or os.path.splitext(url.split("?", 1)[0])[1] or ".csv"
    filename = f"data{ext}"

    fd, tmp = tempfile.mkstemp(suffix=ext)
    os.close(fd)
    try:
        with requests.get(url, stream=True, timeout=120, auth=auth) as resp:
            resp.raise_for_status()
            first = True
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    # Never let an ArcGIS "Pending" placeholder reach the
                    # landing zone: it is a 200 with a plausible-looking body,
                    # and once landed it validates as a one-row dataset.
                    if first and reject_pending and is_arcgis_pending(chunk[:4096]):
                        raise dg.Failure(
                            f"ArcGIS returned its 'export not ready' placeholder "
                            f"for {url} — refusing to land it"
                        )
                    first = False
                    fh.write(chunk)
            headers = resp.headers

        # HTTP caching headers are the http analogue of SFTP's mtime/size —
        # recorded for audit and drift, though idempotency keys off the
        # content checksum, not these.
        source_meta = {
            "url": url,
            "etag": headers.get("ETag"),
            "last_modified": headers.get("Last-Modified"),
            "content_length": headers.get("Content-Length"),
            "content_type": headers.get("Content-Type"),
            **(extra_meta or {}),
        }
        manifest = landing.land_file(
            cfg.publisher,
            cfg.dataset,
            partition,
            tmp,
            source_meta=source_meta,
            department=cfg.department,
            filename=filename,
        )
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)

    context.add_output_metadata(
        {
            "sha256": manifest["sha256"],
            "size": manifest["size"],
            "skipped": manifest["skipped"],
            "url": url,
            "filename": filename,
            "last_modified": source_meta["last_modified"] or "",
        }
    )
    return manifest


# ArcGIS generates an export asynchronously. Until it is ready the download
# endpoint answers **HTTP 200** with a small JSON placeholder — so
# raise_for_status() sees nothing wrong and a naive client lands the
# placeholder as the dataset. Detecting this is not optional.
ARCGIS_PENDING_WAITS = (5, 10, 20, 30, 30)


def is_arcgis_pending(head: bytes) -> bool:
    """True when `head` is ArcGIS's "not generated yet" placeholder.

    Keyed on the JSON body rather than the content type, because a GeoJSON
    export is legitimately application/json too. A real GeoJSON opens with
    "type": "FeatureCollection", never with a bare "status": "Pending".
    """
    if not head.lstrip().startswith(b"{"):
        return False
    try:
        doc = json.loads(head.decode("utf-8", "replace"))
    except (ValueError, UnicodeDecodeError):
        return False
    return isinstance(doc, dict) and doc.get("status") in ("Pending", "InProgress")


def arcgis_ready_url(url: str, *, sleep: Any = None) -> None:
    """Block until `url` serves real content, or raise.

    ArcGIS starts generating the export when it is first requested, so the
    first call can legitimately come back Pending; the retries give it time.
    """
    import time

    import requests

    sleep = sleep or time.sleep
    for attempt, wait in enumerate((0, *ARCGIS_PENDING_WAITS)):
        if wait:
            sleep(wait)
        resp = requests.get(url, stream=True, timeout=120)
        try:
            resp.raise_for_status()
            head = next(resp.iter_content(chunk_size=4096), b"")
        finally:
            resp.close()
        if not is_arcgis_pending(head):
            return
    raise dg.Failure(
        f"ArcGIS is still generating this export after "
        f"{sum(ARCGIS_PENDING_WAITS)}s ({url}). It answers 200 with a "
        "'Pending' placeholder until the file is built — re-run shortly."
    )


# Per-process cache of fetched data.json documents. A publisher's catalogue is
# hundreds of KB and every one of its datasets wants the same copy; the legacy
# rocket-etl cached it per module for exactly this reason.
_CATALOG_CACHE: dict[str, list[dict[str, Any]]] = {}


def fetch_catalog(url: str, *, refresh: bool = False) -> list[dict[str, Any]]:
    """Return the `dataset` array of an ArcGIS Hub DCAT catalogue."""
    if refresh or url not in _CATALOG_CACHE:
        import requests

        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        _CATALOG_CACHE[url] = resp.json().get("dataset", [])
    return _CATALOG_CACHE[url]


# our name -> (DCAT format, distribution title, landed extension).
#
# The title matters because `format` alone is ambiguous: an ArcGIS entry
# carries TWO "ZIP" distributions, the Shapefile and the File Geodatabase. The
# extension matters because the reader downstream dispatches on it.
ARCGIS_FORMATS = {
    "csv": ("CSV", "CSV", ".csv"),
    "geojson": ("GeoJSON", "GeoJSON", ".geojson"),
    "shapefile": ("ZIP", "Shapefile", ".zip"),
    "kml": ("KML", "KML", ".kml"),
    # The rest of what a Hub site offers. Extensions are what the bytes
    # actually are, confirmed by magic number rather than by the DCAT
    # `format` — which lies twice here: GDB is a SQLite database, not a
    # geodatabase directory, and TXT is JSON.
    "file_geodatabase": ("ZIP", "File Geodatabase", ".zip"),
    "feature_collection": ("TXT", "Feature Collection", ".json"),
    "xlsx": ("XLSX", "Excel", ".xlsx"),
    "geopackage": ("GPKG", "GeoPackage", ".gpkg"),
    "sqlite": ("GDB", "SQLite Geodatabase", ".sqlite"),
}


def _distribution_url(dist: dict[str, Any]) -> str | None:
    """These catalogues put the link in accessURL, not downloadURL (every
    entry in both the county and city sites, as of this writing). Accept
    either — DCAT allows both and other Hub sites do use downloadURL."""
    return dist.get("downloadURL") or dist.get("accessURL")


def resolve_arcgis_distribution(
    catalog: list[dict[str, Any]], title: str, fmt: str
) -> tuple[str, dict[str, Any]]:
    """Find `title` in the catalogue and return (download url, provenance).

    Raises a Failure naming close matches when the title is gone — an ArcGIS
    site renames layers, and a silent 404 much later is far worse than a loud
    "this title is no longer published" here.
    """
    try:
        dcat_format, dist_title, _ = ARCGIS_FORMATS[fmt]
    except KeyError:
        raise dg.Failure(
            f"source.format {fmt!r} is not one of {sorted(ARCGIS_FORMATS)}"
        )

    matches = [d for d in catalog if d.get("title") == title]
    if not matches:
        # Some catalogue titles carry stray whitespace ("Neighborhoods " on
        # the city site). Requiring an exact match would mean the job breaks
        # the day the publisher tidies the title — and a plain YAML scalar
        # can't even hold the trailing space. Difference in surrounding
        # whitespace is not a different dataset.
        wanted = title.strip()
        matches = [d for d in catalog if (d.get("title") or "").strip() == wanted]
    # A Hub site can publish two entries under one title (the county has two
    # "DPW Maintenance Districts"). Taking catalogue order would make the
    # resolved layer depend on how the site happened to serialise its JSON, so
    # take the most recently modified and record that it was ambiguous.
    matches.sort(key=lambda d: d.get("modified") or "", reverse=True)
    entry = matches[0] if matches else None
    if entry is None:
        lowered = title.lower()
        near = [
            d["title"]
            for d in catalog
            if d.get("title") and _overlaps(lowered, d["title"].lower())
        ]
        hint = f" Close titles: {near[:5]}" if near else ""
        raise dg.Failure(
            f"no dataset titled {title!r} in the catalogue "
            f"({len(catalog)} entries).{hint}"
        )

    candidates = [
        d
        for d in entry.get("distribution", [])
        if d.get("format") == dcat_format and _distribution_url(d)
    ]
    # Prefer the exact distribution title, so "shapefile" can't pick up the
    # File Geodatabase that shares its ZIP format.
    dist = next((d for d in candidates if d.get("title") == dist_title), None)
    # No format-only fallback where the format is shared: ZIP is both the
    # Shapefile and the File Geodatabase, and picking the wrong one would
    # publish a geodatabase as a shapefile and look fine.
    if dist is None and dcat_format not in ("ZIP",):
        dist = next(iter(candidates), None)
    if dist is None:
        have = sorted(
            f"{d.get('format')}/{d.get('title')}" for d in entry.get("distribution", [])
        )
        raise dg.Failure(
            f"{title!r} publishes no {dcat_format}/{dist_title} download; "
            f"it offers {have}"
        )

    return _distribution_url(dist), {
        "arcgis_title": title,
        "arcgis_ambiguous_titles": len(matches) if len(matches) > 1 else None,
        "arcgis_identifier": entry.get("identifier"),
        "arcgis_modified": entry.get("modified"),
        "arcgis_landing_page": entry.get("landingPage"),
        "arcgis_format": dcat_format,
    }


def _overlaps(a: str, b: str) -> bool:
    """Cheap fuzzy match for the not-found hint — shared significant words."""
    stop = {"the", "of", "and", "county", "city", "allegheny", "pittsburgh"}
    wa = {w for w in a.split() if w not in stop and len(w) > 3}
    wb = {w for w in b.split() if w not in stop and len(w) > 3}
    return bool(wa & wb)


class ArcGisExtractor(Extractor):
    """Land one layer from an ArcGIS Hub site, addressed by catalogue title.

    `source.catalog` is the site's data.json, `source.title` the layer, and
    `source.format` which distribution to take (csv by default, geojson when
    geometry is wanted). The download URL is resolved per run, so a republished
    layer — which gets a new ArcGIS item id and therefore a new URL — keeps
    working without a config change.

    The catalogue's `modified` timestamp is recorded in the manifest. Landing
    is already checksum-idempotent, so this is provenance rather than a gate.
    """

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        partition = context.partition_key if context.has_partition_key else "current"
        src = cfg.source

        if not src.catalog:
            raise dg.Failure("source.catalog (the site's data.json) is required")
        if not src.title:
            raise dg.Failure("source.title (the layer's catalogue title) is required")

        fmt = (src.format or "csv").lower()
        catalog = fetch_catalog(src.catalog)
        url, provenance = resolve_arcgis_distribution(catalog, src.title, fmt)
        provenance["arcgis_catalog"] = src.catalog

        context.log.info(
            f"arcgis: {src.title!r} [{fmt}] -> {url} "
            f"(modified {provenance.get('arcgis_modified')})"
        )
        arcgis_ready_url(url)
        return _download_and_land(
            context,
            cfg,
            landing=landing,
            url=url,
            partition=partition,
            auth=_basic_auth(src.secret_ref),
            ext=ARCGIS_FORMATS[fmt][2],
            extra_meta=provenance,
            reject_pending=True,
        )


# PASDA landing pages offer the same layer in several formats, each under its
# own directory, and each filename carries its own release date — the CSV and
# the shapefile are routinely a month apart. our name -> (href pattern,
# landed extension).
PASDA_FORMATS = {
    "shapefile": (re.compile(r"/download/.*\.zip$", re.I), ".zip"),
    "csv": (re.compile(r"/spreadsheet/.*\.csv$", re.I), ".csv"),
    "kmz": (re.compile(r"/kmz/.*\.kmz$", re.I), ".kmz"),
}
PASDA_SUMMARY = "https://www.pasda.psu.edu/uci/DataSummary.aspx"


def resolve_pasda_download(dataset_id: str, fmt: str) -> tuple[str, dict[str, Any]]:
    """Find the current download URL for a PASDA dataset, with provenance.

    Scraped rather than read from an API because PASDA publishes no catalogue
    equivalent to a DCAT data.json. The landing page is the only index, and
    the dataset id is the stable handle: filenames change with every release.
    """
    import re as _re
    from urllib.parse import urljoin

    import requests

    try:
        pattern, ext = PASDA_FORMATS[fmt]
    except KeyError:
        raise dg.Failure(f"source.format {fmt!r} is not one of {sorted(PASDA_FORMATS)}")

    page = f"{PASDA_SUMMARY}?dataset={dataset_id}"
    resp = requests.get(
        page, timeout=120, headers={"User-Agent": "wprdc-etl (civic data ETL)"}
    )
    resp.raise_for_status()
    hrefs = _re.findall(r'href="([^"]+)"', resp.text, _re.I)
    match = next((h for h in hrefs if pattern.search(h)), None)
    if match is None:
        offered = sorted(
            name
            for name, (pat, _) in PASDA_FORMATS.items()
            if any(pat.search(h) for h in hrefs)
        )
        raise dg.Failure(
            f"PASDA dataset {dataset_id} offers no {fmt} download "
            f"(found: {offered or 'nothing recognisable'}). See {page}"
        )
    url = urljoin(page, match)
    return url, {
        "pasda_dataset_id": str(dataset_id),
        "pasda_landing_page": page,
        # The filename IS the release marker — there is no modified date in
        # the page, so this is what identifies which vintage was landed.
        "pasda_filename": url.rsplit("/", 1)[-1],
        "pasda_format": fmt,
    }


class PasdaExtractor(Extractor):
    """Land one PASDA layer, addressed by its dataset id.

    `source.dataset_id` is the PASDA dataset, `source.format` which download
    to take (shapefile by default — it is the only format all of these offer,
    and the only one carrying geometry).

    Why not the REST endpoints: the PASDA-hosted layers do expose queryable
    MapServer services, but a full snapshot of parcels is 294 paged requests
    against one 110MB download of the same data. The file wins for a weekly
    full refresh.
    """

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        partition = context.partition_key if context.has_partition_key else "current"
        src = cfg.source
        if not src.dataset_id:
            raise dg.Failure("source.dataset_id is required for pasda sources")

        fmt = (src.format or "shapefile").lower()
        url, provenance = resolve_pasda_download(str(src.dataset_id), fmt)
        context.log.info(f"pasda: dataset {src.dataset_id} [{fmt}] -> {url}")
        return _download_and_land(
            context,
            cfg,
            landing=landing,
            url=url,
            partition=partition,
            ext=PASDA_FORMATS[fmt][1],
            extra_meta=provenance,
        )


class ApiBulkExtractor(Extractor):
    """Hit an endpoint that returns the full dataset, land it. (later step)"""


class ApiIncrementalExtractor(Extractor):
    """Cursor/watermark delta pull. Reads the last high-water mark, calls the
    dataset's co-located fetch.py to pull only newer records, lands the delta,
    and advances the watermark. Pairs with ingest: incremental (upsert load).

    The API shape varies per source, so the actual pull is a co-located hook:

        defs/<publisher>/<department?>/<dataset>/fetch.py

        def fetch(source, since) -> tuple[list[dict], Any]:
            '''Return (records newer than `since`, new_watermark).
            `since` is the stored watermark (None on first run).
            `new_watermark` is the highest cursor value seen — a datetime
            string, an id, a page token, etc. (opaque to the framework).'''

    The delta is landed at the 'current' key as delta.json; downstream
    validate/transform/upsert then apply it. An empty delta is a clean no-op.
    """

    def extract(
        self,
        context: dg.AssetExecutionContext,
        cfg: PipelineConfig,
        *,
        landing: LandingZoneResource,
        sftp: SFTPResource | None = None,
    ) -> dict[str, Any]:
        mod = load_dataset_module(cfg, "fetch")
        fetch = getattr(mod, "fetch", None) if mod is not None else None
        if fetch is None:
            raise dg.Failure(
                "incremental sources require a co-located fetch.py exposing "
                "fetch(source, since) -> (records, new_watermark)"
            )

        since = landing.read_watermark(cfg.publisher, cfg.dataset, cfg.department)
        records, new_watermark = fetch(cfg.source, since)

        fd, tmp = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            with open(tmp, "w") as f:
                json.dump(records, f)
            manifest = landing.land_file(
                cfg.publisher,
                cfg.dataset,
                "current",
                tmp,
                source_meta={"since": since, "count": len(records)},
                department=cfg.department,
                filename="delta.json",
            )
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

        # Advance the watermark only when we actually pulled newer records.
        if records and new_watermark is not None:
            landing.write_watermark(
                cfg.publisher, cfg.dataset, new_watermark, department=cfg.department
            )

        context.add_output_metadata(
            {"records": len(records), "since": since, "new_watermark": new_watermark}
        )
        return manifest


EXTRACTORS: dict[str, Extractor] = {
    "sftp": SftpFileExtractor(),
    "http": HttpFileExtractor(),
    "arcgis": ArcGisExtractor(),
    "pasda": PasdaExtractor(),
    "api_bulk": ApiBulkExtractor(),
    "api_incremental": ApiIncrementalExtractor(),
}


def get_extractor(source_type: str) -> Extractor:
    try:
        return EXTRACTORS[source_type]
    except KeyError:
        raise NotImplementedError(
            f"no extractor registered for source type {source_type!r}"
        )
