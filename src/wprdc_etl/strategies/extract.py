"""Extract strategies: how raw data is acquired and written to the landing zone.

Selected by `source.type` in each component config. An extractor lands the raw
artifact(s) + manifest and returns the manifest dict. Adding a new source type
is a new subclass registered in EXTRACTORS — not a new pipeline.

Only SftpFileExtractor is implemented in this step. The others are registered
as stubs so the dispatch seam exists and `dg check` sees the full surface; they
get filled in later build steps.
"""

from __future__ import annotations

import json
import os
import tempfile

import dagster as dg

from wprdc_etl.dataset_modules import load_dataset_module


class Extractor:
    """Base strategy. Concrete extractors override extract().

    Resources are passed by keyword so each extractor takes only what it needs
    (SFTP needs `sftp`; future HTTP/API extractors will take an http client).
    """

    def extract(self, context, cfg, *, landing, sftp=None) -> dict:
        raise NotImplementedError


class SftpFileExtractor(Extractor):
    """Pull the newest file matching a glob from an SFTP server and land it."""

    def extract(self, context, cfg, *, landing, sftp=None) -> dict:
        partition = context.partition_key if context.has_partition_key else "current"
        src = cfg.source

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
    """Download a file from an HTTP(S) URL and land it. (later step)"""


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

    def extract(self, context, cfg, *, landing, sftp=None) -> dict:
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
