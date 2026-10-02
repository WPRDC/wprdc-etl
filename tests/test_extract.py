"""Unit tests for extract strategies — no real network or S3.

Covers HttpFileExtractor: the download is monkeypatched, and the landing zone
is a fake that records what it was handed.
"""

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

import dagster as dg

from wprdc_etl.strategies.extract import (
    ArcGisExtractor,
    HttpFileExtractor,
    get_extractor,
    is_arcgis_pending,
    resolve_arcgis_distribution,
)


class _FakeLanding:
    """Records land_file() calls; returns a manifest like the real resource."""

    def __init__(self, skipped: bool = False) -> None:
        self.calls = []
        self._skipped = skipped

    def land_file(
        self,
        publisher: str,
        dataset: str,
        partition: str,
        local_path: str,
        source_meta: dict[str, Any],
        department: str | None = None,
        filename: str = "data.csv",
    ) -> dict[str, Any]:
        with open(local_path, "rb") as fh:
            body = fh.read()
        self.calls.append(
            {
                "publisher": publisher,
                "dataset": dataset,
                "partition": partition,
                "filename": filename,
                "department": department,
                "source_meta": source_meta,
                "body": body,
            }
        )
        return {
            "sha256": "0" * 64,
            "size": len(body),
            "skipped": self._skipped,
            "filename": filename,
        }


class _FakeLog:
    """Collects context.log calls so extractors can log freely."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, msg: str) -> None:
        self.messages.append(msg)

    warning = error = debug = info


class _FakeCtx:
    has_partition_key = False

    def __init__(self) -> None:
        self.metadata: dict[str, Any] = {}
        self.log = _FakeLog()

    def add_output_metadata(self, md: dict[str, Any]) -> None:
        self.metadata.update(md)


class _FakeResp:
    def __init__(
        self, chunks: list[bytes], headers: dict[str, str] | None = None
    ) -> None:
        self._chunks = chunks
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        pass

    def close(self) -> None:
        pass

    def iter_content(self, chunk_size: int = 1) -> Iterator[bytes]:
        yield from self._chunks

    def __enter__(self) -> "_FakeResp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def _cfg(
    url: str | None = "https://example.org/open_data/water_features.csv",
    secret_ref: str | None = None,
) -> SimpleNamespace:
    src = SimpleNamespace(
        type="http", url=url, path=None, host=None, port=None, secret_ref=secret_ref
    )
    return SimpleNamespace(
        publisher="city_of_pittsburgh",
        dataset="water_features",
        department=None,
        source=src,
    )


def test_http_extractor_is_registered() -> None:
    assert isinstance(get_extractor("http"), HttpFileExtractor)


def test_http_extractor_streams_and_lands(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kw: Any) -> _FakeResp:
        seen["url"] = url
        seen["kw"] = kw
        return _FakeResp(
            [b"id,name\n", b"1,Foo\n"],
            headers={
                "ETag": '"v1"',
                "Last-Modified": "Mon, 06 Jan 2025 06:00:00 GMT",
                "Content-Length": "14",
                "Content-Type": "text/csv",
            },
        )

    monkeypatch.setattr("requests.get", fake_get)

    landing = _FakeLanding()
    ctx = _FakeCtx()
    manifest = HttpFileExtractor().extract(ctx, _cfg(), landing=landing)

    assert seen["url"].endswith("water_features.csv")
    assert seen["kw"]["stream"] is True
    assert seen["kw"]["auth"] is None

    call = landing.calls[0]
    assert call["filename"] == "data.csv"
    assert call["partition"] == "current"
    assert call["body"] == b"id,name\n1,Foo\n"
    assert call["source_meta"]["etag"] == '"v1"'
    assert call["source_meta"]["url"].endswith("water_features.csv")

    assert manifest["size"] == 14
    assert ctx.metadata["filename"] == "data.csv"
    assert ctx.metadata["last_modified"] == "Mon, 06 Jan 2025 06:00:00 GMT"


def test_http_extractor_keeps_source_extension(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "requests.get", lambda url, **kw: _FakeResp([b"{}"], headers={})
    )
    landing = _FakeLanding()
    HttpFileExtractor().extract(
        _FakeCtx(),
        _cfg(url="https://example.org/d/features.geojson?token=x"),
        landing=landing,
    )
    assert landing.calls[0]["filename"] == "data.geojson"


def test_http_extractor_basic_auth_from_secret_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(
        "requests.get",
        lambda url, **kw: seen.update(kw) or _FakeResp([b"x"], headers={}),
    )
    monkeypatch.setenv("WF_HTTP_SECRET", "svc-user:s3cr3t")

    HttpFileExtractor().extract(
        _FakeCtx(), _cfg(secret_ref="WF_HTTP_SECRET"), landing=_FakeLanding()
    )
    assert seen["auth"] == ("svc-user", "s3cr3t")


def test_http_extractor_requires_a_url() -> None:
    cfg = _cfg()
    cfg.source.url = None
    with pytest.raises(Exception):
        HttpFileExtractor().extract(_FakeCtx(), cfg, landing=_FakeLanding())


def test_http_extractor_missing_secret_env_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WF_HTTP_SECRET", raising=False)
    with pytest.raises(Exception):
        HttpFileExtractor().extract(
            _FakeCtx(), _cfg(secret_ref="WF_HTTP_SECRET"), landing=_FakeLanding()
        )


# --------------------------------------------------------------------------
# ArcGIS Hub (DCAT data.json) sources
# --------------------------------------------------------------------------
def _catalog() -> list[dict[str, Any]]:
    """A cut-down data.json shaped like the real county/city catalogues.

    Two details are copied deliberately because both broke a first cut of the
    resolver: the link lives in `accessURL` (not `downloadURL`), and the
    Shapefile shares its "ZIP" format with the File Geodatabase.
    """
    return [
        {
            "title": "Allegheny County Municipal Boundaries",
            "modified": "2025-09-12T14:53:15.594Z",
            "identifier": "https://www.arcgis.com/home/item.html?id=abc123",
            "landingPage": "https://openac-alcogis.opendata.arcgis.com/datasets/muni",
            "distribution": [
                {"format": "Web Page", "title": "Hub", "accessURL": "https://hub"},
                {"format": "CSV", "title": "CSV", "accessURL": "https://x/csv?a=1"},
                {"format": "GeoJSON", "title": "GeoJSON", "accessURL": "https://x/gj"},
                {"format": "ZIP", "title": "Shapefile", "accessURL": "https://x/shp"},
                {
                    "format": "ZIP",
                    "title": "File Geodatabase",
                    "accessURL": "https://x/gdb",
                },
            ],
        },
        {
            "title": "Allegheny County DPW Maintenance Districts",
            "modified": "2025-07-02T00:00:00.000Z",
            "distribution": [
                {"format": "CSV", "title": "CSV", "accessURL": "https://x/older"}
            ],
        },
        {
            "title": "Allegheny County DPW Maintenance Districts",
            "modified": "2025-07-03T00:00:00.000Z",
            "distribution": [
                {"format": "CSV", "title": "CSV", "accessURL": "https://x/newer"}
            ],
        },
        {
            "title": "Allegheny County Trails",
            "modified": "2026-09-01T00:00:00.000Z",
            "distribution": [
                {"format": "KML", "title": "KML", "accessURL": "https://x/kml"}
            ],
        },
    ]


def _arcgis_cfg(title: str, fmt: str = "csv") -> SimpleNamespace:
    src = SimpleNamespace(
        type="arcgis",
        url=None,
        path=None,
        host=None,
        port=None,
        secret_ref=None,
        catalog="https://openac-alcogis.opendata.arcgis.com/data.json",
        title=title,
        format=fmt,
    )
    return SimpleNamespace(
        publisher="allegheny_county",
        dataset="municipal_boundaries",
        department="gis",
        source=src,
    )


def test_arcgis_extractor_is_registered() -> None:
    assert isinstance(get_extractor("arcgis"), ArcGisExtractor)


def test_resolve_reads_access_url() -> None:
    """These catalogues never populate downloadURL; only accessURL."""
    url, prov = resolve_arcgis_distribution(
        _catalog(), "Allegheny County Municipal Boundaries", "csv"
    )
    assert url == "https://x/csv?a=1"
    assert prov["arcgis_modified"] == "2025-09-12T14:53:15.594Z"
    assert prov["arcgis_ambiguous_titles"] is None


def test_resolve_shapefile_not_geodatabase() -> None:
    """Both are format ZIP — the distribution title disambiguates."""
    url, _ = resolve_arcgis_distribution(
        _catalog(), "Allegheny County Municipal Boundaries", "shapefile"
    )
    assert url == "https://x/shp"


def test_resolve_duplicate_title_takes_newest() -> None:
    """A title can appear twice; catalogue order must not decide the winner."""
    url, prov = resolve_arcgis_distribution(
        _catalog(), "Allegheny County DPW Maintenance Districts", "csv"
    )
    assert url == "https://x/newer"
    assert prov["arcgis_ambiguous_titles"] == 2


def test_resolve_missing_title_suggests_near_matches() -> None:
    """A renamed layer is the common drift; the error has to name the new one."""
    with pytest.raises(dg.Failure) as excinfo:
        resolve_arcgis_distribution(
            _catalog(), "Allegheny County Trails Locations", "csv"
        )
    assert "Allegheny County Trails" in str(excinfo.value)


def test_resolve_missing_format_lists_what_exists() -> None:
    with pytest.raises(dg.Failure) as excinfo:
        resolve_arcgis_distribution(_catalog(), "Allegheny County Trails", "csv")
    assert "KML" in str(excinfo.value)


def test_resolve_unknown_format_rejected() -> None:
    with pytest.raises(dg.Failure):
        resolve_arcgis_distribution(
            _catalog(), "Allegheny County Municipal Boundaries", "xlsx"
        )


def test_arcgis_extractor_lands_with_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kw: Any) -> _FakeResp:
        seen["url"] = url
        return _FakeResp([b"OBJECTID,NAME\n", b"1,Aspinwall\n"], headers={})

    monkeypatch.setattr("requests.get", fake_get)
    monkeypatch.setattr(
        "wprdc_etl.strategies.extract.fetch_catalog", lambda url, **kw: _catalog()
    )

    landing = _FakeLanding()
    ctx = _FakeCtx()
    ArcGisExtractor().extract(
        ctx, _arcgis_cfg("Allegheny County Municipal Boundaries"), landing=landing
    )

    assert seen["url"] == "https://x/csv?a=1"
    call = landing.calls[0]
    assert call["department"] == "gis"
    assert call["filename"] == "data.csv"
    meta = call["source_meta"]
    assert meta["arcgis_title"] == "Allegheny County Municipal Boundaries"
    assert meta["arcgis_modified"] == "2025-09-12T14:53:15.594Z"
    assert meta["arcgis_catalog"].endswith("data.json")


def test_arcgis_geojson_lands_under_geojson_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The download URL has no extension to infer from (.../gj), and the
    reader downstream dispatches on the landed filename."""
    monkeypatch.setattr(
        "requests.get", lambda url, **kw: _FakeResp([b"{}"], headers={})
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.extract.fetch_catalog", lambda url, **kw: _catalog()
    )

    landing = _FakeLanding()
    ArcGisExtractor().extract(
        _FakeCtx(),
        _arcgis_cfg("Allegheny County Municipal Boundaries", "geojson"),
        landing=landing,
    )
    assert landing.calls[0]["filename"] == "data.geojson"


def test_arcgis_requires_catalog_and_title() -> None:
    cfg = _arcgis_cfg("x")
    cfg.source.catalog = None
    with pytest.raises(dg.Failure, match="source.catalog"):
        ArcGisExtractor().extract(_FakeCtx(), cfg, landing=_FakeLanding())

    cfg = _arcgis_cfg("x")
    cfg.source.title = None
    with pytest.raises(dg.Failure, match="source.title"):
        ArcGisExtractor().extract(_FakeCtx(), cfg, landing=_FakeLanding())


def test_pending_placeholder_is_detected() -> None:
    """ArcGIS answers 200 with this until the export is built. It poisoned the
    first generated schemas, so it is guarded in both the extractor and
    scripts/sync_arcgis.py."""
    payload = (
        b'{"message":"Up to date download file is being generated. '
        b'Please check back again later.","status":"Pending",'
        b'"created":"2026-09-29T21:58:22.637Z"}'
    )
    assert is_arcgis_pending(payload)
    assert is_arcgis_pending(b'{"status":"InProgress"}')


def test_real_payloads_are_not_mistaken_for_pending() -> None:
    """A GeoJSON export is application/json too, so the check keys on the
    body, not the content type."""
    assert not is_arcgis_pending(b'{"type":"FeatureCollection","features":[]}')
    assert not is_arcgis_pending(b"OBJECTID,NAME\n1,Aspinwall\n")
    assert not is_arcgis_pending(b"")
    assert not is_arcgis_pending(b"{ truncated json")


def test_extractor_refuses_to_land_a_pending_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The placeholder is a 200 with a plausible body — landed, it would
    validate as a one-row dataset."""
    pending = b'{"message":"generating","status":"Pending"}'
    monkeypatch.setattr(
        "requests.get", lambda url, **kw: _FakeResp([pending], headers={})
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.extract.fetch_catalog", lambda url, **kw: _catalog()
    )
    # No sleeping in tests: exhaust the retries immediately.
    monkeypatch.setattr("wprdc_etl.strategies.extract.ARCGIS_PENDING_WAITS", (0, 0))

    landing = _FakeLanding()
    with pytest.raises(dg.Failure, match="still generating"):
        ArcGisExtractor().extract(
            _FakeCtx(),
            _arcgis_cfg("Allegheny County Municipal Boundaries"),
            landing=landing,
        )
    assert landing.calls == []
