"""Catalogue -> CKAN metadata conversion, and mirrored distributions.

No network and no CKAN: the catalogue entry is a literal and the CKAN client
is a fake that records calls.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pytest

from wprdc_etl.strategies.metadata import (
    html_to_markdown,
    normalise_tag,
    package_description,
    package_tags,
)
from wprdc_etl.strategies.mirror import (
    MIRROR_KINDS,
    _link_url,
    publish_mirror,
    sync_package_metadata,
    validate_mirrors,
)


# --------------------------------------------------------------------------
# html_to_markdown
# --------------------------------------------------------------------------
def test_strips_the_word_paste_styling() -> None:
    """Hub descriptions are pasted out of Word. CKAN renders notes as
    Markdown, so a surviving style attribute shows up as literal text."""
    html = (
        "<p><span style='font-size:10.0pt; font-family:&quot;Tahoma&quot;'>"
        "<span>This dataset contains the Allegheny County boundary.</span>"
        "</span></p>"
    )
    assert html_to_markdown(html) == (
        "This dataset contains the Allegheny County boundary."
    )


def test_converts_structure_to_markdown() -> None:
    assert html_to_markdown("<h2>Title</h2><p>Body</p>") == "## Title\n\nBody"
    assert html_to_markdown("<ul><li>one</li><li>two</li></ul>") == "- one\n- two"
    assert html_to_markdown("<b>bold</b> and <em>it</em>") == "**bold** and *it*"
    assert html_to_markdown("a<br>b") == "a\nb"


def test_links_become_markdown_links() -> None:
    assert (
        html_to_markdown("<a href='https://x.org'>text</a>") == "[text](https://x.org)"
    )
    # A link whose text is its own url reads better bare than as [url](url).
    assert html_to_markdown("<a href='https://x.org'>https://x.org</a>") == (
        "https://x.org"
    )


def test_newlines_inside_emphasis_become_spaces() -> None:
    """A <br> inside bold splits the emphasis across lines, and a following
    "- " then renders as a stray list item."""
    assert html_to_markdown("<b>Frequency<br>- Publishing:</b>") == (
        "**Frequency - Publishing:**"
    )


def test_empty_and_plain_text_pass_through() -> None:
    assert html_to_markdown(None) == ""
    assert html_to_markdown("") == ""
    assert html_to_markdown("already plain") == "already plain"
    assert html_to_markdown("caf&eacute;") == "café"


# --------------------------------------------------------------------------
# description + tags
# --------------------------------------------------------------------------
def test_suffix_is_appended_after_a_blank_line() -> None:
    entry = {"description": "<p>Upstream text.</p>"}
    out = package_description(entry, "## Ours\n\nAdded by the ETL.")
    assert out == "Upstream text.\n\n## Ours\n\nAdded by the ETL."


def test_suffix_alone_when_upstream_has_no_description() -> None:
    assert package_description({}, "## Ours") == "## Ours"
    assert package_description({}) == ""


def test_tags_are_lowercased_and_deduped() -> None:
    """A single catalogue carries both `Environment` and `environment`."""
    entry = {"keyword": ["Environment", "environment", "Civic Vitality"]}
    assert package_tags(entry) == ["environment", "civic vitality"]


def test_tag_order_is_stable() -> None:
    """A churning tag list would make every sync look like a change."""
    # 2 characters is CKAN's minimum, so the fixtures have to clear it.
    entry = {"keyword": ["beta", "alpha", "beta"]}
    assert package_tags(entry, extra=["gamma", "alpha"]) == [
        "beta",
        "alpha",
        "gamma",
    ]


def test_unusable_tags_are_dropped() -> None:
    assert normalise_tag("!") is None  # too short once stripped
    assert normalise_tag("a") is None  # CKAN's minimum is 2
    assert normalise_tag("  Water Quality! ") == "water quality"


# --------------------------------------------------------------------------
# mirrors
# --------------------------------------------------------------------------
def _entry() -> dict[str, Any]:
    return {
        "title": "Allegheny County Boundary",
        "landingPage": "https://hub.example/datasets/boundary",
        "keyword": ["boundaries"],
        "description": "<p>The county boundary.</p>",
        "distribution": [
            {
                "format": "Web Page",
                "title": "ArcGIS Hub Dataset",
                "accessURL": "https://hub",
            },
            {
                "format": "ArcGIS GeoServices REST API",
                "title": "ArcGIS GeoService",
                "accessURL": "https://rest",
            },
            {"format": "CSV", "title": "CSV", "accessURL": "https://csv"},
            {"format": "GeoJSON", "title": "GeoJSON", "accessURL": "https://gj"},
            {"format": "ZIP", "title": "Shapefile", "accessURL": "https://shp"},
            {"format": "ZIP", "title": "File Geodatabase", "accessURL": "https://gdb"},
            {"format": "KML", "title": "KML", "accessURL": "https://kml"},
        ],
    }


def _cfg(**ckan: Any) -> SimpleNamespace:
    return SimpleNamespace(
        publisher="allegheny_county",
        department="gis",
        dataset="county_boundary",
        source=SimpleNamespace(type="arcgis", catalog="https://c/data.json", title="x"),
        ckan=SimpleNamespace(
            resource_id="csv-res",
            package_id="pkg",
            description_suffix=None,
            description=None,
            **ckan,
        ),
    )


class _FakeCkan:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def publish_link(self, resource_id: str, url: str) -> bool:
        self.calls.append(("link", resource_id, url))
        return True

    def publish_file(self, resource_id: str, path: str, filename: str) -> bool:
        self.calls.append(("file", resource_id, filename))
        return True

    def patch_package(self, package_id, *, notes=None, tags=None) -> bool:
        self.calls.append(("patch", package_id, notes, tags))
        return True

    def resource(self, resource_id: str) -> dict:
        return {}

    def find_resource(self, package_id, name, fmt) -> str | None:
        return None

    def add_resource(self, package_id, name, fmt, url="") -> str:
        self.calls.append(("create", package_id, name, fmt))
        return f"new-{fmt}"


def test_csv_is_a_normal_mirror() -> None:
    """The CSV is not privileged — it is one distribution among the others."""
    validate_mirrors([SimpleNamespace(format="csv")])


def test_csv_mirror_clashes_with_a_frame_target() -> None:
    """Both would publish a table to the same package, from different data."""
    with pytest.raises(ValueError, match="publishes the same table twice"):
        validate_mirrors([SimpleNamespace(format="csv")], has_datastore_target=True)


def test_unknown_mirror_format_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown ckan.mirror format"):
        validate_mirrors([SimpleNamespace(format="parquet")])


def test_every_distribution_a_hub_site_offers_is_supported() -> None:
    """Both catalogues publish exactly these eleven per layer — nine files
    and two links. ZIP covers two of them (Shapefile, File Geodatabase) and
    HTML covers two (Hub page, Esri REST), which is why resources are matched
    by name and never by format."""
    assert set(MIRROR_KINDS) == {
        "csv",
        "geojson",
        "shapefile",
        "kml",
        "file_geodatabase",
        "feature_collection",
        "xlsx",
        "geopackage",
        "sqlite",
        "hub_page",
        "rest_api",
    }


def test_link_urls_come_from_the_right_distribution() -> None:
    assert _link_url(_entry(), "hub_page") == "https://hub"
    assert _link_url(_entry(), "rest_api") == "https://rest"


def test_hub_page_falls_back_to_the_landing_page() -> None:
    entry = _entry()
    entry["distribution"] = [
        d for d in entry["distribution"] if d["format"] != "Web Page"
    ]
    assert _link_url(entry, "hub_page") == "https://hub.example/datasets/boundary"


def test_missing_link_distribution_fails_loudly() -> None:
    entry = _entry()
    entry["distribution"] = []
    entry.pop("landingPage")
    with pytest.raises(dg.Failure, match="no ArcGIS GeoServices REST API"):
        _link_url(entry, "rest_api")


def test_link_mirror_publishes_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    ckan = _FakeCkan()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        SimpleNamespace(format="hub_page", resource_id="hub-res", name=None),
        _entry(),
        ckan=ckan,
    )
    assert ckan.calls == [("link", "hub-res", "https://hub")]


def test_mirror_creates_a_missing_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    """A package that doesn't carry the resource yet gets one — which is why
    ckan.package_id is required alongside a mirror."""
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    ckan = _FakeCkan()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        SimpleNamespace(format="rest_api", resource_id=None, name=None),
        _entry(),
        ckan=ckan,
    )
    assert ("create", "pkg", "Esri Rest API", "HTML") in ckan.calls
    assert ("link", "new-HTML", "https://rest") in ckan.calls


def test_dry_run_touches_no_ckan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: "_dryrun")
    ckan = _FakeCkan()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        SimpleNamespace(format="hub_page", resource_id="hub-res", name=None),
        _entry(),
        ckan=ckan,
    )
    sync_package_metadata(_cfg(mirror=None, sync_metadata=True), _entry(), ckan=ckan)
    assert ckan.calls == []


def test_metadata_sync_patches_notes_and_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    ckan = _FakeCkan()
    cfg = _cfg(mirror=None, sync_metadata=True)
    cfg.ckan.description_suffix = "## Ours"
    sync_package_metadata(cfg, _entry(), ckan=ckan)
    ((kind, package_id, notes, tags),) = ckan.calls
    assert (kind, package_id) == ("patch", "pkg")
    assert notes == "The county boundary.\n\n## Ours"
    assert tags == ["boundaries"]


def test_metadata_sync_without_a_catalogue_leaves_tags_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PASDA dataset has no data.json, so `entry` is empty.

    `package_tags({})` is `[]`, and patching a package with an empty tag list
    CLEARS whatever a curator put there — so the sync must send None and push
    only the description the defs.yaml supplies itself.
    """
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    ckan = _FakeCkan()
    cfg = _cfg(mirror=None, sync_metadata=True)
    cfg.source.type = "pasda"
    cfg.ckan.description = "Curated text."
    sync_package_metadata(cfg, {}, ckan=ckan)
    ((kind, package_id, notes, tags),) = ckan.calls
    assert (kind, package_id, notes) == ("patch", "pkg", "Curated text.")
    assert tags is None


def test_metadata_sync_needs_a_package_id() -> None:
    cfg = _cfg(mirror=None, sync_metadata=True)
    cfg.ckan.package_id = None
    with pytest.raises(dg.Failure, match="ckan.package_id"):
        sync_package_metadata(cfg, _entry(), ckan=_FakeCkan())


# --------------------------------------------------------------------------
# DataStore ingest of a mirrored file
# --------------------------------------------------------------------------
class _IngestCkan(_FakeCkan):
    def __init__(self, spatial_load_action: str = "") -> None:
        super().__init__()
        self.spatial_load_action = spatial_load_action

    def find_resource(self, package_id, name, fmt) -> str:
        return f"{fmt.lower()}-res"

    def submit_to_datastore(self, resource_id: str) -> None:
        self.calls.append(("datapusher", resource_id))

    def load_geojson_to_datastore(self, resource_id: str) -> None:
        if not self.spatial_load_action:
            raise NotImplementedError("spatial load endpoint is not built yet")
        self.calls.append(("spatial_load", resource_id))


def _mirror_spec(fmt: str, datastore: bool) -> SimpleNamespace:
    return SimpleNamespace(
        format=fmt, resource_id=f"{fmt}-res", name=None, datastore=datastore
    )


def test_csv_ingest_goes_through_datapusher(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror.arcgis_ready_url", lambda url: None
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: pathlib.Path(path).write_bytes(b"a,b\n1,2\n"),
    )
    ckan = _IngestCkan()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("csv", True),
        _entry(),
        ckan=ckan,
    )
    assert ("datapusher", "csv-res") in ckan.calls


def test_geojson_ingest_refuses_without_the_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DataPusher+ is NOT an acceptable fallback: it would build a table with
    the properties as columns and no geometry, and look like success."""
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror.arcgis_ready_url", lambda url: None
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: pathlib.Path(path).write_bytes(b"{}"),
    )
    ckan = _IngestCkan(spatial_load_action="")
    with pytest.raises(NotImplementedError, match="not built yet"):
        publish_mirror(
            _cfg(mirror=None, sync_metadata=False),
            _mirror_spec("geojson", True),
            _entry(),
            ckan=ckan,
        )
    assert ("datapusher", "geojson-res") not in ckan.calls


def test_geojson_ingest_uses_the_spatial_endpoint_once_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror.arcgis_ready_url", lambda url: None
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: pathlib.Path(path).write_bytes(b"{}"),
    )
    ckan = _IngestCkan(spatial_load_action="dataspatial_load")
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("geojson", True),
        _entry(),
        ckan=ckan,
    )
    assert ("spatial_load", "geojson-res") in ckan.calls


def test_no_ingest_when_the_flag_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror.arcgis_ready_url", lambda url: None
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: pathlib.Path(path).write_bytes(b"{}"),
    )
    ckan = _IngestCkan(spatial_load_action="dataspatial_load")
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("geojson", False),
        _entry(),
        ckan=ckan,
    )
    assert not [c for c in ckan.calls if c[0] in ("datapusher", "spatial_load")]


# --------------------------------------------------------------------------
# The landed artifact is reused, not re-fetched
# --------------------------------------------------------------------------
class _FakeLandingZone:
    """Records download() calls and writes known bytes."""

    def __init__(self, payload: bytes = b'{"type":"FeatureCollection"}') -> None:
        self.downloads: list[str] = []
        self.payload = payload

    def prefix(self, publisher, dataset, partition, department=None) -> str:
        parts = [publisher, *([department] if department else []), dataset, partition]
        return "/".join(parts)

    def download(self, key: str, local_path: str) -> None:
        self.downloads.append(key)
        pathlib.Path(local_path).write_bytes(self.payload)


def _manifest(filename: str = "data.geojson") -> dict[str, Any]:
    return {
        "publisher": "allegheny_county",
        "department": "gis",
        "dataset": "county_boundary",
        "partition": "2026-09-20",
        "filename": filename,
    }


def test_geojson_mirror_reuses_the_landed_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Published bytes must be the SAME bytes region_layer dissolved.

    A second fetch could pick up a regenerated export — ArcGIS rebuilds these
    without warning — leaving CKAN and PostGIS holding different versions
    while every step reports success.
    """
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)

    def boom(*a: Any, **k: Any) -> None:
        raise AssertionError("re-fetched the geojson instead of reading S3")

    monkeypatch.setattr("wprdc_etl.strategies.mirror._download", boom)
    monkeypatch.setattr("wprdc_etl.strategies.mirror._distribution", boom)

    landing = _FakeLandingZone()
    ckan = _IngestCkan()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("geojson", False),
        _entry(),
        ckan=ckan,
        landing=landing,
        manifest=_manifest(),
    )
    assert landing.downloads == [
        "allegheny_county/gis/county_boundary/2026-09-20/data.geojson"
    ]
    assert ("file", "geojson-res", "data.geojson") in ckan.calls


def test_other_formats_still_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the landed format is reused — there is no landed kml to read."""
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._distribution", lambda entry, fmt: "https://kml"
    )
    fetched: list[str] = []
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: (
            fetched.append(url),
            pathlib.Path(path).write_bytes(b"<kml/>"),
        )[0],
    )
    landing = _FakeLandingZone()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("kml", False),
        _entry(),
        ckan=_IngestCkan(),
        landing=landing,
        manifest=_manifest(),
    )
    assert fetched == ["https://kml"]
    assert landing.downloads == []


def test_falls_back_to_fetch_without_a_landed_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A csv-sourced dataset has no landed geojson, so the mirror fetches."""
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._distribution", lambda entry, fmt: "https://gj"
    )
    fetched: list[str] = []
    monkeypatch.setattr(
        "wprdc_etl.strategies.mirror._download",
        lambda url, path: (fetched.append(url), pathlib.Path(path).write_bytes(b"{}"))[
            0
        ],
    )
    landing = _FakeLandingZone()
    publish_mirror(
        _cfg(mirror=None, sync_metadata=False),
        _mirror_spec("geojson", False),
        _entry(),
        ckan=_IngestCkan(),
        landing=landing,
        manifest=_manifest("data.csv"),  # landed a csv, not a geojson
    )
    assert fetched == ["https://gj"]
    assert landing.downloads == []


# --------------------------------------------------------------------------
# ckan.description — replacing the publisher's text
# --------------------------------------------------------------------------
def test_override_replaces_the_catalogue_description() -> None:
    entry = {"description": "<p>The publisher's harvest boilerplate.</p>"}
    assert package_description(entry, override="## Ours\n\nCurated.") == (
        "## Ours\n\nCurated."
    )


def test_override_is_not_html_converted() -> None:
    """It is already Markdown — running it through the HTML converter would
    mangle anything that merely looks like a tag."""
    md = "Use `<geometry>` as the column name.\n\n- one\n- two"
    assert package_description({}, override=md) == md


def test_override_and_suffix_compose() -> None:
    entry = {"description": "<p>ignored</p>"}
    out = package_description(entry, "## Note\n\nAppended.", override="Curated.")
    assert out == "Curated.\n\n## Note\n\nAppended."


def test_blank_override_falls_back_to_the_catalogue() -> None:
    entry = {"description": "<p>Publisher text.</p>"}
    assert package_description(entry, override="   ") == "Publisher text."
    assert package_description(entry, override=None) == "Publisher text."


def test_metadata_sync_sends_the_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wprdc_etl.strategies.mirror.sink_dir", lambda: None)
    ckan = _FakeCkan()
    cfg = _cfg(mirror=None, sync_metadata=True)
    cfg.ckan.description = "Curated WPRDC text."
    sync_package_metadata(cfg, _entry(), ckan=ckan)
    ((_, _, notes, _),) = ckan.calls
    assert notes == "Curated WPRDC text."


def test_find_resource_never_falls_back_to_format() -> None:
    """A name miss must NOT return a same-format resource.

    It used to, and the File Geodatabase uploaded straight over the Shapefile
    — same ZIP format, no resource of its own, every step green. Creating a
    duplicate is the acceptable failure; writing the wrong file under the
    right name is not.
    """
    from wprdc_etl.resources import CkanResource

    class _Stub(CkanResource):
        # CkanResource is a frozen pydantic model, so the read is overridden
        # by subclassing rather than by assignment.
        def package(self, package_id: str) -> dict[str, Any]:
            return {
                "resources": [
                    {"id": "shp-id", "name": "Shapefile", "format": "ZIP"},
                    {"id": "hub-id", "name": "ArcGIS Hub Dataset", "format": "HTML"},
                ]
            }

    ckan = _Stub(base_url="http://localhost:5001")
    assert ckan.find_resource("pkg", "Shapefile", "ZIP") == "shp-id"
    assert ckan.find_resource("pkg", "File Geodatabase", "ZIP") is None
    assert ckan.find_resource("pkg", "Esri Rest API", "HTML") is None
