"""landed attaches where it landed: an s3:// URI, and an S3 console link on AWS."""

from types import SimpleNamespace
from typing import Any

import dagster as dg

from wprdc_etl import definitions as d
from wprdc_etl.components._common import landing_links
from wprdc_etl.resources import LandingZoneResource
from wprdc_etl.strategies import extract

CFG = SimpleNamespace(publisher="allegheny_county", department="gis", dataset="basins")


def test_aws_gets_a_console_link() -> None:
    landing = LandingZoneResource(bucket="wprdc-etl-landing", region="us-east-1")
    links = landing_links(landing, CFG, "2026-10-18")
    assert links["landing_uri"].value == (
        "s3://wprdc-etl-landing/allegheny_county/gis/basins/2026-10-18/"
    )
    assert links["landing_console"].value == (
        "https://us-east-1.console.aws.amazon.com/s3/buckets/wprdc-etl-landing"
        "?region=us-east-1&prefix=allegheny_county/gis/basins/2026-10-18/"
        "&showversions=false"
    )


def test_localstack_gets_only_the_uri() -> None:
    landing = LandingZoneResource(endpoint_url="http://localhost:4566")
    assert set(landing_links(landing, CFG, "current")) == {"landing_uri"}


def test_the_landed_materialization_carries_them(monkeypatch: Any) -> None:
    def fake_extract(self, context, cfg, *, landing, sftp=None):
        context.add_output_metadata({"source_url": "https://example.org/x"})
        return {}

    monkeypatch.setattr(extract.ArcGisExtractor, "extract", fake_extract)
    asset = next(
        a
        for a in d.defs.assets
        if isinstance(a, dg.AssetsDefinition)
        and dg.AssetKey(["allegheny_county", "gis", "basins", "landed"]) in a.keys
    )
    key = asset.partitions_def.get_last_partition_key()
    result = dg.materialize(
        [asset],
        resources={
            "landing": LandingZoneResource(bucket="b", region="us-east-1"),
            "io_manager": dg.mem_io_manager,
        },
        partition_key=key,
    )
    (event,) = result.get_asset_materialization_events()
    meta = event.materialization.metadata
    assert "source_url" in meta  # the extractor's own metadata survives
    assert meta["landing_uri"].value == f"s3://b/allegheny_county/gis/basins/{key}/"
    assert "console.aws.amazon.com" in meta["landing_console"].value


def _spec(*key: str) -> dg.AssetSpec:
    asset_key = dg.AssetKey(list(key))
    asset = next(
        a
        for a in d.defs.assets
        if isinstance(a, dg.AssetsDefinition) and asset_key in a.keys
    )
    return asset.specs_by_key[asset_key]


def test_each_publish_asset_links_its_own_resource() -> None:
    prefix = ("city_of_pittsburgh", "water_features")
    package = "https://data.wprdc.org/dataset/city-water-features"
    loaded = _spec(*prefix, "loaded").metadata
    assert loaded["ckan_package"].value == package
    assert loaded["ckan_resource"].value == (
        f"{package}/resource/513290a6-2bac-4e41-8029-354cbda6a7b7"
    )
    geojson = _spec(*prefix, "published", "geojson").metadata
    assert geojson["ckan_resource"].value == (
        f"{package}/resource/f7c252a5-28be-43ab-95b5-f3eb0f1eef67"
    )
    assert "ckan_package" not in _spec(*prefix, "landed").metadata


def _named_targets() -> set[tuple[str, ...]]:
    """Asset key prefixes of the datasets whose target is `ckan.resource_name`."""
    import pathlib

    import yaml

    root = pathlib.Path(d.__file__).parent / "defs"
    out = set()
    for f in root.rglob("defs.yaml"):
        attrs = (yaml.safe_load(f.read_text()) or {}).get("attributes") or {}
        if (attrs.get("ckan") or {}).get("resource_name"):
            parts = [attrs["publisher"], attrs.get("department"), attrs["dataset"]]
            out.add(tuple(x for x in parts if x))
    return out


_NAMED_TARGETS = _named_targets()


def test_every_loaded_asset_links_its_ckan_pages() -> None:
    for asset in d.defs.assets:
        if not isinstance(asset, dg.AssetsDefinition):
            continue
        for key, spec in asset.specs_by_key.items():
            if key.path[-1] == "loaded_by_year":
                # One resource per year, found by name at run time.
                assert "ckan_package" in spec.metadata, key
            elif key.path[-1] == "loaded":
                assert "ckan_package" in spec.metadata, key
                # A target named by `ckan.resource_name` rolls over each
                # January, so it has no fixed resource page to link.
                if tuple(key.path[:-1]) not in _NAMED_TARGETS:
                    assert "ckan_resource" in spec.metadata, key
