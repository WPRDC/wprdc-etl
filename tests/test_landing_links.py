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
