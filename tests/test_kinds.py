"""Every dataset asset carries kinds: the catalog's icons and `kind:` filter."""

import dagster as dg

from wprdc_etl import definitions as d


def _kinds() -> dict[str, set[str]]:
    return {
        s.key.to_user_string(): set(s.kinds)
        for a in d.defs.assets
        if isinstance(a, dg.AssetsDefinition)
        for s in a.specs
    }


def test_every_dataset_asset_has_kinds() -> None:
    missing = [
        k for k, v in _kinds().items() if not v and not k.startswith("maintenance/")
    ]
    assert missing == []


def test_kinds_per_asset() -> None:
    kinds = _kinds()
    assert kinds["allegheny_county/gis/basins/landed"] == {"s3", "arcgis"}
    assert kinds["allegheny_county/gis/basins/validated"] == {"geopandas"}
    assert kinds["allegheny_county/gis/basins/mirrored/xlsx"] == {"ckan", "xlsx"}
    assert kinds["allegheny_county/gis/basins/package_metadata"] == {"ckan"}
    assert kinds["city_of_pittsburgh/gis/wards/region_layer"] == {"postgis"}
    assert all(len(v) <= 3 for v in kinds.values())  # Dagster's limit
