"""Tests for the PostGIS admin-region store.

Two halves. The write guard is pure and always runs. The round trip needs a real
PostGIS and is skipped unless SPATIAL_DSN is set, so a bare checkout and CI stay
green without a database.
"""

import os
from typing import TYPE_CHECKING

import pytest

from wprdc_etl.resources import SpatialResource, key_text
from wprdc_etl.runtime import guard_spatial_write

if TYPE_CHECKING:
    import geopandas as gpd
    from shapely.geometry import Polygon

LOCAL = "postgresql://dagster:dagster@localhost:5432/spatial"
REMOTE = "postgresql://etl:secret@spatial.prod.internal:5432/spatial"


# --------------------------------------------------------------------------
# write guard
# --------------------------------------------------------------------------
def test_guard_allows_a_local_database_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("WPRDC_ALLOW_REMOTE_SPATIAL", raising=False)
    guard_spatial_write(LOCAL)  # no raise
    guard_spatial_write("postgresql://dagster:dagster@postgres:5432/spatial")


def test_guard_refuses_a_remote_database_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("WPRDC_ALLOW_REMOTE_SPATIAL", raising=False)
    with pytest.raises(RuntimeError, match="refusing to rewrite boundary layers"):
        guard_spatial_write(REMOTE)


def test_guard_allows_a_remote_database_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    guard_spatial_write(REMOTE)  # no raise


def test_guard_has_an_explicit_opt_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("WPRDC_ALLOW_REMOTE_SPATIAL", "1")
    guard_spatial_write(REMOTE)  # no raise


# --------------------------------------------------------------------------
# round trip (needs PostGIS)
# --------------------------------------------------------------------------
pytestmark_dsn = pytest.mark.skipif(
    not os.getenv("SPATIAL_DSN"),
    reason="needs a PostGIS database; set SPATIAL_DSN (see compose.yaml)",
)


def _square(x0: float, y0: float, size: float = 0.01) -> "Polygon":
    from shapely.geometry import Polygon

    return Polygon(
        [
            (x0, y0),
            (x0 + size, y0),
            (x0 + size, y0 + size),
            (x0, y0 + size),
        ]
    )


@pytest.fixture
def layer_frame() -> "gpd.GeoDataFrame":
    import geopandas as gpd

    # Two adjacent squares, and a third feature repeating code "A" — the real
    # layers do this (fire zones ship 102 features for 101 zones), so the
    # dissolve is part of what's under test.
    return gpd.GeoDataFrame(
        {
            "code": ["A", "B", "A"],
            "label": ["Alpha", "Bravo", "Alpha"],
            "extra": [1, 2, 3],
            "geometry": [
                _square(-80.00, 40.40),
                _square(-79.98, 40.40),
                _square(-80.00, 40.42),
            ],
        },
        crs="EPSG:4326",
    )


@pytestmark_dsn
def test_replace_layer_dissolves_and_locate_finds_the_region(
    layer_frame: "gpd.GeoDataFrame",
) -> None:
    spatial = SpatialResource(dsn=os.environ["SPATIAL_DSN"])
    load = spatial.replace_layer(
        "_test_layer",
        layer_frame,
        value_field="code",
        label_field="label",
        source="unit-test",
    )
    assert (load.rows, load.changed) == (2, True)  # three features -> two regions

    try:
        assert spatial.layers()["_test_layer"] == 2

        # The same frame again writes nothing; a changed label rewrites.
        again = spatial.replace_layer(
            "_test_layer", layer_frame, value_field="code", label_field="label"
        )
        assert again.changed is False
        edited = layer_frame.assign(label=["Alpha", "Bravo!", "Alpha"])
        assert spatial.replace_layer(
            "_test_layer", edited, value_field="code", label_field="label"
        ).changed
        assert spatial.replace_layer(
            "_test_layer", layer_frame, value_field="code", label_field="label"
        ).changed  # back again, so the lookups below see "Bravo"

        inside_a = (-79.995, 40.405)
        inside_b = (-79.975, 40.405)
        outside = (-79.900, 40.300)
        found = spatial.locate([inside_a, inside_b, outside], ["_test_layer"])

        assert found[inside_a]["_test_layer"] == ("A", "Alpha")
        assert found[inside_b]["_test_layer"] == ("B", "Bravo")
        assert outside not in found

        # The second "A" polygon was unioned in, not dropped.
        assert spatial.locate([(-79.995, 40.425)], ["_test_layer"])[(-79.995, 40.425)][
            "_test_layer"
        ] == ("A", "Alpha")
    finally:
        with spatial._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM admin_region WHERE layer = %s", ("_test_layer",)
                )
                cur.execute(
                    "DELETE FROM admin_region_layer WHERE layer = %s", ("_test_layer",)
                )


@pytestmark_dsn
def test_locate_fails_loud_on_a_layer_nobody_loaded() -> None:
    import dagster as dg

    spatial = SpatialResource(dsn=os.environ["SPATIAL_DSN"])
    spatial.ensure_schema()
    with pytest.raises(dg.Failure, match="has no layer"):
        spatial.locate([(-79.99, 40.44)], ["_definitely_not_loaded"])


# --------------------------------------------------------------------------
# keyed geometry
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (450843, "450843"),
        (1234567.0, "1234567"),  # `:g` would give "1.23457e+06"
        (" SSAP7 ", "SSAP7"),
        ("", None),
        (None, None),
        (float("nan"), None),
    ],
)
def test_key_text_is_exact(value: object, expected: str | None) -> None:
    assert key_text(value) == expected


@pytestmark_dsn
def test_replace_key_layer_round_trips_and_drops_duplicate_keys() -> None:
    import geopandas as gpd
    from shapely.geometry import Point

    spatial = SpatialResource(dsn=os.environ["SPATIAL_DSN"])
    gdf = gpd.GeoDataFrame(
        {
            "ADDRESS_ID": ["SSAP1", "SSAP2", "SSAP1", None],
            "geometry": [
                Point(-79.99, 40.44),
                Point(-80.01, 40.45),
                Point(-70.0, 30.0),  # repeats SSAP1: dropped, first kept
                Point(-79.0, 40.0),  # no key: skipped
            ],
        },
        crs="EPSG:4326",
    )
    load = spatial.replace_key_layer(
        "_test_keys", gdf, key_field="ADDRESS_ID", source="unit-test"
    )
    try:
        assert (load.rows, load.duplicates, load.changed) == (2, 1, True)
        assert spatial.key_layers()["_test_keys"] == 2
        again = spatial.replace_key_layer("_test_keys", gdf, key_field="ADDRESS_ID")
        assert again.changed is False
        found = spatial.lookup_keys("_test_keys", ["SSAP1", "SSAP2", "SSAP9"])
        assert found["SSAP1"] == pytest.approx((-79.99, 40.44))
        assert found["SSAP2"] == pytest.approx((-80.01, 40.45))
        assert "SSAP9" not in found
    finally:
        with spatial._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM keyed_geometry WHERE layer = '_test_keys'")
                cur.execute(
                    "DELETE FROM keyed_geometry_layer WHERE layer = '_test_keys'"
                )


@pytestmark_dsn
def test_lookup_keys_fails_loud_on_a_layer_nobody_loaded() -> None:
    import dagster as dg

    spatial = SpatialResource(dsn=os.environ["SPATIAL_DSN"])
    spatial.ensure_schema()
    with pytest.raises(dg.Failure, match="no key layer"):
        spatial.lookup_keys("_definitely_not_loaded", ["x"])
