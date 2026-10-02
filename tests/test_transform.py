"""Unit tests for the shared transform primitives that need their own coverage."""

from collections.abc import Sequence

import pandas as pd
import pytest

from wprdc_etl.strategies.transform import (
    apply_declarative,
    coerce_text,
    join_geometry,
    reverse_geocode,
    validate_steps,
)


def test_coerce_text_ints_have_no_trailing_zero() -> None:
    df = pd.DataFrame({"id": [1011, 88627156], "keep": [1.5, 2.5]})
    out = coerce_text(df, ["id"])
    assert list(out["id"]) == ["1011", "88627156"]
    assert out["id"].dtype == "string"
    assert out["keep"].dtype == "float64"  # untouched


def test_coerce_text_compacts_floats_and_preserves_missing() -> None:
    df = pd.DataFrame({"tract": [1011.0, 103.02, None]})
    out = coerce_text(df, ["tract"])
    assert out["tract"][0] == "1011"
    assert out["tract"][1] == "103.02"
    assert pd.isna(out["tract"][2])


def test_coerce_text_missing_column_is_a_noop() -> None:
    df = pd.DataFrame({"a": [1]})
    pd.testing.assert_frame_equal(coerce_text(df, ["nope"]), df)


def test_coerce_text_is_wired_into_the_declarative_runner() -> None:
    validate_steps([{"op": "coerce_text", "columns": ["ward"]}])  # no raise
    out = apply_declarative(
        pd.DataFrame({"ward": [19, 4]}),
        [{"op": "coerce_text", "columns": ["ward"]}],
    )
    assert list(out["ward"]) == ["19", "4"]


# --------------------------------------------------------------------------
# reverse_geocode
# --------------------------------------------------------------------------
class _FakeSpatial:
    """Stands in for SpatialResource: canned point -> region answers, and a
    record of what was asked so we can assert the query is deduplicated."""

    def __init__(
        self,
        hits: (
            dict[tuple[float, float], dict[str, tuple[str, str | None]]] | None
        ) = None,
    ) -> None:
        self.hits = hits or {}
        self.calls: list[tuple[list[tuple[float, float]], list[str]]] = []

    def locate(
        self, points: Sequence[tuple[float, float]], layers: Sequence[str]
    ) -> dict[tuple[float, float], dict[str, tuple[str, str | None]]]:
        self.calls.append((list(points), list(layers)))
        return {p: self.hits[p] for p in points if p in self.hits}


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name": ["fountain", "spray park", "in the river", "duplicate"],
            "latitude": [40.4418, 40.4383, 40.52, 40.4418],
            "longitude": [-80.0089, -79.9436, -80.14, -80.0089],
        }
    )


_HITS = {
    (-80.0089, 40.4418): {
        "neighborhood": ("64", "Central Business District"),
        "public_works_division": ("1", None),
    },
    (-79.9436, 40.4383): {
        "neighborhood": ("76", "Squirrel Hill South"),
        "public_works_division": ("3", None),
    },
}


def test_reverse_geocode_attaches_labels_and_leaves_misses_null() -> None:
    spatial = _FakeSpatial(_HITS)
    out = reverse_geocode(
        _frame(),
        lat="latitude",
        lng="longitude",
        regions=["neighborhood"],
        spatial=spatial,
    )
    assert list(out["neighborhood"][:2]) == [
        "Central Business District",
        "Squirrel Hill South",
    ]
    assert pd.isna(out["neighborhood"][2])  # river point matched nothing
    assert out["neighborhood"][3] == "Central Business District"
    assert out["neighborhood"].dtype == "string"


def test_reverse_geocode_queries_distinct_points_only() -> None:
    spatial = _FakeSpatial(_HITS)
    reverse_geocode(
        _frame(),
        lat="latitude",
        lng="longitude",
        regions=["neighborhood"],
        spatial=spatial,
    )
    points, layers = spatial.calls[0]
    assert len(points) == 3  # four rows, three distinct locations
    assert layers == ["neighborhood"]


def test_reverse_geocode_value_id_and_column_rename() -> None:
    spatial = _FakeSpatial(_HITS)
    out = reverse_geocode(
        _frame(),
        lat="latitude",
        lng="longitude",
        regions=[{"layer": "neighborhood", "column": "hood_no", "value": "id"}],
        spatial=spatial,
    )
    assert list(out["hood_no"][:2]) == ["64", "76"]
    assert "neighborhood" not in out.columns


def test_reverse_geocode_falls_back_to_the_code_when_a_layer_has_no_label() -> None:
    spatial = _FakeSpatial(_HITS)
    out = reverse_geocode(
        _frame(),
        lat="latitude",
        lng="longitude",
        regions=["public_works_division"],  # default value: name
        spatial=spatial,
    )
    assert list(out["public_works_division"][:2]) == ["1", "3"]


def test_reverse_geocode_missing_coordinate_column_degrades_gracefully() -> None:
    spatial = _FakeSpatial(_HITS)
    df = pd.DataFrame({"name": ["a"]})
    out = reverse_geocode(
        df, lat="latitude", lng="longitude", regions=["neighborhood"], spatial=spatial
    )
    assert pd.isna(out["neighborhood"][0])
    assert spatial.calls == []  # never hit the database


def test_reverse_geocode_is_wired_into_the_declarative_runner() -> None:
    step = {
        "op": "reverse_geocode",
        "lat": "latitude",
        "lng": "longitude",
        "regions": ["neighborhood"],
    }
    validate_steps([step])  # no raise
    spatial = _FakeSpatial(_HITS)
    out = apply_declarative(
        _frame(), [step], deps={"spatial": spatial, "context": None}
    )
    assert out["neighborhood"][0] == "Central Business District"


def test_reverse_geocode_without_its_deps_is_a_clear_error() -> None:
    step = {
        "op": "reverse_geocode",
        "lat": "latitude",
        "lng": "longitude",
        "regions": ["neighborhood"],
    }
    with pytest.raises(ValueError, match="needs 'spatial'"):
        apply_declarative(_frame(), [step])


# --------------------------------------------------------------------------
# join_geometry
# --------------------------------------------------------------------------
class _FakeKeys:
    """Stands in for SpatialResource.lookup_keys: canned key -> (lng, lat)."""

    def __init__(self, hits: dict[str, tuple[float, float]]) -> None:
        self.hits = hits
        self.calls: list[tuple[str, list[str]]] = []

    def lookup_keys(self, layer: str, keys) -> dict[str, tuple[float, float]]:
        keys = sorted(set(keys))
        self.calls.append((layer, keys))
        return {k: self.hits[k] for k in keys if k in self.hits}


_POINTS = {"SSAP450843": (-79.99, 40.44), "SSAP7": (-80.01, 40.45)}


def test_join_geometry_formats_keys_and_attaches_coordinates() -> None:
    spatial = _FakeKeys(_POINTS)
    df = pd.DataFrame({"ADDRESS_ID": ["450843", "7", "999", None]})
    out = join_geometry(
        df, "address_point", "ADDRESS_ID", key_format="SSAP{}", spatial=spatial
    )
    assert list(out["longitude"][:2]) == [-79.99, -80.01]
    assert list(out["latitude"][:2]) == [40.44, 40.45]
    # A key the layer lacks, and a row with no key, publish without coordinates.
    assert out["latitude"][2:].isna().all() and out["longitude"][2:].isna().all()


def test_join_geometry_matches_float_keys_exactly() -> None:
    """A CSV id column with a blank reads as float — 450843.0 must still join,
    and must not be rendered as scientific notation the way `:g` would."""
    spatial = _FakeKeys({"SSAP1234567": (-80.0, 40.5)})
    df = pd.DataFrame({"ADDRESS_ID": [1234567.0, float("nan")]})
    out = join_geometry(df, "address_point", "ADDRESS_ID", "SSAP{}", spatial=spatial)
    assert out["latitude"][0] == 40.5
    assert spatial.calls == [("address_point", ["SSAP1234567"])]


def test_join_geometry_queries_distinct_keys_only() -> None:
    spatial = _FakeKeys(_POINTS)
    df = pd.DataFrame({"ADDRESS_ID": ["7", "7", "7", "450843"]})
    join_geometry(df, "address_point", "ADDRESS_ID", "SSAP{}", spatial=spatial)
    assert spatial.calls == [("address_point", ["SSAP450843", "SSAP7"])]


def test_join_geometry_missing_key_column_degrades_gracefully() -> None:
    spatial = _FakeKeys(_POINTS)
    out = join_geometry(
        pd.DataFrame({"other": [1]}), "address_point", "ADDRESS_ID", spatial=spatial
    )
    assert out["latitude"].isna().all() and out["longitude"].isna().all()
    assert spatial.calls == []


def test_join_geometry_is_wired_into_the_declarative_runner() -> None:
    step = {
        "op": "join_geometry",
        "layer": "address_point",
        "key": "ADDRESS_ID",
        "key_format": "SSAP{}",
    }
    validate_steps([step])  # no raise
    out = apply_declarative(
        pd.DataFrame({"ADDRESS_ID": ["7"]}),
        [step],
        deps={"spatial": _FakeKeys(_POINTS), "context": None},
    )
    assert out["latitude"][0] == 40.45


@pytest.mark.parametrize("fmt", ["SSAP", "{}{}", "{0}", "SSAP{x}"])
def test_validate_steps_rejects_a_bad_key_format(fmt: str) -> None:
    step = {"op": "join_geometry", "layer": "l", "key": "k", "key_format": fmt}
    with pytest.raises(ValueError, match="key_format"):
        validate_steps([step])


# --------------------------------------------------------------------------
# load-time step validation
# --------------------------------------------------------------------------
def test_validate_steps_rejects_a_misspelled_param() -> None:
    with pytest.raises(ValueError, match="accepted params"):
        validate_steps([{"op": "coerce_text", "column": ["ward"]}])  # column/columns


def test_validate_steps_rejects_a_missing_required_param() -> None:
    with pytest.raises(ValueError, match="accepted params"):
        validate_steps([{"op": "reverse_geocode", "lat": "latitude"}])


def test_validate_steps_rejects_a_malformed_region_entry() -> None:
    with pytest.raises(ValueError, match="unknown key"):
        validate_steps(
            [
                {
                    "op": "reverse_geocode",
                    "lat": "latitude",
                    "lng": "longitude",
                    "regions": [{"layer": "ward", "colum": "w"}],
                }
            ]
        )


def test_validate_steps_rejects_an_unknown_value_mode() -> None:
    with pytest.raises(ValueError, match="'name' or 'id'"):
        validate_steps(
            [
                {
                    "op": "reverse_geocode",
                    "lat": "latitude",
                    "lng": "longitude",
                    "regions": [{"layer": "ward", "value": "code"}],
                }
            ]
        )
