"""Unit tests for the schema_ok drift signal.

The comparison is a pure function so it can be tested without S3; the state it
compares against is dataset-scoped (not per-partition), which is the whole
reason the flag works at all — see LandingZoneResource.read_schema_state.
"""

from wprdc_etl.components.tabular_pipeline import _column_fingerprint, _drift_report
from wprdc_etl.resources import LandingZoneResource


def _state(columns: list[str]) -> dict[str, object]:
    """A recorded baseline, shaped like what write_schema_state persists."""
    return {"columns": columns, "schema_hash": _column_fingerprint(columns)}


def test_first_check_has_no_baseline_so_nothing_has_drifted() -> None:
    report = _drift_report({"id", "name"}, None)
    assert report["schema_drifted"] is False
    assert report["columns_added"] == []
    assert report["columns_removed"] == []
    assert report["column_hash"]  # still fingerprinted, to become the baseline


def test_unchanged_columns_do_not_drift() -> None:
    report = _drift_report({"id", "name"}, _state(["id", "name"]))
    assert report["schema_drifted"] is False


def test_reordered_columns_do_not_drift() -> None:
    """The fingerprint is over the column SET, not the file's column order."""
    assert _column_fingerprint(["b", "a"]) == _column_fingerprint(["a", "b"])
    report = _drift_report({"b", "a"}, _state(["a", "b"]))
    assert report["schema_drifted"] is False


def test_an_added_column_drifts_and_is_named() -> None:
    report = _drift_report({"id", "name", "ward"}, _state(["id", "name"]))
    assert report["schema_drifted"] is True
    assert report["columns_added"] == ["ward"]
    assert report["columns_removed"] == []


def test_a_dropped_column_drifts_and_is_named() -> None:
    """The water_features case: the source stopped shipping two columns."""
    report = _drift_report(
        {"id", "name"}, _state(["id", "name", "fire_zone", "public_works_division"])
    )
    assert report["schema_drifted"] is True
    assert report["columns_removed"] == ["fire_zone", "public_works_division"]
    assert report["columns_added"] == []


def test_a_renamed_column_shows_as_both_sides() -> None:
    report = _drift_report({"id", "hood"}, _state(["id", "neighborhood"]))
    assert report["schema_drifted"] is True
    assert report["columns_added"] == ["hood"]
    assert report["columns_removed"] == ["neighborhood"]


def test_a_baseline_without_a_column_list_still_reports_the_flag() -> None:
    """Older state (or a hand-written one) may carry only the hash."""
    report = _drift_report({"id", "name"}, {"schema_hash": "stale-hash"})
    assert report["schema_drifted"] is True
    assert report["columns_added"] == []  # nothing to attribute it to
    assert report["columns_removed"] == []


# --------------------------------------------------------------------------
# state keys
# --------------------------------------------------------------------------
def test_schema_state_has_its_own_key_next_to_the_watermark() -> None:
    landing = LandingZoneResource()
    watermark = landing._state_key("city_of_pittsburgh", "water_features")
    schema = landing._state_key(
        "city_of_pittsburgh", "water_features", filename="schema.json"
    )
    assert watermark == "_state/city_of_pittsburgh/water_features/watermark.json"
    assert schema == "_state/city_of_pittsburgh/water_features/schema.json"


def test_schema_state_key_includes_the_department_tier() -> None:
    landing = LandingZoneResource()
    assert (
        landing._state_key(
            "allegheny_county", "assessments", "real_estate", filename="schema.json"
        )
        == "_state/allegheny_county/real_estate/assessments/schema.json"
    )
