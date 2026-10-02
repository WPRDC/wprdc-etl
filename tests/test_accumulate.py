"""Merge semantics for the cumulative canonical table, plus the mode derivation
that decides whether a dataset accumulates at all.

The merge runs on plain frames with no resources, which is the whole reason it
lives in strategies/accumulate.py rather than inside the component.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from wprdc_etl.components._common import resolve_modes
from wprdc_etl.strategies.accumulate import merge


def frame(rows):
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# merge
# --------------------------------------------------------------------------
def test_first_run_delta_is_the_table():
    delta = frame([{"id": "a", "v": 1}, {"id": "b", "v": 2}])
    out = merge(None, delta, ["id"])
    assert out.to_dict("records") == delta.to_dict("records")


def test_empty_prior_treated_as_first_run():
    delta = frame([{"id": "a", "v": 1}])
    out = merge(frame([]), delta, ["id"])
    assert len(out) == 1


def test_new_keys_append():
    prior = frame([{"id": "a", "v": 1}])
    delta = frame([{"id": "b", "v": 2}])
    out = merge(prior, delta, ["id"])
    assert sorted(out["id"]) == ["a", "b"]


def test_delta_wins_on_collision():
    prior = frame([{"id": "a", "v": 1}])
    delta = frame([{"id": "a", "v": 99}])
    out = merge(prior, delta, ["id"])
    assert len(out) == 1
    assert out.loc[0, "v"] == 99


def test_composite_primary_key():
    prior = frame([{"k1": "a", "k2": 1, "v": "old"}, {"k1": "a", "k2": 2, "v": "keep"}])
    delta = frame([{"k1": "a", "k2": 1, "v": "new"}])
    out = merge(prior, delta, ["k1", "k2"])
    assert len(out) == 2
    assert set(out["v"]) == {"new", "keep"}


def test_column_added_by_delta_is_null_for_prior_rows():
    prior = frame([{"id": "a", "v": 1}])
    delta = frame([{"id": "b", "v": 2, "extra": "x"}])
    out = merge(prior, delta, ["id"])
    assert list(out.columns) == ["id", "v", "extra"]  # prior order, then new
    assert pd.isna(out.loc[out["id"] == "a", "extra"]).all()


def test_column_dropped_by_delta_is_retained():
    prior = frame([{"id": "a", "v": 1, "legacy": "keep"}])
    delta = frame([{"id": "b", "v": 2}])
    out = merge(prior, delta, ["id"])
    assert "legacy" in out.columns
    assert out.loc[out["id"] == "a", "legacy"].iloc[0] == "keep"
    assert pd.isna(out.loc[out["id"] == "b", "legacy"]).all()


def test_idempotent_on_replay():
    """Re-merging the same delta must not change the table."""
    prior = frame([{"id": "a", "v": 1}])
    delta = frame([{"id": "b", "v": 2}])
    once = merge(prior, delta, ["id"])
    twice = merge(once, delta, ["id"])
    assert twice.to_dict("records") == once.to_dict("records")


def test_never_shrinks():
    prior = frame([{"id": c, "v": i} for i, c in enumerate("abcde")])
    delta = frame([{"id": "a", "v": 99}])
    assert len(merge(prior, delta, ["id"])) == len(prior)


def test_null_primary_key_is_fatal():
    """Null keys never dedupe, so they'd be re-appended on every run."""
    delta = frame([{"id": None, "v": 1}])
    with pytest.raises(ValueError, match="null values in primary key"):
        merge(None, delta, ["id"])


def test_missing_primary_key_column_is_fatal():
    delta = frame([{"other": 1}])
    with pytest.raises(ValueError, match="not in the incoming frame"):
        merge(None, delta, ["id"])


def test_empty_primary_key_is_fatal():
    with pytest.raises(ValueError, match="requires ckan.primary_key"):
        merge(None, frame([{"id": "a"}]), [])


# --------------------------------------------------------------------------
# resolve_modes
# --------------------------------------------------------------------------
def cfg(**kw):
    return SimpleNamespace(
        ingest=kw.pop("ingest", "snapshot"),
        publish=kw.pop("publish", None),
        accumulate=kw.pop("accumulate", None),
        **kw,
    )


def test_snapshot_defaults_to_replace_without_accumulating():
    assert resolve_modes(cfg(ingest="snapshot")) == ("replace", False)


def test_incremental_defaults_to_upsert_without_accumulating():
    assert resolve_modes(cfg(ingest="incremental")) == ("upsert", False)


def test_incremental_plus_replace_forces_accumulate():
    """A delta can't be published as a whole file, so the table must be kept."""
    assert resolve_modes(cfg(ingest="incremental", publish="replace")) == (
        "replace",
        True,
    )


def test_snapshot_can_opt_into_accumulate():
    """Complete file per year, cumulative target — delinquent_all, crashes."""
    assert resolve_modes(cfg(ingest="snapshot", accumulate=True)) == ("replace", True)


def test_explicit_accumulate_false_overrides_the_forced_case():
    assert resolve_modes(
        cfg(ingest="incremental", publish="replace", accumulate=False)
    ) == ("replace", False)
