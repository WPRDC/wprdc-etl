"""Accumulate strategy: fold a run's frame into the dataset's canonical table.

Why this exists: `publish: replace` hands DataPusher+ a COMPLETE file, but an
`ingest: incremental` run only ever holds a delta. The full table therefore has
to live somewhere we own — the landing zone's `_state/` prefix — and be rebuilt
here on the way through. See LandingZoneResource.read_table_state.

It is also useful with a snapshot source whose target is cumulative: the county
ships one complete file per year and the published dataset spans all years
(`delinquent_all`, cumulative crashes). That case sets `accumulate: true`
explicitly; the incremental+replace case implies it.

Kept out of the component so the merge is testable against plain frames with no
resources involved.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def merge(
    prior: pd.DataFrame | None,
    delta: pd.DataFrame,
    primary_key: list[str],
    carry_forward: list[str] | None = None,
) -> pd.DataFrame:
    """Return the canonical table after folding `delta` into `prior`.

    * First run (`prior` is None or empty) — the delta IS the table.
    * The delta wins on a primary-key collision, so a corrected row replaces the
      one already stored.
    * Columns are unioned, prior order first: a column the delta dropped is kept
      (NaN for the new rows), a column it added is NaN for the old ones. This
      keeps the emitted CSV's header stable run to run, which matters because
      DataPusher+ reloads against an existing table.

    Rows are NOT deleted — a delta can't express a deletion. Same limitation the
    DataStore upsert path has.

    `carry_forward` columns are the exception to "the delta wins": where the
    incoming value is missing, the stored row's value is kept. For a column
    the source no longer supplies but the table must keep (PLI permits'
    `owner_name`, which only the stored table still has).
    """
    if not primary_key:
        raise ValueError("accumulate requires ckan.primary_key — it's the merge key")

    _require_columns(delta, primary_key, "the incoming frame")
    _reject_null_keys(delta, primary_key)

    if prior is None or prior.empty:
        return delta.reset_index(drop=True)

    _require_columns(prior, primary_key, "the stored canonical table")
    _require_comparable_keys(prior, delta, primary_key)

    import pandas as pd

    # Prior order first, then whatever the delta added, so the header only ever
    # grows rightward.
    columns = list(prior.columns) + [c for c in delta.columns if c not in prior.columns]
    combined = pd.concat(
        [prior.reindex(columns=columns), delta.reindex(columns=columns)],
        ignore_index=True,
    )
    # keep="last" => the delta's copy of a key wins over the stored one.
    merged = combined.drop_duplicates(subset=primary_key, keep="last").reset_index(
        drop=True
    )
    if carry_forward:
        merged = _carry_forward(merged, prior, primary_key, carry_forward)
    return merged


def _carry_forward(
    merged: pd.DataFrame,
    prior: pd.DataFrame,
    primary_key: list[str],
    columns: list[str],
) -> pd.DataFrame:
    """Fill `columns` in `merged` from `prior` (by key) where merged is missing."""
    missing = [c for c in columns if c not in prior.columns]
    if missing:
        raise ValueError(
            f"accumulate: carry_forward column(s) {missing} are not in the "
            "stored table, so there is nothing to carry forward"
        )
    stored = prior.drop_duplicates(subset=primary_key, keep="last").set_index(
        primary_key
    )[columns]
    keyed = merged.set_index(primary_key)
    for c in columns:
        keyed[c] = keyed[c].fillna(stored[c].reindex(keyed.index))
    return keyed.reset_index()[list(merged.columns)]


def _require_columns(df: pd.DataFrame, primary_key: list[str], what: str) -> None:
    """Checks either side of the merge, so `what` names which one for the error."""
    missing = [c for c in primary_key if c not in df.columns]
    if missing:
        raise ValueError(
            f"accumulate: primary key column(s) {missing} are not in {what} "
            f"(has {sorted(df.columns)})"
        )


def _key_kind(series: pd.Series) -> str:
    import pandas as pd

    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "text"


def _require_comparable_keys(
    prior: pd.DataFrame, delta: pd.DataFrame, primary_key: list[str]
) -> None:
    """Refuse to merge when a key column is a different KIND on each side.

    drop_duplicates never matches the text "1234" against the number 1234, or
    a datetime against its ISO string, so such a merge keeps both copies of
    every row and the table doubles without an error. That is exactly what a
    seeded table (`bin/seed-accumulator`) and a pipeline that types the key
    differently would do on their first merge.
    """
    clashes = {
        c: (_key_kind(prior[c]), _key_kind(delta[c]))
        for c in primary_key
        if _key_kind(prior[c]) != _key_kind(delta[c])
    }
    if clashes:
        detail = ", ".join(
            f"{c}: stored {a}, incoming {b}" for c, (a, b) in clashes.items()
        )
        raise ValueError(
            f"accumulate: primary key column(s) differ in kind ({detail}), so no "
            "row would ever match its stored copy. Coerce the incoming column "
            "(coerce_text / coerce_numeric / to_datetime) to match."
        )


def _reject_null_keys(delta: pd.DataFrame, primary_key: list[str]) -> None:
    """A null in the merge key is fatal, not a warning.

    drop_duplicates compares NaN != NaN, so a null key never collapses against
    anything — the row would be re-appended on every single run and the table
    would grow without bound. The old rocket-etl liens job hit exactly this and
    guarded against it by hand.
    """
    nulls = [c for c in primary_key if delta[c].isna().any()]
    if nulls:
        counts = {c: int(delta[c].isna().sum()) for c in nulls}
        raise ValueError(
            f"accumulate: null values in primary key column(s) {counts}. "
            "Null keys never dedupe, so these rows would be re-appended every "
            "run. Fix them in a transform step (drop_nulls or fill_na) first."
        )
