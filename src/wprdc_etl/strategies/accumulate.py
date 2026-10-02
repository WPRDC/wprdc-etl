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
    """
    if not primary_key:
        raise ValueError("accumulate requires ckan.primary_key — it's the merge key")

    _require_columns(delta, primary_key, "the incoming frame")
    _reject_null_keys(delta, primary_key)

    if prior is None or prior.empty:
        return delta.reset_index(drop=True)

    _require_columns(prior, primary_key, "the stored canonical table")

    import pandas as pd

    # Prior order first, then whatever the delta added, so the header only ever
    # grows rightward.
    columns = list(prior.columns) + [c for c in delta.columns if c not in prior.columns]
    combined = pd.concat(
        [prior.reindex(columns=columns), delta.reindex(columns=columns)],
        ignore_index=True,
    )
    # keep="last" => the delta's copy of a key wins over the stored one.
    return combined.drop_duplicates(subset=primary_key, keep="last").reset_index(
        drop=True
    )


def _require_columns(df: pd.DataFrame, primary_key: list[str], what: str) -> None:
    """Checks either side of the merge, so `what` names which one for the error."""
    missing = [c for c in primary_key if c not in df.columns]
    if missing:
        raise ValueError(
            f"accumulate: primary key column(s) {missing} are not in {what} "
            f"(has {sorted(df.columns)})"
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
