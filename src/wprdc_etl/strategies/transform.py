"""Transform strategies: a shared primitive library + a declarative runner.

Two ways to transform a dataset, and they compose:

  1. Declarative steps in the component YAML (the shared 80% — dates, trims,
     renames). Listed under `transforms:` and run in order by apply_declarative.
  2. A co-located transform.py for bespoke logic, run *after* the declarative
     steps. It imports these same primitives, so it composes rather than
     reimplements.

Primitive signature: fn(df, **params) -> df. Each guards missing columns so a
schema hiccup degrades gracefully instead of raising mid-batch.
"""

from __future__ import annotations

import re

import pandas as pd


# --------------------------------------------------------------------------
# Primitive library
# --------------------------------------------------------------------------
def iso_date(df, columns, fmt=None):
    """Normalize date columns to ISO 8601 (YYYY-MM-DD). Unparseable -> NaT."""
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce").dt.strftime(
                "%Y-%m-%d"
            )
    return df


def strip(df, columns=None):
    """Trim leading/trailing whitespace on the given text columns (or all
    object columns if none named)."""
    cols = columns or list(df.select_dtypes(include="object").columns)
    for c in cols:
        if c in df.columns:
            df[c] = df[c].str.strip()
    return df


def rename(df, mapping):
    """Rename columns via {old: new}."""
    return df.rename(columns=mapping)


def coerce_numeric(df, columns):
    """Coerce columns to numeric; non-numeric -> NaN."""
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def drop_nulls(df, columns):
    """Drop rows null in any of the named columns (e.g. a missing key)."""
    present = [c for c in columns if c in df.columns]
    return df.dropna(subset=present) if present else df


def fill_na(df, value, columns=None):
    """Fill NA with a constant, on named columns or the whole frame."""
    if columns:
        for c in columns:
            if c in df.columns:
                df[c] = df[c].fillna(value)
        return df
    return df.fillna(value)


def select(df, columns):
    """Keep only the named columns (those that exist), preserving order."""
    return df[[c for c in columns if c in df.columns]]


def drop_columns(df, columns):
    """Remove the named columns if present."""
    return df.drop(columns=[c for c in columns if c in df.columns])


def snake_case_columns(df):
    """Normalize column names to snake_case."""
    df.columns = [_to_snake(c) for c in df.columns]
    return df


def to_crs(df, epsg):
    """Reproject a GeoDataFrame to the given EPSG code (e.g. 4326 for web).
    No-op on a plain DataFrame with no geometry."""
    if hasattr(df, "to_crs"):
        return df.to_crs(epsg=epsg)
    return df


def _to_snake(name: str) -> str:
    s = re.sub(r"[\s\-]+", "_", str(name).strip())
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    return s.lower()


# name used in YAML -> primitive
PRIMITIVES = {
    "iso_date": iso_date,
    "strip": strip,
    "rename": rename,
    "coerce_numeric": coerce_numeric,
    "drop_nulls": drop_nulls,
    "fill_na": fill_na,
    "select": select,
    "drop_columns": drop_columns,
    "snake_case_columns": snake_case_columns,
    "to_crs": to_crs,
}


# --------------------------------------------------------------------------
# Declarative runner + config-time validation
# --------------------------------------------------------------------------
def validate_steps(steps) -> None:
    """Fail-loud at component load (dg check / dg dev) if a step names an
    unknown op — rather than at 3am when the asset runs."""
    for i, step in enumerate(steps or []):
        if "op" not in step:
            raise ValueError(f"transform step {i} is missing 'op'")
        if step["op"] not in PRIMITIVES:
            known = ", ".join(sorted(PRIMITIVES))
            raise ValueError(
                f"transform step {i}: unknown op {step['op']!r} (known: {known})"
            )


def apply_declarative(df, steps):
    """Run the declarative steps in order. Works on a copy so the IO-manager
    input isn't mutated."""
    df = df.copy()
    for step in steps:
        fn = PRIMITIVES[step["op"]]
        params = {k: v for k, v in step.items() if k != "op"}
        df = fn(df, **params)
    return df
