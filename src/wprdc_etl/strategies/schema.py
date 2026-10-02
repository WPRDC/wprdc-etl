"""Shared schema vocabulary: column builders + reusable column groups.

Mirrors strategies/transform.py — the building blocks are shared here, while
each dataset's full column list stays in its own co-located schema.py.

Each builder returns a FRESH pandera Column. Never share a single Column
instance across multiple keys: pandera assigns the column name during
validation, so a reused instance can collide.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
import pandera.pandas as pa


# --------------------------------------------------------------------------
# Column builders
# --------------------------------------------------------------------------
def txt(**checks: Any) -> pa.Column:
    """Nullable text column."""
    return pa.Column(str, nullable=True, **checks)


def num(**checks: Any) -> pa.Column:
    """Nullable numeric column (coerced at the schema level)."""
    return pa.Column(float, nullable=True, **checks)


def key() -> pa.Column:
    """Required, non-null identifier column (e.g. a primary key)."""
    return pa.Column(str, nullable=False, checks=pa.Check.str_length(min_value=1))


def ge0() -> pa.Column:
    """Nullable numeric that must be >= 0 (prices, areas, counts)."""
    return num(checks=pa.Check.ge(0))


def ranged(low: float, high: float) -> pa.Column:
    """Nullable numeric constrained to [low, high]."""
    return num(checks=pa.Check.in_range(low, high))


def coded(values: Iterable[str]) -> pa.Column:
    """Nullable text constrained to a small set of codes."""
    return txt(checks=pa.Check.isin(list(values)))


def year(low: int = 1700) -> pa.Column:
    """Nullable year in [low, current_year + 1]."""
    return ranged(low, pd.Timestamp.now().year + 1)


def frame(
    columns: dict[str, pa.Column], *, strict: bool = False, coerce: bool = True
) -> pa.DataFrameSchema:
    """Build a DataFrameSchema with the project defaults: superset-friendly
    (strict=False allows extra columns) and coercing CSV values to dtype.
    Columns are required by pandera default, so the file must be a superset."""
    return pa.DataFrameSchema(columns=columns, strict=strict, coerce=coerce)


# --------------------------------------------------------------------------
# Reusable column groups (spread into a schema's columns dict with **)
# --------------------------------------------------------------------------
def geo_point_columns(lat: str = "lat", lng: str = "lng") -> dict[str, pa.Column]:
    """A lat/lng pair with valid coordinate ranges. Names are configurable
    since sources vary (lat/lon, latitude/longitude, y/x)."""
    return {
        lat: ranged(-90, 90),
        lng: ranged(-180, 180),
    }
