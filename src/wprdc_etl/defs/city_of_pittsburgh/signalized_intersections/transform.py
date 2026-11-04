"""Bespoke transform for city_of_pittsburgh / signalized_intersections.

Runs AFTER the declarative steps, as water_features' does:

* `pli_division` has no boundary layer on WPRDC; the legacy job filled it
  from the ward (old/payload/pgh/cartegraph_dpw.py: `data['pli_division'] =
  geo_data['pittsburgh_ward']`), so it is copied from `ward`.
* Keep exactly the published columns, in the published order: `select`
  can't, from the YAML, because it runs before pli_division exists.
"""

from __future__ import annotations

import pandas as pd

PUBLISHED_COLUMNS = [
    "id",
    "name",
    "description",
    "flash_time",
    "flash_yellow",
    "operation_type",
    "neighborhood",
    "council_district",
    "ward",
    "tract",
    "public_works_division",
    "pli_division",
    "police_zone",
    "fire_zone",
    "latitude",
    "longitude",
]


def transform(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    if "ward" in df.columns:
        df["pli_division"] = df["ward"]
    return df[[c for c in PUBLISHED_COLUMNS if c in df.columns]]
