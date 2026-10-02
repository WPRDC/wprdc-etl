"""Bespoke transform for city_of_pittsburgh / water_features.

Runs AFTER the declarative steps. Two things the shared primitives can't say:

* `pli_division` has no boundary layer anywhere on WPRDC, but it is the ward:
  the two agree on all 232 rows live in CKAN as of Oct 2026. So it is copied
  from `ward` rather than dropped.
* The Cartegraph export carries 47 columns and CKAN publishes 17. Keeping
  exactly the published set, in the published order, means a replace never
  meets a column change (which the loader blocks without `ckan.rebuild`).
  `select` can't do it from the YAML: it runs before pli_division exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from wprdc_etl.components.tabular_pipeline import TabularPipeline

# The live DataStore columns, in their published order.
PUBLISHED_COLUMNS = [
    "id",
    "name",
    "control_type",
    "feature_type",
    "inactive",
    "make",
    "image",
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


def transform(df: pd.DataFrame, cfg: TabularPipeline | None = None) -> pd.DataFrame:
    if "ward" in df.columns:
        df["pli_division"] = df["ward"]
    return df[[c for c in PUBLISHED_COLUMNS if c in df.columns]]
