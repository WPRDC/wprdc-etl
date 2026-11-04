"""Bespoke transform for city_of_pittsburgh / requests_311.

Runs AFTER the declarative steps. Ports what the legacy RequestsSchema
(old/payload/pgh/311_v3.py) did in its pre_load / pre_dump hooks:

* `unique_id` = case number + "-" + the two-digit year the case was
  created (`99-00082323-16`). Case numbers alone repeat across years; this is
  the merge key, and the live table's.
* A 0,0 coordinate pair is a missing location, not the Gulf of Guinea.
* `final_obfuscation`: unless the city geocoded the request EXACTLY, its
  `street` and `street_id` are blanked. That is a privacy rule, not a cleanup
  — keep it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from wprdc_etl.components.tabular_pipeline import TabularPipeline

# The live DataStore columns, in their published order.
PUBLISHED_COLUMNS = [
    "case_number",
    "status",
    "case_owner",
    "subject",
    "subject_code",
    "legacy_subject",
    "created_date_et",
    "created_date_utc",
    "last_modified_date_et",
    "last_modified_date_utc",
    "closed_date_et",
    "closed_date_utc",
    "origin",
    "street",
    "street_id",
    "city",
    "neighborhood",
    "census_tract",
    "council_district",
    "ward",
    "police_zone",
    "latitude",
    "longitude",
    "geo_accuracy",
    "unique_id",
]


def transform(df: pd.DataFrame, cfg: TabularPipeline | None = None) -> pd.DataFrame:
    created = df["created_date_et"]
    year = created.dt.year.mod(100).astype("Int64").astype("string").str.zfill(2)
    df["unique_id"] = (df["case_number"] + "-" + year).astype("string")
    # A row with no case number or creation date has no key; it would never
    # dedupe and the accumulator refuses null keys outright.
    df = df[df["unique_id"].notna()].copy()

    zero = (df["latitude"].isin(["0", "0.0"])) & (df["longitude"].isin(["0", "0.0"]))
    df.loc[zero, ["latitude", "longitude"]] = pd.NA

    inexact = df["geo_accuracy"].fillna("") != "EXACT"
    df.loc[inexact, ["street", "street_id"]] = pd.NA

    return df[PUBLISHED_COLUMNS]
