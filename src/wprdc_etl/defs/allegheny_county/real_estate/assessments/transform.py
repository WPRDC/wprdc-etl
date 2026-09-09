"""Bespoke transform for allegheny_county / real_estate / assessments.

Runs AFTER the declarative steps in defs.yaml (dates, numeric coercion,
strip, drop_nulls). Only keep here what the shared primitives can't express.

Below: derive a single normalized `full_address` from the county's separate
address parts — dataset-specific composition, not a generic op.
"""

from __future__ import annotations

import pandas as pd

# CONFIRM against the real extract header.
ADDRESS_PARTS = ["PROPERTYHOUSENUM", "PROPERTYADDRESS", "PROPERTYCITY", "PROPERTYZIP"]


def transform(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    parts = [c for c in ADDRESS_PARTS if c in df.columns]
    if parts:
        df["full_address"] = (
            df[parts]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.title()
        )
    return df
