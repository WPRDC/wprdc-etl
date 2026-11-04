"""Bespoke transform for pwsa / gis / inlets: the legacy InletsSchema's
`normalize_owner` hook, which publishes PWSA's `PENNDOT` as `PennDOT`."""

from __future__ import annotations

import pandas as pd


def transform(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    df["owner"] = df["owner"].replace({"PENNDOT": "PennDOT"})
    return df
