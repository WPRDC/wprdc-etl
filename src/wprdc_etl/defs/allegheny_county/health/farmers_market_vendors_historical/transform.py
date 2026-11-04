"""Add `year`: the legacy cumulative job stamped each row with the year of
the file it came from, which is what keeps one market's (or vendor's) rows
from different seasons apart in the merge key."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd


def transform(df: pd.DataFrame, cfg=None) -> pd.DataFrame:
    # Same year the source path's {now:%Y} resolved to.
    df.insert(0, "year", dt.datetime.now(ZoneInfo("America/New_York")).year)
    return df
