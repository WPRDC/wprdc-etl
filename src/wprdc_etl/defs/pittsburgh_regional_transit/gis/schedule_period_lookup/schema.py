"""Pandera schema for pittsburgh_regional_transit / gis / schedule_period_lookup.

The CSV export of PRT's "PRT Schedule Period Lookup - Historical" Hub table,
read from the live export on 2026-11-02 (the legacy SchedSchema's load_from
names, which it excluded `OBJECTID` from).
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "feed_version": txt(),
        "start_date": txt(),
        "end_date": txt(),
        "pick_type": txt(),
    }
)
