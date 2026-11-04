"""Pandera schema for allegheny_county / health / air_quality_hourly.

The raw landed file, as the legacy job old/payload/ac_hd/air_quality.py read it
(schema AQIHourlySchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "datetimeest": txt(),
        "sitename": txt(),
        "parametername": txt(),
        "isvalid": txt(),
        "reportvalue": txt(),
        "reportedunitname": txt(),
        "reportedunitdescription": txt(),
        "highestflag": txt(),
        "aqsparametercategory": txt(),
    }
)
