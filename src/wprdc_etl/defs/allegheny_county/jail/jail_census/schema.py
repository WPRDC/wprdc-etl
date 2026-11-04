"""Pandera schema for allegheny_county / jail / jail_census.

The raw landed file, as the legacy job old/payload/ac/jail.py read it
(schema JailCensusSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "recordid": num(),
        "census_date": txt(),
        "gender": txt(),
        "race": txt(),
        "age_at_booking_yrs": num(),
        "age_at_census_yrs": num(),
    }
)
