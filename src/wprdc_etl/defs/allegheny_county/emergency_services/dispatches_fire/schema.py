"""Pandera schema for allegheny_county / emergency_services / dispatches_fire.

The raw landed file, as the legacy job old/payload/ac/911.py read it
(schema CallSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "call_id_hash": txt(),
        "service": txt(),
        "priority": txt(),
        "priority_desc": txt(),
        "call_quarter": txt(),
        "call_year": num(),
        "description_short": txt(),
        "city_code": txt(),
        "city_name": txt(),
        "geoid": txt(),
        "censusblockgroupcenter_x": num(),
        "censusblockgroupcenter_y": num(),
    }
)
