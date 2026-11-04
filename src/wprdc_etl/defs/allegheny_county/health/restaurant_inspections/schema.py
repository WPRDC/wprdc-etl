"""Pandera schema for allegheny_county / health / restaurant_inspections.

The raw landed file, as the legacy job old/payload/ac_hd/restaurant_inspections.py read it
(schema InspectionsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "inspection_id": txt(),
        "placard_desc": txt(),
        "facility_name": txt(),
        "bus_st_date": txt(),
        "facility_type": txt(),
        "category": txt(),
        "nonprofit": txt(),
        "num": txt(),
        "street": txt(),
        "city": txt(),
        "state": txt(),
        "zip": txt(),
        "municipal": txt(),
        "inspect_dt": txt(),
        "inspection_purpose": txt(),
        "reinspection_need": txt(),
        "permit_status": txt(),
    }
)
