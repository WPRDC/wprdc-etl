"""Pandera schema for allegheny_county / health / plumbing_inspections.

The raw landed file, as the legacy job old/payload/ac_hd/plumbing_inspections.py read it
(schema PlumbingInspectionsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "inspect_id": txt(),
        "permitid": txt(),
        "inspect_date": txt(),
        "streetaddress": txt(),
        "city": txt(),
        "state": txt(),
        "zip_code": txt(),
        "municipal": txt(),
        "building_type": txt(),
        "plumber_name": txt(),
        "final_insp_yn": txt(),
        "storm_drain": txt(),
        "building_drain": txt(),
        "water_service": txt(),
        "storm_sewer": txt(),
        "rough_above_ground": txt(),
        "water_heater": txt(),
        "building_sewer": txt(),
        "water_distribution": txt(),
        "indirect_waste": txt(),
    }
)
