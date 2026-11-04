"""Pandera schema for allegheny_county / health / pool_inspections.

The raw landed file, as the legacy job old/payload/ac_hd/pool_inspections.py read it
(schema ACPoolInspectionsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "inspect_id": txt(),
        "facility_id": txt(),
        "facility_name": txt(),
        "facility_address": txt(),
        "facility_address_2": txt(),
        "muni_name": txt(),
        "city": txt(),
        "county": txt(),
        "state": txt(),
        "zip": txt(),
        "latitude": num(),
        "longitude": num(),
        "venue_type": txt(),
        "insp_date": txt(),
        "insp_end": txt(),
        "inspection_purpose": txt(),
        "inspection_passed": txt(),
        "inspect_number": num(),
        "insp_name": txt(),
        "free_cl_shallow": num(),
        "free_cl_deep": num(),
        "combined_cl_shallow": num(),
        "combined_cl_deep": num(),
        "br_shallow": num(),
        "br_deep": num(),
        "ph_shallow": txt(),
        "ph_deep": txt(),
        "cyanuric_acid": txt(),
        "turnover": num(),
        "enclosure": txt(),
        "main_drain_visible": txt(),
        "safety_equipment": txt(),
        "disinfectant_level": txt(),
        "ph_balance": txt(),
        "no_imminent_health_hazards": txt(),
        "abated_date": txt(),
    }
)
