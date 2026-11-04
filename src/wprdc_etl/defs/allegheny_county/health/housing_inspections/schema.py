"""Pandera schema for allegheny_county / health / housing_inspections.

The raw landed file, as the legacy job old/payload/ac_hd/hc_environment_inspections.py read it
(schema InspectionsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "inspection_date": txt(),
        "service_request_number": txt(),
        "inspectionid": num(),
        "inspection_type": txt(),
        "street": txt(),
        "city": txt(),
        "state": txt(),
        "zip": txt(),
        "request_type": txt(),
        "property_type": txt(),
        "emergency_inspection_type": txt(),
    }
)
