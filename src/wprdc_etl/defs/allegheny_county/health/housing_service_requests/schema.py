"""Pandera schema for allegheny_county / health / housing_service_requests.

The raw landed file, as the legacy job old/payload/ac_hd/hc_environment_inspections.py read it
(schema ServiceRequestsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "request_date": txt(),
        "service_request_number": txt(),
        "address": txt(),
        "address_2": txt(),
        "city": txt(),
        "zip": txt(),
        "request_type": txt(),
        "property_type": txt(),
        "emergency_inspection_type": txt(),
        "units": num(),
    }
)
