"""Pandera schema for allegheny_county / health / asbestos_permits.

The raw landed file, as the legacy job old/payload/ac_hd/asbestos_permits.py read it
(schema AsbestosPermitV2Schema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "permit_number": txt(),
        "facility_name": txt(),
        "s_address": txt(),
        "city": txt(),
        "z_code": txt(),
        "permit_fee": num(),
        "pa_liscense_no": txt(),
        "contractor_name": txt(),
        "permit_specifications": txt(),
        "square_feet": num(),
        "is_building_occupied": txt(),
        "permit_issue_date": txt(),
        "permit_expire_date": txt(),
        "project_type": txt(),
        "status_value": txt(),
        "job_complete": txt(),
        "longitude": txt(),
        "latitude": txt(),
        "parcel": txt(),
    }
)
