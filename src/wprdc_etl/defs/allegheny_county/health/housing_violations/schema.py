"""Pandera schema for allegheny_county / health / housing_violations.

The raw landed file, as the legacy job old/payload/ac_hd/hc_environment_inspections.py read it
(schema ViolationsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "completeddate": txt(),
        "inspectionid": txt(),
        "service_request_number": txt(),
        "violationid": txt(),
        "violation": txt(),
        "description": txt(),
        "status": txt(),
    }
)
