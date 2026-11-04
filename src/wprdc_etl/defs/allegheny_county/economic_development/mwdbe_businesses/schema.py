"""Pandera schema for allegheny_county / economic_development / mwdbe_businesses.

The raw landed file, as the legacy job old/payload/ac/mwdbe_businesses.py read it
(schema MWDBEBusinesseschema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "vendorid": txt(),
        "certification_type": txt(),
        "firm_name": txt(),
        "owners": txt(),
        "work_description": txt(),
        "naics": txt(),
        "physical_address": txt(),
        "phone_number": txt(),
        "fax_number": txt(),
        "email_address": txt(),
        "website": txt(),
    }
)
