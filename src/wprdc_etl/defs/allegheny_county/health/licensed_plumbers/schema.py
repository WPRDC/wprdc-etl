"""Pandera schema for allegheny_county / health / licensed_plumbers.

The raw landed file, as the legacy job old/payload/ac_hd/licensed_plumbers.py read it
(schema PlumbersSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "registration_number": txt(),
        "first_name": txt(),
        "last_name": txt(),
        "city": txt(),
        "postalcode": txt(),
    }
)
