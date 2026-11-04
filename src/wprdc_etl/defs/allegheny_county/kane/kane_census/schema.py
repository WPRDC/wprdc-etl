"""Pandera schema for allegheny_county / kane / kane_census.

The raw landed file, as the legacy job old/payload/ac/kane.py read it
(schema KaneCensusSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "facility": txt(),
        "report_date": txt(),
        "race": txt(),
        "ethnicity": txt(),
        "sex": txt(),
        "resident_count": num(),
    }
)
