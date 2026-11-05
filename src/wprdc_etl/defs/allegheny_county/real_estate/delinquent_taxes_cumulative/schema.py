"""Pandera schema for allegheny_county / real_estate / delinquent_taxes_cumulative.

The raw landed file, as the legacy job old/payload/ac/delinquent.py read it
(schema DelinquentSchema — the one it read the county's per-year
files with; DelinquentAllSchema read its own concatenated output). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "ar_id": txt(),
        "year_id": txt(),
        "tax_map": txt(),
        "tax_map_ufmt": txt(),
        "muni_name": txt(),
        "school_dist": txt(),
        "last_pay_date": txt(),
        "penalties": num(),
        "interest": num(),
        "orig_bill": num(),
        "total_pymnts": num(),
        "asof_date": txt(),
    }
)
