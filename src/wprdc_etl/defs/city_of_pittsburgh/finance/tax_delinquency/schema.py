"""Pandera schema for city_of_pittsburgh / finance / tax_delinquency.

The raw CSV columns of finance/tax_delinquency.csv in the city's public bucket,
as the legacy job old/payload/pgh/treasury.py read them (schema TaxDelinquencySchema),
lower-cased on read (`source.header_case: lower`). Checked against the
live file 2026-11-02.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "parc_num": txt(),
        "address": txt(),
        "billing_city": txt(),
        "current_delq": num(),
        "current_delq_pi": num(),
        "prior_years": num(),
        "prior_delq_tax": num(),
        "prior_delq_pi": num(),
        "state_description": txt(),
        "neighborhood": txt(),
        "council_district": txt(),
        "ward": txt(),
        "public_works_division": txt(),
        "pli_division": txt(),
        "police_zone": txt(),
        "fire_zone": txt(),
        "longitude": num(),
        "latitude": num(),
    }
)
