"""Pandera schema for city_of_pittsburgh / finance / tax_abatements.

The raw GeoJSON properties of finance/property_tax_abatement.geojson in the city's public bucket,
as the legacy job old/payload/pgh/treasury.py read them (schema TaxAbatementSchema),
lower-cased on read (`source.header_case: lower`). Checked against the
live file 2026-11-02. Coordinates come from the geometry, so they
aren't declared here.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "parc_num": txt(),
        "address": txt(),
        "start_year": num(),
        "approved_date_est": txt(),
        "program_name": txt(),
        "num_years": num(),
        "abatement_amount": num(),
        "neighborhood_name": txt(),
        "council_district": txt(),
        "ward": txt(),
        "dpw_streets": txt(),
        "dpw_enviro": txt(),
        "dpw_parks": txt(),
        "police_zone": txt(),
        "fire_zone": txt(),
    }
)
