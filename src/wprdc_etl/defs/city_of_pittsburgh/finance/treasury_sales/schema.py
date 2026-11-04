"""Pandera schema for city_of_pittsburgh / finance / treasury_sales.

The raw GeoJSON properties of finance/treasury_sale_properties.geojson in the city's public bucket,
as the legacy job old/payload/pgh/treasury.py read them (schema TreasurySalesSchema),
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
        "treas_sale_date": txt(),
        "treas_sale_flag": txt(),
        "total_tax_due": num(),
        "city_tax_due": num(),
        "county_tax_due": num(),
        "school_tax_due": num(),
        "library_tax_due": num(),
        "pwsa_tax_due": num(),
        "parks_tax_due": num(),
        "demo_cost_due": num(),
        "clean_lien_due": num(),
        "neighborhood_name": txt(),
        "council_district": txt(),
        "ward": txt(),
        "police_zone": txt(),
        "fire_zone": txt(),
        "dpw_streets": txt(),
        "dpw_enviro": txt(),
        "dpw_parks": txt(),
    }
)
