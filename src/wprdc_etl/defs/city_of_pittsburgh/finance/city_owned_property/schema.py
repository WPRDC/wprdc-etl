"""Pandera schema for city_of_pittsburgh / finance / city_owned_property.

The raw GeoJSON properties of finance/city_owned_properties.geojson in the city's public bucket,
as the legacy job old/payload/pgh/treasury.py read them (schema CityOwnedSchema),
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
        "block_lot": txt(),
        "sale_price": num(),
        "act77_gent_date": txt(),
        "last_payment_date": txt(),
        "treasury_sale_date": txt(),
        "latest_sale_date": txt(),
        "neighborhood_name": txt(),
        "council_district": txt(),
        "ward": num(),
        "fire_zone": txt(),
        "police_zone": txt(),
        "dpw_streets": txt(),
        "dpw_enviro": txt(),
        "dpw_parks": txt(),
    }
)
