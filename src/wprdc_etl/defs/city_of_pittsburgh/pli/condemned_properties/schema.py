"""Pandera schema for city_of_pittsburgh / pli / condemned_properties.

The raw GeoJSON properties of agency_counter/condemned_dead_end_properties.geojson
in the city's public bucket, as the legacy job old/payload/pgh/condemned_v2.py
read them (schema CondemnedSchema), lower-cased on read
(`source.header_case: lower`). Checked against the live file 2026-11-04.
Coordinates come from the geometry, so they aren't declared here.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "parc_num": txt(),
        "address": txt(),
        "zipcode": txt(),
        "owner": txt(),
        "inspection_type": txt(),
        "create_date_est": txt(),
        "latest_inspec_result": txt(),
        "latest_inspec_score": txt(),
        "record_number": txt(),
        "insp_status": txt(),
        "neighborhood": txt(),
        "council_district": txt(),
        "ward": txt(),
    }
)
