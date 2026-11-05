"""Pandera schema for city_of_pittsburgh / pli / permits.

The raw GeoJSON properties of agency_counter/wprdc_pli_permits.geojson in the
city's public bucket, as the legacy job old/payload/pgh/pli.py read them
(schema PermitsSchema), lower-cased on read (`source.header_case: lower`).
Checked against the live file 2026-11-04. Coordinates come from the geometry
(the file's own `lat`/`long` properties are ignored, as the legacy job did),
so they aren't declared here. There is NO owner column: see defs.yaml.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "ext_file_num": txt(),
        "permit_type": txt(),
        "all_contractors_name": txt(),
        "work_desc": txt(),
        "type_work_desc": txt(),
        "property_type": txt(),
        "total_proj_value": num(),
        "issue_date": txt(),
        "parc_num": txt(),
        "address": txt(),
        "council_district": txt(),
        "neighborhood": txt(),
        "ward": txt(),
        "zipcode": txt(),
        "status": txt(),
    }
)
