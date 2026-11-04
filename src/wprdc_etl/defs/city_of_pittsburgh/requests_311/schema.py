"""Pandera schema for city_of_pittsburgh / requests_311.

The raw 311_open_data.csv in the city's public bucket, as the legacy job
old/payload/pgh/311_v3.py read it (BaseRequestsSchema + RequestsSchema).
Checked against the live file's header on 2026-11-02. `unique_id` is derived
in transform.py, so it isn't declared here.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "case_number": txt(),
        "status": txt(),
        "case_owner": txt(),
        "subject": txt(),
        "subject_code": txt(),
        "legacy_subject": txt(),
        "created_date_et": txt(),
        "created_date_utc": txt(),
        "last_modified_date_et": txt(),
        "last_modified_date_utc": txt(),
        "closed_date_et": txt(),
        "closed_date_utc": txt(),
        "origin": txt(),
        "street": txt(),
        "street_id": txt(),
        "city": txt(),
        "neighborhood": txt(),
        "census_tract": txt(),
        "council_district": txt(),
        "ward": txt(),
        "police_zone": txt(),
        "lat": txt(),
        "lon": txt(),
        "geo_accuracy": txt(),
    }
)
