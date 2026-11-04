"""Pandera schema for wprdc / asset_map.

The raw asset dump (https://assets.wprdc.org/asset_dump.csv), as the legacy
job old/payload/wprdc/assets_to_ckan.py read it (AssetSchema). Checked against
the live file's header on 2026-11-02.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "name": txt(),
        "asset_type": txt(),
        "id": txt(),
        "tags": txt(),
        "location_id": txt(),
        "street_address": txt(),
        "unit": txt(),
        "unit_type": txt(),
        "municipality": txt(),
        "city": txt(),
        "state": txt(),
        "zip_code": txt(),
        "latitude": num(),
        "longitude": num(),
        "parcel_id": txt(),
        "residence": txt(),
        "iffy_geocoding": txt(),
        "available_transportation": txt(),
        "parent_location_id": num(),
        "parent_location": txt(),
        "url": txt(),
        "email": txt(),
        "phone": txt(),
        "hours_of_operation": txt(),
        "holiday_hours_of_operation": txt(),
        "periodicity": txt(),
        "capacity": num(),
        "wifi_network": txt(),
        "internet_access": txt(),
        "computers_available": txt(),
        "accessibility": txt(),
        "open_to_public": txt(),
        "child_friendly": txt(),
        "sensitive": txt(),
        "do_not_display": txt(),
        "localizability": txt(),
        "services": txt(),
        "hard_to_count_population": txt(),
        "data_source_names": txt(),
        "data_source_urls": txt(),
        "organization_name": txt(),
        "organization_phone": txt(),
        "organization_email": txt(),
        "etl_notes": txt(),
        "geocoding_properties": txt(),
    }
)
