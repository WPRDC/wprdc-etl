"""Pandera schema for pittsburgh_regional_transit / gis / current_routes.

The GeoJSON feature properties of PRT's "PRT Routes - Current (full system)" Hub layer, read
from the live export on 2026-11-02 (274 features). That is what schema_ok
validates: the mirrors copy this file, and wait for the check.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "OBJECTID": num(),
        "route_code": txt(),
        "route_name": txt(),
        "route_filter": txt(),
        "route_sort": txt(),
        "mode": txt(),
        "direction": txt(),
        "trip_headsign": txt(),
        "shape_id": txt(),
        "svc_days": txt(),
        "trips_wd": num(),
        "trips_sa": num(),
        "trips_su": num(),
        "trips_7d": num(),
        "agency": txt(),
        "feed_version": txt(),
        "start_date": txt(),
        "end_date": txt(),
        "from_gtfs": num(),
        "change_log": txt(),
        "comments": txt(),
        "empty_feat": num(),
    }
)
