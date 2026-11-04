"""Pandera schema for pittsburgh_regional_transit / gis / transit_stops.

The GeoJSON feature properties of PRT's "PRT Stops - Current (full system)" Hub layer, read
from the live export on 2026-11-02 (6,389 features). That is what schema_ok
validates: the mirrors copy this file, and wait for the check.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "OBJECTID": num(),
        "stop_id": txt(),
        "stop_code": txt(),
        "stop_name": txt(),
        "stop_lat": num(),
        "stop_lon": num(),
        "stop_route": txt(),
        "route_code": txt(),
        "route_filter": txt(),
        "route_sort": txt(),
        "mode": txt(),
        "direction": txt(),
        "trips_wd": num(),
        "trips_sa": num(),
        "trips_su": num(),
        "trips_7d": num(),
        "svc_days": txt(),
        "agency": txt(),
        "feed_version": txt(),
        "start_date": txt(),
        "end_date": txt(),
        "from_gtfs": num(),
        "change_log": txt(),
        "comments": txt(),
        "status_code": num(),
        "status_notes": txt(),
        "status_start_date": txt(),
        "status_end_date": txt(),
        "GEOIDFQ": txt(),
        "county": txt(),
        "muni": txt(),
        "hood": txt(),
        "munihood_full": txt(),
        "munihood_display": txt(),
        "empty_feat": num(),
    }
)
