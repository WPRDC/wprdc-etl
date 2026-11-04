"""Pandera schema for city_of_pittsburgh / signalized_intersections.

The published columns as they appear in the city's Cartegraph export
(cartegraph/signalized_intersections.csv, 117 columns), checked 2026-11-02.
The region columns are NOT declared: reverse_geocode overwrites them.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, ranged, txt

SCHEMA = frame(
    {
        "id": key(),
        "name": txt(),
        "description": txt(),
        "flash_time": txt(),
        "flash_yellow": txt(),
        "operation_type": txt(),
        "latitude": ranged(40.2, 40.55),
        "longitude": ranged(-80.15, -79.8),
    }
)
