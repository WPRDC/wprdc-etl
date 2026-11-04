"""Pandera schema for city_of_pittsburgh / playground_equipment.

The raw monthly Cartegraph export (cartegraph_playground_equipment/MM-YYYY.csv),
checked against 11-2026.csv on 2026-11-02. These are the SOURCE names; the
renames in defs.yaml map them onto the published columns.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, ranged, txt

SCHEMA = frame(
    {
        "id": key(),
        "playground": txt(),
        "type": txt(),
        "manufacturer": txt(),
        "safety_surface_type": txt(),
        "ada_accessible": txt(),
        "address_num": txt(),
        "street_name": txt(),
        "lat": ranged(40.2, 40.55),
        "long": ranged(-80.15, -79.8),
    }
)
