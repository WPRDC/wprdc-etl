"""Pandera schema for pwsa / gis / inlets.

The CSV export of PWSA's Inlets layer (ArcGIS item 0c59b699..._2), lower-cased
on read as the legacy InletsSchema expected. Checked against the live export
on 2026-11-02; pandas drops the file's UTF-8 BOM, so the first column is `x`.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "x": num(),
        "y": num(),
        "objectid": txt(),
        "node_id": txt(),
        "node_type": txt(),
        "status": txt(),
        "owner": txt(),
        "sub_type": txt(),
    }
)
