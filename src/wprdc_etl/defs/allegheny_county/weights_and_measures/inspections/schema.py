"""Pandera schema for allegheny_county / weights_and_measures / inspections.

The raw landed file, as the legacy job old/payload/ac/weights.py read it
(schema InspectionsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "inspectionid": txt(),
        "storeid": txt(),
        "deviceid": txt(),
        "date": txt(),
        "result": txt(),
        "reinspection": txt(),
    }
)
