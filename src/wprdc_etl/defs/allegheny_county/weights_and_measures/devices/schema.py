"""Pandera schema for allegheny_county / weights_and_measures / devices.

The raw landed file, as the legacy job old/payload/ac/weights.py read it
(schema DevicesSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "deviceid": txt(),
        "storeid": txt(),
        "devicetype": txt(),
        "devicegroup": txt(),
        "make": txt(),
        "model": txt(),
        "serialnumber": txt(),
        "pump": txt(),
        "grade": txt(),
        "capacity": txt(),
        "inspectioncycle": txt(),
        "remarks": txt(),
        "deleted": txt(),
    }
)
