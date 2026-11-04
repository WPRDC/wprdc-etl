"""Pandera schema for allegheny_county / weights_and_measures / stores.

The raw landed file, as the legacy job old/payload/ac/weights.py read it
(schema StoresSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "storeid": txt(),
        "storename": txt(),
        "address": txt(),
        "mailingcity": txt(),
        "state": txt(),
        "zip": txt(),
        "municipality": txt(),
        "corpid": txt(),
        "neighborhood": txt(),
        "point_x": num(),
        "point_y": num(),
        "businessphone": txt(),
        "alternatephone": txt(),
        "price_verification": txt(),
        "fueldispenser": txt(),
        "scale": txt(),
        "timing": txt(),
        "miscinspection": txt(),
        "oob": txt(),
        "unit": txt(),
        "shoppingcenter": txt(),
        "newmunicipality": txt(),
    }
)
