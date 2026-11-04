"""Pandera schema for allegheny_county / real_estate / sales.

The raw landed file, as the legacy job old/payload/ac/sales_v2.py read it
(schema SalesSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "parid": txt(),
        "full_address": txt(),
        "propertyhousenum": txt(),
        "propertyfraction": txt(),
        "propertyaddressdir": txt(),
        "propertyaddressstreet": txt(),
        "propertyaddresssuf": txt(),
        "propertyaddressunitdesc": txt(),
        "propertyunitno": txt(),
        "propertycity": txt(),
        "propertystate": txt(),
        "propertyzip": txt(),
        "schoolcode": txt(),
        "schooldesc": txt(),
        "municode": txt(),
        "munidesc": txt(),
        "recorddate": txt(),
        "saledate": txt(),
        "price": num(),
        "deedbook": txt(),
        "deedpage": txt(),
        "salecode": txt(),
        "saledesc": txt(),
        "instrtyp": txt(),
        "instrtypdesc": txt(),
    }
)
