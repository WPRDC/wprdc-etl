"""Pandera schema for allegheny_county / treasurer / dog_licenses_current_year.

The raw landed file, as the legacy job old/payload/ac/dog_licenses.py read it
(schema DogLicensesSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "licensetype": txt(),
        "breed": txt(),
        "color": txt(),
        "dogname": txt(),
        "ownerzip": txt(),
        "expyear": num(),
        "validdate": txt(),
    }
)
