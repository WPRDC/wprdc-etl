"""Pandera schema for allegheny_county / health / air_temperature_inversions.

The raw landed file, as the legacy job old/payload/ac_hd/inversions_achd.py read it
(schema InversionDataSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "date": txt(),
        "inversion..y.n.": txt(),
        "temperature..c..": txt(),
        "depth..m.": num(),
        "strength": txt(),
        "notes": txt(),
    }
)
