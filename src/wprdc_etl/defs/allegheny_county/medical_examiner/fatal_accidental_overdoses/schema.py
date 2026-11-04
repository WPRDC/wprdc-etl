"""Pandera schema for allegheny_county / medical_examiner / fatal_accidental_overdoses.

The raw landed file, as the legacy job old/payload/ac/overdoses.py read it
(schema FatalODSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "death.date.time": txt(),
        "manner.of.death": txt(),
        "age": num(),
        "sex": txt(),
        "race": txt(),
        "case.dispo": txt(),
        "combined.od1": txt(),
        "combined.od2": txt(),
        "combined.od3": txt(),
        "combined.od4": txt(),
        "combined.od5": txt(),
        "combined.od6": txt(),
        "combined.od7": txt(),
        "combined.od8": txt(),
        "combined.od9": txt(),
        "combined.od10": txt(),
        "incident.zip": txt(),
        "decedent_zip": txt(),
        "case.year": num(),
    }
)
