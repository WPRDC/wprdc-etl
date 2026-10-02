"""Pandera schema STUB for allegheny_county / public_safety / dispatches_ems.

911 EMS Dispatches. Generated from the legacy rocket-etl marshmallow schema
`CallSchema` in `old/payload/ac/911.py`. Exposes SCHEMA.

Source file 911-EMS-dispatches.csv. The Fire feed (911-fire-dispatches.csv) is
a separate CKAN resource in the same package with an identical layout - one
legacy `CallSchema` covered both - so `public_safety/dispatches_fire`
re-exports this contract. Edit it here; the two only need to split if the
feeds diverge.

Columns are the legacy `load_from` names -- the RAW source column names, which
is what schema_ok validates (it runs pre-transform). The `dump_to` name each
column was published under is kept in a trailing comment; port those as
`rename` steps in defs.yaml rather than declaring them here.

STUB -- NOT VERIFIED against a real extract. Types are a mechanical mapping of
the marshmallow field classes, nothing more:

    fields.String                -> txt()   (key() where allow_none=False)
    fields.Date/DateTime/Boolean -> txt()   (still strings in the raw file)
    fields.Integer/Float         -> num()
    a *_year integer             -> year()
    a lat/lon float              -> ranged(...)

No coded-value sets or ranges were invented beyond that. Before wiring this to
a pipeline:
  * confirm every column below is actually in the landed file -- pandera
    columns are REQUIRED, so a stale one fails schema_ok as ERROR;
  * tighten txt() -> coded() and num() -> ge0()/ranged() from the data
    dictionary;
  * do NOT add derived/reverse-geocoded columns here.

HEADER CASE is the other thing to check. The legacy engine lowercased every
CSV header before matching, so the casing a legacy field was written in is not
evidence of the casing in the file. None of the columns below carried a
`.lower()` call, so these names are just how the legacy author typed them.

The legacy schema also carried load hooks -- fix_nas (pre_load) -- which did
the cleaning this project does with transform steps. Port them there, not
here.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, num, txt, year

SCHEMA = frame(
    {
        "call_id_hash": key(),
        "service": txt(),
        "priority": txt(),
        "priority_desc": txt(),
        "call_quarter": key(),
        "call_year": year(),
        "description_short": txt(),
        "city_code": txt(),
        "city_name": txt(),
        "geoid": txt(),
        "censusblockgroupcenter_x": num(),  # -> census_block_group_center__x
        "censusblockgroupcenter_y": num(),  # -> census_block_group_center__y
        "agency": txt(),  # load_only: dropped before publish
    }
)
