"""Pandera schema STUB for allegheny_county / parks / ranger_outreach.

Parks Ranger Outreach. Generated from the legacy rocket-etl marshmallow schema
`RangersOutreachSchema` in `old/payload/ac/rangers.py`. Exposes SCHEMA.

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

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, ranged, txt

SCHEMA = frame(
    {
        "date": txt(),  # date; raw source-format string pre-transform
        "location": txt(),
        "program": txt(),
        "program_category": txt(),
        "special_event_name": txt(),
        "contact_type": txt(),
        "outreach_group": txt(),
        "outreach_group_type": txt(),
        "school_district": txt(),
        "number": num(),
        "participant_avg_age": txt(),
        "type": txt(),
        "volunteer_hours": txt(),
        "ranger_work_day_hours": txt(),
        "start_time": txt(),  # datetime; raw source-format string pre-transform
        "end_time": txt(),  # datetime; raw source-format string pre-transform
        "latitude": ranged(-90, 90),  # coordinate
        "longitude": ranged(-180, 180),  # coordinate
        "notes": txt(),
    }
)
