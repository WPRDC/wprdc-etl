"""Pandera schema STUB for allegheny_county / real_estate / liens_raw.

Raw tax-lien records. Generated from the legacy rocket-etl marshmallow schema
`RawLiensSchema` in `old/payload/ac/liens.py`. Exposes SCHEMA.

Inherits the ten shared columns from the legacy `TemplateSchmea` base; they
are inlined here. Also backs the satisfactions job (same layout).

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
evidence of the casing in the file. 2 of the columns below were written
`'X'.lower()`, so the pre-.lower() spelling is what is declared below -- that
is the real header, and the legacy code was matching its lowercased form.

The legacy schema also carried load hooks -- assign_assignee_and_omit_owners
(pre_dump), avoid_null_keys (pre_load), fix_date (pre_load) -- which did the
cleaning this project does with transform steps. Port them there, not here.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, num, txt, year

SCHEMA = frame(
    {
        "PIN": key(),  # -> pin
        "block_lot": key(),
        "filing_date": txt(),  # date; raw source-format string pre-transform
        "tax_year": year(),
        "DTD": key(),  # -> dtd
        "description": key(),  # -> lien_description
        "municipality": txt(),
        "ward": txt(),
        "last_docket_entry": txt(),
        "amount": num(),
        "party_type": txt(),  # load_only: dropped before publish
        # load_only: dropped before publish; This was previously called "party_name".
        "last_name": txt(),
    }
)
