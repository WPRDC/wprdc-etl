"""Pandera schema STUB for allegheny_county / real_estate / delinquent_taxes.

Delinquent Real Estate Taxes. Generated from the legacy rocket-etl marshmallow
schema `DelinquentSchema` in `old/payload/ac/delinquent.py`. Exposes SCHEMA.

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

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "ar_id": txt(),
        "year_id": txt(),  # -> year
        "tax_map": txt(),  # -> parcel_id_formatted
        "tax_map_ufmt": txt(),  # -> parcel_id
        "muni_name": txt(),
        "school_dist": txt(),  # -> school_district
        "last_pay_date": txt(),  # datetime; raw source-format string pre-transform
        "penalties": num(),
        "interest": num(),
        "orig_bill": num(),
        "total_pymnts": num(),  # -> total_payments
        "asof_date": txt(),  # date; raw source-format string pre-transform
    }
)
