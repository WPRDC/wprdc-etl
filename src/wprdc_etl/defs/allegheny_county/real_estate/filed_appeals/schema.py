"""Pandera schema STUB for allegheny_county / real_estate / filed_appeals.

Filed Annual Appeals. Generated from the legacy rocket-etl marshmallow schema
`FiledAppealsSchema` in `old/payload/ac/appeals.py`. Exposes SCHEMA.

Source arrives as HT877PAALL_<year>.xls and is renamed to .tsv before the ETL.

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
evidence of the casing in the file. 4 of the columns below were written
`'X'.lower()`, so the pre-.lower() spelling is what is declared below -- that
is the real header, and the legacy code was matching its lowercased form.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt, year

SCHEMA = frame(
    {
        "tax_year": year(),
        "parcel_id": txt(),
        "class": txt(),
        "class_group": txt(),
        "tax_status": txt(),
        "hear_type": txt(),
        "on_behalf_of": txt(),
        "hrstatus": txt(),
        "hearing_status": txt(),
        "owner_name": txt(),
        "school_district_code": txt(),
        "school_district_name": txt(),
        "muni_code": txt(),
        "municipality": txt(),
        "prev_taxyr_mkt_value": num(),
        "cur_mkt_value": num(),
        "as_of": txt(),  # date; raw source-format string pre-transform
    }
)
