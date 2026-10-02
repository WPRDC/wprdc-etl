"""Pandera schema STUB for allegheny_county / real_estate / sales.

Property Sales Transactions. Generated from the legacy rocket-etl marshmallow
schema `SalesSchema` in `old/payload/ac/sales_v2.py`. Exposes SCHEMA.

Same source file as the delinquent-taxes job (AA301PAALL.csv).

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

The legacy schema also carried load hooks -- fix_dates (pre_load),
add_full_address (pre_dump) -- which did the cleaning this project does with
transform steps. Port them there, not here.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "parid": txt(),  # -> PARID
        "propertyhousenum": txt(),  # -> PROPERTYHOUSENUM
        "propertyfraction": txt(),  # -> PROPERTYFRACTION
        "propertyaddressdir": txt(),  # -> PROPERTYADDRESSDIR
        "propertyaddressstreet": txt(),  # -> PROPERTYADDRESSSTREET
        "propertyaddresssuf": txt(),  # -> PROPERTYADDRESSSUF
        "propertyaddressunitdesc": txt(),  # -> PROPERTYADDRESSUNITDESC
        "propertyunitno": txt(),  # -> PROPERTYUNITNO
        "propertycity": txt(),  # -> PROPERTYCITY
        "propertystate": txt(),  # -> PROPERTYSTATE
        "propertyzip": txt(),  # -> PROPERTYZIP
        "schoolcode": txt(),  # -> SCHOOLCODE
        "schooldesc": txt(),  # -> SCHOOLDESC
        "municode": txt(),  # -> MUNICODE
        "munidesc": txt(),  # -> MUNIDESC
        # date; raw source-format string pre-transform; -> RECORDDATE
        "recorddate": txt(),
        "saledate": txt(),  # date; raw source-format string pre-transform; -> SALEDATE
        "price": num(),  # -> PRICE
        "deedbook": txt(),  # -> DEEDBOOK
        "deedpage": txt(),  # -> DEEDPAGE
        "salecode": txt(),  # -> SALECODE
        "saledesc": txt(),  # -> SALEDESC
        "instrtyp": txt(),  # -> INSTRTYP
        "instrtypdesc": txt(),  # -> INSTRTYPDESC
    }
)
