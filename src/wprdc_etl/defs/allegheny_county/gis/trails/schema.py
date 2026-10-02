"""Pandera schema STUB for allegheny_county / gis / trails.

Allegheny County Trails Locations. Generated from the legacy rocket-etl
marshmallow schema `AllTrailsSchema` in `old/payload/ac/gis_jobs.py`. Exposes
SCHEMA.

Read from the County ArcGIS Open Data server
(openac-alcogis.opendata.arcgis.com). Covers blazed and unblazed trails.

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
evidence of the casing in the file. 23 of the columns below were written
`'X'.lower()`, so the pre-.lower() spelling is what is declared below -- that
is the real header, and the legacy code was matching its lowercased form.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "OBJECTID": txt(),  # -> objectid; BOM-prefixed in the legacy source
        "Trail_ID": txt(),  # -> trail_id
        "Trail_Name": txt(),  # -> trail_name
        "abbreviated_park_name": txt(),
        "Park_Name": txt(),  # -> park_name
        "Full_Park_Name": txt(),  # -> full_park_name
        "Full_Blaze_Color": txt(),  # -> full_blaze_color
        "abbreviated_Blaze_Color": txt(),  # -> abbreviated_blaze_color
        "Base_Color": txt(),  # -> base_color
        "Cap_Color": txt(),  # -> cap_color
        "Dot_Color": txt(),  # -> dot_color
        "Dash_Color": txt(),  # -> dash_color
        "Blaze_Image_Link": txt(),  # -> blaze_image_link
        "Mileage": num(),  # -> mileage
        "Track": txt(),  # -> track
        "track_(number_format)": txt(),  # -> track_num
        "Difficulty": txt(),  # -> difficulty
        "Configuration": txt(),  # -> configuration
        "Surface": txt(),  # -> surface
        "Service_Road": txt(),  # -> service_road
        "GlobalID": txt(),  # -> globalid
        "Trail_Status": txt(),  # -> trail_status
        "SHAPE__Length": num(),  # -> shape_length
    }
)
