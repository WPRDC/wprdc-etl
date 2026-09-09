"""Pandera schema for allegheny_county / real_estate / assessments.

Full-column validation for the assessments parcel file, drawn from the WPRDC
data dictionary (resource 9a1c60bd-f9f7-4aba-aeb7-af8c3aaa44e5). Exposes SCHEMA.

Contract:
  * EVERY documented column is declared, and pandera columns are REQUIRED by
    default — a file missing any declared column fails. It must be a superset.
  * strict=False (via frame()) — EXTRA/new columns are allowed and pass; the
    drift check surfaces them as informational.
  * coerce=True — CSV values are coerced to the declared dtype before checks.
  * Validates the RAW landed file (schema_ok runs pre-transform): dates are
    still source-format strings, values are the county's raw numbers.

Column type/check vocabulary comes from wprdc_etl.strategies.schema.

IMPORTANT — date formats are inconsistent in the dictionary:
  SALEDATE and RECORDDATE are documented dd/mm/yyyy, PREVSALEDATE and
  PREVSALEDATE2 as mm/dd/yyyy. Verify against a real sample. The iso_date
  transform steps in defs.yaml set explicit formats accordingly.

Value checks (coded/ranges) assume trimmed values; since this runs pre-strip
they can occasionally warn on stray whitespace. They're WARN-level (data
quality), separate from the hard ERROR of a missing column.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import coded, frame, ge0, key, num, ranged, txt, year

SCHEMA = frame(
    {
        # --- identity ---
        "PARID": key(),
        # --- location ---
        "PROPERTYHOUSENUM": num(),
        "PROPERTYFRACTION": txt(),
        "PROPERTYADDRESS": txt(),
        "PROPERTYCITY": txt(),
        "PROPERTYSTATE": txt(),
        "PROPERTYUNIT": txt(),
        "PROPERTYZIP": num(),
        "MUNICODE": num(),
        "MUNIDESC": txt(),
        "SCHOOLCODE": txt(),
        "SCHOOLDESC": txt(),
        # --- legal / neighborhood ---
        "LEGAL1": txt(),
        "LEGAL2": txt(),
        "LEGAL3": txt(),
        "NEIGHCODE": txt(),
        "NEIGHDESC": txt(),
        # --- tax status ---
        "TAXCODE": coded(["E", "T", "P"]),
        "TAXDESC": txt(),
        "TAXSUBCODE": txt(),
        "TAXSUBCODE_DESC": txt(),
        # --- owner / classification / use ---
        "OWNERCODE": num(),
        "OWNERDESC": txt(),
        "CLASS": coded(["R", "U", "I", "C", "O", "G", "F"]),
        "CLASSDESC": txt(),
        "USECODE": txt(),
        "USEDESC": txt(),
        # --- land / reductions ---
        "LOTAREA": ge0(),
        "HOMESTEADFLAG": coded(["HOM"]),
        "FARMSTEADFLAG": coded(["FRM"]),
        "CLEANGREEN": txt(),
        "ABATEMENTFLAG": coded(["Y"]),
        # --- sale history ---
        "RECORDDATE": txt(),
        "SALEDATE": txt(),
        "SALEPRICE": ge0(),
        "SALECODE": txt(),
        "SALEDESC": txt(),
        "DEEDBOOK": txt(),
        "DEEDPAGE": txt(),
        "PREVSALEDATE": txt(),
        "PREVSALEPRICE": ge0(),
        "PREVSALEDATE2": txt(),
        "PREVSALEPRICE2": ge0(),
        # --- change-notice (owner mailing) address ---
        "CHANGENOTICEADDRESS1": txt(),
        "CHANGENOTICEADDRESS2": txt(),
        "CHANGENOTICEADDRESS3": txt(),
        "CHANGENOTICEADDRESS4": txt(),
        # --- assessed values (county / local / fair-market) ---
        "COUNTYBUILDING": ge0(),
        "COUNTYLAND": ge0(),
        "COUNTYTOTAL": ge0(),
        "COUNTYEXEMPTBLDG": ge0(),
        "LOCALBUILDING": ge0(),
        "LOCALLAND": ge0(),
        "LOCALTOTAL": ge0(),
        "FAIRMARKETBUILDING": ge0(),
        "FAIRMARKETLAND": ge0(),
        "FAIRMARKETTOTAL": ge0(),
        # --- dwelling characteristics (residential; null for vacant/other) ---
        "STYLE": txt(),
        "STYLEDESC": txt(),
        "STORIES": ge0(),
        "YEARBLT": year(),
        "EXTERIORFINISH": num(),
        "EXTFINISH_DESC": txt(),
        "ROOF": num(),
        "ROOFDESC": txt(),
        "BASEMENT": num(),
        "BASEMENTDESC": txt(),
        "GRADE": txt(),
        "GRADEDESC": txt(),
        "CONDITION": ranged(1, 8),
        "CONDITIONDESC": txt(),
        "CDU": txt(),
        "CDUDESC": txt(),
        "TOTALROOMS": ge0(),
        "BEDROOMS": ge0(),
        "FULLBATHS": ge0(),
        "HALFBATHS": ge0(),
        "HEATINGCOOLING": txt(),
        "HEATINGCOOLINGDESC": txt(),
        "FIREPLACES": ge0(),
        "BSMTGARAGE": ge0(),
        "FINISHEDLIVINGAREA": ge0(),
        "CARDNUMBER": num(),
        # --- identifiers / provenance ---
        "ALT_ID": txt(),
        "TAXYEAR": num(),
        "ASOFDATE": txt(),  # run date of the file
    }
)
