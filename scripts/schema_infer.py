"""Infer a pandera schema builder for a column, from sampled values.

Shared by the generators (`sync_arcgis.py`, `sync_pasda.py`) so both produce
the same types from the same data. The rules here are deliberately
conservative — see `builder_for`.
"""

from __future__ import annotations

import re

# Column names that are identifiers even though they look numeric. A FIPS
# code, a GEOID or a zip is a label that happens to be spelled with digits;
# typing it num() coerces away the leading zeros the value depends on.
#
# Two tiers, because one pattern can't do both jobs. These words are
# unambiguous wherever they appear, including glued into a compound name —
# COUNTYFIPS is a FIPS code, and anchoring on a word boundary misses it:
_ID_SUBSTRING = re.compile(
    r"(fips|geoid|objectid|municode|zipcode|statefp|countyfp|tractce|blkgrpce"
    r"|blockce|censusblock|censustract)",
    re.I,
)
# ...while these are too short or too common to match mid-word ("id" would
# hit "width", "code" would hit "zipcode" harmlessly but also "barcode"), so
# they only count at a name boundary:
_ID_TOKEN = re.compile(
    r"(^|_)(id|ids|fid|oid|zip|code|tract|block|ward|district|sector|zone"
    r"|parcel|pin)\d*($|_)",
    re.I,
)


def builder_for(values: list[str], name: str = "") -> str:
    """Pick a schema builder for one column from its sampled values.

    Conservative by design — txt() unless the column is unambiguously a
    measure. Two things push a numeric-looking column back to txt():

      * an identifier-ish NAME (geoid10, statefp10, MUNICODE). These are
        labels spelled with digits, and num() eats their leading zeros.
      * a LEADING ZERO in any sampled value, which is the same signal
        observed in the data rather than the header.

    Getting this wrong is not cosmetic: the generated schema validates the raw
    landed file, and `num()` on a blank-bearing identifier column fails
    schema_ok outright (it did, on city neighborhoods' seven Census columns).
    """
    if not values:
        return "txt"
    if _ID_SUBSTRING.search(name or "") or _ID_TOKEN.search(name or ""):
        return "txt"
    for v in values:
        try:
            float(v)
        except ValueError:
            return "txt"
        # An explicit "+" is formatting, not arithmetic — Census internal
        # points ship as "+40.4523867" / "-079.9073195".
        if v[0] == "+":
            return "txt"
        digits = v.lstrip("+-")
        # "01", "007", "079.90" — significant zeros, so the value is a
        # fixed-width label. Checked after the sign so a zero-padded negative
        # (that longitude) is caught too.
        if len(digits) > 1 and digits[0] == "0" and not digits.startswith("0."):
            return "txt"
    return "num"
