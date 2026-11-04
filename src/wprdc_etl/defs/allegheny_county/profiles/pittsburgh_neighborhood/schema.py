"""Pandera schema for allegheny_county / profiles / pittsburgh_neighborhood.

The raw landed file, as the legacy job old/payload/ac/ac_profiles.py read it
(schema PittsburghNeighborhoodProfilesSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "metric_name": txt(),
        "report_group": txt(),
        "caldr_yr": num(),
        "kpi_count": num(),
        "geo_area_name": txt(),
    }
)
