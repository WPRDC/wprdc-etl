"""Pandera schema for allegheny_county / health / pool_violations.

The raw landed file, as the legacy job old/payload/ac_hd/pool_inspections.py read it
(schema ACPoolViolationsSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "viol_uid": txt(),
        "inspect_id": txt(),
        "inspect_number": num(),
        "viol_date": txt(),
        "viol_class": txt(),
        "allow_prom_yn": txt(),
        "emergency_yn": txt(),
        "sect_num": num(),
        "sub_sect": txt(),
        "struct_or_dwell": txt(),
        "owner_or_tenant": txt(),
        "violation": txt(),
        "remedy": txt(),
    }
)
