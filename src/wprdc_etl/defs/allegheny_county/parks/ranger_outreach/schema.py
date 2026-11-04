"""Pandera schema for allegheny_county / parks / ranger_outreach.

The raw landed file, as the legacy job old/payload/ac/rangers.py read it
(schema RangersOutreachSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "date": txt(),
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
        "start_time": txt(),
        "end_time": txt(),
        "latitude": num(),
        "longitude": num(),
        "notes": txt(),
    }
)
