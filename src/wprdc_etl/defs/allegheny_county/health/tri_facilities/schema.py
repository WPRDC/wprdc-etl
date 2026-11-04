"""Pandera schema for allegheny_county / health / tri_facilities.

The raw landed file, as the legacy job old/payload/ac_hd/toxic_inventory.py read it
(schema ToxicFacilitiesSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "id": txt(),
        "facility_name": txt(),
        "street_address": txt(),
        "city_name": txt(),
        "county_name": txt(),
        "state_county_fips_code": txt(),
        "state_abbr": txt(),
        "zip_code": txt(),
        "region": txt(),
        "fac_closed_ind": txt(),
        "mail_name": txt(),
        "mail_street_address": txt(),
        "mail_city": txt(),
        "mail_state_abbr": txt(),
        "mail_province": txt(),
        "mail_country": txt(),
        "mail_zip_code": txt(),
        "asgn_federal_ind": txt(),
        "asgn_agency": txt(),
        "frs_id": txt(),
        "parent_co_db_num": txt(),
        "parent_co_name": txt(),
        "fac_latitude": txt(),
        "fac_longitude": txt(),
        "pref_latitude": num(),
        "pref_longitude": num(),
        "pref_accuracy": num(),
        "pref_collect_meth": txt(),
        "pref_desc_category": txt(),
        "pref_horizontal_datum": txt(),
        "pref_source_scale": txt(),
        "pref_qa_code": txt(),
        "asgn_partial_ind": txt(),
        "asgn_public_contact": txt(),
        "asgn_public_phone": txt(),
        "asgn_public_phone_ext": txt(),
        "asgn_public_contact_email": txt(),
        "bia_code": txt(),
        "standardized_parent_company": txt(),
        "epa_registry_id": txt(),
    }
)
