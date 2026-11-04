"""Pandera schema for allegheny_county / health / tri_offsite.

The raw landed file, as the legacy job old/payload/ac_hd/toxic_inventory.py read it
(schema ToxicOffsiteSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, num, txt

SCHEMA = frame(
    {
        "tri_facility_id": txt(),
        "doc_ctrl_num": txt(),
        "facility_name": txt(),
        "street_address": txt(),
        "city_name": txt(),
        "county_name": txt(),
        "state_county_fips_code": txt(),
        "state_abbr": txt(),
        "zip_code": txt(),
        "region": txt(),
        "fac_closed_ind": txt(),
        "asgn_federal_ind": txt(),
        "asgn_agency": txt(),
        "parent_co_db_num": txt(),
        "parent_co_name": txt(),
        "standardized_parent_company": txt(),
        "epa_registry_id": txt(),
        "trade_secret_ind": txt(),
        "reporting_year": num(),
        "cas_num": txt(),
        "elemental_metal_included": txt(),
        "tri_chem_id": txt(),
        "chem_name": txt(),
        "metal_ind": txt(),
        "pbt_ind": txt(),
        "list_3350": txt(),
        "carcinogen": txt(),
        "clean_air": txt(),
        "primary_sic_code": txt(),
        "sic_codes": txt(),
        "primary_naics_code": txt(),
        "naics_codes": txt(),
        "industry_code": num(),
        "srs_id": txt(),
        "pfas_ind": txt(),
        "transfer_loc_num": num(),
        "off_site_amount_sequence": num(),
        "type_of_waste_management": txt(),
        "transfer_range_code": txt(),
        "transfer_amt": num(),
        "transfer_est_na": txt(),
        "transfer_basis_est_code": txt(),
        "off_site_name": txt(),
        "off_site_street_address": txt(),
        "transfer_city_name": txt(),
        "transfer_county_name": txt(),
        "transfer_state_abbr": txt(),
        "transfer_zip_code": txt(),
        "transfer_country_code": txt(),
        "rcra_num": txt(),
        "controlled_loc": txt(),
        "transfer_loc_epa_registry_id": txt(),
    }
)
