"""Pandera schema for allegheny_county / sustainability / energy_and_water_use.

The raw landed file, as the legacy job old/payload/ac/energy_use.py read it
(schema EnergyWaterUseSchema). Headers are lower-cased on read
(`source.header_case: lower`), the same folding the legacy engine did, so
these are its `load_from` names verbatim. Exposes SCHEMA.

UNVERIFIED against a real extract. Every declared column is required (the
file must be a superset); extra columns pass and show up as drift.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, txt

SCHEMA = frame(
    {
        "bldg_grp": txt(),
        "location": txt(),
        "date": txt(),
        "total_energy_use_mmbtu": txt(),
        "elec_use_kwh": txt(),
        "nat_gas_use_mcf": txt(),
        "steam_use_klbs": txt(),
        "water_use_kgal": txt(),
        "wastewater_use_kgal": txt(),
        "total_energy_cost": txt(),
        "elec_cost": txt(),
        "nat_gas_cost": txt(),
        "steam_cost": txt(),
        "water_cost": txt(),
        "wastewater_cost": txt(),
        "total_utility_cost": txt(),
        "total_tons_co2": txt(),
        "elec_tons_co2": txt(),
        "nat_gas_tons_co2": txt(),
        "steam_tons_co2": txt(),
    }
)
