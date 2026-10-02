"""Pandera schema STUB for allegheny_county / public_safety / crashes.

Crash Data. Generated from the legacy rocket-etl marshmallow schema
`CrashSchema` in `old/payload/ac/_crashes_one_shot.py`. Exposes SCHEMA.

PennDOT crash extract, republished by the County.
`_cumulative_crashes_retl.py` defines RETLCrashSchema with the same 190
columns - one stub covers both.

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

The legacy schema also carried load hooks -- fix_lane_count (pre_load),
fix_types (pre_load) -- which did the cleaning this project does with
transform steps. Port them there, not here.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, num, ranged, txt, year

SCHEMA = frame(
    {
        # -> CRASH_CRN; also read as FLAG_CRN, ROADWAY_CRN; The source field changed
        # name in the 2021 data.
        "crn": key(),
        "district": txt(),  # -> DISTRICT
        # -> CRASH_COUNTY; also read as ROADWAY_COUNTY; The source field changed from
        # 'crash_county' to 'county' for the 2021 data.
        "county": txt(),
        "municipality": txt(),  # -> MUNICIPALITY
        "police_agcy": txt(),  # -> POLICE_AGCY
        "crash_year": year(),  # -> CRASH_YEAR
        "crash_month": txt(),  # -> CRASH_MONTH
        "day_of_week": num(),  # -> DAY_OF_WEEK
        "time_of_day": txt(),  # -> TIME_OF_DAY
        "hour_of_day": txt(),  # -> HOUR_OF_DAY
        "illumination": txt(),  # -> ILLUMINATION
        "weather": txt(),  # -> WEATHER
        "road_condition": txt(),  # -> ROAD_CONDITION
        "collision_type": txt(),  # -> COLLISION_TYPE
        "relation_to_road": txt(),  # -> RELATION_TO_ROAD
        "intersect_type": txt(),  # -> INTERSECT_TYPE
        "tcd_type": txt(),  # -> TCD_TYPE
        "urban_rural": txt(),  # -> URBAN_RURAL
        "location_type": txt(),  # -> LOCATION_TYPE
        "sch_bus_ind": txt(),  # -> SCH_BUS_IND
        "sch_zone_ind": txt(),  # -> SCH_ZONE_IND
        "total_units": num(),  # -> TOTAL_UNITS
        "person_count": num(),  # -> PERSON_COUNT
        "vehicle_count": num(),  # -> VEHICLE_COUNT
        "automobile_count": num(),  # -> AUTOMOBILE_COUNT
        "motorcycle_count": num(),  # -> MOTORCYCLE_COUNT
        "bus_count": num(),  # -> BUS_COUNT
        "small_truck_count": num(),  # -> SMALL_TRUCK_COUNT
        "heavy_truck_count": num(),  # -> HEAVY_TRUCK_COUNT
        "suv_count": num(),  # -> SUV_COUNT
        "van_count": num(),  # -> VAN_COUNT
        "bicycle_count": num(),  # -> BICYCLE_COUNT
        "fatal_count": num(),  # -> FATAL_COUNT
        "injury_count": num(),  # -> INJURY_COUNT
        # -> MAJ_INJ_COUNT; PennDOT is not supplying this field in
        "maj_inj_count": num(),
        # -> MOD_INJ_COUNT; The 2019 data provided by the County has
        "mod_inj_count": num(),
        "min_inj_count": num(),  # -> MIN_INJ_COUNT
        "unk_inj_deg_count": num(),  # -> UNK_INJ_DEG_COUNT
        "unk_inj_per_count": num(),  # -> UNK_INJ_PER_COUNT
        "unbelted_occ_count": num(),  # -> UNBELTED_OCC_COUNT
        "unb_death_count": num(),  # -> UNB_DEATH_COUNT
        "unb_maj_inj_count": num(),  # -> UNB_MAJ_INJ_COUNT
        "belted_death_count": num(),  # -> BELTED_DEATH_COUNT
        "belted_maj_inj_count": num(),  # -> BELTED_MAJ_INJ_COUNT
        "mcycle_death_count": num(),  # -> MCYCLE_DEATH_COUNT
        "mcycle_maj_inj_count": num(),  # -> MCYCLE_MAJ_INJ_COUNT
        "bicycle_death_count": num(),  # -> BICYCLE_DEATH_COUNT
        "bicycle_maj_inj_count": num(),  # -> BICYCLE_MAJ_INJ_COUNT
        "ped_count": num(),  # -> PED_COUNT
        "ped_death_count": num(),  # -> PED_DEATH_COUNT
        "ped_maj_inj_count": num(),  # -> PED_MAJ_INJ_COUNT
        "comm_veh_count": num(),  # -> COMM_VEH_COUNT
        "max_severity_level": num(),  # -> MAX_SEVERITY_LEVEL
        "driver_count_16yr": num(),  # -> DRIVER_COUNT_16YR
        "driver_count_17yr": num(),  # -> DRIVER_COUNT_17YR
        "driver_count_18yr": num(),  # -> DRIVER_COUNT_18YR
        "driver_count_19yr": num(),  # -> DRIVER_COUNT_19YR
        "driver_count_20yr": num(),  # -> DRIVER_COUNT_20YR
        "driver_count_50_64yr": num(),  # -> DRIVER_COUNT_50_64YR
        "driver_count_65_74yr": num(),  # -> DRIVER_COUNT_65_74YR
        "driver_count_75plus": num(),  # -> DRIVER_COUNT_75PLUS
        "latitude": txt(),  # -> LATITUDE
        "longitude": txt(),  # -> LONGITUDE
        "dec_lat": ranged(-90, 90),  # coordinate; -> DEC_LAT
        "dec_long": ranged(-180, 180),  # coordinate; -> DEC_LONG
        "est_hrs_closed": num(),  # -> EST_HRS_CLOSED
        "lane_closed": num(),  # -> LANE_CLOSED
        "ln_close_dir": txt(),  # -> LN_CLOSE_DIR
        "ntfy_hiwy_maint": txt(),  # -> NTFY_HIWY_MAINT
        "rdwy_surf_type_cd": txt(),  # -> RDWY_SURF_TYPE_CD
        "spec_juris_cd": txt(),  # -> SPEC_JURIS_CD
        "tcd_func_cd": txt(),  # -> TCD_FUNC_CD
        "tfc_detour_ind": txt(),  # -> TFC_DETOUR_IND
        "work_zone_ind": txt(),  # -> WORK_ZONE_IND
        "work_zone_type": txt(),  # -> WORK_ZONE_TYPE
        "work_zone_loc": txt(),  # -> WORK_ZONE_LOC
        "cons_zone_spd_lim": num(),  # -> CONS_ZONE_SPD_LIM
        "workers_pres": txt(),  # -> WORKERS_PRES
        "wz_close_detour": txt(),  # -> WZ_CLOSE_DETOUR
        "wz_flagger": txt(),  # -> WZ_FLAGGER
        "wz_law_offcr_ind": txt(),  # -> WZ_LAW_OFFCR_IND
        "wz_ln_closure": txt(),  # -> WZ_LN_CLOSURE
        "wz_moving": txt(),  # -> WZ_MOVING
        "wz_other": txt(),  # -> WZ_OTHER
        "wz_shlder_mdn": txt(),  # -> WZ_SHLDER_MDN
        "interstate": num(),  # -> INTERSTATE
        "state_road": num(),  # -> STATE_ROAD
        "local_road": num(),  # -> LOCAL_ROAD
        "local_road_only": num(),  # -> LOCAL_ROAD_ONLY
        "turnpike": num(),  # -> TURNPIKE
        "wet_road": num(),  # -> WET_ROAD
        "snow_slush_road": num(),  # -> SNOW_SLUSH_ROAD
        "icy_road": num(),  # -> ICY_ROAD
        "sudden_deer": num(),  # -> SUDDEN_DEER
        "shldr_related": num(),  # -> SHLDR_RELATED
        "rear_end": num(),  # -> REAR_END
        "ho_oppdir_sdswp": num(),  # -> HO_OPPDIR_SDSWP
        "hit_fixed_object": num(),  # -> HIT_FIXED_OBJECT
        "sv_run_off_rd": num(),  # -> SV_RUN_OFF_RD
        "work_zone": num(),  # -> WORK_ZONE
        "property_damage_only": num(),  # -> PROPERTY_DAMAGE_ONLY
        "fatal_or_maj_inj": num(),  # -> FATAL_OR_MAJ_INJ
        "injury": num(),  # -> INJURY
        "fatal": num(),  # -> FATAL
        "non_intersection": num(),  # -> NON_INTERSECTION
        "intersection": num(),  # -> INTERSECTION
        "signalized_int": num(),  # -> SIGNALIZED_INT
        "stop_controlled_int": num(),  # -> STOP_CONTROLLED_INT
        "unsignalized_int": num(),  # -> UNSIGNALIZED_INT
        "school_bus": num(),  # -> SCHOOL_BUS
        "school_zone": num(),  # -> SCHOOL_ZONE
        "hit_deer": num(),  # -> HIT_DEER
        "hit_tree_shrub": num(),  # -> HIT_TREE_SHRUB
        "hit_embankment": num(),  # -> HIT_EMBANKMENT
        "hit_pole": num(),  # -> HIT_POLE
        "hit_gdrail": num(),  # -> HIT_GDRAIL
        "hit_gdrail_end": num(),  # -> HIT_GDRAIL_END
        "hit_barrier": num(),  # -> HIT_BARRIER
        "hit_bridge": num(),  # -> HIT_BRIDGE
        "overturned": num(),  # -> OVERTURNED
        "motorcycle": num(),  # -> MOTORCYCLE
        "bicycle": num(),  # -> BICYCLE
        "hvy_truck_related": num(),  # -> HVY_TRUCK_RELATED
        "vehicle_failure": num(),  # -> VEHICLE_FAILURE
        "train_trolley": num(),  # -> TRAIN_TROLLEY
        "phantom_vehicle": num(),  # -> PHANTOM_VEHICLE
        "alcohol_related": num(),  # -> ALCOHOL_RELATED
        "drinking_driver": num(),  # -> DRINKING_DRIVER
        "underage_drnk_drv": num(),  # -> UNDERAGE_DRNK_DRV
        "unlicensed": num(),  # -> UNLICENSED
        "cell_phone": num(),  # -> CELL_PHONE
        "no_clearance": num(),  # -> NO_CLEARANCE
        "running_red_lt": num(),  # -> RUNNING_RED_LT
        "tailgating": num(),  # -> TAILGATING
        "cross_median": num(),  # -> CROSS_MEDIAN
        "curve_dvr_error": num(),  # -> CURVE_DVR_ERROR
        "limit_65mph": num(),  # -> LIMIT_65MPH
        "speeding": num(),  # -> SPEEDING
        "speeding_related": num(),  # -> SPEEDING_RELATED
        "aggressive_driving": num(),  # -> AGGRESSIVE_DRIVING
        "fatigue_asleep": num(),  # -> FATIGUE_ASLEEP
        "driver_16yr": num(),  # -> DRIVER_16YR
        "driver_17yr": num(),  # -> DRIVER_17YR
        "driver_65_74yr": num(),  # -> DRIVER_65_74YR
        "driver_75plus": num(),  # -> DRIVER_75PLUS
        "unbelted": num(),  # -> UNBELTED
        "pedestrian": num(),  # -> PEDESTRIAN
        "distracted": num(),  # -> DISTRACTED
        "curved_road": num(),  # -> CURVED_ROAD
        "driver_18yr": num(),  # -> DRIVER_18YR
        "driver_19yr": num(),  # -> DRIVER_19YR
        "driver_20yr": num(),  # -> DRIVER_20YR
        "driver_50_64yr": num(),  # -> DRIVER_50_64YR
        "vehicle_towed": num(),  # -> VEHICLE_TOWED
        "fire_in_vehicle": num(),  # -> FIRE_IN_VEHICLE
        "hit_parked_vehicle": num(),  # -> HIT_PARKED_VEHICLE
        "mc_drinking_driver": num(),  # -> MC_DRINKING_DRIVER
        "drugged_driver": num(),  # -> DRUGGED_DRIVER
        "injury_or_fatal": num(),  # -> INJURY_OR_FATAL
        "comm_vehicle": num(),  # -> COMM_VEHICLE
        "impaired_driver": num(),  # -> IMPAIRED_DRIVER
        "deer_related": num(),  # -> DEER_RELATED
        "drug_related": num(),  # -> DRUG_RELATED
        "hazardous_truck": num(),  # -> HAZARDOUS_TRUCK
        "illegal_drug_related": num(),  # -> ILLEGAL_DRUG_RELATED
        "illumination_dark": num(),  # -> ILLUMINATION_DARK
        "minor_injury": num(),  # -> MINOR_INJURY
        "moderate_injury": num(),  # -> MODERATE_INJURY
        "major_injury": num(),  # -> MAJOR_INJURY
        "nhtsa_agg_driving": num(),  # -> NHTSA_AGG_DRIVING
        "psp_reported": num(),  # -> PSP_REPORTED
        "running_stop_sign": num(),  # -> RUNNING_STOP_SIGN
        "train": num(),  # -> TRAIN
        "trolley": num(),  # -> TROLLEY
        "rdwy_seq_num": num(),  # -> RDWY_SEQ_NUM
        "adj_rdwy_seq": num(),  # -> ADJ_RDWY_SEQ
        "access_ctrl": txt(),  # -> ACCESS_CTRL
        "lane_count": num(),  # -> LANE_COUNT
        "rdwy_orient": txt(),  # -> RDWY_ORIENT
        "road_owner": txt(),  # -> ROAD_OWNER
        "route": txt(),  # -> ROUTE
        "speed_limit": num(),  # -> SPEED_LIMIT
        "segment": txt(),  # -> SEGMENT
        "offset": num(),  # -> OFFSET
        "street_name": txt(),  # -> STREET_NAME
        "tot_inj_count": num(),  # -> TOT_INJ_COUNT
        "school_bus_unit": txt(),  # -> SCHOOL_BUS_UNIT
    }
)
