# Legacy package migration checklist

Packages from the legacy ETL (`old/packages.csv`), checked against the
`ckan.package_id` / `resource_id` of every `defs.yaml` under
`src/wprdc_etl/defs/`. Status as of 2026-11-03.

## Done

- [x] property-assessments → `allegheny_county/real_estate/assessments`
- [x] allegheny-county-parcel-boundaries1 → `allegheny_county/gis/parcels`
- [x] allegheny-county-addressing-street-centerlines → `allegheny_county/gis/street_centerlines`
- [x] allegheny-county-addressing-address-points2 → `allegheny_county/gis/address_points`
- [x] city-water-features → `city_of_pittsburgh/water_features`
- [x] ~~allegheny-county-addressing-landmarks3~~ (`cd24b8f3`): replaced by
      `allegheny_county/gis/addressing_landmarks` (new package `5a07d365`).
      The old package still needs deleting.

## Check before building: a GIS pipeline exists, but it targets a different package

- [x] ~~city-of-pittsburgh-bridges (`d6e6c012`) vs `city_of_pittsburgh/gis/bridges` (`a6a84421`)~~
- [x] ~~city-steps (`e9aa627c`) vs `city_of_pittsburgh/gis/steps` (`9d35d609`)~~
- [x] ~~city-council-districts-2012 (`8249c8b6`) vs `city_of_pittsburgh/gis/council_districts_2012` (`340c39a2`, a minted id)~~
- [x] ~~parks (`874bb5ef`) vs `city_of_pittsburgh/gis/pghwebparks` (`e298e2ae`)~~
- [x] ~~playing-fields (`87c77ec3`) vs `city_of_pittsburgh/gis/ballfields_pgh` (`e43ad211`)~~


## Built 2026-11-03 (one pipeline per CKAN resource)

County SFTP pipelines are ported from the legacy job's schema and are
UNVERIFIED against a real extract (no county SFTP access from dev); each
`defs.yaml` says so. They use `source.header_case: lower`, the legacy
engine's header folding. Datasets marked *(accumulates)* must be seeded
before their first publishing run — `deploy/seed-accumulator`, ROLLOUT §4.

### Allegheny County (SFTP, unverified)

- [x] allegheny-county-air-quality → `allegheny_county/health/air_quality_{hourly,daily_aqi,h2s}` *(accumulate)*, `air_quality_sensor_locations` (+ GeoJSON representation)
  - not refreshed: the "[old format]" archives and the static HTML/PDF links
- [x] allegheny-county-human-services-community-profiles → `allegheny_county/profiles/{municipality,pittsburgh_neighborhood}`
- [x] real-estate-sales → `allegheny_county/real_estate/sales` (the cumulative resource only — the per-year resources need a rolling target, see below)
- [x] allegheny-county-911-dispatches-ems-and-fire → `allegheny_county/emergency_services/dispatches_{ems,fire}` *(accumulate)*
- [~] allegheny-county-dog-licenses → `allegheny_county/treasurer/dog_licenses` (the Lifetime resource) only; the current-year and last-year resources need a rolling target
- [x] allegheny-county-fatal-accidental-overdoses → `allegheny_county/medical_examiner/fatal_accidental_overdoses`
- [x] allegheny-county-park-rangers-outreach → `allegheny_county/parks/ranger_outreach`
- [x] allegheny-county-jail-daily-census → `allegheny_county/jail/jail_census` *(accumulate)*
- [x] allegheny-county-kane-regional-center-census → `allegheny_county/kane/kane_census` *(accumulate)*
- [x] allegheny-county-weights-and-measures-inspections → `allegheny_county/weights_and_measures/{devices,inspections,stores}`
- [x] allegheny-county-energy-and-water-use → `allegheny_county/sustainability/energy_and_water_use`
- [~] delinquent-real-estate-taxes → `allegheny_county/real_estate/delinquent_taxes_cumulative` (cumulative, *accumulate*, reads `delinquent_{now:%Y}.csv`); per-year resources need a rolling target
- [x] allegheny-county-certified-mwdbe-businesses → `allegheny_county/economic_development/mwdbe_businesses`
- [x] allegheny-county-plumbing-inspections → `allegheny_county/health/plumbing_inspections`
- [x] housing-and-community-environment-inspections → `allegheny_county/health/housing_{inspections,service_requests,violations}`
- [x] allegheny-county-asbestos-permit → `allegheny_county/health/asbestos_permits` *(accumulate)*
- [x] allegheny-county-restaurant-food-facility-inspection-violations → `allegheny_county/health/restaurant_inspections` (the legacy violations job has no live resource; only its 2014-2025 archive exists)
- [x] allegheny-county-plumbers → `allegheny_county/health/licensed_plumbers`
- [x] allegheny-county-public-swimming-pool-hot-tub-and-spa-inspections → `allegheny_county/health/pool_{inspections,violations}`
- [x] toxic-release-inventory → `allegheny_county/health/tri_{air,water,land,underground,offsite,facilities}`
- [x] allegheny-county-farmers-markets-locations → `allegheny_county/health/farmers_markets_{current,historical}`, `farmers_market_vendors_{current,historical}` (historical *accumulate*; files named `{now:%Y}`)
- [x] air-temperature-inversions → `allegheny_county/health/air_temperature_inversions` *(accumulate)*

### City of Pittsburgh (public bucket; columns and rows checked against CKAN in a dry run)

- [x] pittsburgh-311-data → `city_of_pittsburgh/requests_311` *(accumulate)*, with the legacy street masking
- [x] city-of-pittsburgh-property-tax-delinquency → `city_of_pittsburgh/finance/tax_delinquency`
- [x] city-property-tax-abatements → `city_of_pittsburgh/finance/tax_abatements`
- [x] city-treasury-sales → `city_of_pittsburgh/finance/treasury_sales`
- [x] city-owned-property (`d93bb0b3`) → `city_of_pittsburgh/finance/city_owned_property`
- [x] city-of-pittsburgh-signalized-intersections → `city_of_pittsburgh/signalized_intersections` (current Cartegraph export)
- [x] playground-equipment → `city_of_pittsburgh/playground_equipment` (monthly `{now:%m-%Y}.csv`)

### Others

- [x] allegheny-county-assets → `wprdc/asset_map`
- [x] prt-schedule-period-lookup → `pittsburgh_regional_transit/gis/schedule_period_lookup`
- [x] prt-of-allegheny-county-transit-stops → `pittsburgh_regional_transit/gis/transit_stops` (GeoJSON + Shapefile mirrors)
- [x] prt-current-transit-routes → `pittsburgh_regional_transit/gis/current_routes` (GeoJSON + Shapefile mirrors)
- [~] pwsa-inlets → `pwsa/gis/inlets`: the CSV table only. PWSA's Hub catalogue is empty, so there is nothing to mirror, and its x/y are state-plane feet, which a representation can't label yet. The GeoJSON/KML/Shapefile resources aren't refreshed.

## Not built — needs a decision or new capability

### Needs a framework feature

- [ ] dog licenses (current / last year), real-estate sales and delinquent taxes per-year resources — the TARGET resource changes every January (`"{year} Dog Licenses"`); needs a resource looked up by templated name
- [ ] filed-property-assessment-appeals — XLS input (no Excel reader) and a new resource per year
- [ ] gtfs-archive — a new resource per feed version

### Privacy logic to port first

- [ ] condemned-properties, pli-permits — the legacy jobs blank the owner of homestead-exempt parcels by looking each parcel up in property assessments. Porting without that would publish homeowners' names.

### Stateful custom processing, not a file-to-table ETL

- [ ] allegheny-county-tax-liens-filed-and-satisfied, allegheny-county-mortgage-foreclosure-records, allegheny-county-conservatorship-filings — court SFTP + a local database the jobs rebuild
- [ ] 211-requests — `_211_v2.py` builds a local cache
- [ ] air-quality-forecast (NASA GEOS-CF), temperature-inversions — model runs that produce CSVs and PNG maps

### Source gone, changed or unknown

- [ ] prt-monthly-average-ridership-by-route, port-authority-monthly-average-on-time-performance-by-route — both source URLs 404 (CKAN last updated 2026-06)
- [ ] allegheny-county-tif-exclusions — no longer in the county catalogue; legacy job commented out
- [ ] street-closures, pittsburgh-pli-violations-report — the legacy bucket paths are gone; the `ddb/` replacements have different columns
- [ ] city-owned-properties (`53b09a0f`) — its `eproperty` feed is gone
- [ ] fire-incidents, police-incident-blotter, business-contractors-trades, daily-community-center-attendance, city-revenues-and-expenses, smart-trash-containers — legacy read local files staged by a script not in `old/`
- [ ] Cartegraph layers whose current export lacks published columns (legacy jobs not run since 2022-12):
  - playgrounds — no `name`, `type`
  - city-of-pittsburgh-courts — no `name`, `location` (`type`/`surface_material` are `court_type`/`court_surface_material`)
  - city-of-pittsburgh-public-art — no `name` (an asset number), `art_type` is `public_art_type`
  - city-of-pittsburgh-retaining-walls — no `name`, `height`, `length`
  - city-of-pittsburgh-speed-humps — no `hump_id`
  - city-of-pittsburgh-markings — no `id_field`, `route_ahead`, `route_back`; crosswalks resource unmatched
  - city-traffic-signs — no `description`
  - traffic-count-data-city-of-pittsburgh — none of the 14 count/speed columns
  - Building them means publishing fewer columns (a deliberate `ckan.rebuild`).
- [ ] city-of-pittsburgh-facilities, city-trees, waste-recovery-locations, capital-projects, paving-schedule — no file in the bucket; CKAN frozen since 2022

### Not in `old/`

- [ ] parking-transactions, pittsburgh-parking-meters-and-payment-points — the separate `park-shark` repo
- [ ] sheriff-sales, bigburgh-social-service-listings — no legacy job; CKAN frozen since 2022-23
- [ ] allegheny-county-crash-data — annual one-shot
