# Legacy package migration checklist

Packages from the legacy ETL (`old/packages.csv`), checked against the
`ckan.package_id` / `resource_id` of every `defs.yaml` under
`src/wprdc_etl/defs/`. Status as of 2026-11-02.

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

- [ ] ~~city-of-pittsburgh-bridges (`d6e6c012`) vs `city_of_pittsburgh/gis/bridges` (`a6a84421`)~~
- [ ] ~~city-steps (`e9aa627c`) vs `city_of_pittsburgh/gis/steps` (`9d35d609`)~~
- [ ] city-council-districts-2012 (`8249c8b6`) vs `city_of_pittsburgh/gis/council_districts_2012` (`340c39a2`, a minted id)
- [ ] parks (`874bb5ef`) vs `city_of_pittsburgh/gis/pghwebparks` (`e298e2ae`)
- [ ] playing-fields (`87c77ec3`) vs `city_of_pittsburgh/gis/ballfields_pgh` (`e43ad211`)
- [ ] metered-streets-pittsburgh-parking-authority / pittsburgh-parking-meters-and-payment-points
      vs `city_of_pittsburgh/gis/parking_authority_meter_areas` / `parking_authority_terminals`

## Probably duplicates or retired: decide whether to skip

- [ ] temperature-inversions **and** air-temperature-inversions
- [ ] city-owned-properties **and** city-owned-property
- [ ] pittsburgh-311-data **and** 311-data
- [ ] allegheny-county-farmers-markets-locations **and** allegheny-county-farmers-markets-locations-2017
- [ ] test-pgh-today-vacancy-rate-housing (looks like a test package)
- [ ] allegheny-county-covid-19-vaccinations, pittsburgh-fish-fry-map,
      internet-accessibility-in-pennsylvania (may be static or stale)

## To do: Allegheny County

- [ ] allegheny-county-air-quality
- [ ] air-quality-forecast
- [ ] allegheny-county-human-services-community-profiles
- [ ] real-estate-sales
- [ ] allegheny-county-911-dispatches-ems-and-fire
- [ ] allegheny-county-dog-licenses
- [ ] allegheny-county-fatal-accidental-overdoses
- [ ] allegheny-county-park-rangers-outreach
- [ ] allegheny-county-assets
- [ ] allegheny-county-jail-daily-census
- [ ] allegheny-county-kane-regional-center-census
- [ ] allegheny-county-weights-and-measures-inspections
- [ ] allegheny-county-energy-and-water-use
- [ ] delinquent-real-estate-taxes
- [ ] filed-property-assessment-appeals
- [ ] allegheny-county-tax-liens-filed-and-satisfied
- [ ] allegheny-county-crash-data
- [ ] allegheny-county-mortgage-foreclosure-records
- [ ] allegheny-county-conservatorship-filings
- [ ] allegheny-county-certified-mwdbe-businesses
- [ ] allegheny-county-plumbing-inspections
- [ ] housing-and-community-environment-inspections
- [ ] allegheny-county-asbestos-permit
- [ ] allegheny-county-restaurant-food-facility-inspection-violations
- [ ] allegheny-county-plumbers
- [ ] allegheny-county-public-swimming-pool-hot-tub-and-spa-inspections
- [ ] allegheny-county-tif-exclusions
- [ ] sheriff-sales
- [ ] toxic-release-inventory
- [ ] 211-requests
- [ ] bigburgh-social-service-listings

## To do: City of Pittsburgh

- [ ] street-closures
- [ ] pittsburgh-pli-violations-report
- [ ] condemned-properties
- [ ] pli-permits
- [ ] daily-community-center-attendance
- [ ] pwsa-inlets
- [ ] city-property-tax-abatements
- [ ] city-treasury-sales
- [ ] city-of-pittsburgh-property-tax-delinquency
- [ ] business-contractors-trades
- [ ] parking-transactions
- [ ] smart-trash-containers
- [ ] city-revenues-and-expenses
- [ ] city-of-pittsburgh-facilities
- [ ] playground-equipment
- [ ] playgrounds
- [ ] city-of-pittsburgh-speed-humps
- [ ] traffic-count-data-city-of-pittsburgh
- [ ] fire-incidents-in-city-of-pittsburgh
- [ ] police-incident-blotter
- [ ] city-trees
- [ ] waste-recovery-locations
- [ ] capital-projects
- [ ] city-of-pittsburgh-signalized-intersections
- [ ] city-of-pittsburgh-courts
- [ ] city-of-pittsburgh-public-art
- [ ] city-traffic-signs
- [ ] paving-schedule
- [ ] city-of-pittsburgh-markings
- [ ] city-of-pittsburgh-retaining-walls

## To do: PRT (Port Authority)

- [ ] prt-schedule-period-lookup
- [ ] prt-of-allegheny-county-transit-stops
- [ ] prt-current-transit-routes
- [ ] gtfs-archive
- [ ] prt-monthly-average-ridership-by-route
- [ ] port-authority-monthly-average-on-time-performance-by-route

## To do: WPRDC internal

- [ ] wprdc-statistics
