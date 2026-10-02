"""Pandera schema for city_of_pittsburgh / water_features.

Contract for the Cartegraph water-features export. Exposes SCHEMA.

Contract:
  * Declares the columns PUBLISHED STRAIGHT FROM the export, and pandera
    columns are REQUIRED by default — a file missing any of them fails
    schema_ok. The export ships 47 columns; the other 30 aren't published.
  * NOT declared: the region columns reverse_geocode writes (neighborhood,
    council_district, ward, police_zone, tract, fire_zone,
    public_works_division) and pli_division (copied from ward). The export's own copies are overwritten,
    so requiring them would fail a run over values that are never published.
  * strict=False (via frame()) — new/extra columns pass; the drift check
    surfaces them.
  * coerce=True — CSV values are coerced to the declared dtype before checks.
  * Validates the RAW landed file (schema_ok runs pre-transform): `id` and the
    admin codes are still the CSV's numbers here, not the post-transform text.

Column vocabulary comes from wprdc_etl.strategies.schema.
"""

from __future__ import annotations

from wprdc_etl.strategies.schema import frame, key, ranged, txt

SCHEMA = frame(
    {
        # --- identity ---
        "id": key(),
        "name": txt(),
        # --- classification ---
        "control_type": txt(),  # On/Off | Continuous | "" (blank)
        "feature_type": txt(),  # Drinking Fountain | Spray | Decorative | Pond
        "inactive": txt(),  # bool in the CSV; coerced to "True"/"False" here
        "make": txt(),  # manufacturer; often blank
        "image": txt(),  # photo URL; sometimes blank
        # --- coordinates (Pittsburgh bbox, padded — trips a broken extract) ---
        "latitude": ranged(40.2, 40.55),
        "longitude": ranged(-80.15, -79.8),
    }
)
