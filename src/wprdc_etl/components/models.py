"""Shared typed config models for component instances.

Typed (not bare dicts) so a malformed YAML fails at `dg check`, pointing at
the exact field, rather than at runtime.
"""

from dagster.components import Model


class SourceModel(Model):
    type: str  # "sftp" | "http" | "api_bulk" | "api_incremental"
    host: str | None = None
    port: int | None = None  # SFTP port (defaults to 22 when unset)
    path: str | None = None  # glob, e.g. /outbound/crime/*.csv
    secret_ref: str | None = None  # env var name holding "user:password"


class CkanModel(Model):
    resource_id: str  # CKAN resource target
    primary_key: list[str] | None = None  # required for incremental (upsert)


class RepresentationModel(Model):
    format: str  # "geojson" | "shapefile"
    resource_id: str  # the CKAN file resource for this representation
    lat: str | None = None  # build point geometry from these columns when
    lng: str | None = None  # the canonical frame isn't already geospatial
