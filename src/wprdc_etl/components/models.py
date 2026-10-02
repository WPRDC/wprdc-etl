"""Shared typed config models for component instances.

Typed (not bare dicts) so a malformed YAML fails at `dg check`, pointing at
the exact field, rather than at runtime.

EVERY model here is `Resolvable`, not just `Model`. That is what makes
`{{ env('VAR', 'default') }}` work INSIDE a `source:` / `ckan:` block. Template
injection is applied per-field on a Resolvable and only to values that are
still strings (dagster/components/resolved/model.py), and the recursion in
ResolutionContext.resolve_value descends into dict/list/tuple — but not into an
already-constructed pydantic model. So a bare `Model` nested under a Resolvable
gets built by pydantic first and its fields are never rendered: a templated
`host` would reach paramiko as the literal string "{{ env(...) }}" and surface
as a DNS failure at run time, with `dg check` having reported no problem.

Being Resolvable also widens each non-str field to `T | str` in the derived
model, so a template string survives schema validation and is coerced after
rendering. Type errors are still caught — `port: not-a-number` fails `dg check`
with the file, line, and field — they just surface on the resolve pass rather
than the YAML-schema pass.
"""

from typing import Protocol

from dagster.components import Model, Resolvable


class SourceModel(Model, Resolvable):
    # "sftp" | "http" | "arcgis" | "pasda" | "api_bulk" | "api_incremental"
    #
    type: str
    host: str | None = None
    port: int | None = None  # SFTP port (defaults to 22 when unset)
    path: str | None = None  # glob, e.g. /outbound/crime/*.csv
    url: str | None = None  # full URL for http sources, e.g. https://host/data.csv
    secret_ref: str | None = None  # env var name holding "user:password"

    # --- arcgis sources ---------------------------------------------------
    # An ArcGIS Hub site publishes a DCAT catalogue at <site>/data.json listing
    # every layer with its download URLs. We address a layer by TITLE rather
    # than URL because the URL embeds the ArcGIS item id
    # (.../items/6d406961.../geojson) and changes whenever the layer is
    # republished, while the title is the stable handle. The legacy rocket-etl
    # keyed on title for the same reason.
    catalog: str | None = None  # the site's data.json
    # --- pasda sources ----------------------------------------------------
    # Penn State's PASDA archive hosts several county layers the county's own
    # Hub only links to. A dataset is addressed by its PASDA dataset id — the
    # download filenames embed a release date
    # (AlleghenyCounty_Parcels20260928.zip), so a stored URL rots every time
    # the layer is republished, exactly as a Hub item id does. The extractor
    # resolves the current link off the landing page per run.
    # int as well as str: dagster's resolve pass coerces a numeric-looking
    # scalar, so a quoted "1219" in the YAML still arrives as an int and a
    # str-only field rejects it at load.
    dataset_id: str | int | None = None
    title: str | None = None  # dataset title, matched exactly
    # Which distribution to land: csv | geojson | shapefile | kml. Defaults to
    # csv. Nearly every entry carries both csv and geojson of the same layer,
    # so a tabular publish and a geometry read are the same dataset.
    format: str | None = None


class GeometryModel(Model, Resolvable):
    """How to get geometry out of this dataset's validated frame.

    Declared ONCE per dataset, because every geospatial output wants the same
    answer — the GeoJSON/Shapefile exports and ckanext-spatialdata's
    `dataspatial_wkb` column all read it. Unnecessary when the frame is already
    a GeoDataFrame (a geojson/shapefile source), which carries its own geometry.
    """

    wkt: str | None = None  # column holding WKT geometry
    lat: str | None = None  # or a coordinate pair, when there's no WKT column
    lng: str | None = None


class MirrorModel(Model, Resolvable):
    """One upstream distribution copied onto a CKAN resource, as-is.

    NOT the same thing as a RepresentationModel, and the difference matters.
    A representation is DERIVED — the validated frame is re-serialised to
    GeoJSON or Shapefile. That needs geometry in the frame, and an ArcGIS
    **CSV export of a polygon layer carries none**: it ships `Shape__Area`
    and `Shape__Length`, which are measurements, not shapes. So a boundary
    layer's GeoJSON cannot be derived from the CSV we validate — it has to be
    fetched from the catalogue and copied through byte for byte.

    `format` is one of:
      geojson | shapefile | kml   the file distributions, uploaded
      hub_page | rest_api         link-only resources (CKAN stores the URL,
                                  no upload), matching the "ArcGIS Hub
                                  Dataset" and "Esri Rest API" resources an
                                  existing WPRDC GIS package carries
    `csv` belongs here too — it is not privileged. It is refused only when
    `ckan.resource_id` is ALSO set, which would publish the same table twice:
    once as the DataStore load of the validated frame, once as a file copy.

    `datastore` additionally ingests the uploaded file so it is queryable. An
    uploaded file and a DataStore table coexist happily — the existing
    allegheny-county-boundary GeoJSON resource is both `url_type=upload` and
    `datastore_active`.
    """

    format: str
    resource_id: str | None = None  # target resource; created when absent
    name: str | None = None  # CKAN resource name; a per-format default if unset
    # Also ingest the uploaded file into the DataStore, making it queryable.
    # The route differs by format: a csv goes through DataPusher+, a geojson
    # through the spatial load endpoint that also builds the geometry column
    # (CkanResource.spatial_load_action — NOT BUILT YET, so a geojson with
    # this set raises rather than quietly producing a geometry-less table).
    datastore: bool = False


class CkanModel(Model, Resolvable):
    """The CKAN DataStore resource this dataset publishes to."""

    # DataStore target for the VALIDATED FRAME, published as CSV by
    # strategies/load.py. Optional: a dataset that mirrors the publisher's
    # own distributions has no frame of its own to publish — its CKAN
    # resources are fed byte-for-byte by `mirror` instead. Leave it unset
    # there, and note that doing so builds no `loaded` asset, which makes
    # `schema_ok` the only pre-publish check on that dataset.
    resource_id: str | None = None
    primary_key: list[str] | None = None  # merge/upsert key
    # drop and rebuild the table -- only necessary when changing schema
    rebuild: bool = False
    # make the DataStore table spatial-ready via ckanext-spatialdata; reads the
    # dataset's `geometry:` block, which it therefore requires
    spatial: bool = False

    # --- catalogue mirroring (arcgis sources) -----------------------------
    # The CKAN *package* this resource belongs to. Needed for anything that
    # isn't a write to `resource_id` itself: patching the description/tags,
    # and finding or creating the mirrored resources.
    package_id: str | None = None
    # Upstream distributions to copy onto their own CKAN resources.
    mirror: list[MirrorModel] | None = None
    # Overwrite the package's description and tags from the catalogue entry.
    # Off by default: it replaces whatever a human curated in CKAN.
    sync_metadata: bool = False
    # Markdown that REPLACES the catalogue's description instead of being
    # derived from it. Use when the publisher's own text is not what should
    # appear on the portal — the county's ArcGIS descriptions are boilerplate
    # harvest notes, so those datasets carry the curated WPRDC text here.
    # `description_suffix` still applies on top, so the two compose.
    description: str | None = None
    # Markdown appended to the catalogue's description, after a blank line.
    # Ours to write — the upstream description is the publisher's. Use a YAML
    # block scalar (`description_suffix: |`) so it stays readable.
    description_suffix: str | None = None


class RepresentationModel(Model, Resolvable):
    """One alternative representation of the dataset, published as its own CKAN
    file resource alongside the primary DataStore table.

    "Representation" is deliberately broader than the formats implemented today.
    The shape — same canonical frame, serialised differently, its own resource —
    fits any export: Parquet or XLSX for analysts, a fixed-width extract for a
    legacy consumer, an aggregated cut for a dashboard.

    Only the geospatial formats (geojson, shapefile) exist so far. Adding
    another means one branch in `publish_representation` that serialises the
    frame and hands the file to `ckan.publish_file`. Geometry is gated on the
    format there, so a non-geospatial representation doesn't need — and won't
    ask for — the dataset's `geometry:` block.

    WHERE NEW CONFIG GOES, on the rare format that needs some: shared across
    outputs -> dataset level; read by exactly one output -> a field on this
    model. `geometry:` earns its place at dataset level because TWO things read
    it — these representations and `ckan.spatial` — so one declaration keeps
    them from drifting. A serialisation option (a fixed-width layout, a sheet
    name) has a single consumer, so hoisting it would just put it further from
    the thing that uses it. Most formats — parquet, xlsx — need nothing at all.
    """

    format: str  # "geojson" | "shapefile"
    resource_id: str  # the CKAN file resource for this representation


class RegionLayerModel(Model, Resolvable):
    """Marks a dataset as a source of administrative boundaries.

    The dataset's validated GeoDataFrame is written to the PostGIS admin-region
    store under `name`, where the reverse_geocode transform op reads it. Set on
    the boundary dataset itself so the store refreshes as part of publishing it,
    rather than through a separate sync job.
    """

    name: str  # layer name, e.g. "council_district"
    value_field: str  # source column -> the stable region code
    label_field: str | None = None  # source column -> the human label, if any


class KeyLayerModel(Model, Resolvable):
    """Marks a dataset as a geometry lookup keyed by an identifier.

    The dataset's validated GeoDataFrame is written to the PostGIS
    keyed-geometry store under `name`, where the join_geometry transform op
    reads it: a table that carries an address id but no coordinates (the
    county's Addressing Landmarks) gets them from the address points. Separate
    from region_layer because nothing here is a region — a point contains
    nothing, and the lookup is by key, not by location.
    """

    name: str  # layer name, e.g. "address_point"
    key_field: str  # source column -> the key join_geometry looks rows up by


class PipelineConfig(Protocol):
    """The config surface every component type exposes.

    The helpers in components/_common.py and the strategy libraries take a
    component instance duck-typed on these fields, so they work for both
    TabularPipeline and FilePipeline (and for the SimpleNamespace stand-ins the
    tests use). A strategy that reaches past this surface — load.py needs
    `ckan`, for instance — annotates the concrete component type instead.
    """

    publisher: str
    dataset: str
    source: SourceModel
    department: str | None
    schedule: str | None
    partition: str
