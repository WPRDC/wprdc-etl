"""TabularPipeline component type.

Each publisher/dataset is one YAML instance of this component (see
defs/pittsburgh_police/defs.yaml). `dg` discovers instances by walking
the defs/ tree, validates their YAML against the typed Model fields below,
and calls build_defs() on each to produce that publisher's assets, checks,
and schedule/sensor.

Adding a publisher = drop a new folder under defs/ with a defs.yaml.
No Python is touched.

NOTE ON IMPORTS / VERSION: the Components framework is new and its import
paths have moved between releases (it graduated from the separate
`dagster-components` package into `dagster` core). This file targets the
current `dagster.components` shape. If `dg check` complains about the
imports, run `dg --version` and check the docs for your version — you may
need `from dagster_components import ...` instead. The build_defs logic
below is stable regardless.
"""

import hashlib
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

import dagster as dg

import pandas as pd
from dagster.components import Component, Model, Resolvable

from wprdc_etl.resources import (
    CkanResource,
    GeocoderResource,
    LandingZoneResource,
    SFTPResource,
    SpatialResource,
)
from wprdc_etl.components._common import (
    partitions_for,
    resolve_modes,
    network_retry_policy,
    run_tags,
    schedule_or_sensor,
    taxonomy,
)
from wprdc_etl.components.models import (
    MirrorModel,
    CkanModel,
    GeometryModel,
    KeyLayerModel,
    RepresentationModel,
    RegionLayerModel,
    SourceModel,
)
from wprdc_etl.dataset_modules import load_dataset_module
from wprdc_etl.strategies import get_extractor, get_loader
from wprdc_etl.strategies.accumulate import merge as merge_into_table
from wprdc_etl.strategies.emit import publish_representation
from wprdc_etl.strategies.load import BOOL_FORMATS
from wprdc_etl.strategies.mirror import (
    publish_mirror,
    sync_package_metadata,
    validate_mirrors,
)
from wprdc_etl.strategies.read import read_landed
from wprdc_etl.strategies.transform import apply_declarative, validate_steps

if TYPE_CHECKING:
    import pandera.pandas as pa


class TabularPipeline(Component, Model, Resolvable):
    publisher: str
    dataset: str
    source: SourceModel
    # Optional so a dataset can exist purely to feed the admin-region store (a
    # boundary layer we consume but don't republish). At least one of
    # ckan / region_layer / key_layer is required — enforced in build_defs.
    ckan: CkanModel | None = None
    # Optional middle tier: the agency/department within a publisher (e.g.
    # allegheny_county -> real_estate -> assessments). When set, it becomes
    # part of the asset key. Left None for publishers with no sub-org.
    department: str | None = None
    schedule: str | None = None  # cron; if None, a sensor is used instead
    partition: str = "daily"  # "daily" | "weekly" | "monthly"
    # How the data ARRIVES. Drives the partition/state model:
    #   snapshot    -> the source hands over the whole current state each run
    #   incremental -> cursor/watermark delta pull (forces the assets unpartitioned)
    ingest: str = "snapshot"
    # How it REACHES CKAN. Independent of `ingest`; unset derives the pairing
    # `ingest` used to imply (snapshot -> replace, incremental -> upsert).
    #   replace -> upload the whole file, DataPusher+/qsv reloads the DataStore
    #   upsert  -> write rows straight to the DataStore by primary key
    # `replace` is what gives the resource a real, downloadable file: an upsert
    # never touches it, so CKAN has to generate a CSV from the DataStore on
    # every download request.
    publish: str | None = None
    # Keep a cumulative canonical table in the landing zone's _state/ prefix and
    # publish THAT rather than just this run's frame. Forced on for
    # incremental + replace (a delta can't be published as a whole file); set it
    # by hand for a snapshot source whose target is cumulative. Needs
    # ckan.primary_key as the merge key.
    accumulate: bool | None = None
    # Declarative transform steps, run in order before any co-located
    # transform.py. Each is {op: <primitive>, ...params}. Validated at load.
    transforms: list[dict] | None = None
    # Column contract. Omit when the dataset ships a co-located schema.py
    # (which declares all columns, each required). Use `required: [...]` for a
    # lightweight presence-only check when there's no schema.py.
    # Lightweight column contract used only when the dataset has NO co-located
    # schema.py. Lists columns that must be present. Omit when schema.py ships
    # the full contract. (Named required_columns, not schema, to avoid shadowing
    # a reserved attribute on the base Model.)
    required_columns: list[str] | None = None
    # How to build geometry from the validated frame. Declared once and read by
    # every geospatial output — the exports below and ckan.spatial. Unnecessary
    # when the source is already geospatial (geojson/shapefile).
    geometry: GeometryModel | None = None
    # Additional geospatial files (GeoJSON, Shapefile) published as their own
    # CKAN file resources from the same canonical frame. The primary tabular
    # resource is still ckan.resource_id. Intended for snapshot datasets.
    representations: list[RepresentationModel] | None = None
    # Set when this dataset IS a boundary layer: its validated geometry is
    # written to the PostGIS admin-region store, where the reverse_geocode
    # transform op reads it.
    region_layer: RegionLayerModel | None = None
    # Set when this dataset is a geometry lookup BY KEY (address points by
    # ADDRESS_ID): its validated geometry is written to the PostGIS
    # keyed-geometry store, where the join_geometry transform op reads it.
    key_layer: KeyLayerModel | None = None
    geocode: bool = False
    # Set for datasets whose source file is large enough that a full-frame
    # decode is memory-heavy (hundreds of MB). Emits the wprdc/heavy run tag,
    # which the prod QueuedRunCoordinator limits to one concurrent run.
    heavy: bool = False

    # ----------------------------------------------------------------------
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        publisher, dataset = self.publisher, self.dataset
        cfg = self  # readable alias
        validate_steps(cfg.transforms)  # fail loud at load on an unknown op
        key_prefix, group, stem = taxonomy(cfg)
        if not (cfg.ckan or cfg.region_layer or cfg.key_layer):
            raise ValueError(
                f"{stem}: needs a destination — set `ckan` to publish to CKAN, "
                "`region_layer` to feed the admin-region store, and/or "
                "`key_layer` to feed the keyed-geometry store"
            )
        publish, accumulate = resolve_modes(cfg)
        if publish not in ("replace", "upsert"):
            raise ValueError(
                f"{stem}: unknown publish mode {publish!r} "
                "(expected 'replace' or 'upsert')"
            )
        if publish == "upsert" and not (cfg.ckan and cfg.ckan.primary_key):
            raise ValueError(
                f"{stem}: publish 'upsert' requires ckan.primary_key (the upsert key)"
            )
        if accumulate:
            if not (cfg.ckan and cfg.ckan.primary_key):
                raise ValueError(
                    f"{stem}: accumulate requires ckan.primary_key (the merge key)"
                )
            if publish == "upsert":
                raise ValueError(
                    f"{stem}: accumulate with publish 'upsert' would accumulate "
                    "twice — the DataStore upsert already keeps prior rows. Use "
                    "publish 'replace' to publish the accumulated table as a file, "
                    "or drop accumulate."
                )
        if cfg.ckan and cfg.ckan.bool_format not in BOOL_FORMATS:
            raise ValueError(
                f"{stem}: ckan.bool_format must be one of {BOOL_FORMATS}, "
                f"got {cfg.ckan.bool_format!r}"
            )
        if cfg.ckan:
            validate_mirrors(
                cfg.ckan.mirror, has_datastore_target=bool(cfg.ckan.resource_id)
            )
            if not (cfg.ckan.resource_id or cfg.ckan.mirror or cfg.ckan.sync_metadata):
                raise ValueError(
                    f"{stem}: a `ckan:` block does nothing without at least "
                    "one of resource_id (a table from the frame), mirror "
                    "(copied distributions) or sync_metadata (description "
                    "and tags)"
                )
            needs_catalog = bool(cfg.ckan.mirror) or cfg.ckan.sync_metadata
            if cfg.ckan.mirror and cfg.source.type != "arcgis":
                raise ValueError(
                    f"{stem}: ckan.mirror copies the distributions listed in "
                    f"the publisher's data.json, so it needs "
                    f"`source.type: arcgis` (this one is {cfg.source.type!r})"
                )
            if (
                cfg.ckan.sync_metadata
                and cfg.source.type != "arcgis"
                and not cfg.ckan.description
            ):
                raise ValueError(
                    f"{stem}: ckan.sync_metadata takes the description and "
                    f"tags from the publisher's data.json, which a "
                    f"{cfg.source.type!r} source has none of — so it needs an "
                    "explicit ckan.description to have anything to push"
                )
            if needs_catalog and not cfg.ckan.package_id:
                raise ValueError(
                    f"{stem}: ckan.mirror / ckan.sync_metadata need "
                    "ckan.package_id — a resource id alone can't identify the "
                    "package whose metadata and resources they touch"
                )
        if cfg.ckan and cfg.ckan.spatial and not cfg.geometry:
            raise ValueError(
                f"{stem}: `ckan.spatial` needs a `geometry:` block naming the wkt "
                "or lat/lng columns ckanext-spatialdata builds the geometry from"
            )
        # Watermark drives increments, not the calendar.
        partitions = (
            None if cfg.ingest == "incremental" else partitions_for(cfg.partition)
        )

        # -- landed -------------------------------------------------------
        @dg.asset(
            key=[*key_prefix, "landed"],
            retry_policy=network_retry_policy(),
            partitions_def=partitions,
            group_name=group,
        )
        def landed(
            context: dg.AssetExecutionContext,
            landing: LandingZoneResource,
            sftp: SFTPResource,
        ) -> dict[str, Any]:
            # Extractor is chosen by source.type. It lands the raw artifact +
            # manifest and returns the manifest.
            extractor = get_extractor(cfg.source.type)
            return extractor.extract(context, cfg, landing=landing, sftp=sftp)

        # -- validated ----------------------------------------------------
        @dg.asset(
            key=[*key_prefix, "validated"],
            partitions_def=partitions,
            deps=[landed],
            group_name=group,
        )
        def validated(
            context: dg.AssetExecutionContext,
            landing: LandingZoneResource,
            geocoder: GeocoderResource,
            spatial: SpatialResource,
        ) -> pd.DataFrame:
            partition = (
                context.partition_key if context.has_partition_key else "current"
            )
            df = read_landed(landing, cfg, partition)
            if cfg.geocode:
                df = geocoder.geocode_frame(df, address_col="address")
            return _transform(df, cfg, deps={"spatial": spatial, "context": context})

        # -- accumulated --------------------------------------------------
        # The dataset's full current state, when this run only produced part of
        # it. Sits between validated and loaded so the merge is observable and
        # independently re-executable; `loaded` then publishes the whole table
        # instead of the delta.
        accumulated_assets: list[dg.AssetsDefinition] = []
        if accumulate:

            @dg.asset(
                key=[*key_prefix, "accumulated"],
                partitions_def=partitions,
                ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
                group_name=group,
            )
            def accumulated(
                context: dg.AssetExecutionContext,
                validated_df: pd.DataFrame,
                landing: LandingZoneResource,
            ) -> pd.DataFrame:
                prior = landing.read_table_state(
                    publisher, dataset, department=cfg.department
                )
                _guard_state_loss(
                    prior,
                    landing.read_table_meta(
                        publisher, dataset, department=cfg.department
                    ),
                    stem,
                )
                merged = merge_into_table(prior, validated_df, cfg.ckan.primary_key)
                landing.write_table_state(
                    publisher, dataset, merged, department=cfg.department
                )
                prior_rows = 0 if prior is None else len(prior)
                context.add_output_metadata(
                    {
                        "prior_rows": prior_rows,
                        "incoming_rows": len(validated_df),
                        "total_rows": len(merged),
                        "rows_added": len(merged) - prior_rows,
                    }
                )
                return merged

            accumulated_assets.append(accumulated)

        # -- loaded -------------------------------------------------------
        # Only when there's a DataStore target for the frame. Two kinds of
        # dataset have none: a boundary layer we consume but don't republish
        # (it stops after the region store), and a catalogue mirror, whose
        # CKAN resources are byte-for-byte copies of the publisher's own
        # distributions rather than anything derived from the frame.
        loaded_assets: list[dg.AssetsDefinition] = []
        if cfg.ckan and cfg.ckan.resource_id:
            # Publish the accumulated table when there is one, else this run's
            # frame. Everything downstream of here is identical either way.
            upstream = "accumulated" if accumulate else "validated"

            @dg.asset(
                key=[*key_prefix, "loaded"],
                retry_policy=network_retry_policy(),
                partitions_def=partitions,
                ins={"to_publish": dg.AssetIn(key=[*key_prefix, upstream])},
                group_name=group,
            )
            def loaded(
                context: dg.AssetExecutionContext,
                to_publish: pd.DataFrame,
                ckan: CkanResource,
            ) -> None:
                # `to_publish` is the upstream asset's return value, delivered by
                # the IO manager (configure an S3 IO manager in prod; the default
                # fs manager is fine locally). Loader comes from the publish mode.
                loader = get_loader(publish)
                loader.load(cfg, to_publish, ckan=ckan, context=context)

            loaded_assets.append(loaded)

        # -- schema / drift check on the validated asset ------------------
        @dg.asset_check(asset=validated)
        def schema_ok(
            context: dg.AssetCheckExecutionContext,
            landing: LandingZoneResource,
        ) -> dg.AssetCheckResult:
            import pandera.pandas as pa

            partition = (
                context.partition_key if context.has_partition_key else "current"
            )
            df = read_landed(landing, cfg, partition)

            present = set(df.columns)
            rich = _load_schema(cfg)

            # Declared columns are required by default. With a rich schema.py
            # that's ALL its columns; the file must be a superset of them.
            # Missing any -> not a superset -> ERROR.
            if rich is not None:
                declared = set(rich.columns.keys())
            elif cfg.required_columns:
                declared = set(cfg.required_columns)
            else:
                declared = set()
            missing = sorted(declared - present)

            # Value/type checks (data quality) on present data -> WARN only.
            # Skipped when structurally broken, since pandera would just
            # re-report the missing columns.
            pandera_ok = True
            if not missing and rich is not None:
                try:
                    rich.validate(df, lazy=True)
                except pa.errors.SchemaErrors:
                    pandera_ok = False

            # Cheap drift signal: compare this run's column set to the last one
            # this dataset presented, then record it as the next baseline. The
            # first check of a dataset only establishes the baseline.
            drift = _drift_report(
                present,
                landing.read_schema_state(
                    publisher, dataset, department=cfg.department
                ),
            )
            landing.write_schema_state(
                publisher,
                dataset,
                sorted(present),
                drift["column_hash"],
                department=cfg.department,
                partition=partition,
            )

            passed = not missing and pandera_ok
            return dg.AssetCheckResult(
                passed=passed,
                severity=(
                    dg.AssetCheckSeverity.ERROR
                    if missing
                    else dg.AssetCheckSeverity.WARN
                ),
                metadata={
                    "missing_columns": missing,  # ERROR: not a superset
                    # INFO: column_hash + what changed since the last check
                    **drift,
                },
            )

        # -- additional representations (geojson/shapefile) ----------------
        # One publish asset per representation, each serializing the canonical
        # validated frame to its format and uploading to its own resource.
        rep_assets = [
            _make_representation_asset(cfg, rep, key_prefix, partitions, group)
            for rep in (cfg.representations or [])
        ]

        # -- mirrored distributions + package metadata (arcgis sources) ----
        mirror_assets = [
            _make_mirror_asset(cfg, m, key_prefix, partitions, group)
            for m in (cfg.ckan.mirror if cfg.ckan else None) or []
        ]
        metadata_assets = (
            [_make_metadata_asset(cfg, key_prefix, partitions, group)]
            if cfg.ckan and cfg.ckan.sync_metadata
            else []
        )

        # -- admin-region store refresh (boundary layers only) -------------
        region_assets = (
            [_make_region_layer_asset(cfg, key_prefix, partitions, group)]
            if cfg.region_layer
            else []
        )
        key_assets = (
            [_make_key_layer_asset(cfg, key_prefix, partitions, group)]
            if cfg.key_layer
            else []
        )

        downstream = [
            *accumulated_assets,
            *loaded_assets,
            *rep_assets,
            *mirror_assets,
            *metadata_assets,
            *region_assets,
            *key_assets,
        ]

        # -- schedule (reliable cadence) OR sensor (irregular arrival) -----
        job = dg.define_asset_job(
            name=f"{stem}__job",
            selection=dg.AssetSelection.assets(landed, validated, *downstream),
            tags=run_tags(cfg, stem),
        )
        schedules, sensors = schedule_or_sensor(cfg, stem, job)

        return dg.Definitions(
            assets=[landed, validated, *downstream],
            asset_checks=[schema_ok],
            jobs=[job],
            schedules=schedules,
            sensors=sensors,
            # NOTE: resources are intentionally NOT declared here — they live
            # once in the top-level Definitions and bind to these assets by
            # parameter name (landing, sftp, ckan, geocoder, spatial).
        )


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _guard_state_loss(
    prior: pd.DataFrame | None, meta: dict[str, Any] | None, stem: str
) -> None:
    """Refuse to accumulate onto a canonical table that came back short.

    This is the dangerous failure for an accumulating dataset. If the stored
    parquet is missing or truncated, the merge quietly treats the delta as the
    whole dataset, `loaded` uploads that as the complete file, and DataPusher+
    reloads CKAN down to it — the published dataset loses its history without
    anything erroring. The sidecar says what we last wrote, so a mismatch is
    detectable, and it has to stop the run BEFORE the publish.

    Nothing to compare on a genuine first run (no sidecar). To reset an
    accumulator deliberately, delete both `current.parquet` and `current.json`.
    """
    if meta is None:
        return  # first run, or state predates the sidecar
    expected = meta.get("rows")
    if not isinstance(expected, int):
        return
    actual = 0 if prior is None else len(prior)
    if actual >= expected:
        return
    raise dg.Failure(
        description=(
            f"{stem}: the accumulated table came back with {actual} rows but "
            f"{expected} were last written ({meta.get('updated_at')}). Refusing "
            "to continue: merging onto a short table would publish it as the "
            "complete dataset and DataPusher+ would reload CKAN down to it. "
            "Restore the state object, or — if you mean to start the "
            "accumulation over — delete both current.parquet and current.json."
        ),
        metadata={"rows_found": actual, "rows_expected": expected},
    )


def _make_representation_asset(
    cfg: "TabularPipeline",
    rep: RepresentationModel,
    key_prefix: list[str],
    partitions: dg.PartitionsDefinition | None,
    group: str,
) -> dg.AssetsDefinition:
    """Build a publish asset for one additional representation. It reads the
    canonical validated frame and serializes/uploads it in `rep.format`."""
    fmt = rep.format.lower()

    @dg.asset(
        key=[*key_prefix, "published", fmt],
        retry_policy=network_retry_policy(),
        partitions_def=partitions,
        ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
        group_name=group,
    )
    def _representation(
        context: dg.AssetExecutionContext,
        validated_df: pd.DataFrame,
        ckan: CkanResource,
    ) -> None:
        publish_representation(cfg, rep, validated_df, ckan=ckan, context=context)

    return _representation


def _catalog_entry(cfg: "TabularPipeline") -> dict:
    """The catalogue entry backing this dataset, for mirroring/metadata.

    Re-resolved per run, like the extractor does, so a republished layer (new
    ArcGIS item id, new download URLs) keeps working without a config change.
    `fetch_catalog` caches per process, so the sibling assets in one run share
    a single fetch.

    A source that is not a Hub layer (PASDA, SFTP, ...) has no catalogue at
    all, and returns an empty entry: `sync_package_metadata` then publishes
    only what the defs.yaml states, and leaves the package's tags alone.
    """
    from wprdc_etl.strategies.extract import fetch_catalog

    if cfg.source.type != "arcgis":
        return {}

    catalog = fetch_catalog(cfg.source.catalog)
    title = cfg.source.title
    matches = [d for d in catalog if d.get("title") == title]
    if not matches:
        wanted = (title or "").strip()
        matches = [d for d in catalog if (d.get("title") or "").strip() == wanted]
    if not matches:
        raise dg.Failure(
            f"no catalogue entry titled {title!r} in {cfg.source.catalog}",
            allow_retries=False,
        )
    matches.sort(key=lambda d: d.get("modified") or "", reverse=True)
    return matches[0]


def _make_mirror_asset(
    cfg: "TabularPipeline",
    mirror: MirrorModel,
    key_prefix: list[str],
    partitions: dg.PartitionsDefinition | None,
    group: str,
) -> dg.AssetsDefinition:
    """Build the publish asset for one mirrored distribution.

    It depends on `landed` rather than `validated`: a mirror is a copy of the
    upstream file, so it neither reads nor needs the parsed frame. The
    dependency is there to keep the mirrors inside the dataset's job and
    ordered after the extract that proved the layer resolves.
    """
    fmt = mirror.format.lower()

    @dg.asset(
        key=[*key_prefix, "mirrored", fmt],
        retry_policy=network_retry_policy(),
        partitions_def=partitions,
        ins={"manifest": dg.AssetIn(key=[*key_prefix, "landed"])},
        group_name=group,
    )
    def _mirror(
        context: dg.AssetExecutionContext,
        manifest: dict,
        ckan: CkanResource,
        landing: LandingZoneResource,
    ) -> None:
        # `landing` + `manifest` let the mirror reuse the landed bytes when the
        # distribution is the one this dataset read, rather than fetching the
        # same file a second time. That is what keeps the published geojson and
        # the PostGIS region layer byte-identical.
        publish_mirror(
            cfg,
            mirror,
            _catalog_entry(cfg),
            ckan=ckan,
            landing=landing,
            manifest=manifest,
            context=context,
        )

    return _mirror


def _make_metadata_asset(
    cfg: "TabularPipeline",
    key_prefix: list[str],
    partitions: dg.PartitionsDefinition | None,
    group: str,
) -> dg.AssetsDefinition:
    """Build the asset that pushes the catalogue's description + tags to CKAN."""

    @dg.asset(
        key=[*key_prefix, "package_metadata"],
        retry_policy=network_retry_policy(),
        partitions_def=partitions,
        ins={"manifest": dg.AssetIn(key=[*key_prefix, "landed"])},
        group_name=group,
    )
    def _metadata(
        context: dg.AssetExecutionContext,
        manifest: dict,
        ckan: CkanResource,
    ) -> None:
        sync_package_metadata(cfg, _catalog_entry(cfg), ckan=ckan, context=context)

    return _metadata


def _make_region_layer_asset(
    cfg: "TabularPipeline",
    key_prefix: list[str],
    partitions: dg.PartitionsDefinition | None,
    group: str,
) -> dg.AssetsDefinition:
    """Build the asset that refreshes this dataset's layer in the PostGIS
    admin-region store, so publishing a boundary dataset is what keeps reverse
    geocoding current."""
    spec = cfg.region_layer

    @dg.asset(
        key=[*key_prefix, "region_layer"],
        retry_policy=network_retry_policy(),
        partitions_def=partitions,
        ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
        group_name=group,
    )
    def _region_layer(
        context: dg.AssetExecutionContext,
        validated_df: pd.DataFrame,
        spatial: SpatialResource,
    ) -> None:
        load = spatial.replace_layer(
            spec.name,
            validated_df,
            value_field=spec.value_field,
            label_field=spec.label_field,
            source=cfg.source.url or cfg.source.path,
        )
        context.log.info(
            f"admin_region: loaded {load.rows} {spec.name!r} regions"
            if load.changed
            else f"unchanged: the store already holds these {load.rows} "
            f"{spec.name!r} regions — not rewritten"
        )
        context.add_output_metadata(
            {"layer": spec.name, "regions": load.rows, "changed": load.changed}
        )

    return _region_layer


def _make_key_layer_asset(
    cfg: "TabularPipeline",
    key_prefix: list[str],
    partitions: dg.PartitionsDefinition | None,
    group: str,
) -> dg.AssetsDefinition:
    """Build the asset that refreshes this dataset's layer in the PostGIS
    keyed-geometry store, so publishing it is what keeps join_geometry
    current."""
    spec = cfg.key_layer

    @dg.asset(
        key=[*key_prefix, "key_layer"],
        retry_policy=network_retry_policy(),
        partitions_def=partitions,
        ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
        group_name=group,
    )
    def _key_layer(
        context: dg.AssetExecutionContext,
        validated_df: pd.DataFrame,
        spatial: SpatialResource,
    ) -> None:
        load = spatial.replace_key_layer(
            spec.name,
            validated_df,
            key_field=spec.key_field,
            source=(
                cfg.source.url
                or cfg.source.path
                or cfg.source.title
                or (f"pasda:{cfg.source.dataset_id}" if cfg.source.dataset_id else None)
            ),
        )
        context.log.info(
            f"keyed_geometry: loaded {load.rows} {spec.name!r} rows"
            if load.changed
            else f"unchanged: the store already holds these {load.rows} "
            f"{spec.name!r} rows — not rewritten"
        )
        if load.duplicates:
            context.log.warning(
                f"keyed_geometry: {load.duplicates} rows repeated an existing "
                f"{spec.key_field!r} and were dropped (first one kept)"
            )
        context.add_output_metadata(
            {
                "layer": spec.name,
                "rows": load.rows,
                "duplicate_keys": load.duplicates,
                "changed": load.changed,
            }
        )

    return _key_layer


def _column_fingerprint(columns: Iterable[str]) -> str:
    """Hash of a column SET — order-insensitive, so a source that reorders its
    columns isn't reported as drift."""
    return hashlib.sha256(",".join(sorted(columns)).encode()).hexdigest()


def _drift_report(present: set[str], previous: dict[str, Any] | None) -> dict[str, Any]:
    """Compare this run's columns to the last set recorded for the dataset.

    With no recorded baseline (a dataset's first check) nothing has drifted —
    that run just establishes one. `columns_added` / `columns_removed` are
    filled in only when the baseline actually carries a column list, so a
    hash-only state degrades to the bare flag rather than reporting every
    column as new.
    """
    fingerprint = _column_fingerprint(present)
    prev = previous or {}
    prev_hash = prev.get("schema_hash")
    drifted = prev_hash is not None and prev_hash != fingerprint

    prev_columns = prev.get("columns")
    if drifted and prev_columns is not None:
        added = sorted(present - set(prev_columns))
        removed = sorted(set(prev_columns) - present)
    else:
        added, removed = [], []

    return {
        "column_hash": fingerprint,
        "schema_drifted": drifted,
        "columns_added": added,
        "columns_removed": removed,
    }


def _load_schema(cfg: TabularPipeline) -> "pa.DataFrameSchema | None":
    """Return the co-located schema.py's SCHEMA (a pandera DataFrameSchema),
    or None if the dataset has no rich schema."""
    mod = load_dataset_module(cfg, "schema")
    return getattr(mod, "SCHEMA", None) if mod is not None else None


def _load_transform(
    cfg: TabularPipeline,
) -> Callable[[pd.DataFrame, TabularPipeline], pd.DataFrame] | None:
    """Look for a co-located transform.py next to the defs.yaml and return
    its `transform` callable, or None if the dataset has no custom step."""
    mod = load_dataset_module(cfg, "transform")
    return getattr(mod, "transform", None) if mod is not None else None


def _transform(
    df: pd.DataFrame,
    cfg: TabularPipeline,
    *,
    deps: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Declarative steps first (shared vocabulary), then the per-dataset escape
    hatch. A dataset can use either, both, or neither.

    `deps` carries the runtime objects primitives request via @needs — today the
    spatial store and the run context, for reverse_geocode."""
    if cfg.transforms:
        df = apply_declarative(df, cfg.transforms, deps=deps)
    fn = _load_transform(cfg)
    if fn is not None:
        df = fn(df, cfg)
    return df
