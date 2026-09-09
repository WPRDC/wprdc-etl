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

import dagster as dg
from dagster.components import Component, Model, Resolvable

from wprdc_etl.resources import (
    CkanResource,
    GeocoderResource,
    LandingZoneResource,
    SFTPResource,
)
from wprdc_etl.components._common import (
    partitions_for,
    run_tags,
    schedule_or_sensor,
    taxonomy,
)
from wprdc_etl.components.models import CkanModel, RepresentationModel, SourceModel
from wprdc_etl.dataset_modules import load_dataset_module
from wprdc_etl.strategies import get_extractor, get_loader
from wprdc_etl.strategies.emit import publish_representation
from wprdc_etl.strategies.read import read_landed
from wprdc_etl.strategies.transform import apply_declarative, validate_steps


class TabularPipeline(Component, Model, Resolvable):
    publisher: str
    dataset: str
    source: SourceModel
    ckan: CkanModel
    # Optional middle tier: the agency/department within a publisher (e.g.
    # allegheny_county -> real_estate -> assessments). When set, it becomes
    # part of the asset key. Left None for publishers with no sub-org.
    department: str | None = None
    schedule: str | None = None  # cron; if None, a sensor is used instead
    partition: str = "daily"  # "daily" | "weekly" | "monthly"
    # High-level mode. Selects the loader and (later) the partition/state model:
    #   snapshot    -> full-state extract + DataPusher+ replace
    #   incremental -> cursor/watermark delta extract + DataStore upsert
    ingest: str = "snapshot"
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
    # Additional non-tabular representations (GeoJSON, Shapefile) published as
    # their own CKAN file resources from the same canonical frame. The primary
    # tabular resource is still ckan.resource_id. Intended for snapshot datasets.
    representations: list[RepresentationModel] | None = None
    geocode: bool = False
    # Set for datasets whose source file is large enough that a full-frame
    # decode is memory-heavy (hundreds of MB). Emits the wprdc/heavy run tag,
    # which the prod QueuedRunCoordinator limits to one concurrent run.
    heavy: bool = False

    # ----------------------------------------------------------------------
    def build_defs(self, context) -> dg.Definitions:
        publisher, dataset = self.publisher, self.dataset
        cfg = self  # readable alias
        validate_steps(cfg.transforms)  # fail loud at load on an unknown op
        key_prefix, group, stem = taxonomy(cfg)
        if cfg.ingest == "incremental":
            # Watermark drives increments, not the calendar; and upsert needs a
            # primary key. Both are load-time (fail-loud) invariants.
            if not (cfg.ckan and cfg.ckan.primary_key):
                raise ValueError(
                    f"{stem}: ingest 'incremental' requires ckan.primary_key (for upsert)"
                )
            partitions = None
        else:
            partitions = partitions_for(cfg.partition)

        # -- landed -------------------------------------------------------
        @dg.asset(
            key=[*key_prefix, "landed"],
            partitions_def=partitions,
            group_name=group,
        )
        def landed(
            context: dg.AssetExecutionContext,
            landing: LandingZoneResource,
            sftp: SFTPResource,
        ) -> dict:
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
        ):
            partition = (
                context.partition_key if context.has_partition_key else "current"
            )
            df = read_landed(landing, cfg, partition)
            if cfg.geocode:
                df = geocoder.geocode_frame(df, address_col="address")
            return _transform(df, cfg)

        # -- loaded -------------------------------------------------------
        @dg.asset(
            key=[*key_prefix, "loaded"],
            partitions_def=partitions,
            ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
            group_name=group,
        )
        def loaded(
            context: dg.AssetExecutionContext,
            validated_df,
            ckan: CkanResource,
        ):
            # validated_df is the validated asset's return value, delivered by
            # the IO manager (configure an S3 IO manager in prod; the default fs
            # manager is fine locally). Loader is derived from the ingest mode.
            loader = get_loader(cfg.ingest)
            loader.load(cfg, validated_df, ckan=ckan, context=context)

        # -- schema / drift check on the validated asset ------------------
        @dg.asset_check(asset=validated)
        def schema_ok(
            context: dg.AssetCheckExecutionContext,
            landing: LandingZoneResource,
        ) -> dg.AssetCheckResult:
            import hashlib

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

            # cheap drift signal: hash the sorted column set, compare to stored
            col_hash = hashlib.sha256(",".join(sorted(present)).encode()).hexdigest()
            prev = (
                landing.read_manifest(
                    publisher, dataset, partition, department=cfg.department
                )
                or {}
            )
            drift = prev.get("schema_hash") not in (None, col_hash)

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
                    "schema_drifted": drift,  # INFO: extra/changed columns
                    "column_hash": col_hash,
                },
            )

        # -- additional representations (geojson/shapefile) ----------------
        # One publish asset per representation, each serializing the canonical
        # validated frame to its format and uploading to its own resource.
        rep_assets = [
            _make_representation_asset(cfg, rep, key_prefix, partitions, group)
            for rep in (cfg.representations or [])
        ]

        # -- schedule (reliable cadence) OR sensor (irregular arrival) -----
        job = dg.define_asset_job(
            name=f"{stem}__job",
            selection=dg.AssetSelection.assets(landed, validated, loaded, *rep_assets),
            tags=run_tags(cfg, stem),
        )
        schedules, sensors = schedule_or_sensor(cfg, stem, job)

        return dg.Definitions(
            assets=[landed, validated, loaded, *rep_assets],
            asset_checks=[schema_ok],
            jobs=[job],
            schedules=schedules,
            sensors=sensors,
            # NOTE: resources are intentionally NOT declared here — they live
            # once in the top-level Definitions and bind to these assets by
            # parameter name (landing, sftp, ckan, geocoder).
        )


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def _make_representation_asset(cfg, rep, key_prefix, partitions, group):
    """Build a publish asset for one additional representation. It reads the
    canonical validated frame and serializes/uploads it in `rep.format`."""
    fmt = rep.format.lower()

    @dg.asset(
        key=[*key_prefix, "published", fmt],
        partitions_def=partitions,
        ins={"validated_df": dg.AssetIn(key=[*key_prefix, "validated"])},
        group_name=group,
    )
    def _representation(
        context: dg.AssetExecutionContext,
        validated_df,
        ckan: CkanResource,
    ):
        publish_representation(cfg, rep, validated_df, ckan=ckan, context=context)

    return _representation


def _load_schema(cfg: TabularPipeline):
    """Return the co-located schema.py's SCHEMA (a pandera DataFrameSchema),
    or None if the dataset has no rich schema."""
    mod = load_dataset_module(cfg, "schema")
    return getattr(mod, "SCHEMA", None) if mod is not None else None


def _load_transform(cfg: TabularPipeline):
    """Look for a co-located transform.py next to the defs.yaml and return
    its `transform` callable, or None if the dataset has no custom step."""
    mod = load_dataset_module(cfg, "transform")
    return getattr(mod, "transform", None) if mod is not None else None


def _transform(df, cfg: TabularPipeline):
    """Declarative steps first (shared vocabulary), then the per-dataset escape
    hatch. A dataset can use either, both, or neither."""
    if cfg.transforms:
        df = apply_declarative(df, cfg.transforms)
    fn = _load_transform(cfg)
    if fn is not None:
        df = fn(df, cfg)
    return df
