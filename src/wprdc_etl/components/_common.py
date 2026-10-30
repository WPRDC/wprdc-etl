"""Helpers shared by the component types (tabular + file).

These operate on the config duck-typed: any cfg with publisher / department /
dataset / partition / schedule fields works.
"""

from __future__ import annotations

import os
from collections.abc import Iterator  # runtime import — see arrival_sensor
from typing import TYPE_CHECKING, Any

import dagster as dg

if TYPE_CHECKING:
    # No public alias for what define_asset_job() returns, so it comes from the
    # private path. Type-checking only — nothing imports it at runtime.
    from dagster._core.definitions.unresolved_asset_job_definition import (
        UnresolvedAssetJobDefinition,
    )

    from wprdc_etl.components.models import PipelineConfig

# The first partition of every partitioned dataset: the month before the
# production launch. Earlier partitions would hold no data, crowd the
# launchpad, show as "missing" and invite a backfill of hundreds of runs —
# and most sources are snapshots, so running one would file TODAY's data
# under that old date. Not October: a monthly partition only exists once its
# month has ended, and the monthly datasets needed one to launch by hand.
_START = "2026-09-01"

# Every publisher is a Pittsburgh-area civic agency, so schedules fire on
# Eastern wall-clock time, not UTC, and partitions turn over at Eastern
# midnight too (partitions_for builds them in this timezone).
SCHEDULE_TZ = "America/New_York"


def network_retry_policy() -> dg.RetryPolicy | None:
    """The retry policy for steps that talk to the outside world.

    Production only. Sources and CKAN fail transiently — an ArcGIS export still
    building after the in-code wait, a 5xx, a dropped SFTP session — and each
    unretried one is a failed run and a Slack alert. Backoff of ~1, 2, 4 min
    (jittered, so a Monday burst of 100 layers doesn't retry in lockstep).

    Off in dev: a failure there should surface in seconds, not after seven
    minutes of retries. Failures that can't fix themselves (missing config, a
    title gone from the catalogue, the replace column guard) are raised with
    `allow_retries=False` and skip this entirely.
    """
    from wprdc_etl.runtime import is_production

    if not is_production():
        return None
    return dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    )


def resolve_modes(cfg: PipelineConfig) -> tuple[str, bool]:
    """Fill in `publish` and `accumulate` from `ingest` when they're unset.

    `ingest` (how data arrives) and `publish` (how it reaches CKAN) are separate
    axes, but the old one-field world is still the common case, so an unset
    `publish` derives the pairing it used to imply:

        snapshot    -> replace   (source is already the whole state)
        incremental -> upsert    (delta straight into the DataStore)

    `accumulate` — whether we keep a cumulative canonical table — is forced on
    for incremental+replace, because a delta on its own cannot be published as a
    complete file. Elsewhere it's an explicit opt-in: a snapshot source whose
    target spans more than the file it just got (cumulative crashes,
    delinquent_all) sets it by hand.
    """
    ingest = getattr(cfg, "ingest", "snapshot")
    publish = getattr(cfg, "publish", None) or (
        "upsert" if ingest == "incremental" else "replace"
    )
    accumulate = getattr(cfg, "accumulate", None)
    if accumulate is None:
        accumulate = ingest == "incremental" and publish == "replace"
    return publish, bool(accumulate)


def partitions_for(cadence: str | None) -> dg.PartitionsDefinition | None:
    """Map a cadence string to a PartitionsDefinition. 'none'/None -> None
    (an unpartitioned asset, e.g. a single evolving blob)."""
    # In Eastern time, not UTC: a partition schedule takes its timezone from
    # here (Dagster refuses one passed to the schedule), and a Pittsburgh
    # week or month should turn over at Pittsburgh midnight.
    if cadence == "monthly":
        return dg.MonthlyPartitionsDefinition(start_date=_START, timezone=SCHEDULE_TZ)
    if cadence == "weekly":
        return dg.WeeklyPartitionsDefinition(start_date=_START, timezone=SCHEDULE_TZ)
    if cadence in (None, "none"):
        return None
    return dg.DailyPartitionsDefinition(start_date=_START, timezone=SCHEDULE_TZ)


def taxonomy(cfg: PipelineConfig) -> tuple[list[str], str, str]:
    """Return (key_prefix, group_name, stem) from publisher/department/dataset.

    key_prefix -> asset key, department included only when present.
    group_name -> UI group (no slashes allowed, so underscore-joined).
    stem       -> collision-safe base for job/schedule/sensor names.
    """
    key_prefix = [
        cfg.publisher,
        *([cfg.department] if cfg.department else []),
        cfg.dataset,
    ]
    group = f"{cfg.publisher}_{cfg.department}" if cfg.department else cfg.publisher
    stem = "__".join(k for k in [cfg.publisher, cfg.department, cfg.dataset] if k)
    return key_prefix, group, stem


def run_tags(cfg: PipelineConfig, stem: str) -> dict[str, str]:
    """Run tags stamped on every dataset's asset job. The prod
    QueuedRunCoordinator's tag_concurrency_limits key off these
    (deploy/prod/dagster.yaml): one run per dataset, a small cap per publisher,
    one memory-heavy run at a time. Duck-typed on cfg — FilePipeline has no
    `ingest`."""
    tags = {
        "wprdc/publisher": cfg.publisher,
        "wprdc/dataset": stem,
        "wprdc/source": cfg.source.type,
    }
    if getattr(cfg, "ingest", None):
        tags["wprdc/ingest"] = cfg.ingest
    if getattr(cfg, "heavy", False):
        tags["wprdc/heavy"] = "true"
    # Which PostGIS layer the job feeds, so the runs that must go first on a
    # fresh database (deploy/ROLLOUT.md §4) can be filtered in the UI. The UI
    # filters on key AND value, hence one constant tag to select them all.
    if getattr(cfg, "region_layer", None):
        tags["wprdc/region_layer"] = cfg.region_layer.name
    if getattr(cfg, "key_layer", None):
        tags["wprdc/key_layer"] = cfg.key_layer.name
    if "wprdc/region_layer" in tags or "wprdc/key_layer" in tags:
        tags["wprdc/postgis_layer"] = "true"
    return tags


def landed_resources(cfg: PipelineConfig) -> set[str]:
    """Resource keys the `landed` asset requests: landing, plus whatever the
    source's extractor declares (only SFTP sources need `sftp`)."""
    from wprdc_etl.strategies import get_extractor

    return {"landing", *get_extractor(cfg.source.type).resources}


def land(context: dg.AssetExecutionContext, cfg: PipelineConfig) -> dict[str, Any]:
    """The `landed` asset's body, shared by both pipeline types: run the
    source's extractor with the resources `landed_resources` requested."""
    from wprdc_etl.strategies import get_extractor

    extractor = get_extractor(cfg.source.type)
    landing = context.resources.landing
    sftp = context.resources.sftp if "sftp" in extractor.resources else None
    manifest = extractor.extract(context, cfg, landing=landing, sftp=sftp)
    partition = context.partition_key if context.has_partition_key else "current"
    try:
        context.add_output_metadata(landing_links(landing, cfg, partition))
    except Exception as e:  # a link is a nicety; it must never fail a landing
        context.log.warning(f"couldn't attach the landing links: {e}")
    return manifest


def landing_links(landing: Any, cfg: PipelineConfig, partition: str) -> dict:
    """Where this run landed, as run/asset metadata: the s3:// URI, and on
    real AWS a link to the prefix in the S3 console (LocalStack has none)."""
    from urllib.parse import quote

    prefix = landing.prefix(cfg.publisher, cfg.dataset, partition, cfg.department)
    links: dict[str, Any] = {
        "landing_uri": dg.MetadataValue.path(f"s3://{landing.bucket}/{prefix}/")
    }
    if not landing.endpoint_url:
        region = landing.region
        links["landing_console"] = dg.MetadataValue.url(
            f"https://{region}.console.aws.amazon.com/s3/buckets/{landing.bucket}"
            f"?region={region}&prefix={quote(prefix + '/')}&showversions=false"
        )
    return links


# Source formats that read into a GeoDataFrame.
_GEO_FORMATS = {"geojson", "shapefile"}


def landed_kinds(cfg: PipelineConfig) -> set[str]:
    """Kinds for `landed`: it writes to S3, from this kind of source. Kinds
    are the catalog's icons and its `kind:` filter (at most three each)."""
    source = cfg.source.type
    return {"s3", "api" if source.startswith("api") else source}


def frame_kinds(cfg: PipelineConfig) -> set[str]:
    """Kinds for `validated`. The frame's type is only known at run time, but
    the source format decides it: geojson and shapefile read as geopandas."""
    fmt = (getattr(cfg.source, "format", None) or "").lower()
    return {"geopandas" if fmt in _GEO_FORMATS else "pandas"}


def asset_tags(cfg: PipelineConfig) -> dict[str, str]:
    """The run tags, for every asset of the dataset, so the asset catalog can
    filter on them (`tag:wprdc/source=sftp`) — the Jobs list can't filter on
    tags at all. Minus `wprdc/dataset`: asset tag values are capped at 63
    characters and some stems are longer; the asset key names the dataset
    anyway."""
    tags = run_tags(cfg, "")
    del tags["wprdc/dataset"]
    return tags


def default_schedule_status() -> dg.DefaultScheduleStatus:
    """RUNNING in production, STOPPED everywhere else.

    Dagster creates every schedule stopped, so a production deploy would run
    nothing until someone switched ~110 schedules on by hand. In dev they stay
    stopped: `dg dev` with a daemon must not start pulling sources and
    publishing on its own. Same gate as the Slack failure sensor.

    `WPRDC_SCHEDULES_PAUSED=1` holds them STOPPED in production too — the
    brake for a first production boot (deploy/ROLLOUT.md), so schedules are
    switched on in batches rather than all at once. A schedule started or
    stopped in the UI keeps that state whatever this returns; the default only
    applies to a schedule nobody has touched.
    """
    from wprdc_etl.runtime import is_production

    paused = os.getenv("WPRDC_SCHEDULES_PAUSED", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    return (
        dg.DefaultScheduleStatus.RUNNING
        if is_production() and not paused
        else dg.DefaultScheduleStatus.STOPPED
    )


def partitioned_schedule_fields(cadence: str, cron: str) -> dict[str, int]:
    """The minute/hour/day of `cron`, checked against a partition cadence.

    A partitioned job's schedule has to say WHICH partition to run, and
    `build_schedule_from_partitioned_job` (which runs the latest complete one)
    can only express a cron that fits the cadence: a weekly job fires on one
    day of the week, a monthly job on one day of the month, a daily job every
    day. Raises ValueError otherwise, so `dg check` fails rather than the
    daemon.
    """
    parts = cron.split()
    if len(parts) != 5 or parts[3] != "*":
        raise ValueError(f"schedule {cron!r}: expected 'M H DOM * DOW'")
    minute, hour, dom, _, dow = parts
    if not (minute.isdigit() and hour.isdigit()):
        raise ValueError(f"schedule {cron!r}: minute and hour must be single numbers")
    fields = {"minute_of_hour": int(minute), "hour_of_day": int(hour)}
    if cadence == "weekly" and dom == "*" and dow.isdigit():
        return {**fields, "day_of_week": int(dow)}
    if cadence == "monthly" and dom.isdigit() and dow == "*":
        return {**fields, "day_of_month": int(dom)}
    if cadence not in ("weekly", "monthly") and dom == "*" and dow == "*":
        return fields
    raise ValueError(
        f"schedule {cron!r} doesn't fit {cadence!r} partitions — weekly needs "
        "'M H * * DOW', monthly 'M H DOM * *', daily 'M H * * *'"
    )


def schedule_or_sensor(
    cfg: PipelineConfig,
    stem: str,
    job: UnresolvedAssetJobDefinition,
    partitions: dg.PartitionsDefinition | None = None,
) -> tuple[list[dg.ScheduleDefinition], list[dg.SensorDefinition]]:
    """A cron schedule when cfg.schedule is set, else a placeholder arrival
    sensor. Returns (schedules, sensors).

    A partitioned job gets `build_schedule_from_partitioned_job`, so each tick
    runs the latest COMPLETE partition. A plain ScheduleDefinition requests
    no partition at all, and every scheduled run of a partitioned job failed
    for want of one (bin/run hid it by choosing the partition itself).
    """
    if cfg.schedule and partitions is not None:
        # No timezone here: for a time-partitioned job Dagster takes it from
        # the partitions definition, which partitions_for() sets to Eastern.
        fields = partitioned_schedule_fields(cfg.partition, cfg.schedule)
        return (
            [
                dg.build_schedule_from_partitioned_job(
                    job,
                    name=f"{stem}__schedule",
                    default_status=default_schedule_status(),
                    **fields,
                )
            ],
            [],
        )
    if cfg.schedule:
        return (
            [
                dg.ScheduleDefinition(
                    name=f"{stem}__schedule",
                    job=job,
                    cron_schedule=cfg.schedule,
                    execution_timezone=SCHEDULE_TZ,
                    default_status=default_schedule_status(),
                )
            ],
            [],
        )

    # Iterator is imported at runtime, not under TYPE_CHECKING: @dg.sensor
    # resolves this function's annotations when it builds the definition, and
    # this module stringizes them (`from __future__ import annotations`).
    @dg.sensor(job=job, name=f"{stem}__sensor")
    def arrival_sensor(context: dg.SensorEvaluationContext) -> Iterator[dg.SkipReason]:
        yield dg.SkipReason("arrival sensor not yet implemented")

    return ([], [arrival_sensor])
