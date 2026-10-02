"""Helpers shared by the component types (tabular + file).

These operate on the config duck-typed: any cfg with publisher / department /
dataset / partition / schedule fields works.
"""

from __future__ import annotations

from collections.abc import Iterator  # runtime import — see arrival_sensor
from typing import TYPE_CHECKING

import dagster as dg

if TYPE_CHECKING:
    # No public alias for what define_asset_job() returns, so it comes from the
    # private path. Type-checking only — nothing imports it at runtime.
    from dagster._core.definitions.unresolved_asset_job_definition import (
        UnresolvedAssetJobDefinition,
    )

    from wprdc_etl.components.models import PipelineConfig

_START = "2024-01-01"

# Every publisher is a Pittsburgh-area civic agency, so schedules fire on
# Eastern wall-clock time, not UTC. (Partition boundaries are still UTC — see
# partitions_for; aligning those to Eastern is a separate, larger change.)
SCHEDULE_TZ = "America/New_York"


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
    if cadence == "monthly":
        return dg.MonthlyPartitionsDefinition(start_date=_START)
    if cadence == "weekly":
        return dg.WeeklyPartitionsDefinition(start_date=_START)
    if cadence in (None, "none"):
        return None
    return dg.DailyPartitionsDefinition(start_date=_START)


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
    return tags


def schedule_or_sensor(
    cfg: PipelineConfig,
    stem: str,
    job: UnresolvedAssetJobDefinition,
) -> tuple[list[dg.ScheduleDefinition], list[dg.SensorDefinition]]:
    """A cron schedule when cfg.schedule is set, else a placeholder arrival
    sensor. Returns (schedules, sensors)."""
    if cfg.schedule:
        return (
            [
                dg.ScheduleDefinition(
                    name=f"{stem}__schedule",
                    job=job,
                    cron_schedule=cfg.schedule,
                    execution_timezone=SCHEDULE_TZ,
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
