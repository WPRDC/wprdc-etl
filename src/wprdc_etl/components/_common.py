"""Helpers shared by the component types (tabular + file).

These operate on the config duck-typed: any cfg with publisher / department /
dataset / partition / schedule fields works.
"""

from __future__ import annotations

import dagster as dg

_START = "2024-01-01"

# Every publisher is a Pittsburgh-area civic agency, so schedules fire on
# Eastern wall-clock time, not UTC. (Partition boundaries are still UTC — see
# partitions_for; aligning those to Eastern is a separate, larger change.)
SCHEDULE_TZ = "America/New_York"


def partitions_for(cadence):
    """Map a cadence string to a PartitionsDefinition. 'none'/None -> None
    (an unpartitioned asset, e.g. a single evolving blob)."""
    if cadence == "monthly":
        return dg.MonthlyPartitionsDefinition(start_date=_START)
    if cadence == "weekly":
        return dg.WeeklyPartitionsDefinition(start_date=_START)
    if cadence in (None, "none"):
        return None
    return dg.DailyPartitionsDefinition(start_date=_START)


def taxonomy(cfg):
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


def run_tags(cfg, stem):
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


def schedule_or_sensor(cfg, stem, job):
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

    @dg.sensor(job=job, name=f"{stem}__sensor")
    def arrival_sensor(context):
        yield dg.SkipReason("arrival sensor not yet implemented")

    return ([], [arrival_sensor])
