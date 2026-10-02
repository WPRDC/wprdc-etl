"""FilePipeline component type — for blob datasets (PDFs, GeoTIFFs, images).

A blob is extract -> land -> publish: no parse, no transform, no validation,
no DataStore. It shares the tabular pipeline's substrate (extract strategies,
the S3 landing zone, taxonomy, scheduling) and differs only in that the
payload is never read into a DataFrame.

`partition` controls history, reusing the same vocabulary as tabular:
  * daily|weekly|monthly -> a snapshot series (e.g. a daily weather GeoTIFF).
    Every run's file is kept in the immutable landing bucket; CKAN carries a
    single "latest" resource that re-points at the newest partition.
  * none (default)        -> a single evolving object (e.g. a report PDF).
    Landed at .../current/, replace-in-place.

Publish uploads the landed object to ONE CKAN resource via a normal resource
upload (local-disk filestore). It streams S3 -> temp file -> CKAN so a large
blob is never buffered in memory, and it never touches the DataStore/DataPusher.

See the NOTE ON IMPORTS / VERSION in tabular_pipeline.py — same caveat here.
"""

import os
import shutil
import tempfile
from typing import Any

import dagster as dg
from dagster.components import Component, Model, Resolvable

from wprdc_etl.components._common import (
    network_retry_policy,
    partitions_for,
    run_tags,
    schedule_or_sensor,
    taxonomy,
)
from wprdc_etl.components.models import CkanModel, SourceModel
from wprdc_etl.resources import CkanResource, LandingZoneResource, SFTPResource
from wprdc_etl.runtime import sink_dir
from wprdc_etl.strategies import get_extractor


class FilePipeline(Component, Model, Resolvable):
    publisher: str
    dataset: str
    source: SourceModel
    ckan: CkanModel
    department: str | None = None
    schedule: str | None = None  # cron; if None, a sensor is used instead
    partition: str = "none"  # "none" | "daily" | "weekly" | "monthly"
    # Emits the wprdc/heavy run tag (prod coordinator: one heavy run at a time).
    heavy: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self
        key_prefix, group, stem = taxonomy(cfg)
        partitions = partitions_for(cfg.partition)  # None when partition == "none"

        # -- landed: pull the blob and land it (raw bytes + manifest) --------
        @dg.asset(
            key=[*key_prefix, "landed"],
            partitions_def=partitions,
            group_name=group,
            retry_policy=network_retry_policy(),
        )
        def landed(
            context: dg.AssetExecutionContext,
            landing: LandingZoneResource,
            sftp: SFTPResource,
        ) -> dict[str, Any]:
            return get_extractor(cfg.source.type).extract(
                context, cfg, landing=landing, sftp=sftp
            )

        # -- published: upload the landed object to the single CKAN resource -
        @dg.asset(
            key=[*key_prefix, "published"],
            partitions_def=partitions,
            retry_policy=network_retry_policy(),
            deps=[landed],
            group_name=group,
        )
        def published(
            context: dg.AssetExecutionContext,
            landing: LandingZoneResource,
            ckan: CkanResource,
        ) -> None:
            partition = (
                context.partition_key if context.has_partition_key else "current"
            )
            manifest = (
                landing.read_manifest(
                    cfg.publisher, cfg.dataset, partition, cfg.department
                )
                or {}
            )
            filename = manifest.get("filename", "data.bin")
            prefix = landing.prefix(
                cfg.publisher, cfg.dataset, partition, cfg.department
            )

            # Stream S3 -> temp file, then either copy to the dry-run sink or
            # upload to CKAN. Dry-run is the default unless ENVIRONMENT=production.
            fd, tmp = tempfile.mkstemp(suffix=os.path.splitext(filename)[1])
            os.close(fd)
            try:
                landing.download(f"{prefix}/{filename}", tmp)
                sink = sink_dir()
                if sink:
                    os.makedirs(sink, exist_ok=True)
                    stem_name = "__".join(
                        x for x in [cfg.publisher, cfg.department, cfg.dataset] if x
                    )
                    out = os.path.join(
                        sink, f"{stem_name}{os.path.splitext(filename)[1]}"
                    )
                    shutil.copyfile(tmp, out)
                    context.log.info(f"[dry-run] wrote {out} (skipped CKAN)")
                elif not ckan.publish_file(cfg.ckan.resource_id, tmp, filename):
                    context.log.info(
                        f"unchanged: {cfg.ckan.resource_id} already holds this file"
                    )
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)

        job = dg.define_asset_job(
            name=f"{stem}__job",
            selection=dg.AssetSelection.assets(landed, published),
            tags=run_tags(cfg, stem),
        )
        schedules, sensors = schedule_or_sensor(cfg, stem, job)

        return dg.Definitions(
            assets=[landed, published],
            jobs=[job],
            schedules=schedules,
            sensors=sensors,
        )
