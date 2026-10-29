"""Queue every dataset job's latest partition and let the run queue pace them.

    deploy/launch-all [--dry-run] [--layers] [--only SUBSTR] [--source TYPE]

Runs inside the webserver container (deploy/launch-all wraps it). Each job is
submitted through the webserver's GraphQL API, the same path the Launchpad
uses, so every run is a real job run: it carries the job's tags and the
QueuedRunCoordinator's limits apply (DAGSTER_MAX_CONCURRENT_RUNS, two per
publisher, one heavy decode, one per dataset). Selecting assets in the
catalog and clicking Materialize would launch one ad-hoc run without them.

The PostGIS layer jobs are queued at a higher `dagster/priority`, so the
datasets that read those layers (reverse_geocode, join_geometry) find them
loaded. A job that already has a run queued or in progress is skipped, so the
script can be re-run safely.
"""

import argparse
import sys
from dataclasses import dataclass, field

import dagster as dg

# The queue launches higher priorities first; Dagster's default is 0.
LAYER_PRIORITY = "10"
# A run in one of these states means the job is already on its way.
_PENDING = {
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
}


@dataclass
class Launch:
    job: str
    partition: str | None
    tags: dict[str, str] = field(default_factory=dict)


def plan(
    defs: dg.Definitions,
    only: str | None = None,
    source: str | None = None,
    layers: bool = False,
) -> list[Launch]:
    """One Launch per dataset job, PostGIS layers first. Dataset jobs are the
    ones run_tags() stamped with `wprdc/dataset`; maintenance jobs aren't."""
    launches = []
    for job in defs.resolve_all_job_defs():
        tags = job.tags or {}
        if "wprdc/dataset" not in tags:
            continue
        if only and only not in job.name:
            continue
        if source and tags.get("wprdc/source") != source:
            continue
        if layers and tags.get("wprdc/postgis_layer") != "true":
            continue
        pdef = job.partitions_def
        partition = pdef.get_last_partition_key() if pdef else None
        if pdef and partition is None:
            continue  # no complete partition yet; nothing to run
        run_tags = {"wprdc/launched_by": "launch-all"}
        if partition:
            run_tags["dagster/partition"] = partition
        if tags.get("wprdc/postgis_layer") == "true":
            run_tags["dagster/priority"] = LAYER_PRIORITY
        launches.append(Launch(job.name, partition, run_tags))
    return sorted(launches, key=_layers_first)


def _layers_first(launch: Launch) -> tuple[bool, str]:
    return (launch.tags.get("dagster/priority") != LAYER_PRIORITY, launch.job)


def pending_jobs(instance: dg.DagsterInstance) -> set[str]:
    """Jobs with a run already queued or in progress."""
    runs = instance.get_runs(dg.RunsFilter(statuses=list(_PENDING)))
    return {run.job_name for run in runs}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="list, submit nothing")
    parser.add_argument("--layers", action="store_true", help="PostGIS layer jobs only")
    parser.add_argument("--only", help="job name contains this (e.g. __gis__)")
    parser.add_argument("--source", help="sftp | http | arcgis | pasda | api_…")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3000)
    args = parser.parse_args(argv)

    from wprdc_etl.definitions import defs

    launches = plan(defs, args.only, args.source, args.layers)
    busy = pending_jobs(dg.DagsterInstance.get())

    client = None
    if not args.dry_run:
        from dagster_graphql import DagsterGraphQLClient

        client = DagsterGraphQLClient(args.host, port_number=args.port)

    submitted = skipped = 0
    for launch in launches:
        label = f"{launch.job} [{launch.partition or 'unpartitioned'}]"
        if "dagster/priority" in launch.tags:
            label += " (layer, first)"
        if launch.job in busy:
            print(f"skip    {label}: already queued or running")
            skipped += 1
            continue
        if client is None:
            print(f"would   {label}")
            continue
        run_id = client.submit_job_execution(launch.job, tags=launch.tags)
        print(f"queued  {label}  {run_id[:8]}")
        submitted += 1

    verb = "would queue" if args.dry_run else "queued"
    count = len(launches) - skipped if args.dry_run else submitted
    print(f"{verb} {count} run(s); skipped {skipped} already pending")
    return 0


if __name__ == "__main__":
    sys.exit(main())
