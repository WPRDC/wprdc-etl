"""Top-level Definitions for wprdc-etl.

This is the single place shared resources are instantiated. Every
component instance's assets reference these by parameter name (landing,
sftp, ckan, geocoder, spatial), so there is exactly one of each across all
100+ publishers. The landing zone is AWS S3; see compose.yaml for the local
LocalStack option.

`load_defs` walks the defs/ tree, finds every component.yaml, validates it
against its component type's Model fields, and merges each component's
build_defs() output together. Resource binding happens at this merge.
"""

import logging
import os
from typing import Any

import dagster as dg
from dagster.components import load_defs

import wprdc_etl.defs
from wprdc_etl.resources import (
    CkanResource,
    GeocoderResource,
    LandingZoneResource,
    SFTPResource,
    SpatialResource,
)
from wprdc_etl.runtime import is_production


def _io_manager() -> dg.ConfigurableIOManagerFactory:
    """The IO manager for asset-to-asset handoffs (only `validated -> loaded`
    and the representation assets actually cross it; everything else re-reads
    from the S3 landing zone).

    - production           -> S3 pickle IO manager on real S3 (boto3 IAM chain)
    - WPRDC_IO_MANAGER=s3   -> S3 pickle IO manager, honouring S3_ENDPOINT_URL,
                              so a dev/CI run can exercise the prod path against
                              LocalStack (also set AWS_S3_ADDRESSING_STYLE=path)
    - WPRDC_IO_MANAGER=fs   -> force the local filesystem manager
    - otherwise (dev)       -> local filesystem manager (today's implicit default)

    dagster_aws is imported lazily so a checkout without it still loads.
    """
    mode = os.getenv("WPRDC_IO_MANAGER", "").strip().lower()
    if mode == "fs":
        return dg.FilesystemIOManager()
    if mode == "s3" or is_production():
        from dagster_aws.s3 import S3PickleIOManager, S3Resource

        endpoint = os.getenv("S3_ENDPOINT_URL")  # set -> LocalStack (dev/CI)
        s3_kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
        if endpoint:
            s3_kwargs.update(
                endpoint_url=endpoint,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            )
        # else prod: no endpoint, no keys -> boto3 default credential chain,
        # same model as LandingZoneResource._client().
        return S3PickleIOManager(
            s3_resource=S3Resource(**s3_kwargs),
            s3_bucket=(
                os.getenv("DAGSTER_RUNTIME_BUCKET")
                or os.getenv("LANDING_BUCKET", "wprdc-etl-landing")
            ),
            s3_prefix=os.getenv("DAGSTER_IO_PREFIX", "dagster/io"),
        )
    return dg.FilesystemIOManager()


def _alert_sensors() -> list[dg.SensorDefinition]:
    """Run-failure alerting — production only, and only when the Slack
    credentials are present (a misconfigured box still loads, just without
    alerts). dagster_slack is imported lazily."""
    if not is_production():
        return []
    token = os.getenv("DAGSTER_SLACK_BOT_TOKEN")
    channel = os.getenv("WPRDC_ALERT_SLACK_CHANNEL")
    if not (token and channel):
        logging.getLogger("dagster").warning(
            "ENVIRONMENT=production but DAGSTER_SLACK_BOT_TOKEN / "
            "WPRDC_ALERT_SLACK_CHANNEL are unset; run-failure alerting disabled"
        )
        return []
    from dagster_slack import make_slack_on_run_failure_sensor

    return [
        make_slack_on_run_failure_sensor(
            channel=channel,
            slack_token=token,
            default_status=dg.DefaultSensorStatus.RUNNING,  # live on deploy
            monitor_all_code_locations=True,
            webserver_base_url=os.getenv("DAGSTER_WEBSERVER_URL") or None,
        )
    ]


# Discover and load all component instances under defs/. load_defs takes the
# defs MODULE (not a path) — it reads defs.__file__ to find the tree, which is
# why wprdc_etl/defs/__init__.py must exist.
#
# NOTE: don't bind the load_defs() result to a module-level name — Dagster
# rejects more than one Definitions object at module scope, and it's a
# Definitions too. It's merged directly into `defs` below.

# Landing zone config. In production (on AWS) we pass no endpoint and no keys:
# boto3 resolves credentials from the instance/task/IRSA IAM role, and talks to
# real S3. For offline local dev, set S3_ENDPOINT_URL (LocalStack) and the flag
# below flips to path-style with static dev creds.
landing_kwargs: dict[str, Any] = {
    "bucket": os.getenv("LANDING_BUCKET", "wprdc-etl-landing"),
    "region": os.getenv("AWS_REGION", "us-east-1"),
}
if os.getenv("S3_ENDPOINT_URL"):  # local dev only
    landing_kwargs.update(
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        force_path_style=True,
    )

defs = dg.Definitions.merge(
    load_defs(wprdc_etl.defs),
    dg.Definitions(
        resources={
            "io_manager": _io_manager(),
            "landing": LandingZoneResource(**landing_kwargs),
            "sftp": SFTPResource(),
            "ckan": CkanResource(
                base_url=os.getenv("CKAN_URL", "https://data.wprdc.org"),
                api_key=dg.EnvVar("CKAN_API_TOKEN"),
            ),
            "geocoder": GeocoderResource(),
            # PostGIS admin-region store: written by region_layer pipelines,
            # read by the reverse_geocode transform op. Empty DSN is tolerated
            # at load time so a checkout with no database still loads; it fails
            # loud only when a dataset actually needs it.
            "spatial": SpatialResource(dsn=os.getenv("SPATIAL_DSN", "")),
        },
        sensors=_alert_sensors(),
    ),
)
