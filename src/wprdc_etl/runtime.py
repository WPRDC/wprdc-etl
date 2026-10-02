"""Runtime environment guard.

Safety default: everything is a DRY RUN unless ENVIRONMENT is explicitly set to
'production'. In dry-run, publish steps write output to a local folder and never
call CKAN — so a forgotten or misconfigured environment can't write to a live
portal. Running dev settings in prod is recoverable; running prod writes from a
dev box is not, so we fail safe toward dry-run.

Lives at the top level so components and strategies can both import it without a
circular dependency.
"""

import os


def is_production() -> bool:
    return os.getenv("ENVIRONMENT", "").strip().lower() == "production"


def publishes_to_ckan() -> bool:
    """True when the publish steps should really call CKAN.

    Production always does. Outside production it is an explicit opt-in, for
    testing a job end-to-end against a LOCAL CKAN — the same shape as
    WPRDC_IO_MANAGER=s3, which exercises the production IO path in dev without
    touching ENVIRONMENT.

    This unlocks CKAN calls and NOTHING else: the S3 landing guard, the spatial
    write guard, the IO-manager choice and the alert sensor all still key off
    ENVIRONMENT. `guard_ckan_write` separately refuses a non-local portal, so
    this cannot be pointed at data.wprdc.org by setting one variable.
    """
    if is_production():
        return True
    return os.getenv("WPRDC_CKAN_WRITE", "").strip().lower() in ("1", "true", "yes")


def sink_dir() -> str | None:
    """Local output directory for a dry-run publish, or None when publishing for
    real. Dry-run is the default; real publishing needs ENVIRONMENT=production or
    the local-CKAN opt-out (see publishes_to_ckan). The dir is WPRDC_ETL_SINK_DIR
    if set, else _dryrun/."""
    if publishes_to_ckan():
        return None
    return os.getenv("WPRDC_ETL_SINK_DIR") or "_dryrun"


def guard_real_s3_write(endpoint_url: str | None) -> None:
    """Raise unless a write to the given S3 endpoint is permitted.

    Outside production, refuse writes to REAL AWS S3 (no endpoint_url -> default
    AWS) so a misconfigured dev run can't write to the production bucket. A
    local emulator (endpoint_url set, e.g. LocalStack) is always fine, and
    WPRDC_ALLOW_REAL_S3 is an explicit opt-out for using a real dev bucket."""
    if is_production() or endpoint_url:
        return
    if os.getenv("WPRDC_ALLOW_REAL_S3", "").strip().lower() in ("1", "true", "yes"):
        return
    raise RuntimeError(
        "refusing to write to real AWS S3 outside production: no S3_ENDPOINT_URL "
        "is set (so this targets real S3) and ENVIRONMENT != production. Point "
        "S3_ENDPOINT_URL at LocalStack for local dev, or set WPRDC_ALLOW_REAL_S3=1 "
        "to use a real dev bucket intentionally."
    )


# Hosts treated as a developer's own machine / the compose stack. Anything else
# is assumed to be a shared or production database.
_LOCAL_DB_HOSTS = frozenset(
    {"localhost", "127.0.0.1", "::1", "postgres", "postgis", "db", ""}
)

# Same idea for HTTP services: a CKAN on the dev box or in the compose network.
_LOCAL_HTTP_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "ckan", ""})


def guard_ckan_write(base_url: str) -> None:
    """Raise unless a WRITE to the given CKAN is permitted.

    Mirrors guard_spatial_write. Outside production, refuse to publish to a
    NON-local portal, so testing a job with WPRDC_CKAN_WRITE=1 can't upload to
    data.wprdc.org because CKAN_URL was left at its default. A local CKAN is
    always fine, and WPRDC_ALLOW_REMOTE_CKAN is the explicit opt-out for a real
    staging portal.

    READS (live_fields, the compatibility check) are not guarded — they're
    harmless and only run on the way to a write that is.
    """
    if is_production():
        return
    from urllib.parse import urlsplit

    host = (urlsplit(base_url).hostname or "").strip().lower()
    if host in _LOCAL_HTTP_HOSTS:
        return
    if os.getenv("WPRDC_ALLOW_REMOTE_CKAN", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return
    raise RuntimeError(
        f"refusing to publish to a non-local CKAN ({host!r}) outside production: "
        "ENVIRONMENT != production and CKAN_URL does not point at localhost or "
        "the compose stack. Point CKAN_URL at your dev CKAN, or set "
        "WPRDC_ALLOW_REMOTE_CKAN=1 to target a real staging portal intentionally."
    )


def guard_spatial_write(dsn: str) -> None:
    """Raise unless a WRITE to the given spatial-store DSN is permitted.

    Mirrors guard_real_s3_write: outside production, refuse to rewrite boundary
    layers in a NON-local PostGIS, so a dev run of a region_layer pipeline can't
    clobber the shared store. A local server (compose/localhost) is always fine,
    and WPRDC_ALLOW_REMOTE_SPATIAL is the explicit opt-out for a real dev DB.

    READS are deliberately not guarded — reverse_geocode has to work in dry-run,
    which is the default mode.
    """
    if is_production():
        return
    from urllib.parse import urlsplit

    host = (urlsplit(dsn).hostname or "").strip().lower()
    if host in _LOCAL_DB_HOSTS:
        return
    if os.getenv("WPRDC_ALLOW_REMOTE_SPATIAL", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return
    raise RuntimeError(
        f"refusing to rewrite boundary layers in a non-local PostGIS ({host!r}) "
        "outside production: ENVIRONMENT != production and SPATIAL_DSN does not "
        "point at localhost or the compose stack. Point SPATIAL_DSN at the local "
        "server for dev, or set WPRDC_ALLOW_REMOTE_SPATIAL=1 to target a real dev "
        "database intentionally."
    )
