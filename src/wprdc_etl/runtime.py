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


def sink_dir():
    """Local output directory when in dry-run, or None in production (real
    writes). Dry-run is the default; production must be opted into via
    ENVIRONMENT=production. The dir is WPRDC_ETL_SINK_DIR if set, else _dryrun/."""
    if is_production():
        return None
    return os.getenv("WPRDC_ETL_SINK_DIR") or "_dryrun"


def guard_real_s3_write(endpoint_url):
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
