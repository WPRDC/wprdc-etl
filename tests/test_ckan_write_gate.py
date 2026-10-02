"""The local-CKAN opt-out: enable CKAN writes in dev without ENVIRONMENT=production.

The point of these tests is the blast radius. Turning on CKAN writes must unlock
CKAN and nothing else — the S3 landing guard, the spatial write guard and the
production-only wiring all still key off ENVIRONMENT.
"""

import pytest

from wprdc_etl import runtime as rt


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "ENVIRONMENT",
        "WPRDC_CKAN_WRITE",
        "WPRDC_ALLOW_REMOTE_CKAN",
        "WPRDC_ALLOW_REAL_S3",
        "WPRDC_ALLOW_REMOTE_SPATIAL",
        "WPRDC_ETL_SINK_DIR",
    ):
        monkeypatch.delenv(var, raising=False)


# -- the gate ---------------------------------------------------------------
def test_dry_run_is_still_the_default():
    assert rt.publishes_to_ckan() is False
    assert rt.sink_dir() == "_dryrun"


def test_opt_in_disables_the_dry_run_sink():
    """sink_dir() is what every publish step checks; None means 'really publish'."""
    import os

    os.environ["WPRDC_CKAN_WRITE"] = "1"
    assert rt.publishes_to_ckan() is True
    assert rt.sink_dir() is None


def test_production_still_publishes_without_the_opt_in():
    import os

    os.environ["ENVIRONMENT"] = "production"
    assert rt.publishes_to_ckan() is True
    assert rt.sink_dir() is None


# -- the host guard ---------------------------------------------------------
@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:5001",  # the dev CKAN stack
        "http://localhost:5000",  # the port is irrelevant — the host is checked
        "http://127.0.0.1:5001",
        "http://ckan:5000",  # compose network
    ],
)
def test_local_ckan_is_allowed(url):
    rt.guard_ckan_write(url)


def test_remote_ckan_is_refused_outside_production():
    """The whole point: WPRDC_CKAN_WRITE=1 alone can't reach the live portal."""
    with pytest.raises(RuntimeError, match="non-local CKAN"):
        rt.guard_ckan_write("https://data.wprdc.org")


def test_remote_ckan_allowed_with_explicit_opt_out():
    import os

    os.environ["WPRDC_ALLOW_REMOTE_CKAN"] = "1"
    rt.guard_ckan_write("https://data.wprdc.org")


def test_remote_ckan_allowed_in_production():
    import os

    os.environ["ENVIRONMENT"] = "production"
    rt.guard_ckan_write("https://data.wprdc.org")


# -- blast radius -----------------------------------------------------------
def test_ckan_opt_in_does_not_unlock_real_s3():
    import os

    os.environ["WPRDC_CKAN_WRITE"] = "1"
    with pytest.raises(RuntimeError, match="real AWS S3"):
        rt.guard_real_s3_write(None)


def test_ckan_opt_in_does_not_unlock_remote_spatial():
    import os

    os.environ["WPRDC_CKAN_WRITE"] = "1"
    with pytest.raises(RuntimeError, match="non-local PostGIS"):
        rt.guard_spatial_write("postgresql://u@prod-db.example.com/spatial")


def test_ckan_opt_in_does_not_make_it_production():
    """is_production() drives the IO manager and the alert sensor."""
    import os

    os.environ["WPRDC_CKAN_WRITE"] = "1"
    assert rt.is_production() is False
