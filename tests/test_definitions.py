"""Smoke + unit tests for the top-level Definitions wiring.

These don't touch S3, CKAN, or SFTP — they only check that the code location
builds and that the env-driven IO-manager / alert-sensor selection behaves.
"""

import dagster as dg
import pytest

from wprdc_etl import definitions as d


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Every test starts from a dev-like environment."""
    for var in (
        "ENVIRONMENT",
        "WPRDC_IO_MANAGER",
        "S3_ENDPOINT_URL",
        "DAGSTER_SLACK_BOT_TOKEN",
        "WPRDC_ALERT_SLACK_CHANNEL",
    ):
        monkeypatch.delenv(var, raising=False)


def test_code_location_builds():
    """`dg dev` / `dg check` load this module; make sure it resolves."""
    defs = d.defs
    assert isinstance(defs, dg.Definitions)
    # the single live dataset's assets are present
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "allegheny_county/real_estate/assessments/loaded" in keys


def test_io_manager_defaults_to_filesystem_in_dev():
    assert isinstance(d._io_manager(), dg.FilesystemIOManager)


def test_io_manager_fs_override(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("WPRDC_IO_MANAGER", "fs")
    assert isinstance(d._io_manager(), dg.FilesystemIOManager)


def test_io_manager_s3_mode_selects_s3_pickle(monkeypatch):
    monkeypatch.setenv("WPRDC_IO_MANAGER", "s3")
    from dagster_aws.s3 import S3PickleIOManager

    assert isinstance(d._io_manager(), S3PickleIOManager)


def test_io_manager_production_selects_s3_pickle(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    from dagster_aws.s3 import S3PickleIOManager

    assert isinstance(d._io_manager(), S3PickleIOManager)


def test_alert_sensors_empty_outside_production():
    assert d._alert_sensors() == []


def test_alert_sensors_empty_in_production_without_slack_creds(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert d._alert_sensors() == []


def test_alert_sensors_configured_in_production_with_slack_creds(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DAGSTER_SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("WPRDC_ALERT_SLACK_CHANNEL", "#wprdc-etl-alerts")
    sensors = d._alert_sensors()
    assert len(sensors) == 1
    assert isinstance(sensors[0], dg.SensorDefinition)
