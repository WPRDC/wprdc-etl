"""Unit tests for the shared component helpers in components/_common.py."""

from types import SimpleNamespace
from typing import Any

import dagster as dg

from wprdc_etl.components._common import SCHEDULE_TZ, run_tags, schedule_or_sensor


def _tabular_cfg(**over: Any) -> SimpleNamespace:
    base = dict(
        publisher="allegheny_county",
        source=SimpleNamespace(type="sftp"),
        ingest="snapshot",
        heavy=False,
        schedule=None,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_run_tags_tabular() -> None:
    tags = run_tags(_tabular_cfg(), "allegheny_county__real_estate__assessments")
    assert tags == {
        "wprdc/publisher": "allegheny_county",
        "wprdc/dataset": "allegheny_county__real_estate__assessments",
        "wprdc/source": "sftp",
        "wprdc/ingest": "snapshot",
    }


def test_run_tags_heavy_flag() -> None:
    tags = run_tags(_tabular_cfg(heavy=True), "p__d")
    assert tags["wprdc/heavy"] == "true"


def test_run_tags_file_pipeline_has_no_ingest() -> None:
    """FilePipeline cfg has no `ingest` attribute at all."""
    cfg = SimpleNamespace(publisher="x", source=SimpleNamespace(type="http"))
    tags = run_tags(cfg, "x__blob")
    assert "wprdc/ingest" not in tags
    assert tags["wprdc/source"] == "http"


def test_schedule_uses_eastern_timezone() -> None:
    @dg.asset
    def _a() -> int:
        return 1

    job = dg.define_asset_job("j", selection=[_a])
    schedules, sensors = schedule_or_sensor(
        _tabular_cfg(schedule="0 6 1 * *"), "s", job
    )
    assert not sensors
    assert schedules[0].execution_timezone == SCHEDULE_TZ == "America/New_York"


def test_no_schedule_falls_back_to_sensor() -> None:
    @dg.asset
    def _a() -> int:
        return 1

    job = dg.define_asset_job("j", selection=[_a])
    schedules, sensors = schedule_or_sensor(_tabular_cfg(schedule=None), "s", job)
    assert not schedules
    assert len(sensors) == 1
