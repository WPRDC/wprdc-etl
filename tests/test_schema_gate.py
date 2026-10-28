"""schema_ok is a blocking check, and the mirrors wait on `validated` so it
gates them: a landed file missing a declared column is never copied to CKAN.

Runs the real basins job in process with every outside call faked."""

from typing import Any

import dagster as dg
import pandas as pd
import pytest

from wprdc_etl import definitions as d
from wprdc_etl.components import tabular_pipeline as tp
from wprdc_etl.strategies import extract

JOB = "allegheny_county__gis__basins__job"


class _Landing:
    def read_schema_state(self, *a: Any, **k: Any) -> None:
        return None

    def write_schema_state(self, *a: Any, **k: Any) -> None:
        pass


def _declared_columns() -> list[str]:
    from wprdc_etl.defs.allegheny_county.gis.basins.schema import SCHEMA

    return list(SCHEMA.columns)


@pytest.fixture
def mirrored(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    published: list[str] = []
    monkeypatch.setattr(extract.ArcGisExtractor, "extract", lambda *a, **k: {})
    monkeypatch.setattr(tp, "_catalog_entry", lambda cfg: {})
    monkeypatch.setattr(
        tp, "publish_mirror", lambda cfg, m, *a, **k: published.append(m.format)
    )
    monkeypatch.setattr(tp, "sync_package_metadata", lambda *a, **k: None)
    return published


def _run(monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame) -> Any:
    monkeypatch.setattr(tp, "read_landed", lambda landing, cfg, part: frame)
    job = d.defs.resolve_job_def(JOB)
    return job.execute_in_process(
        resources={
            "landing": _Landing(),
            "ckan": object(),
            "io_manager": dg.mem_io_manager,
        },
        partition_key=job.partitions_def.get_last_partition_key(),
        raise_on_error=False,
    )


def test_a_missing_declared_column_holds_back_the_mirrors(
    monkeypatch: pytest.MonkeyPatch, mirrored: list[str]
) -> None:
    columns = _declared_columns()
    assert columns, "basins should declare columns in schema.py"
    _run(monkeypatch, pd.DataFrame({c: [] for c in columns[1:]}))
    assert mirrored == []


def test_a_complete_file_is_mirrored(
    monkeypatch: pytest.MonkeyPatch, mirrored: list[str]
) -> None:
    _run(monkeypatch, pd.DataFrame({c: [] for c in _declared_columns()}))
    assert "geojson" in mirrored and "csv" in mirrored
