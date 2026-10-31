"""schema_ok shows both sides of its comparison: the landed file's columns
(source_schema) and the contract's (destination_schema), plus what differs."""

from typing import Any

import dagster as dg
import pandas as pd
import pandera.pandas as pa
import pytest

from wprdc_etl import definitions as d
from wprdc_etl.components import tabular_pipeline as tp
from wprdc_etl.strategies import extract


class _Landing:
    def read_schema_state(self, *a: Any, **k: Any) -> None:
        return None

    def write_schema_state(self, *a: Any, **k: Any) -> None:
        pass


def _check_metadata(monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame) -> dict:
    monkeypatch.setattr(extract.ArcGisExtractor, "extract", lambda *a, **k: {})
    monkeypatch.setattr(tp, "read_landed", lambda landing, cfg, part: frame)
    monkeypatch.setattr(tp, "_catalog_entry", lambda cfg: {})
    monkeypatch.setattr(tp, "publish_mirror", lambda *a, **k: None)
    monkeypatch.setattr(tp, "sync_package_metadata", lambda *a, **k: None)
    job = d.defs.resolve_job_def("allegheny_county__gis__basins__job")
    result = job.execute_in_process(
        resources={
            "landing": _Landing(),
            "ckan": object(),
            "io_manager": dg.mem_io_manager,
        },
        partition_key=job.partitions_def.get_last_partition_key(),
        raise_on_error=False,
    )
    (evaluation,) = result.get_asset_check_evaluations()
    return evaluation.metadata


def _declared() -> list[str]:
    from wprdc_etl.defs.allegheny_county.gis.basins.schema import SCHEMA

    return list(SCHEMA.columns)


def _names(meta: dict, key: str) -> list[str]:
    return [c.name for c in meta[key].schema.columns]


def test_both_schemas_and_the_differences(monkeypatch) -> None:
    declared = _declared()
    frame = pd.DataFrame({c: [] for c in declared[1:]} | {"NEW_COL": []})
    meta = _check_metadata(monkeypatch, frame)
    assert _names(meta, "source_schema") == [*declared[1:], "NEW_COL"]
    assert _names(meta, "destination_schema") == declared
    assert meta["missing_columns"].value == [declared[0]]
    assert meta["undeclared_columns"].value == ["NEW_COL"]


def test_the_destination_schema_carries_types() -> None:
    schema = pa.DataFrameSchema({"n": pa.Column(int, nullable=True)})
    (col,) = tp._contract_schema(schema, None).schema.columns
    assert col.type == "int64" and col.constraints.nullable is True
    (req,) = tp._contract_schema(None, ["x"]).schema.columns
    assert (req.name, req.type) == ("x", "any")


def test_value_failures_are_summarised_per_column_and_check() -> None:
    schema = pa.DataFrameSchema({"lat": pa.Column(float, pa.Check.in_range(40, 41))})
    with pytest.raises(pa.errors.SchemaErrors) as e:
        schema.validate(pd.DataFrame({"lat": [40.5, 99.0, -1.0, 99.0]}), lazy=True)
    table = tp._value_failures(e.value.failure_cases)["value_failures"].value
    assert "| lat |" in table and "| 3 |" in table and "99.0" in table
