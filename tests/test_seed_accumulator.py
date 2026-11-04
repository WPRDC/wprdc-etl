"""bin/seed-accumulator: the seeded table must merge with what pipelines emit.

The point of seeding is that the first run's merge MATCHES the stored rows, so
the typing here is load-bearing: a timestamp or id typed differently from the
pipeline's frame would never dedupe (accumulate.py now refuses that outright).
"""

import pathlib

import pandas as pd

from wprdc_etl import seed_accumulator as seed
from wprdc_etl.strategies.accumulate import merge
from wprdc_etl.strategies.transform import coerce_text, to_datetime

DUMP = (
    "_id,datetime,site,zip,value,count,ok,day\n"
    "1,2020-12-03 12:00:00.000,North Braddock,01234,1.5,3,t,2020-12-03\n"
    "2,2020-12-03 13:00:00.000,North Braddock,,,,f,2020-12-03\n"
)
FIELDS = {
    "datetime": "timestamp",
    "site": "text",
    "zip": "text",
    "value": "numeric",
    "count": "int4",
    "ok": "bool",
    "day": "date",
}


def _typed(tmp_path: pathlib.Path) -> pd.DataFrame:
    p = tmp_path / "dump.csv"
    p.write_text(DUMP)
    return seed.typed_frame(str(p), FIELDS)


def test_dump_is_typed_by_datastore_type(tmp_path: pathlib.Path) -> None:
    df = _typed(tmp_path)
    assert "_id" not in df.columns
    assert df["datetime"].iloc[0] == pd.Timestamp("2020-12-03 12:00")
    assert df["zip"].iloc[0] == "01234"  # leading zero survives
    assert pd.isna(df["zip"].iloc[1])
    assert str(df["count"].dtype) == "Int64"  # a blank makes it nullable
    assert df["ok"].tolist() == [True, False]
    assert df["day"].iloc[0] == pd.Timestamp("2020-12-03")


def test_seeded_table_merges_with_a_pipeline_frame(tmp_path: pathlib.Path) -> None:
    prior = _typed(tmp_path)
    # What a pipeline produces for the same reading, re-reported with a new value.
    delta = pd.DataFrame(
        {"datetime": ["2020-12-03 12:00"], "site": ["North Braddock"], "value": [2.0]}
    )
    delta = coerce_text(to_datetime(delta, ["datetime"]), ["site"])
    out = merge(prior, delta, ["datetime", "site"])
    assert len(out) == 2  # matched, not appended
    hit = out["datetime"] == pd.Timestamp("2020-12-03 12:00")
    assert out.loc[hit, "value"].item() == 2.0


def test_discovers_only_accumulating_datasets() -> None:
    for t in seed.accumulating_targets():
        assert t.resource_id and t.primary_key, t.key
