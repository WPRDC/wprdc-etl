"""Resources that roll over each year, and tables split across years.

Three legacy packages carry a resource per year: dog licenses (the current
and last year are refreshed, from a file per year), property sales (every
year re-filtered by SALEDATE from one file) and delinquent taxes (a file per
tax year, plus a cumulative table of all of them). The pieces:

  * `{now-1y:%Y}` — a year offset in the date tokens
  * `ckan.resource_name` — a target found (or created) by name every run
  * `ckan.split_by_year` — one frame published to a resource per year
  * `source.combine` — land every file an sftp glob matches, as one CSV
  * `drop_duplicates` — what the legacy upsert did to repeated rows
"""

import datetime as dt
import io
from types import SimpleNamespace
from typing import Any

import dagster as dg
import pandas as pd
import pytest

from wprdc_etl.components.models import CkanModel, SourceModel, YearSplitModel
from wprdc_etl.components.tabular_pipeline import TabularPipeline
from wprdc_etl.strategies import load
from wprdc_etl.strategies.extract import (
    SftpFileExtractor,
    _append_csv,
    render_date_tokens,
)
from wprdc_etl.strategies.transform import drop_duplicates

NOW = dt.datetime(2027, 1, 1, 7, 9)


# --------------------------------------------------------------------------
# Date tokens
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value,expected",
    [
        ("DL_gvData_{now:%Y}.csv", "DL_gvData_2027.csv"),
        ("DL_gvData_{now-1y:%Y}.csv", "DL_gvData_2026.csv"),
        ("{now+2y:%Y}", "2029"),
        ("{now-1y:%Y} Dog Licenses", "2026 Dog Licenses"),
        ("no tokens {here}", "no tokens {here}"),
    ],
)
def test_year_offsets(value: str, expected: str) -> None:
    assert render_date_tokens(value, now=NOW) == expected


def test_a_leap_day_offset_lands_on_the_28th() -> None:
    leap = dt.datetime(2028, 2, 29)
    assert render_date_tokens("{now-1y:%Y-%m-%d}", now=leap) == "2027-02-28"


# --------------------------------------------------------------------------
# source.combine
# --------------------------------------------------------------------------
def _combine(tmp_path: Any, *files: bytes) -> bytes:
    out = io.BytesIO()
    header = None
    for i, body in enumerate(files):
        p = tmp_path / f"f{i}.csv"
        p.write_bytes(body)
        header = _append_csv(out, str(p), header, p.name)
    return out.getvalue()


def test_combine_keeps_one_header(tmp_path: Any) -> None:
    got = _combine(tmp_path, b"a,b\n1,2\n", b"a,b\n3,4\n")
    assert got == b"a,b\n1,2\n3,4\n"


def test_combine_tolerates_bom_case_and_missing_newline(tmp_path: Any) -> None:
    got = _combine(tmp_path, b"\xef\xbb\xbfA,B\r\n1,2", b"a,b\r\n3,4\r\n", b"")
    assert pd.read_csv(io.BytesIO(got), encoding="utf-8-sig").to_dict("list") == {
        "A": [1, 3],
        "B": [2, 4],
    }


def test_combine_refuses_mismatched_headers(tmp_path: Any) -> None:
    with pytest.raises(dg.Failure, match="different header"):
        _combine(tmp_path, b"a,b\n1,2\n", b"b,a\n3,4\n")


def test_the_extractor_lands_every_match_in_path_order(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SFTP_X", "u:p")
    files = {
        "/d/delinquent_2010.csv": b"ar_id,year_id\n2,2010\n",
        "/d/delinquent_2009.csv": b"ar_id,year_id\n1,2009\n",
    }
    sftp = SimpleNamespace(
        list_matching=lambda *a, **k: [
            {"path": p, "mtime": 0, "size": len(b)} for p, b in files.items()
        ],
        fetch_to_file=lambda h, u, pw, remote, local, port: open(local, "wb").write(
            files[remote]
        ),
    )
    landed: dict[str, Any] = {}

    def land_file(publisher, dataset, partition, path, **kw):
        landed["bytes"] = open(path, "rb").read()
        landed.update(kw)
        return {"sha256": "x", "size": 1, "skipped": False}

    cfg = SimpleNamespace(
        publisher="p",
        dataset="d",
        department=None,
        source=SourceModel(
            type="sftp",
            host="h",
            path="/d/delinquent_[0-9][0-9][0-9][0-9].csv",
            secret_ref="SFTP_X",
            combine=True,
        ),
    )
    context = SimpleNamespace(
        partition_key="2026-11-01",
        has_partition_key=True,
        add_output_metadata=lambda m: landed.setdefault("meta", m),
    )
    SftpFileExtractor().extract(
        context, cfg, landing=SimpleNamespace(land_file=land_file), sftp=sftp
    )
    assert landed["bytes"] == b"ar_id,year_id\n1,2009\n2,2010\n"
    assert landed["filename"] == "data.csv"
    assert landed["meta"]["files"] == 2


# --------------------------------------------------------------------------
# drop_duplicates
# --------------------------------------------------------------------------
def test_drop_duplicates_keeps_the_last_by_key() -> None:
    df = pd.DataFrame({"k": [1, 2, 1], "v": ["a", "b", "c"]})
    assert drop_duplicates(df, columns=["k"]).to_dict("list") == {
        "k": [2, 1],
        "v": ["b", "c"],
    }
    assert len(drop_duplicates(df)) == 3  # whole-row: none repeat


def test_drop_duplicates_rejects_a_missing_key() -> None:
    with pytest.raises(ValueError, match="no column"):
        drop_duplicates(pd.DataFrame({"k": [1]}), columns=["nope"])


# --------------------------------------------------------------------------
# Publishing by name and by year
# --------------------------------------------------------------------------
class FakeCkan:
    def __init__(self, resources: dict[str, str]) -> None:
        self.resources = dict(resources)  # name -> id
        self.created: list[str] = []
        self.replaced: dict[str, int] = {}
        self.package_reads = 0

    def package(self, package_id: str) -> dict[str, Any]:
        self.package_reads += 1
        return {"resources": [{"name": n, "id": i} for n, i in self.resources.items()]}

    def find_resource(self, package_id: str, name: str, fmt: str) -> str | None:
        wanted = name.strip().lower()  # as the real one compares
        return {n.lower(): i for n, i in self.resources.items()}.get(wanted)

    def add_resource(self, package_id: str, name: str, fmt: str) -> str:
        rid = f"new-{name}"
        self.resources[name] = rid
        self.created.append(name)
        return rid

    def live_fields(self, resource_id: str) -> dict[str, str]:
        return {}

    def _infer_fields(self, df: pd.DataFrame) -> list[dict[str, str]]:
        return [{"id": c, "type": "text"} for c in df.columns]

    def replace(self, resource_id: str, df: pd.DataFrame, **kw: Any) -> bool:
        self.replaced[resource_id] = len(df)
        return True


def _real_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the loader take the CKAN path (the fake is all it can reach)."""
    monkeypatch.setattr(load, "sink_dir", lambda: None)


def _cfg(**ckan: Any) -> Any:
    return SimpleNamespace(
        publisher="p",
        department=None,
        dataset="d",
        geometry=None,
        ckan=CkanModel(package_id="pkg", **ckan),
    )


def _log() -> Any:
    lines: list[str] = []
    return SimpleNamespace(
        run=SimpleNamespace(tags={}),
        log=SimpleNamespace(info=lines.append, warning=lines.append),
        lines=lines,
    )


def test_a_named_target_is_created_on_its_first_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _real_publish(monkeypatch)
    ckan = FakeCkan({"2026 Dog Licenses": "r2026"})
    df = pd.DataFrame({"DogName": ["Rex"]})
    rid = load.publish_named(_cfg(), df, "2027 Dog Licenses", ckan=ckan, context=None)
    assert ckan.created == ["2027 Dog Licenses"]
    assert ckan.replaced == {rid: 1}


def test_a_named_target_that_exists_is_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    _real_publish(monkeypatch)
    ckan = FakeCkan({"2026 Dog Licenses": "r2026"})
    df = pd.DataFrame({"DogName": ["Rex"]})
    load.publish_named(_cfg(), df, "2026 dog licenses ", ckan=ckan, context=None)
    assert ckan.created == [] and ckan.replaced == {"r2026": 1}


def test_no_rows_publishes_nothing_new_or_old(monkeypatch: pytest.MonkeyPatch) -> None:
    """1 January: the new year has no rows. Neither create an empty
    resource nor wipe an existing one."""
    _real_publish(monkeypatch)
    ckan = FakeCkan({"2026 Dog Licenses": "r2026"})
    empty = pd.DataFrame({"DogName": pd.Series([], dtype="string")})
    for name in ("2027 Dog Licenses", "2026 Dog Licenses"):
        assert load.publish_named(_cfg(), empty, name, ckan=ckan, context=None) is None
    assert ckan.created == [] and ckan.replaced == {}


def test_the_loader_renders_the_resource_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _real_publish(monkeypatch)
    monkeypatch.setattr(
        load, "render_date_tokens", lambda v: v.replace("{now:%Y}", "2027")
    )
    ckan = FakeCkan({})
    cfg = _cfg(resource_name="{now:%Y} Dog Licenses", bool_format="text")
    load.ReplaceLoader().load(cfg, pd.DataFrame({"a": [1]}), ckan=ckan, context=_log())
    assert ckan.created == ["2027 Dog Licenses"]


@pytest.mark.parametrize(
    "column",
    [
        pd.to_datetime(["2011-05-01", "2012-02-01", "2012-09-09", "2014-01-01", None]),
        pd.Series(["2011", "2012", "2012", "2014", None], dtype="string"),
    ],
    ids=["datetime", "text"],
)
def test_split_by_year(monkeypatch: pytest.MonkeyPatch, column: Any) -> None:
    _real_publish(monkeypatch)
    ckan = FakeCkan({"2012 Sales": "r2012"})
    cfg = _cfg(
        resource_id="all",
        split_by_year=YearSplitModel(
            column="when", since=2012, resource_name="{year} Sales"
        ),
    )
    df = pd.DataFrame({"when": column, "v": range(5)})
    context = _log()
    rows = load.publish_by_year(
        cfg, df, ckan=ckan, context=context, now=dt.datetime(2015, 1, 1)
    )
    assert rows == {2012: 2, 2013: 0, 2014: 1, 2015: 0}
    # 2012 reused, 2014 created; the empty years (2013, and 2015 on 1 Jan)
    # get no resource; 2011 and the null are before `since` and reported.
    assert ckan.created == ["2014 Sales"]
    assert ckan.replaced == {"r2012": 2, "new-2014 Sales": 1}
    assert ckan.package_reads == 1
    assert any("2 row(s)" in line for line in context.lines)


def test_split_dry_run_writes_a_file_per_year(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(load, "sink_dir", lambda: str(tmp_path))
    cfg = _cfg(
        resource_id="all",
        split_by_year=YearSplitModel(column="y", since=2020, resource_name="{year} X"),
    )
    df = pd.DataFrame({"y": ["2020", "2021", "2021"]})
    load.publish_by_year(cfg, df, ckan=None, context=None, now=dt.datetime(2021, 6, 1))
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "p__d__2020.csv",
        "p__d__2021.csv",
    ]


# --------------------------------------------------------------------------
# Load-time checks and graph shape
# --------------------------------------------------------------------------
def _pipeline(source: SourceModel | None = None, **kw: Any) -> TabularPipeline:
    return TabularPipeline(
        publisher="p",
        dataset="d",
        source=source or SourceModel(type="http", url="https://example.invalid/d.csv"),
        **kw,
    )


def _leaves(defs: dg.Definitions) -> set[str]:
    return {
        k.to_user_string().rsplit("/", 1)[-1]
        for a in defs.assets or []  # type: ignore[union-attr]
        for k in a.keys
    }


def test_split_adds_a_loaded_by_year_asset() -> None:
    defs = _pipeline(
        ckan=CkanModel(
            package_id="pkg",
            resource_id="all",
            split_by_year=YearSplitModel(
                column="y", since=2012, resource_name="{year} X"
            ),
        )
    ).build_defs(None)
    assert _leaves(defs) == {"landed", "validated", "loaded", "loaded_by_year"}


def test_a_named_target_builds_loaded() -> None:
    defs = _pipeline(
        ckan=CkanModel(package_id="pkg", resource_name="{now:%Y} X")
    ).build_defs(None)
    assert "loaded" in _leaves(defs)


@pytest.mark.parametrize(
    "kw,match",
    [
        (dict(ckan=CkanModel(resource_name="X")), "package_id"),
        (
            dict(ckan=CkanModel(package_id="pkg", resource_id="r", resource_name="X")),
            "not both",
        ),
        (
            dict(
                publish="upsert",
                ckan=CkanModel(package_id="pkg", resource_name="X", primary_key=["k"]),
            ),
            "replace",
        ),
        (
            dict(
                accumulate=True,
                ckan=CkanModel(package_id="pkg", resource_name="X", primary_key=["k"]),
            ),
            "roll",
        ),
        (
            dict(
                ckan=CkanModel(
                    package_id="pkg",
                    split_by_year=YearSplitModel(
                        column="y", since=1, resource_name="X"
                    ),
                )
            ),
            "{year}",
        ),
        (
            dict(
                ckan=CkanModel(
                    package_id="pkg",
                    split_by_year=YearSplitModel(
                        column="y", since=1, resource_name="{year} {other}"
                    ),
                )
            ),
            "placeholder other than",
        ),
    ],
)
def test_misconfigured_targets_fail_at_load(kw: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _pipeline(**kw).build_defs(None)


def test_combine_needs_an_sftp_source() -> None:
    src = SourceModel(type="http", url="https://example.invalid/d.csv", combine=True)
    with pytest.raises(ValueError, match="combine"):
        _pipeline(source=src, ckan=CkanModel(resource_id="r")).build_defs(None)
