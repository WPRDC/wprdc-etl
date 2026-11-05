"""Owner redaction for homestead parcels (condemned properties).

The rule is the legacy condemned job's: publish an owner only when the
parcel is in property assessments and is NOT homestead-exempt. Anything the
rule can't establish — no parcel id, a parcel the table doesn't have, a
lookup that fails — redacts or stops the run. Never publishes on doubt.
"""

from types import SimpleNamespace
from typing import Any

import dagster as dg
import pandas as pd
import pytest
import requests

from wprdc_etl.resources import CkanResource
from wprdc_etl.strategies.transform import (
    coerce_numeric,
    redact_homestead_owners,
    regex_replace,
    steps_need,
)


class FakeCkan:
    def __init__(self, flags: dict[str, str | None], fail: bool = False) -> None:
        self.flags, self.fail = flags, fail
        self.asked: list[str] = []

    def lookup(self, resource_id, key, values, fields):
        if self.fail:
            raise requests.ConnectionError("portal down")
        self.asked.extend(values)
        return {
            v: {key: v, "HOMESTEADFLAG": self.flags[v]}
            for v in values
            if v in self.flags
        }


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "parcel_id": ["P-HOM", "P-RENT", "P-NULLFLAG", "P-GONE", None, "P-RENT"],
            "owner": ["A HOMEOWNER", "B LANDLORD LLC", "C OWNER", "D", "E", ""],
        }
    )


def _run(df: pd.DataFrame, ckan: Any) -> pd.DataFrame:
    return redact_homestead_owners(
        df, owner="owner", parcel="parcel_id", resource_id="r", ckan=ckan
    )


def test_publishes_only_owners_of_known_non_homestead_parcels() -> None:
    ckan = FakeCkan({"P-HOM": "HOM", "P-RENT": None, "P-NULLFLAG": None})
    out = _run(_frame(), ckan)
    owners = [None if pd.isna(v) else v for v in out["owner"]]
    assert owners == [
        None,  # homestead-exempt: someone lives there
        "B LANDLORD LLC",
        "C OWNER",  # no flag at all is not an exemption
        None,  # not in assessments: can't check, so redact
        None,  # no parcel id: can't check, so redact
        None,  # was blank already
    ]
    # Only parcels with a named owner are looked up, once each.
    assert sorted(ckan.asked) == ["P-GONE", "P-HOM", "P-NULLFLAG", "P-RENT"]


def test_a_failed_lookup_publishes_nothing() -> None:
    with pytest.raises(dg.Failure, match="unredacted"):
        _run(_frame(), FakeCkan({}, fail=True))


def test_a_lookup_that_finds_almost_nothing_stops_the_run() -> None:
    """An empty or wrong table would blank every owner without complaint."""
    df = pd.DataFrame({"parcel_id": [f"P{i}" for i in range(60)], "owner": ["X"] * 60})
    with pytest.raises(dg.Failure, match="only 1 of 60") as e:
        _run(df, FakeCkan({"P0": None}))
    assert e.value.allow_retries is False


def test_it_asks_for_the_ckan_resource() -> None:
    assert steps_need([{"op": "redact_homestead_owners"}]) == {"ckan", "context"}


def test_missing_columns_fail() -> None:
    with pytest.raises(ValueError, match="needs columns"):
        _run(pd.DataFrame({"owner": ["x"]}), FakeCkan({}))


# --------------------------------------------------------------------------
# CkanResource.lookup
# --------------------------------------------------------------------------
def test_lookup_batches_and_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def action(self, name, json=None, **kw):
        calls.append(json)
        keys = json["filters"]["PARID"]
        rows = [{"PARID": k, "HOMESTEADFLAG": "HOM"} for k in keys]
        page = rows[json["offset"] : json["offset"] + 2]  # a 2-row page size
        return {"records": page, "total": len(rows)}

    monkeypatch.setattr(CkanResource, "_action", action)
    ckan = CkanResource(base_url="http://localhost:5000", api_key="")
    got = ckan.lookup(
        "r", "PARID", ["a", "b", "c", "a", "", None], ["HOMESTEADFLAG"], batch=2
    )
    assert sorted(got) == ["a", "b", "c"]
    assert [c["filters"]["PARID"] for c in calls] == [["a", "b"], ["c"]]
    assert calls[0]["fields"] == ["PARID", "HOMESTEADFLAG"]


def test_lookup_on_a_missing_table_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def action(self, name, json=None, **kw):
        err = requests.HTTPError("404")
        err.response = SimpleNamespace(status_code=404)
        raise err

    monkeypatch.setattr(CkanResource, "_action", action)
    ckan = CkanResource(base_url="http://localhost:5000", api_key="")
    with pytest.raises(RuntimeError, match="no DataStore table"):
        ckan.lookup("r", "PARID", ["a"], ["HOMESTEADFLAG"])


# --------------------------------------------------------------------------
# regex_replace, coerce_numeric(integer=True)
# --------------------------------------------------------------------------
def test_regex_replace_trims_a_trailing_dash() -> None:
    df = pd.DataFrame({"address": ["1 MAIN ST, 15210-", "2 ELM ST", None, 5]})
    out = regex_replace(df, columns=["address"], pattern="-$")
    assert out["address"].tolist()[:3] == ["1 MAIN ST, 15210", "2 ELM ST", None]
    assert out["address"].tolist()[3] == 5


def test_integer_numerics_publish_without_a_decimal() -> None:
    df = pd.DataFrame({"n": ["3", "", "0"], "k": ["a", "b", "c"]})
    df = coerce_numeric(df, ["n"], integer=True)
    assert str(df["n"].dtype) == "Int64"
    assert df.to_csv(index=False) == "n,k\n3,a\n,b\n0,c\n"


def test_integer_refuses_a_fraction() -> None:
    with pytest.raises(ValueError, match="non-whole"):
        coerce_numeric(pd.DataFrame({"n": ["2.5"]}), ["n"], integer=True)


# --------------------------------------------------------------------------
# PLI permits: owner names carried forward, and a tripwire if they return
# --------------------------------------------------------------------------
from wprdc_etl.components.models import CkanModel, SourceModel  # noqa: E402
from wprdc_etl.components.tabular_pipeline import TabularPipeline  # noqa: E402
from wprdc_etl.strategies.accumulate import merge  # noqa: E402
from wprdc_etl.strategies.transform import (  # noqa: E402
    add_column,
    forbid_columns,
    to_datetime,
)


def test_carry_forward_keeps_stored_values_the_source_no_longer_has() -> None:
    prior = pd.DataFrame(
        {
            "permit_id": ["A", "B", "C"],
            "owner_name": ["OLD A", pd.NA, "OLD C"],
            "status": ["Issued", "Issued", "Issued"],
        }
    ).astype("string")
    delta = add_column(
        pd.DataFrame({"permit_id": ["A", "B", "D"], "status": ["Done", "Done", "New"]}),
        "owner_name",
    ).astype("string")
    out = merge(prior, delta, ["permit_id"], carry_forward=["owner_name"])
    rows = {r.permit_id: (r.owner_name, r.status) for r in out.itertuples()}
    assert rows["A"] == ("OLD A", "Done")  # updated, owner kept
    assert pd.isna(rows["B"][0])  # nothing stored, nothing invented
    assert rows["C"] == ("OLD C", "Issued")  # dropped from the feed, kept
    assert pd.isna(rows["D"][0]) and rows["D"][1] == "New"  # new permit
    assert list(out.columns) == ["permit_id", "owner_name", "status"]


def test_carry_forward_needs_the_column_stored() -> None:
    prior = pd.DataFrame({"k": ["a"]}).astype("string")
    with pytest.raises(ValueError, match="not in the stored table"):
        merge(prior, prior.copy(), ["k"], carry_forward=["owner_name"])


def test_carry_forward_without_accumulate_fails_at_load() -> None:
    p = TabularPipeline(
        publisher="p",
        dataset="d",
        source=SourceModel(type="http", url="https://example.invalid/d.csv"),
        ckan=CkanModel(resource_id="r", carry_forward=["owner_name"]),
    )
    with pytest.raises(ValueError, match="needs accumulate"):
        p.build_defs(None)


def test_owner_names_in_the_feed_stop_the_run() -> None:
    df = pd.DataFrame({"ext_file_num": ["A"], "owner_name": ["SOMEONE"]})
    with pytest.raises(dg.Failure, match="owner_name") as e:
        forbid_columns(df, ["owner_name", "owner"], reason="needs redaction")
    assert e.value.allow_retries is False
    assert (
        forbid_columns(df.drop(columns="owner_name"), ["owner_name"], "x") is not None
    )


def test_geojson_utc_datetimes_become_eastern_dates() -> None:
    """The GeoJSON reader hands `2024-08-19T00:00:00-04:00` over as UTC."""
    df = pd.DataFrame(
        {"d": pd.to_datetime(["2024-08-19T04:00:00Z", "2024-11-07T05:00:00Z", None])}
    )
    out = to_datetime(df, ["d"], tz="America/New_York", date_only=True)
    assert out["d"].dt.strftime("%Y-%m-%d %H:%M").tolist()[:2] == [
        "2024-08-19 00:00",
        "2024-11-07 00:00",
    ]
    assert out["d"].dt.tz is None and pd.isna(out["d"].iloc[2])


def test_round_then_integer() -> None:
    df = coerce_numeric(
        pd.DataFrame({"v": [5124.5, 12154.2, 2.0]}), ["v"], round_to=0, integer=True
    )
    assert df["v"].tolist() == [5124, 12154, 2]  # half to even, like round()


@pytest.mark.parametrize(
    "step,match",
    [
        ({"pattern": None, "repl": "x"}, "pattern"),
        ({"pattern": "\\r\\n", "repl": None}, "repl"),
        ({"pattern": "("}, "missing"),
    ],
)
def test_a_mangled_regex_step_fails_at_load(step: dict, match: str) -> None:
    """dagster's templating trims whitespace-only strings to None."""
    from wprdc_etl.strategies.transform import validate_steps

    with pytest.raises(Exception, match=match):
        validate_steps([{"op": "regex_replace", "columns": ["a"], **step}])
