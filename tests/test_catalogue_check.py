"""The weekly catalogue check (wprdc_etl.catalogue_check).

It reports layers a publisher added (no pipeline yet), pipelines whose title
left the catalogue (their next run fails), and PASDA ids that stopped
resolving — and says nothing when there is nothing to act on, which is what
keeps a weekly Slack message worth reading.
"""

import pathlib

import dagster as dg
import pytest

import wprdc_etl.catalogue_check as cc

CATALOG = "https://example.org/data.json"


def _defs(tmp_path: pathlib.Path) -> pathlib.Path:
    """A miniature defs tree: two arcgis layers and one PASDA layer."""
    files = {
        "pub/gis/roads/defs.yaml": f"""
type: x
attributes:
  publisher: pub
  dataset: roads
  source: {{type: arcgis, catalog: "{CATALOG}", title: "Roads"}}
""",
        "pub/gis/parks/defs.yaml": f"""
type: x
attributes:
  publisher: pub
  dataset: parks
  source: {{type: arcgis, catalog: "{CATALOG}", title: "Parks "}}
""",
        "pub/gis/parcels/defs.yaml": """
type: x
attributes:
  publisher: pub
  dataset: parcels
  source: {type: pasda, dataset_id: 1214, format: shapefile}
""",
    }
    for rel, body in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return tmp_path


@pytest.fixture
def catalogue(monkeypatch: pytest.MonkeyPatch):
    titles: list[str] = []
    monkeypatch.setattr(
        "wprdc_etl.strategies.extract.fetch_catalog",
        lambda url, refresh=False: [{"title": t} for t in titles],
    )
    monkeypatch.setattr(
        "wprdc_etl.strategies.extract.resolve_pasda_download",
        lambda dataset_id, fmt: ("https://pasda/x.zip", {}),
    )
    monkeypatch.setitem(
        cc.CATALOGUE_EXCLUSIONS, "pub", {"Withdrawn Thing": "elsewhere"}
    )
    return titles


def test_nothing_to_report_when_the_catalogue_matches(tmp_path, catalogue) -> None:
    catalogue += ["Roads", "Parks", "Withdrawn Thing", "{{template row}}"]
    report = cc.check(_defs(tmp_path))
    assert report.empty()


def test_a_new_layer_and_a_vanished_title_are_reported(tmp_path, catalogue) -> None:
    catalogue += ["Roads", "Bridges"]  # Parks is gone; Bridges is new
    report = cc.check(_defs(tmp_path))
    assert report.new == {"pub": ["Bridges"]}
    assert report.drift == {"pub": [("pub/gis/parks", "Parks")]}
    text = report.message()
    assert "NEW layer 'Bridges'" in text and "DRIFT pub/gis/parks" in text


def test_a_pasda_id_that_stopped_resolving_is_reported(
    tmp_path, catalogue, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalogue += ["Roads", "Parks"]

    def gone(dataset_id, fmt):
        raise dg.Failure(f"PASDA dataset {dataset_id} offers no {fmt} download")

    monkeypatch.setattr("wprdc_etl.strategies.extract.resolve_pasda_download", gone)
    report = cc.check(_defs(tmp_path))
    assert report.pasda == [
        ("pub/gis/parcels", "PASDA dataset 1214 offers no shapefile download")
    ]
    assert not report.empty()


def test_slack_is_not_posted_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wprdc_etl.catalogue_check.is_production", lambda: False)
    monkeypatch.setenv("DAGSTER_SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("WPRDC_ALERT_SLACK_CHANNEL", "#test")
    assert cc.post_to_slack("hello") is False


def test_it_is_scheduled_saturday_morning_eastern() -> None:
    import wprdc_etl.definitions as d

    sched = d.defs.get_schedule_def("maintenance__catalogue_check__schedule")
    assert sched.cron_schedule == "0 6 * * 6"
    assert sched.execution_timezone == "America/New_York"
