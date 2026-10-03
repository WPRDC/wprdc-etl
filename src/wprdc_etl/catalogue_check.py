"""Weekly check: have the publishers' GIS catalogs added any new datasets

The pipelines address an ArcGIS layer by its catalogue title and a PASDA layer
by its dataset id, so nothing at run time notices a NEW layer — and a layer
that is renamed or withdrawn is only noticed when its run fails. This job looks
ahead, every Saturday (the day before the Sunday runs):

  new    a catalogue layer no pipeline uses and CATALOGUE_EXCLUSIONS doesn't
         account for — a candidate for `bin/arcgis --write`
  drift  a pipeline whose catalogue title is gone — its next run will fail
  pasda  a PASDA pipeline whose dataset id no longer resolves to a download

It REPORTS only: run metadata in the Dagster UI, plus a Slack message in
production when there is something to act on. Turning a new layer into a
pipeline stays a reviewed `bin/arcgis --write` and commit.

CATALOGUE_EXCLUSIONS lives here (not in scripts/sync_arcgis.py, which imports
it) because the production image ships the package but not scripts/.
"""

# No `from __future__ import annotations`: Dagster reads the asset function's
# annotations at definition time, and a stringized one is rejected.
import logging
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
import yaml

from wprdc_etl.components._common import (
    SCHEDULE_TZ,
    default_schedule_status,
    network_retry_policy,
)
from wprdc_etl.runtime import is_production

# Catalogue titles deliberately NOT wired from the Hub, per publisher, with the
# reason. `bin/arcgis` skips them and lists them as ELSEWHERE; this check
# doesn't report them as new. Add a title here rather than special-casing it.
CATALOGUE_EXCLUSIONS: dict[str, dict[str, str]] = {
    "allegheny_county": {
        "Allegheny County Parcel Boundaries": "pasda (dataset 1214)",
        "Allegheny County Addressing Address Points": "pasda (dataset 1219)",
        "Allegheny County Addressing Street Centerlines": "pasda (dataset 1224)",
        "Allegheny County Building Footprint Locations": "pasda (dataset 1195)",
        # On hold. A plain table with no WPRDC package of its own; its
        # FOLDER_ALIASES and PACKAGE_OVERRIDES entries are kept so removing
        # this line is all it takes to wire it as addressing_street_aliases.
        "Allegheny County Addressing Street Aliases": "nobody - on hold",
        "Allegheny County Addressing Data Model": "nobody - web page only, no file",
    },
    "city_of_pittsburgh": {
        "Pittsburgh schematic": "nobody - web page only, no file",
    },
}

SCHEDULE = "0 6 * * 6"  # Saturday 06:00 ET, the day before the Sunday runs


@dataclass
class CatalogueReport:
    """What changed under the pipelines, per publisher."""

    new: dict[str, list[str]] = field(default_factory=dict)
    drift: dict[str, list[tuple[str, str]]] = field(
        default_factory=dict
    )  # folder, title
    pasda: list[tuple[str, str]] = field(default_factory=list)  # folder, error

    def empty(self) -> bool:
        return not (any(self.new.values()) or any(self.drift.values()) or self.pasda)

    def message(self) -> str:
        """The Slack text — one line per finding, grouped by publisher."""
        lines = ["*Weekly catalogue check* — the pipelines need attention:"]
        for publisher in sorted(set(self.new) | set(self.drift)):
            for title in self.new.get(publisher, []):
                lines.append(f"• {publisher}: NEW layer {title!r} (bin/arcgis --write)")
            for folder, title in self.drift.get(publisher, []):
                lines.append(
                    f"• {publisher}: DRIFT {folder} — {title!r} is gone from the "
                    "catalogue; its next run will fail"
                )
        for folder, error in self.pasda:
            lines.append(f"• PASDA {folder}: {error}")
        return "\n".join(lines)


def _defs_root() -> pathlib.Path:
    import wprdc_etl.defs

    return pathlib.Path(wprdc_etl.defs.__file__).parent


def wired_sources(root: pathlib.Path | None = None) -> dict[str, Any]:
    """What the defs.yaml files point at.

    {"arcgis": {publisher: {"catalog": url, "titles": {title: folder}}},
     "pasda": [(folder, dataset_id, format)]}
    """
    arcgis: dict[str, dict[str, Any]] = {}
    pasda: list[tuple[str, str, str]] = []
    for path in (root or _defs_root()).rglob("defs.yaml"):
        attrs = (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}
        src = attrs.get("source") or {}
        folder = str(path.parent.relative_to(root or _defs_root()))
        if src.get("type") == "arcgis":
            pub = arcgis.setdefault(
                attrs["publisher"], {"catalog": src["catalog"], "titles": {}}
            )
            pub["titles"][src["title"].strip()] = folder
        elif src.get("type") == "pasda":
            pasda.append(
                (folder, str(src["dataset_id"]), src.get("format") or "shapefile")
            )
    return {"arcgis": arcgis, "pasda": pasda}


def check(root: pathlib.Path | None = None) -> CatalogueReport:
    """Compare every catalogue with the pipelines wired to it."""
    from wprdc_etl.strategies.extract import fetch_catalog, resolve_pasda_download

    wired = wired_sources(root)
    report = CatalogueReport()
    for publisher, pub in sorted(wired["arcgis"].items()):
        titles = {
            (d.get("title") or "").strip()
            for d in fetch_catalog(pub["catalog"], refresh=True)
            # The county catalogue carries unrendered template rows.
            if d.get("title") and "{{" not in d["title"]
        }
        excluded = {t.strip() for t in CATALOGUE_EXCLUSIONS.get(publisher, {})}
        report.new[publisher] = sorted(titles - set(pub["titles"]) - excluded)
        report.drift[publisher] = sorted(
            (folder, title)
            for title, folder in pub["titles"].items()
            if title not in titles
        )
    for folder, dataset_id, fmt in sorted(wired["pasda"]):
        try:
            resolve_pasda_download(dataset_id, fmt)
        except Exception as exc:  # noqa: BLE001 - every failure is a finding
            report.pasda.append((folder, str(exc).split("\n")[0][:200]))
    return report


def post_to_slack(text: str) -> bool:
    """Post `text` to the alert channel. Production only; False if not sent."""
    token = os.getenv("DAGSTER_SLACK_BOT_TOKEN")
    channel = os.getenv("WPRDC_ALERT_SLACK_CHANNEL")
    if not (is_production() and token and channel):
        return False
    from slack_sdk import WebClient

    WebClient(token=token).chat_postMessage(channel=channel, text=text)
    return True


@dg.asset(
    key=["maintenance", "catalogue_check"],
    group_name="maintenance",
    retry_policy=network_retry_policy(),
)
def catalogue_check(context: dg.AssetExecutionContext) -> None:
    """Report new, drifted and broken catalogue sources (see module docstring)."""
    report = check()
    context.add_output_metadata(
        {
            "new": sum(len(v) for v in report.new.values()),
            "drift": sum(len(v) for v in report.drift.values()),
            "pasda_broken": len(report.pasda),
            "report": dg.MetadataValue.md(
                "Nothing to act on." if report.empty() else report.message()
            ),
        }
    )
    if report.empty():
        context.log.info("catalogues match the pipelines; nothing to report")
        return
    context.log.warning(report.message())
    if not post_to_slack(report.message()):
        logging.getLogger("dagster").info(
            "catalogue check: not posted to Slack (dev, or no Slack credentials)"
        )


def catalogue_check_defs() -> dg.Definitions:
    """The asset, its job and its Saturday schedule."""
    job = dg.define_asset_job(
        "maintenance__catalogue_check__job",
        selection=dg.AssetSelection.assets(catalogue_check),
    )
    return dg.Definitions(
        assets=[catalogue_check],
        jobs=[job],
        schedules=[
            dg.ScheduleDefinition(
                name="maintenance__catalogue_check__schedule",
                job=job,
                cron_schedule=SCHEDULE,
                execution_timezone=SCHEDULE_TZ,
                default_status=default_schedule_status(),
            )
        ],
    )
