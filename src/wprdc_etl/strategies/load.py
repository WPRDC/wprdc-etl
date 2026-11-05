"""Load strategies: how transformed data reaches CKAN.

Selected by the component's `publish` mode, which is independent of `ingest`
(how the data arrived) — a watermark delta can be accumulated and published as a
whole file, which is the point of the split:

    replace -> ReplaceLoader   (hand DataPusher+ the whole file; full reload)
    upsert  -> UpsertLoader    (DataStore upsert by primary key; deltas only)

Both run a COMPATIBILITY CHECK against the live CKAN schema before writing,
comparing the output's columns/types to what's currently published. The policy
differs by mode:
  * replace  -> BLOCK on any structural change. The reload TRUNCATES the table
                rather than dropping it (so ckanext-spatialdata's columns and
                indexes survive), which means the table keeps its existing
                types and can't absorb a new or retyped column. `ckan.rebuild`
                opts into the drop-and-recreate, and downgrades this to a WARN.
  * upsert   -> BLOCK on type changes to shared columns (upserting into an
                existing typed table can fail or corrupt), WARN on columns the
                table has that the output drops.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import dagster as dg

from wprdc_etl.runtime import sink_dir
from wprdc_etl.strategies.extract import render_date_tokens, run_now

if TYPE_CHECKING:
    import pandas as pd

    from wprdc_etl.components.models import PipelineConfig
    from wprdc_etl.components.tabular_pipeline import TabularPipeline
    from wprdc_etl.resources import CkanResource


BOOL_FORMATS = ("text", "int", "bool")


def publishable_booleans(dataframe: pd.DataFrame, bool_format: str) -> pd.DataFrame:
    """Convert boolean columns to what a replace can publish and keep.

    Production's DataPusher+ re-creates the table on every load and has no
    bool type_override, so a bool column always comes back as text. Converting
    first makes the frame say what the table will hold — so the column-change
    guard compares like with like — and lets a dataset choose the form:
    "text" -> "True"/"False", "int" -> 1/0. Missing values stay missing.

    "bool" keeps them boolean: the CSV still says True/False (PostgreSQL
    reads both into a boolean column), and `replace` pins the column with
    `type_override: bool`. DataPusher+ honours that only when its
    `ckanext.datapusher_plus.type_mapping` lists `bool` among its values —
    a CKAN server setting. Without it the column comes back text, and the
    next run's column guard reports `text -> bool`.
    Title case on purpose: it is what most of WPRDC's text booleans already
    use (Python's spelling), and what DataPusher+ was seen storing even when
    sent "false"/"true" — so the file and the DataStore agree.
    """
    import pandas as pd

    if bool_format not in BOOL_FORMATS:
        raise dg.Failure(
            f"ckan.bool_format must be one of {BOOL_FORMATS}, got {bool_format!r}",
            allow_retries=False,
        )
    out = dataframe
    for col in dataframe.columns:
        if not pd.api.types.is_bool_dtype(dataframe[col].dtype):
            continue
        if out is dataframe:
            out = dataframe.copy()
        values = dataframe[col].astype("boolean")
        if bool_format == "bool":
            out[col] = values
        elif bool_format == "int":
            out[col] = values.astype("Int64")
        else:
            out[col] = values.map({True: "True", False: "False"}).astype("string")
    return out


def _stem(cfg: PipelineConfig) -> str:
    return "__".join(x for x in [cfg.publisher, cfg.department, cfg.dataset] if x)


def dump_local(
    cfg: PipelineConfig,
    dataframe: pd.DataFrame,
    context: dg.AssetExecutionContext | None,
    suffix: str = "csv",
    part: str | None = None,
) -> bool:
    """Write the frame to the dry-run sink and return True, or return False in
    production (real writes). Dry-run is the default (see wprdc_etl.runtime).
    `part` names one of several outputs (a year of a split)."""
    d = sink_dir()
    if not d:
        return False
    os.makedirs(d, exist_ok=True)
    stem = _stem(cfg) + (f"__{part}" if part else "")
    path = os.path.join(d, f"{stem}.{suffix}")
    dataframe.to_csv(path, index=False)
    if context is not None:
        context.log.info(
            f"[dry-run] wrote {len(dataframe)} rows to {path} (skipped CKAN)"
        )
    return True


# Coarse type buckets so CKAN's postgres-ish types and our inferred types
# compare on like terms (int4/float8/numeric all "number", etc.).
_COARSE = {
    "text": "text",
    "varchar": "text",
    "char": "text",
    "json": "text",
    "jsonb": "text",
    "int": "number",
    "int2": "number",
    "int4": "number",
    "int8": "number",
    "integer": "number",
    "bigint": "number",
    "smallint": "number",
    "numeric": "number",
    "float": "number",
    "float4": "number",
    "float8": "number",
    "double precision": "number",
    "real": "number",
    "timestamp": "time",
    "timestamptz": "time",
    "timestamp without time zone": "time",
    "date": "time",
    "bool": "bool",
    "boolean": "bool",
}


def _coarse(t: object) -> str:
    return _COARSE.get(str(t).lower(), "text")


def compat_report(
    cfg: TabularPipeline,
    dataframe: pd.DataFrame,
    ckan: CkanResource,
    resource_id: str | None = None,
) -> dict[str, Any]:
    """Compare the output schema to CKAN's live schema.

    Returns {live_exists, removed, added, type_changes}. `removed` are columns
    the live table has that the output no longer produces; `added` are new;
    `type_changes` maps shared columns whose coarse type differs to (live, out).
    """
    live = ckan.live_fields(resource_id or cfg.ckan.resource_id)  # {} if no table
    intended = {f["id"]: f["type"] for f in ckan._infer_fields(dataframe)}
    if not live:
        return {
            "live_exists": False,
            "removed": [],
            "added": sorted(intended),
            "type_changes": {},
        }

    live_ids, out_ids = set(live), set(intended)
    type_changes = {
        c: (live[c], intended[c])
        for c in sorted(live_ids & out_ids)
        if _coarse(live[c]) != _coarse(intended[c])
    }
    return {
        "live_exists": True,
        "removed": sorted(live_ids - out_ids),
        "added": sorted(out_ids - live_ids),
        "type_changes": type_changes,
    }


def _fmt_changes(type_changes: dict[str, tuple[str, str]]) -> str:
    return ", ".join(f"{c}: {a}->{b}" for c, (a, b) in type_changes.items())


# A run launched with this tag rebuilds as if `ckan.rebuild: true` were set,
# for that run only: Launchpad -> Edit tags. Schedules never carry it, so
# there is no flag to revert afterwards (and no image to build twice).
REBUILD_TAG = "wprdc/rebuild"


def wants_rebuild(
    cfg: TabularPipeline, context: dg.AssetExecutionContext | None
) -> bool:
    """`ckan.rebuild` in the YAML, or the run's `wprdc/rebuild=true` tag."""
    if cfg.ckan.rebuild:
        return True
    if context is None:
        return False
    tag = context.run.tags.get(REBUILD_TAG, "")
    if tag.strip().lower() in ("1", "true", "yes"):
        context.log.warning(
            f"{REBUILD_TAG}={tag} on this run: dropping and re-creating the "
            "DataStore table"
        )
        return True
    return False


def _guard_replace(
    report: dict[str, Any],
    rebuild: bool,
    context: dg.AssetExecutionContext | None,
) -> None:
    """Structural drift policy for a replace publish.

    A truncating reload keeps the table's existing columns and types, so a new,
    dropped, or retyped column has nowhere to land — DataPusher+ either fails or
    silently coerces. That's a hard stop unless `ckan.rebuild` (or the run's
    `wprdc/rebuild` tag) says to drop and recreate, in which case it's back to
    being informational.
    """
    if not report["live_exists"]:
        return  # first load — replace() creates the table from the frame

    if rebuild:
        if context is None:
            return
        if report["removed"]:
            context.log.warning(
                "replace (rebuild): columns dropped vs live CKAN "
                f"(consumers may break): {report['removed']}"
            )
        if report["type_changes"]:
            context.log.warning(
                "replace (rebuild): column type changes vs live CKAN: "
                f"{_fmt_changes(report['type_changes'])}"
            )
        return

    problems = []
    if report["added"]:
        problems.append(f"new columns {report['added']}")
    if report["removed"]:
        problems.append(f"columns no longer produced {report['removed']}")
    if report["type_changes"]:
        problems.append(f"type changes ({_fmt_changes(report['type_changes'])})")
    if problems:
        # allow_retries=False: the column change is still there a minute later.
        raise dg.Failure(
            allow_retries=False,
            description="replace aborted: the output's columns differ from the live CKAN "
            f"table — {'; '.join(problems)}. Consumers of the table would see "
            "the change, so it isn't made silently. If it is intended, "
            f"re-launch this job with the run tag `{REBUILD_TAG}=true` "
            "(Launchpad -> Edit tags): that one run drops the table and "
            "re-creates it with the new columns and types (and their data "
            "dictionary overrides). That also discards anything else added to "
            "the table, such as ckanext-spatialdata's geometry column.",
        )


def _guard_upsert(
    report: dict[str, Any], context: dg.AssetExecutionContext | None
) -> None:
    if not report["live_exists"]:
        return
    if report["type_changes"]:
        raise dg.Failure(
            allow_retries=False,
            description="upsert aborted: column type(s) differ from the live CKAN table "
            f"({_fmt_changes(report['type_changes'])}). If this change is "
            "intended, reset the DataStore table (datastore_delete) so it can be "
            "recreated with the new types, then re-run.",
        )
    if report["removed"] and context is not None:
        context.log.warning(
            f"upsert: columns in CKAN not produced by output (won't be updated): "
            f"{report['removed']}"
        )


class Loader:
    def load(
        self,
        cfg: TabularPipeline,
        dataframe: pd.DataFrame,
        *,
        ckan: CkanResource,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        raise NotImplementedError


class ReplaceLoader(Loader):
    """Full-refresh load via DataPusher+."""

    def load(
        self,
        cfg: TabularPipeline,
        dataframe: pd.DataFrame,
        *,
        ckan: CkanResource,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        if cfg.ckan.resource_name:
            publish_named(
                cfg,
                dataframe,
                render_date_tokens(cfg.ckan.resource_name),
                ckan=ckan,
                context=context,
            )
            return
        replace_one(cfg, dataframe, cfg.ckan.resource_id, ckan=ckan, context=context)


def replace_one(
    cfg: TabularPipeline,
    dataframe: pd.DataFrame,
    resource_id: str,
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None,
    part: str | None = None,
) -> None:
    """One full-refresh load of `dataframe` into `resource_id`, guarded."""
    dataframe = publishable_booleans(dataframe, cfg.ckan.bool_format)
    if dump_local(cfg, dataframe, context, part=part):
        return
    rebuild = wants_rebuild(cfg, context)
    if not dataframe.empty:
        _guard_replace(
            compat_report(cfg, dataframe, ckan, resource_id), rebuild, context
        )
    changed = ckan.replace(
        resource_id,
        dataframe,
        rebuild=rebuild,
        # Only a spatial table needs regenerating; geometry lives on the
        # dataset, so `spatial` is just the switch.
        spatial=cfg.geometry if cfg.ckan.spatial else None,
    )
    if context is not None:
        context.log.info(
            f"replaced {len(dataframe)} rows in {resource_id}"
            if changed
            else f"unchanged: CKAN already holds these {len(dataframe)} rows in "
            f"{resource_id} — skipped the upload and the DataPusher+ reload"
        )


def publish_named(
    cfg: TabularPipeline,
    dataframe: pd.DataFrame,
    name: str,
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None,
    part: str | None = None,
    resource_ids: dict[str, str] | None = None,
) -> str | None:
    """Replace the resource called `name` in ckan.package_id, creating it
    first if the package has none — but only for a frame with rows.

    A rolling target ("2027 Dog Licenses") doesn't exist until its year has
    data, and on 1 January it has none: an empty frame is never published,
    neither into a new resource nor over an existing one (which would wipe a
    table to nothing because a file wasn't posted yet). Returns the resource
    id published to, or None when skipped.

    `resource_ids` is a name -> id map from one package_show, so a split over
    15 years doesn't fetch the package 15 times; names compare as
    `find_resource` does (trimmed, case-insensitive).
    """
    if dataframe.empty:
        if context is not None:
            context.log.info(f"{name!r}: no rows this run — published nothing")
        return None
    if sink_dir():
        replace_one(cfg, dataframe, "", ckan=ckan, context=context, part=part)
        return None
    package_id = cfg.ckan.package_id
    if resource_ids is not None:
        resource_id = resource_ids.get(name.strip().lower())
    else:
        resource_id = ckan.find_resource(package_id, name, "CSV")
    if resource_id is None:
        resource_id = ckan.add_resource(package_id, name, "CSV")
        if context is not None:
            context.log.info(f"created resource {name!r} ({resource_id})")
    replace_one(cfg, dataframe, resource_id, ckan=ckan, context=context, part=part)
    return resource_id


def year_values(series: pd.Series) -> pd.Series:
    """The year of each value: a datetime's year, or the value read as one."""
    import pandas as pd

    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return series.dt.year.astype("Int64")
    years = pd.to_numeric(series, errors="coerce")
    whole = years.notna() & (years % 1 == 0)
    return years.where(whole).astype("Int64")


def publish_by_year(
    cfg: TabularPipeline,
    dataframe: pd.DataFrame,
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None = None,
    now: Any = None,
) -> dict[int, int]:
    """`ckan.split_by_year`: publish each year's rows to its own resource.

    Every year from `since` to the current one is published on every run,
    because the county corrects back years (sales dates change with
    corrective deeds; old tax years are re-posted weekly). Unchanged years
    cost a fingerprint check, not a reload. Returns {year: rows}.
    """
    split = cfg.ckan.split_by_year
    if split.column not in dataframe.columns:
        raise dg.Failure(
            f"ckan.split_by_year.column {split.column!r} is not in the frame "
            f"(columns: {list(dataframe.columns)})",
            allow_retries=False,
        )
    current = (now or run_now()).year
    years = year_values(dataframe[split.column])
    outside = int((~years.between(split.since, current)).fillna(True).sum())
    if outside and context is not None:
        context.log.warning(
            f"{outside} row(s) have a {split.column} year outside "
            f"{split.since}-{current} (or none) and are in no per-year resource"
        )
    resource_ids = None
    if not sink_dir():
        resource_ids = {
            (r.get("name") or "").strip().lower(): r["id"]
            for r in ckan.package(cfg.ckan.package_id).get("resources", [])
        }
    published: dict[int, int] = {}
    for year in range(split.since, current + 1):
        rows = dataframe[(years == year).fillna(False).to_numpy()]
        published[year] = len(rows)
        publish_named(
            cfg,
            rows.reset_index(drop=True),
            split.resource_name.format(year=year),
            ckan=ckan,
            context=context,
            part=str(year),
            resource_ids=resource_ids,
        )
    return published


class UpsertLoader(Loader):
    """Delta load: upsert changed rows by primary key via the DataStore API."""

    def load(
        self,
        cfg: TabularPipeline,
        dataframe: pd.DataFrame,
        *,
        ckan: CkanResource,
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        if dump_local(cfg, dataframe, context):
            return
        if dataframe.empty:
            return  # empty delta -> no-op
        _guard_upsert(compat_report(cfg, dataframe, ckan), context)
        ckan.upsert(cfg.ckan.resource_id, dataframe, primary_key=cfg.ckan.primary_key)


LOADERS: dict[str, Loader] = {
    "replace": ReplaceLoader(),
    "upsert": UpsertLoader(),
}


def get_loader(publish: str) -> Loader:
    try:
        return LOADERS[publish]
    except KeyError:
        raise ValueError(
            f"unknown publish mode {publish!r} (expected 'replace' or 'upsert')"
        )
