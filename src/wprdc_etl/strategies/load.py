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

if TYPE_CHECKING:
    import pandas as pd

    from wprdc_etl.components.models import PipelineConfig
    from wprdc_etl.components.tabular_pipeline import TabularPipeline
    from wprdc_etl.resources import CkanResource


BOOL_FORMATS = ("text", "int")


def publishable_booleans(dataframe: pd.DataFrame, bool_format: str) -> pd.DataFrame:
    """Convert boolean columns to what a replace can publish and keep.

    Production's DataPusher+ re-creates the table on every load and has no
    bool type_override, so a bool column always comes back as text. Converting
    first makes the frame say what the table will hold — so the column-change
    guard compares like with like — and lets a dataset choose the form:
    "text" -> "True"/"False", "int" -> 1/0. Missing values stay missing.
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
        if bool_format == "int":
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
) -> bool:
    """Write the frame to the dry-run sink and return True, or return False in
    production (real writes). Dry-run is the default (see wprdc_etl.runtime)."""
    d = sink_dir()
    if not d:
        return False
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{_stem(cfg)}.{suffix}")
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
    cfg: TabularPipeline, dataframe: pd.DataFrame, ckan: CkanResource
) -> dict[str, Any]:
    """Compare the output schema to CKAN's live schema.

    Returns {live_exists, removed, added, type_changes}. `removed` are columns
    the live table has that the output no longer produces; `added` are new;
    `type_changes` maps shared columns whose coarse type differs to (live, out).
    """
    live = ckan.live_fields(cfg.ckan.resource_id)  # {} if no table yet
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


def _guard_replace(
    report: dict[str, Any],
    rebuild: bool,
    context: dg.AssetExecutionContext | None,
) -> None:
    """Structural drift policy for a replace publish.

    A truncating reload keeps the table's existing columns and types, so a new,
    dropped, or retyped column has nowhere to land — DataPusher+ either fails or
    silently coerces. That's a hard stop unless `ckan.rebuild` says to drop and
    recreate, in which case it's back to being informational.
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
            "the change, so it isn't made silently. If it is intended, set "
            "`ckan.rebuild: true` for one run: the table is dropped and "
            "re-created with the new columns and types (and their data "
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
        dataframe = publishable_booleans(dataframe, cfg.ckan.bool_format)
        if dump_local(cfg, dataframe, context):
            return
        rebuild = bool(cfg.ckan.rebuild)
        if not dataframe.empty:
            _guard_replace(compat_report(cfg, dataframe, ckan), rebuild, context)
        changed = ckan.replace(
            cfg.ckan.resource_id,
            dataframe,
            rebuild=rebuild,
            # Only a spatial table needs regenerating; geometry lives on the
            # dataset, so `spatial` is just the switch.
            spatial=cfg.geometry if cfg.ckan.spatial else None,
        )
        if context is not None:
            context.log.info(
                f"replaced {len(dataframe)} rows in {cfg.ckan.resource_id}"
                if changed
                else f"unchanged: CKAN already holds these {len(dataframe)} rows — "
                "skipped the upload and the DataPusher+ reload"
            )


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
