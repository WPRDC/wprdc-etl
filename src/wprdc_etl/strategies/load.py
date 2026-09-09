"""Load strategies: how transformed data reaches CKAN.

Derived from the component's `ingest` mode — the two are one-to-one, so the
loader isn't a separately configurable field:

    snapshot    -> ReplaceLoader   (hand DataPusher+ the whole file; full reload)
    incremental -> UpsertLoader    (DataStore upsert by primary key; deltas only)

Both run a COMPATIBILITY CHECK against the live CKAN schema before writing,
comparing the output's columns/types to what's currently published. The policy
differs by mode:
  * replace  -> WARN on removed columns / type changes (schema evolution is
                often intentional, and DataPusher+ rebuilds the table anyway),
                but surface it so consumer-breaking changes are visible.
  * upsert   -> BLOCK on type changes to shared columns (upserting into an
                existing typed table can fail or corrupt), WARN on columns the
                table has that the output drops.
"""

from __future__ import annotations

import os

from wprdc_etl.runtime import sink_dir


def _stem(cfg):
    return "__".join(x for x in [cfg.publisher, cfg.department, cfg.dataset] if x)


def dump_local(cfg, dataframe, context, suffix="csv") -> bool:
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


def _coarse(t):
    return _COARSE.get(str(t).lower(), "text")


def compat_report(cfg, dataframe, ckan) -> dict:
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


def _fmt_changes(type_changes) -> str:
    return ", ".join(f"{c}: {a}->{b}" for c, (a, b) in type_changes.items())


def _warn_replace(report, context) -> None:
    if not report["live_exists"] or context is None:
        return
    if report["removed"]:
        context.log.warning(
            "replace: columns dropped vs live CKAN (consumers may break): "
            f"{report['removed']}"
        )
    if report["type_changes"]:
        context.log.warning(
            f"replace: column type changes vs live CKAN: {_fmt_changes(report['type_changes'])}"
        )


def _guard_upsert(report, context) -> None:
    if not report["live_exists"]:
        return
    if report["type_changes"]:
        raise RuntimeError(
            "upsert aborted: column type(s) differ from the live CKAN table "
            f"({_fmt_changes(report['type_changes'])}). If this change is "
            "intended, reset the DataStore table (datastore_delete) so it can be "
            "recreated with the new types, then re-run."
        )
    if report["removed"] and context is not None:
        context.log.warning(
            f"upsert: columns in CKAN not produced by output (won't be updated): "
            f"{report['removed']}"
        )


class Loader:
    def load(self, cfg, dataframe, *, ckan, context=None) -> None:
        raise NotImplementedError


class ReplaceLoader(Loader):
    """Full-refresh load via DataPusher+."""

    def load(self, cfg, dataframe, *, ckan, context=None) -> None:
        if dump_local(cfg, dataframe, context):
            return
        if not dataframe.empty:
            _warn_replace(compat_report(cfg, dataframe, ckan), context)
        ckan.replace(cfg.ckan.resource_id, dataframe)


class UpsertLoader(Loader):
    """Delta load: upsert changed rows by primary key via the DataStore API."""

    def load(self, cfg, dataframe, *, ckan, context=None) -> None:
        if dump_local(cfg, dataframe, context):
            return
        if dataframe.empty:
            return  # empty delta -> no-op
        _guard_upsert(compat_report(cfg, dataframe, ckan), context)
        ckan.upsert(cfg.ckan.resource_id, dataframe, primary_key=cfg.ckan.primary_key)


LOADERS: dict[str, Loader] = {
    "snapshot": ReplaceLoader(),
    "incremental": UpsertLoader(),
}


def get_loader(ingest: str) -> Loader:
    try:
        return LOADERS[ingest]
    except KeyError:
        raise ValueError(
            f"unknown ingest mode {ingest!r} (expected 'snapshot' or 'incremental')"
        )
