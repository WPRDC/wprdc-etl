"""Transform strategies: a shared primitive library + a declarative runner.

Two ways to transform a dataset, and they compose:

  1. Declarative steps in the component YAML (the shared 80% — dates, trims,
     renames). Listed under `transforms:` and run in order by apply_declarative.
  2. A co-located transform.py for bespoke logic, run *after* the declarative
     steps. It imports these same primitives, so it composes rather than
     reimplements.

Primitive signature: fn(df, **params) -> df. Each guards missing columns so a
schema hiccup degrades gracefully instead of raising mid-batch.

Most primitives are pure frame->frame. A few need something from the run — the
spatial store, the Dagster context — and declare it with @needs(...); the runner
injects those by name and only for the primitives that asked. Keeping the opt-in
explicit means the common case stays a plain testable function.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

import pandas as pd

if TYPE_CHECKING:
    import dagster as dg

    from wprdc_etl.resources import SpatialResource

# Every primitive has the same shape: fn(df, **params) -> df.
Primitive = Callable[..., pd.DataFrame]
_P = TypeVar("_P", bound=Primitive)


def needs(*names: str) -> Callable[[_P], _P]:
    """Declare the runtime deps apply_declarative should inject into a primitive.

    Tagged names are passed as keyword arguments and are NOT part of the YAML
    surface — validate_steps ignores them when checking a step's params.
    """

    def deco(fn: _P) -> _P:
        fn._needs = tuple(names)
        return fn

    return deco


# --------------------------------------------------------------------------
# Primitive library
# --------------------------------------------------------------------------
def iso_date(
    df: pd.DataFrame, columns: list[str], fmt: str | None = None
) -> pd.DataFrame:
    """Normalize date columns to ISO 8601 (YYYY-MM-DD). Unparseable -> NaT."""
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce").dt.strftime(
                "%Y-%m-%d"
            )
    return df


def to_datetime(
    df: pd.DataFrame,
    columns: list[str],
    fmt: str | None = None,
    tz: str | None = None,
    date_only: bool = False,
) -> pd.DataFrame:
    """Parse columns into real (tz-naive) datetimes. Unparseable -> NaT.

    Unlike iso_date, which writes text, this keeps the dtype a datetime, so
    the published column stays a DataStore `timestamp` (the column guard
    reads a text column against a live timestamp as a retype) and a timestamp
    in an accumulating dataset's merge key compares equal to the copy
    `bin/seed-accumulator` stored.

    A tz-aware value keeps its wall time in whatever zone it was parsed in —
    for a GeoJSON source that is UTC (the reader normalises offsets), so
    midnight Eastern becomes 04:00/05:00. `tz: America/New_York` converts to
    that zone first; `date_only: true` then drops the time of day, for a
    column the live table holds as a `date`.
    """
    for c in columns:
        if c in df.columns:
            parsed = pd.to_datetime(
                df[c], format=fmt, errors="coerce", utc=tz is not None
            )
            if getattr(parsed.dt, "tz", None) is not None:
                if tz is not None:
                    parsed = parsed.dt.tz_convert(tz)
                parsed = parsed.dt.tz_localize(None)
            if date_only:
                parsed = parsed.dt.normalize()
            df[c] = parsed
    return df


def point_coordinates(
    df: pd.DataFrame, lat: str = "latitude", lng: str = "longitude"
) -> pd.DataFrame:
    """Turn a point GeoDataFrame into a plain table with lat/lng columns.

    For a GeoJSON source published as a CSV DataStore table, the way the
    legacy geojson-to-csv converter did it: the coordinates become two
    columns and the geometry column is dropped, so the published header
    doesn't gain a WKT column (which the column guard would block). A
    non-point or empty geometry gives NA coordinates.
    """
    import pandas as pd

    geom = _active_geometry(df)
    if geom is None:
        return df
    g = df[geom]
    points = (g.geom_type == "Point") & ~g.is_empty
    out = pd.DataFrame(df.drop(columns=[geom]))
    out[lat] = pd.Series(pd.NA, index=out.index, dtype="Float64")
    out[lng] = pd.Series(pd.NA, index=out.index, dtype="Float64")
    out.loc[points, lat] = g[points].y
    out.loc[points, lng] = g[points].x
    return out


def strip(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Trim leading/trailing whitespace on the given text columns (or all
    object columns if none named)."""
    cols = columns or list(df.select_dtypes(include="object").columns)
    for c in cols:
        if c in df.columns:
            # An object column can mix strings with other values (a parsed
            # list, a stray number); `.str` refuses those outright, so only
            # the strings are trimmed and everything else passes through.
            df[c] = df[c].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def rename(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename columns via {old: new}. Renaming a GeoDataFrame's geometry
    column keeps it the geometry (e.g. `geometry: wkt` for a legacy table)."""
    active = _active_geometry(df)
    return _repoint_geometry(df.rename(columns=mapping), active, mapping.get(active))


def coerce_numeric(
    df: pd.DataFrame,
    columns: list[str],
    integer: bool = False,
    round_to: int | None = None,
) -> pd.DataFrame:
    """Coerce columns to numeric; non-numeric -> NaN.

    `round_to: n` rounds to n decimal places (half to even, as Python's
    round does). `integer: true` makes them nullable integers, so a column
    with blanks still publishes `3`, not `3.0`; a value with a fractional
    part fails rather than being truncated — round it first (`round_to: 0`).
    """
    for c in columns:
        if c not in df.columns:
            continue
        values = pd.to_numeric(df[c], errors="coerce")
        if round_to is not None:
            values = values.round(round_to)
        if integer:
            fractional = values.notna() & (values % 1 != 0)
            if fractional.any():
                raise ValueError(
                    f"coerce_numeric: {c!r} has {int(fractional.sum())} "
                    f"non-whole value(s) (e.g. {values[fractional].iloc[0]}) "
                    "but integer: true"
                )
            values = values.astype("Int64")
        df[c] = values
    return df


def coerce_text(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Coerce columns to nullable string — the inverse of coerce_numeric.

    For identifiers / codes that a CSV parser reads as numbers but that must
    stay text downstream (zip codes, FIPS, district numbers, account ids). An
    integral value like 1011 renders as "1011", not "1011.0"; missing stays NA.
    """
    for c in columns:
        if c not in df.columns:
            continue
        s = df[c]
        if pd.api.types.is_float_dtype(s):
            df[c] = s.map(_float_text).astype("string")
        else:
            df[c] = s.astype("string")
    return df


def _float_text(v: Any) -> Any:
    """A float as the text it stood for: 1011.0 -> "1011", 2.5 -> "2.5".

    Not `:g`, which keeps six significant digits — a phone number or parcel
    id read as a float came out as "1.41235e+10".
    """
    if pd.isna(v):
        return pd.NA
    return str(int(v)) if float(v).is_integer() else repr(float(v))


def drop_nulls(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Drop rows null in any of the named columns (e.g. a missing key)."""
    present = [c for c in columns if c in df.columns]
    return df.dropna(subset=present) if present else df


def drop_duplicates(
    df: pd.DataFrame, columns: list[str] | None = None, keep: str = "last"
) -> pd.DataFrame:
    """Drop rows repeating an earlier row's values in `columns` (all columns
    when unset), keeping the `last` (default) or `first` occurrence.

    What a legacy upsert on a primary key did to a file with repeated rows,
    for a snapshot that now publishes the file whole.
    """
    if keep not in ("first", "last"):
        raise ValueError(
            f"drop_duplicates: keep must be 'first' or 'last', not {keep!r}"
        )
    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"drop_duplicates: no column(s) {missing}")
    return df.drop_duplicates(subset=columns or None, keep=keep).reset_index(drop=True)


def add_column(df: pd.DataFrame, name: str, value: Any = None) -> pd.DataFrame:
    """Add column `name` holding `value` (missing by default), if absent.

    For a column the published table keeps but the source no longer
    supplies, so the frame's shape doesn't depend on the source (pair it
    with `ckan.carry_forward` to keep the stored values).
    """
    if name not in df.columns:
        df[name] = pd.Series(
            [value] * len(df),
            index=df.index,
            dtype="string" if value is None or isinstance(value, str) else None,
        )
    return df


def forbid_columns(df: pd.DataFrame, columns: list[str], reason: str) -> pd.DataFrame:
    """Fail the run if the source has any of `columns` — a tripwire.

    For a column whose values must not be published as they arrive, and
    that the source doesn't send today (PLI permits' owner names: if the
    city sends them again, they need redaction rules first). No retries:
    the column will still be there in a minute.
    """
    import dagster as dg

    present = [c for c in columns if c in df.columns]
    if present:
        raise dg.Failure(
            f"the source now has column(s) {present}: {reason}",
            allow_retries=False,
        )
    return df


def regex_replace(
    df: pd.DataFrame, columns: list[str], pattern: str, repl: str = ""
) -> pd.DataFrame:
    """Replace regex `pattern` with `repl` in text columns; non-strings and
    missing values pass through. `\\1`-style backreferences work in `repl`."""
    import re

    rx = re.compile(pattern)
    for c in columns:
        if c not in df.columns:
            raise ValueError(f"regex_replace: no column {c!r}")
        df[c] = df[c].map(lambda v: rx.sub(repl, v) if isinstance(v, str) else v)
    return df


# Below this share of parcels found, the lookup table is presumed wrong or
# empty rather than the parcels unknown.
_MIN_PARCELS_FOUND = 0.5


@needs("ckan", "context")
def redact_homestead_owners(
    df: pd.DataFrame,
    owner: str,
    parcel: str,
    resource_id: str,
    ckan: Any = None,
    context: Any = None,
    parcel_key: str = "PARID",
    flag_column: str = "HOMESTEADFLAG",
    flag: str = "HOM",
) -> pd.DataFrame:
    """Blank `owner` where the parcel is homestead-exempt — someone lives
    there — or can't be checked.

    The legacy condemned-properties job's rule, kept exactly: look the
    parcel up in the property-assessments table (`resource_id`, on the CKAN
    the pipeline publishes to) and redact when its `flag_column` is `flag`,
    when the parcel is missing from the table, or when the row has no parcel
    id. Fails closed: any lookup error fails the step, so nothing is
    published unredacted. Verified 2026-11-04 against the live table: it
    reproduces all 584 blanks among 3,594 condemned properties.

    A lookup that finds under half of the parcels fails too (no retries): the
    table is presumably empty or the wrong resource, and blanking every
    owner would quietly gut the column.
    """
    import dagster as dg

    if owner not in df.columns or parcel not in df.columns:
        raise ValueError(
            f"redact_homestead_owners: needs columns {owner!r} and {parcel!r} "
            f"(has {list(df.columns)})"
        )
    named = df[owner].notna() & (df[owner].astype("string").str.strip() != "")
    pins = df[parcel].astype("string").str.strip()
    wanted = sorted(set(pins[named & pins.notna() & (pins != "")]))
    try:
        found = ckan.lookup(resource_id, parcel_key, wanted, [flag_column])
    except Exception as e:
        raise dg.Failure(
            f"redact_homestead_owners: couldn't read homestead flags from "
            f"{resource_id}; publishing nothing rather than unredacted owners "
            f"({type(e).__name__}: {e})"
        ) from e
    if len(wanted) >= 50 and len(found) < _MIN_PARCELS_FOUND * len(wanted):
        raise dg.Failure(
            f"redact_homestead_owners: only {len(found)} of {len(wanted)} "
            f"parcels found in {resource_id} — is it the property-assessments "
            "table, and is it loaded?",
            allow_retries=False,
        )
    homestead = {p for p, r in found.items() if (r.get(flag_column) or "") == flag}
    keep = pins.isin(set(found) - homestead).fillna(False)
    redact = named & ~keep
    df[owner] = df[owner].astype("string").mask(redact | ~named)  # blanks too
    if context is not None:
        missing = len(set(wanted) - set(found))
        context.log.info(
            f"redact_homestead_owners: blanked {int(redact.sum())} of "
            f"{int(named.sum())} owners ({len(homestead)} homestead parcels, "
            f"{missing} parcels not in the assessments table)"
        )
    return df


def fill_na(
    df: pd.DataFrame, value: Any, columns: list[str] | None = None
) -> pd.DataFrame:
    """Fill NA with a constant, on named columns or the whole frame."""
    if columns:
        for c in columns:
            if c in df.columns:
                df[c] = df[c].fillna(value)
        return df
    return df.fillna(value)


def select(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Keep only the named columns (those that exist), preserving order."""
    return df[[c for c in columns if c in df.columns]]


def drop_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Remove the named columns if present."""
    return df.drop(columns=[c for c in columns if c in df.columns])


def snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case."""
    active = _active_geometry(df)
    df.columns = [_to_snake(c) for c in df.columns]
    return _repoint_geometry(df, active, _to_snake(active) if active else None)


def to_crs(df: pd.DataFrame, epsg: int) -> pd.DataFrame:
    """Reproject a GeoDataFrame to the given EPSG code (e.g. 4326 for web).
    No-op on a plain DataFrame with no geometry."""
    if hasattr(df, "to_crs"):
        return df.to_crs(epsg=epsg)
    return df


@needs("spatial", "context")
def reverse_geocode(
    df: pd.DataFrame,
    lat: str,
    lng: str,
    regions: list[str | dict[str, str]],
    *,
    spatial: SpatialResource,
    context: dg.AssetExecutionContext | None = None,
) -> pd.DataFrame:
    """Attach administrative region columns by point-in-polygon.

    Resolves each row's coordinates against boundary layers held in the PostGIS
    admin-region store, adding one column per requested layer. Use it for
    columns a source stopped shipping (or never shipped) but that the published
    table needs — neighborhood, council district, fire zone, municipality.

    `regions` entries are either a bare layer name (column takes the layer's
    name) or a mapping:

        regions:
          - neighborhood                  # -> column "neighborhood", the label
          - layer: council_district       # -> column "district", the stable code
            column: district
            value: id

    `value` defaults to "name", which yields the layer's human label and falls
    back to the code for layers that have none (public_works_division). Rows
    whose point falls outside every region — river coordinates, a broken
    extract — are left null and counted in a warning rather than failing the run.
    """
    specs = _region_specs(regions)

    missing = [c for c in (lat, lng) if c not in df.columns]
    if missing:
        # Same guard-and-degrade contract as every other primitive: a schema
        # hiccup shouldn't raise mid-batch. schema_ok is what fails the run.
        if context is not None:
            context.log.warning(
                f"reverse_geocode: coordinate column(s) {missing} not in the "
                f"frame; leaving {[s['column'] for s in specs]} unset"
            )
        for spec in specs:
            df[spec["column"]] = pd.Series(pd.NA, index=df.index, dtype="string")
        return df

    ys = pd.to_numeric(df[lat], errors="coerce")
    xs = pd.to_numeric(df[lng], errors="coerce")
    locatable = xs.notna() & ys.notna()
    pairs = list(zip(xs[locatable], ys[locatable]))

    # Distinct points only — many rows commonly share a location, and the
    # lookup cost is per point, not per row.
    found = (
        spatial.locate(sorted(set(pairs)), [s["layer"] for s in specs]) if pairs else {}
    )

    for spec in specs:
        values = pd.Series(pd.NA, index=df.index, dtype="string")
        if pairs:
            values[locatable] = pd.array(
                [_pick(found.get(p), spec["layer"], spec["value"]) for p in pairs],
                dtype="string",
            )
        df[spec["column"]] = values
        if context is not None:
            unresolved = int(locatable.sum() - values[locatable].notna().sum())
            if unresolved:
                context.log.warning(
                    f"reverse_geocode: {unresolved} of {int(locatable.sum())} "
                    f"located rows fell outside every {spec['layer']!r} region"
                )
    if context is not None and (~locatable).any():
        context.log.warning(
            f"reverse_geocode: {int((~locatable).sum())} rows have no usable "
            f"{lat}/{lng} and were left unresolved"
        )
    return df


@needs("spatial", "context")
def join_geometry(
    df: pd.DataFrame,
    layer: str,
    key: str,
    key_format: str = "{}",
    lat: str = "latitude",
    lng: str = "longitude",
    *,
    spatial: SpatialResource,
    context: dg.AssetExecutionContext | None = None,
) -> pd.DataFrame:
    """Attach coordinates by looking each row's key up in a key layer.

    For a table that references geometry by identifier instead of carrying it
    — the county's Addressing Landmarks hold an ADDRESS_ID and nothing else
    spatial. The key layer (address points, loaded by a `key_layer:` pipeline)
    holds the geometry; this adds `lat` / `lng` (WGS84) from it:

        - op: join_geometry
          layer: address_point
          key: ADDRESS_ID
          key_format: "SSAP{}"      # the landmarks store 450843, points SSAP450843

    `key_format` is a str.format template applied to each key before lookup.
    Rows whose key is missing from the layer get null coordinates and are
    counted in a warning rather than failing the run — same contract as
    reverse_geocode. A polygon layer yields a point inside the polygon.
    """
    from wprdc_etl.resources import key_text

    if key not in df.columns:
        if context is not None:
            context.log.warning(
                f"join_geometry: key column {key!r} not in the frame; "
                f"leaving {lat}/{lng} unset"
            )
        df[lat] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df[lng] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        return df

    raw = df[key].map(key_text)
    lookup = raw.map(lambda k: key_format.format(k) if k is not None else None)
    found = spatial.lookup_keys(layer, lookup.dropna())

    hits = lookup.map(found.get)
    df[lng] = pd.array([h[0] if h else None for h in hits], dtype="Float64")
    df[lat] = pd.array([h[1] if h else None for h in hits], dtype="Float64")

    if context is not None:
        keyed = int(lookup.notna().sum())
        matched = int(hits.notna().sum())
        if keyed - matched:
            context.log.warning(
                f"join_geometry: {keyed - matched} of {keyed} keyed rows have no "
                f"{layer!r} geometry and were left without coordinates"
            )
        if keyed < len(df):
            context.log.warning(
                f"join_geometry: {len(df) - keyed} rows have no {key!r} at all"
            )
    return df


def _key_format(params: dict[str, Any]) -> None:
    """A key_format must place the key exactly once."""
    fmt = params.get("key_format", "{}")
    if not isinstance(fmt, str) or fmt.count("{}") != 1 or fmt.count("{") != 1:
        raise ValueError(
            f"join_geometry: key_format {fmt!r} must contain exactly one '{{}}'"
        )


def _region_specs(regions: list[str | dict[str, str]] | None) -> list[dict[str, str]]:
    """Normalize the `regions:` YAML into [{layer, column, value}]. Raises on a
    malformed entry so `dg check` catches it, not the 3am run."""
    if not regions:
        raise ValueError("reverse_geocode: 'regions' must list at least one layer")
    specs = []
    for i, entry in enumerate(regions):
        if isinstance(entry, str):
            specs.append({"layer": entry, "column": entry, "value": "name"})
            continue
        if not isinstance(entry, dict) or "layer" not in entry:
            raise ValueError(
                f"reverse_geocode: regions[{i}] must be a layer name or a "
                f"mapping with 'layer', got {entry!r}"
            )
        unknown = sorted(set(entry) - {"layer", "column", "value"})
        if unknown:
            raise ValueError(
                f"reverse_geocode: regions[{i}] has unknown key(s) {unknown} "
                "(known: layer, column, value)"
            )
        value = entry.get("value", "name")
        if value not in ("name", "id"):
            raise ValueError(
                f"reverse_geocode: regions[{i}] value must be 'name' or 'id', "
                f"got {value!r}"
            )
        specs.append(
            {
                "layer": entry["layer"],
                "column": entry.get("column", entry["layer"]),
                "value": value,
            }
        )
    return specs


def _pick(
    hit: dict[str, tuple[str, str | None]] | None, layer: str, which: str
) -> str | None:
    """Choose the id or the label from one layer's lookup result."""
    if not hit:
        return None
    pair = hit.get(layer)
    if not pair:
        return None
    region_id, region_name = pair
    if which == "id":
        return region_id
    return region_name if region_name not in (None, "") else region_id


def _active_geometry(df: pd.DataFrame) -> str | None:
    """The name of a GeoDataFrame's active geometry column; None otherwise."""
    return getattr(df, "_geometry_column_name", None)


def _repoint_geometry(
    df: pd.DataFrame, old: str | None, new: str | None
) -> pd.DataFrame:
    """After a column rename, point a GeoDataFrame back at its geometry.

    geopandas tracks the active geometry by NAME, so renaming that column
    leaves it pointing at a column that no longer exists, and every spatial
    operation after it fails — the GeoJSON representation included. The CSV
    publish is unaffected either way: it writes the geometry as WKT under
    whatever the column is called.
    """
    if old and new and new != old and hasattr(df, "set_geometry"):
        return df.set_geometry(new)
    return df


def _to_snake(name: str) -> str:
    s = re.sub(r"[\s\-]+", "_", str(name).strip())
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    return s.lower()


# name used in YAML -> primitive
PRIMITIVES: dict[str, Primitive] = {
    "iso_date": iso_date,
    "to_datetime": to_datetime,
    "point_coordinates": point_coordinates,
    "strip": strip,
    "rename": rename,
    "coerce_numeric": coerce_numeric,
    "coerce_text": coerce_text,
    "drop_nulls": drop_nulls,
    "drop_duplicates": drop_duplicates,
    "regex_replace": regex_replace,
    "add_column": add_column,
    "forbid_columns": forbid_columns,
    "redact_homestead_owners": redact_homestead_owners,
    "fill_na": fill_na,
    "select": select,
    "drop_columns": drop_columns,
    "snake_case_columns": snake_case_columns,
    "to_crs": to_crs,
    "reverse_geocode": reverse_geocode,
    "join_geometry": join_geometry,
}


# Ops whose params need more than a name check at load time.
def _regex_params(params: dict[str, Any]) -> None:
    """Checked on the RESOLVED step, i.e. after dagster's YAML templating,
    which trims whitespace-only strings: a pattern written as a real "\r\n"
    arrives as None. Write patterns as regex escapes ('\r\n') instead."""
    pattern, repl = params.get("pattern"), params.get("repl", "")
    if not isinstance(pattern, str) or not pattern:
        raise ValueError(
            f"regex_replace: pattern {pattern!r} isn't a non-empty string — a "
            "whitespace-only pattern is trimmed by the YAML templating; write "
            "it as a regex escape in single quotes ('\\r\\n')"
        )
    if not isinstance(repl, str):
        raise ValueError(
            f"regex_replace: repl {repl!r} isn't a string (a whitespace-only "
            "repl is trimmed to nothing; write it as an escape, '\\n')"
        )
    re.compile(pattern)


_STEP_VALIDATORS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "regex_replace": _regex_params,
    "reverse_geocode": lambda params: _region_specs(params.get("regions")),
    "join_geometry": _key_format,
}


# --------------------------------------------------------------------------
# Declarative runner + config-time validation
# --------------------------------------------------------------------------
def validate_steps(steps: list[dict[str, Any]] | None) -> None:
    """Fail-loud at component load (dg check / dg dev) if a step names an
    unknown op, misspells a param, or omits a required one — rather than at 3am
    when the asset runs."""
    for i, step in enumerate(steps or []):
        if "op" not in step:
            raise ValueError(f"transform step {i} is missing 'op'")
        op = step["op"]
        if op not in PRIMITIVES:
            known = ", ".join(sorted(PRIMITIVES))
            raise ValueError(f"transform step {i}: unknown op {op!r} (known: {known})")

        fn = PRIMITIVES[op]
        params = {k: v for k, v in step.items() if k != "op"}
        # Injected deps aren't part of the YAML surface; stand them in so the
        # bind only judges what the YAML actually supplied.
        sig = inspect.signature(fn)
        injected = {name: None for name in getattr(fn, "_needs", ())}
        try:
            sig.bind(None, **params, **injected)
        except TypeError as e:
            accepted = [p for p in sig.parameters if p != "df" and p not in injected]
            raise ValueError(
                f"transform step {i} ({op}): {e} — accepted params: "
                f"{', '.join(accepted) or '(none)'}"
            ) from None

        extra = _STEP_VALIDATORS.get(op)
        if extra is not None:
            try:
                extra(params)
            except ValueError as e:
                raise ValueError(f"transform step {i}: {e}") from None


def steps_need(steps: list[dict[str, Any]] | None) -> set[str]:
    """The runtime objects a step list asks for via @needs, e.g. {"spatial"}.
    The `validated` asset requests a resource only when its steps need it."""
    return {
        name
        for step in steps or []
        for name in getattr(PRIMITIVES[step["op"]], "_needs", ())
    }


def apply_declarative(
    df: pd.DataFrame,
    steps: list[dict[str, Any]],
    *,
    deps: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Run the declarative steps in order. Works on a copy so the IO-manager
    input isn't mutated.

    `deps` supplies the runtime objects primitives request via @needs (the
    spatial store, the Dagster context). Steps that don't ask for anything run
    unchanged, so a plain apply_declarative(df, steps) stays valid.
    """
    deps = deps or {}
    df = df.copy()
    for step in steps:
        fn = PRIMITIVES[step["op"]]
        params = {k: v for k, v in step.items() if k != "op"}
        for name in getattr(fn, "_needs", ()):
            if name not in deps:
                raise ValueError(
                    f"op {step['op']!r} needs {name!r}, which isn't available here"
                )
            params[name] = deps[name]
        df = fn(df, **params)
    return df
