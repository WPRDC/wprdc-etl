#!/usr/bin/env python3
"""Generate a JSON Schema for defs.yaml, so the IDE can autocomplete it.

    uv run python scripts/editor_schema.py           # write .dg/defs.schema.json
    uv run python scripts/editor_schema.py --check   # exit 1 if it's stale

Why not `dg utils generate-component-schema`: in dagster 1.13 it calls
`list_all_components_schema(entry_points=True, extra_modules=())`, so it only
sees component packages registered through entry points — this project's own
`registry_modules` never reach it, and the schema it writes has no
TabularPipeline or FilePipeline in it at all. This calls the same function with
our modules supplied, and with entry points off so the `type:` suggestions are
our two components rather than eighteen of Dagster's.

It then fills in the VALUE sets the derived models can't express (every field
is a plain string to pydantic), each read from the registry the code itself
dispatches on, so the schema can't drift from what `dg check` accepts:

    source.type            EXTRACTORS              strategies/extract.py
    source.format          ARCGIS_FORMATS | PASDA_FORMATS
    publish                LOADERS                 strategies/load.py
    ckan.mirror[].format   MIRROR_KINDS            strategies/mirror.py
    representations[].format  GEO_FORMATS          strategies/emit.py
    transforms[]           PRIMITIVES + their signatures (op, params, docs)
    reverse_geocode layers every `region_layer:` name declared under defs/
    join_geometry layer    every `key_layer:` name declared under defs/

The IDE side: `bin/editor-schema` also maps the schema onto
`src/wprdc_etl/defs/**/defs.yaml` in PyCharm (.idea/jsonSchemas.xml).
"""

from __future__ import annotations

import argparse
import inspect
import json
import pkgutil
import sys
import tomllib
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
OUTPUT = REPO / ".dg" / "defs.schema.json"
DEFS_GLOB = "src/wprdc_etl/defs/**/defs.yaml"


def registry_modules() -> list[str]:
    """The project's component modules, from pyproject's `registry_modules`
    (a trailing `.*` expands to every submodule, the way dg reads it)."""
    import importlib

    patterns = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["dg"][
        "project"
    ]["registry_modules"]
    modules: list[str] = []
    for pattern in patterns:
        if not pattern.endswith(".*"):
            modules.append(pattern)
            continue
        package = importlib.import_module(pattern[:-2])
        modules += [
            f"{package.__name__}.{m.name}"
            for m in pkgutil.iter_modules(package.__path__)
            if not m.name.startswith("_")
        ]
    return modules


def _first_line(doc: str | None) -> str:
    return (inspect.cleandoc(doc or "").split("\n\n")[0]).replace("\n", " ")


def _string_enum(values: list[str], description: str | None = None) -> dict:
    out: dict[str, Any] = {"type": "string", "enum": sorted(values)}
    if description:
        out["description"] = description
    return out


def _set_string(prop: dict, values: list[str]) -> None:
    """Constrain a field's plain-string branch to `values`, in place.

    Resolvable widens every non-str field to `T | str` so a `{{ env(...) }}`
    template survives validation; the str branch is what gets the enum, which
    for these fields is the only branch there is.
    """
    enum = sorted(values)
    if prop.get("type") == "string":
        prop["enum"] = enum
        return
    for branch in prop.get("anyOf", []):
        if branch.get("type") == "string":
            branch["enum"] = enum


def declared_layers(block: str) -> list[str]:
    """Every `<block>: name:` declared by a defs.yaml — the layers a
    reverse_geocode / join_geometry step can actually ask for."""
    import yaml

    names = set()
    for path in (REPO / "src/wprdc_etl/defs").rglob("defs.yaml"):
        attrs = (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}
        if name := (attrs.get(block) or {}).get("name"):
            names.add(name)
    return sorted(names)


def param_schemas() -> dict[str, dict[str, dict]]:
    """Structured params, for the ops where a bare `{}` would let a real
    mistake through. Mirrors the load-time checks in strategies/transform.py
    (_region_specs, _key_format) so the editor flags what dg check rejects."""
    regions = declared_layers("region_layer")
    region = {"type": "string", "enum": regions}
    return {
        "reverse_geocode": {
            "regions": {
                "type": "array",
                "minItems": 1,
                # A bare layer name, or a mapping. if/then/else rather than
                # anyOf, so a bad key inside a mapping is flagged on that key.
                "items": {
                    "if": {"type": "object"},
                    "else": region,
                    "then": {
                        "type": "object",
                        "required": ["layer"],
                        "additionalProperties": False,
                        "properties": {
                            "layer": region,
                            "column": {
                                "type": "string",
                                "description": "output column; defaults "
                                "to the layer name",
                            },
                            "value": {
                                "type": "string",
                                "enum": ["name", "id"],
                                "description": "name = the label (falls "
                                "back to the code); id = the code",
                            },
                        },
                    },
                },
            }
        },
        "join_geometry": {
            "layer": {"type": "string", "enum": declared_layers("key_layer")},
            "key_format": {
                "type": "string",
                "pattern": "^[^{}]*\\{\\}[^{}]*$",
                "description": "str.format template with exactly one {}",
            },
        },
    }


def transform_steps() -> dict:
    """One schema per transform primitive: `op` const, its params, and the
    first paragraph of its docstring as the hover text. Injected deps
    (`@needs`) are not part of the YAML surface and are left out, the same
    way validate_steps leaves them out."""
    from wprdc_etl.strategies.transform import PRIMITIVES

    structured = param_schemas()
    branches = []
    for op, fn in sorted(PRIMITIVES.items()):
        injected = set(getattr(fn, "_needs", ()))
        params = [
            p
            for p in inspect.signature(fn).parameters.values()
            if p.name != "df" and p.name not in injected
        ]
        doc = _first_line(fn.__doc__)
        branches.append(
            {
                "title": op,
                "description": doc,
                "type": "object",
                "properties": {
                    "op": {"const": op, "description": doc},
                    **{p.name: structured.get(op, {}).get(p.name, {}) for p in params},
                },
                "required": [
                    "op",
                    *(p.name for p in params if p.default is inspect.Parameter.empty),
                ],
                "additionalProperties": False,
            }
        )
    # Dispatch on `op:` rather than anyOf over the branches, for the same
    # reason the document dispatches on `type:`: under anyOf a mistake inside
    # one step fails every branch, and the IDE underlines the whole
    # `transforms:` list instead of the line that's wrong.
    return {
        "type": "array",
        "items": {
            "type": "object",
            "required": ["op"],
            "properties": {"op": {"enum": [b["title"] for b in branches]}},
            "allOf": [
                {
                    "if": {
                        "properties": {"op": {"const": b["title"]}},
                        "required": ["op"],
                    },
                    "then": b,
                }
                for b in branches
            ],
        },
    }


def build() -> dict:
    """The full schema: our components plus the injected value sets."""
    from dagster.components.list import list_all_components_schema

    from wprdc_etl.strategies.emit import GEO_FORMATS
    from wprdc_etl.strategies.extract import ARCGIS_FORMATS, EXTRACTORS, PASDA_FORMATS
    from wprdc_etl.strategies.load import BOOL_FORMATS, LOADERS
    from wprdc_etl.strategies.mirror import MIRROR_KINDS

    schema = list_all_components_schema(
        entry_points=False, extra_modules=registry_modules()
    )
    defs = schema["$defs"]

    for name in ("TabularPipelineModel", "FilePipelineModel"):
        props = defs[name]["properties"]
        # Cadences partitions_for() understands; anything else silently
        # becomes daily, which is exactly why it's worth constraining here.
        _set_string(props["partition"], ["daily", "weekly", "monthly", "none"])
    tabular = defs["TabularPipelineModel"]["properties"]
    _set_string(tabular["ingest"], ["snapshot", "incremental"])
    _set_string(tabular["publish"], list(LOADERS))
    # One schema typed array-or-null, not anyOf [steps, null]: an anyOf here
    # would collapse every error inside a step back onto `transforms:`.
    tabular["transforms"] = {
        **transform_steps(),
        "type": ["array", "null"],
        "default": None,
        "title": "Transforms",
    }

    source = defs["SourceModelModel"]["properties"]
    _set_string(source["type"], list(EXTRACTORS))
    _set_string(source["format"], sorted(set(ARCGIS_FORMATS) | set(PASDA_FORMATS)))
    _set_string(defs["MirrorModelModel"]["properties"]["format"], list(MIRROR_KINDS))
    _set_string(defs["CkanModelModel"]["properties"]["bool_format"], list(BOOL_FORMATS))
    _set_string(
        defs["RepresentationModelModel"]["properties"]["format"], list(GEO_FORMATS)
    )

    # Dispatch on `type:` explicitly. Under the generated top-level anyOf, a
    # typo deep in a TabularPipeline fails BOTH branches, so the error lands on
    # the whole document; with if/then it lands on the key that's wrong.
    refs = [branch["$ref"] for branch in schema.pop("anyOf")]
    types = [defs[ref.rsplit("/", 1)[1]]["properties"]["type"]["const"] for ref in refs]
    schema.update(
        {
            "type": "object",
            "required": ["type"],
            "properties": {"type": {"enum": types}},
            "allOf": [
                {
                    "if": {"properties": {"type": {"const": t}}, "required": ["type"]},
                    "then": {"$ref": ref},
                }
                for t, ref in zip(types, refs)
            ],
        }
    )
    return schema


def main() -> int:
    """Write the schema, or with --check report whether it is current."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if stale")
    args = ap.parse_args()

    text = json.dumps(build(), indent=2, sort_keys=True) + "\n"
    if args.check:
        current = OUTPUT.read_text() if OUTPUT.exists() else ""
        if current != text:
            print(f"{OUTPUT.relative_to(REPO)} is stale — run bin/editor-schema")
            return 1
        print(f"{OUTPUT.relative_to(REPO)} is current")
        return 0
    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(text)
    print(f"wrote {OUTPUT.relative_to(REPO)} ({len(text) // 1024} KiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
