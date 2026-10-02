"""The defs.yaml JSON Schema the IDE autocompletes from (scripts/editor_schema.py).

Its one job is to be USEFUL without being WRONG: a schema that rejected a valid
defs.yaml would underline good files in the editor, which teaches people to
ignore the underlines. So every real defs.yaml must pass, and typos must fail
on the key that's wrong rather than on the whole document.
"""

import pathlib
import sys

import pytest
import yaml

jsonschema = pytest.importorskip("jsonschema")

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from editor_schema import build  # noqa: E402

ALL_DEFS = sorted((REPO / "src/wprdc_etl/defs").rglob("defs.yaml"))


@pytest.fixture(scope="module")
def validator() -> "jsonschema.Draft202012Validator":
    return jsonschema.Draft202012Validator(build())


@pytest.mark.parametrize("path", ALL_DEFS, ids=lambda p: str(p.parent.name))
def test_every_defs_yaml_passes(path: pathlib.Path, validator) -> None:
    errors = list(validator.iter_errors(yaml.safe_load(path.read_text())))
    assert not errors, jsonschema.exceptions.best_match(errors).message


def test_a_typo_is_reported_on_the_key_that_is_wrong(validator) -> None:
    doc = yaml.safe_load(ALL_DEFS[0].read_text())
    doc["attributes"]["partition"] = "wekly"
    paths = {e.json_path for e in validator.iter_errors(doc)}
    assert "$.attributes.partition" in paths


def test_value_sets_come_from_the_code_registries() -> None:
    from wprdc_etl.strategies.extract import EXTRACTORS
    from wprdc_etl.strategies.transform import PRIMITIVES

    defs = build()["$defs"]
    assert defs["SourceModelModel"]["properties"]["type"]["enum"] == sorted(EXTRACTORS)
    steps = defs["TabularPipelineModel"]["properties"]["transforms"]
    assert steps["items"]["properties"]["op"]["enum"] == sorted(PRIMITIVES)
    assert [c["then"]["title"] for c in steps["items"]["allOf"]] == sorted(PRIMITIVES)


def test_a_bad_value_inside_a_step_is_reported_on_that_key(validator) -> None:
    """The transform list and its regions dispatch on op / shape, so a mistake
    deep in a step lands on its own line, not on the whole `transforms:`."""
    doc = yaml.safe_load(ALL_DEFS[0].read_text())
    doc["attributes"]["transforms"] = [
        {
            "op": "reverse_geocode",
            "lat": "latitude",
            "lng": "longitude",
            "regions": [{"layer": "census_tract", "column": "tract", "value": None}],
        }
    ]
    paths = {e.json_path for e in validator.iter_errors(doc)}
    assert "$.attributes.transforms[0].regions[0].value" in paths
