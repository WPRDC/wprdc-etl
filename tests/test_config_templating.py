"""Env templating must reach INSIDE the nested config blocks.

The nested models are `Resolvable` (not plain `Model`) specifically so
`{{ env(...) }}` is rendered in a `source:` / `ckan:` block. A plain `Model`
fails silently — the literal template string becomes the hostname — so this is
a regression test, not a feature test.
"""

import pytest
from dagster.components import Resolvable

from wprdc_etl.components import models


@pytest.mark.parametrize(
    "model",
    [
        models.SourceModel,
        models.CkanModel,
        models.RepresentationModel,
        models.RegionLayerModel,
    ],
)
def test_nested_config_models_are_resolvable(model):
    """Drop Resolvable and templated source/ckan fields silently stop rendering."""
    assert issubclass(model, Resolvable), (
        f"{model.__name__} must be Resolvable or `{{{{ env(...) }}}}` inside its "
        "block resolves to the literal template string"
    )


@pytest.mark.parametrize(
    "model, field, expected",
    [
        (models.SourceModel, "port", int),
        (models.CkanModel, "rebuild", bool),
        (models.CkanModel, "primary_key", list),
    ],
)
def test_non_string_fields_stay_typed(model, field, expected):
    """Resolvable widens fields to `T | str` in the DERIVED model, but the real
    class keeps its types — that's what coerces a rendered template back."""
    annotation = model.__annotations__[field]
    assert expected.__name__ in str(annotation)


# --------------------------------------------------------------------------
# Geometry sources: wkt OR lat/lng
# --------------------------------------------------------------------------
def test_geometry_is_declared_once_per_dataset():
    """Geometry belongs to the dataset, not to each output — every geospatial
    output wants the same answer, and two copies could drift."""
    assert {"wkt", "lat", "lng"} <= set(models.GeometryModel.__annotations__)
    for model in (models.RepresentationModel, models.CkanModel):
        assert not {"wkt", "lat", "lng"} & set(model.__annotations__), (
            f"{model.__name__} should read the dataset's `geometry:` block, "
            "not carry its own copy"
        )
