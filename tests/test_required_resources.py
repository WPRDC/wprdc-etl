"""Each asset requests only the resources its dataset uses, so the UI's
"required resources" is accurate and a missing resource fails at load."""

import dagster as dg

from wprdc_etl import definitions as d
from wprdc_etl.strategies.transform import steps_need


def _required() -> dict[str, set[str]]:
    return {
        key.to_user_string(): set(a.required_resource_keys) - {"io_manager"}
        for a in d.defs.assets
        if isinstance(a, dg.AssetsDefinition)
        for key in a.keys
    }


def test_landed_requests_sftp_only_for_sftp_sources() -> None:
    req = _required()
    landed = {k: v for k, v in req.items() if k.endswith("/landed")}
    assert landed
    for key, keys in landed.items():
        assert keys in ({"landing"}, {"landing", "sftp"}), key
    assert any("sftp" in v for v in landed.values())
    assert req["allegheny_county/gis/basins/landed"] == {"landing"}


def test_validated_requests_spatial_only_where_a_step_needs_it() -> None:
    req = _required()
    assert req["allegheny_county/gis/basins/validated"] == {"landing"}
    assert req["city_of_pittsburgh/water_features/validated"] == {
        "landing",
        "spatial",
    }
    assert req["allegheny_county/gis/addressing_landmarks/validated"] == {
        "landing",
        "spatial",
    }


def test_steps_need() -> None:
    assert steps_need(None) == set()
    assert steps_need([{"op": "reverse_geocode"}]) == {"spatial", "context"}


def _asset(key: str) -> dg.AssetsDefinition:
    for a in d.defs.assets:
        if isinstance(a, dg.AssetsDefinition) and dg.AssetKey.from_user_string(
            key
        ) in set(a.keys):
            return a
    raise KeyError(key)


def _run(asset: dg.AssetsDefinition, resources: dict) -> None:
    pdef = asset.partitions_def
    key = pdef.get_last_partition_key() if pdef else None
    result = dg.materialize(
        [asset],
        resources={**resources, "io_manager": dg.mem_io_manager},
        partition_key=key,
    )
    assert result.success


def test_landed_hands_the_extractor_only_its_resources(monkeypatch) -> None:
    from wprdc_etl.strategies import extract

    seen: dict[str, object] = {}

    def fake_extract(self, context, cfg, *, landing, sftp=None):
        seen[cfg.source.type] = (landing, sftp)
        return {}

    monkeypatch.setattr(extract.Extractor, "extract", fake_extract)
    monkeypatch.setattr(extract.SftpFileExtractor, "extract", fake_extract)
    monkeypatch.setattr(extract.ArcGisExtractor, "extract", fake_extract)

    landing, sftp = object(), object()
    _run(_asset("allegheny_county/gis/basins/landed"), {"landing": landing})
    assert seen["arcgis"] == (landing, None)

    sftp_landed = next(
        a
        for a in d.defs.assets
        if isinstance(a, dg.AssetsDefinition) and "sftp" in a.required_resource_keys
    )
    _run(sftp_landed, {"landing": landing, "sftp": sftp})
    assert seen["sftp"] == (landing, sftp)


def test_validated_passes_spatial_to_the_steps_that_need_it(monkeypatch) -> None:
    import pandas as pd

    from wprdc_etl.components import tabular_pipeline as tp

    seen: dict[str, dict] = {}

    def fake_transform(df, cfg, *, deps=None):
        seen[cfg.dataset] = dict(deps or {})
        return df

    monkeypatch.setattr(tp, "read_landed", lambda landing, cfg, part: pd.DataFrame())
    monkeypatch.setattr(tp, "_transform", fake_transform)

    spatial = object()
    _run(_asset("allegheny_county/gis/basins/validated"), {"landing": object()})
    assert set(seen["basins"]) == {"context"}

    _run(
        _asset("city_of_pittsburgh/water_features/validated"),
        {"landing": object(), "spatial": spatial},
    )
    assert seen["water_features"]["spatial"] is spatial
