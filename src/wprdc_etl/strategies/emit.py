"""Emit strategies: publish additional representations of a dataset.

The primary representation (CSV/tabular) is handled by the load strategy and
goes to CKAN's DataStore. Additional exports declared in a dataset's
`representations:` are geospatial files (GeoJSON, zipped Shapefile) built from
the same canonical validated frame and published as their own CKAN file
resources — one publish asset per export.

Geometry comes from the frame itself when it's already a GeoDataFrame, else from
the dataset's `geometry:` block (assumed EPSG:4326). That block is dataset-level
rather than per-export because every geospatial output wants the same answer —
`ensure_geo` is shared for exactly that reason. These publish as file resources
(publish_file), not DataStore, so they pair with snapshot datasets rather than
incremental.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import TYPE_CHECKING

import dagster as dg

from wprdc_etl.runtime import sink_dir

if TYPE_CHECKING:
    import geopandas as gpd
    import pandas as pd

    from wprdc_etl.components.models import GeometryModel, RepresentationModel
    from wprdc_etl.components.tabular_pipeline import TabularPipeline
    from wprdc_etl.resources import CkanResource


def ensure_geo(
    dataframe: pd.DataFrame, geometry: GeometryModel | None, what: str
) -> gpd.GeoDataFrame:
    """Geometry from the frame if it already has it, else built from the
    dataset's `geometry:` block. WKT wins when both are configured — it carries
    shapes other than points, so a dataset declaring one means it.

    `what` names the caller in the error, since a dataset can have several
    geospatial outputs and they all land here.
    """
    import geopandas as gpd

    if isinstance(dataframe, gpd.GeoDataFrame):
        return dataframe
    if geometry and geometry.wkt:
        if geometry.wkt not in dataframe.columns:
            raise dg.Failure(f"{what}: geometry.wkt column {geometry.wkt!r} not found")
        geom = gpd.GeoSeries.from_wkt(dataframe[geometry.wkt].astype("string"))
        return gpd.GeoDataFrame(
            dataframe.drop(columns=[geometry.wkt]), geometry=geom, crs="EPSG:4326"
        )
    if geometry and geometry.lat and geometry.lng:
        missing = [
            c for c in (geometry.lat, geometry.lng) if c not in dataframe.columns
        ]
        if missing:
            raise dg.Failure(f"{what}: geometry lat/lng columns {missing} not found")
        geom = gpd.points_from_xy(dataframe[geometry.lng], dataframe[geometry.lat])
        return gpd.GeoDataFrame(dataframe.copy(), geometry=geom, crs="EPSG:4326")
    raise dg.Failure(
        f"{what} needs geometry: the frame isn't geospatial and the dataset has "
        "no `geometry:` block naming a wkt column or a lat/lng pair"
    )


# Representation formats that need geometry. A future non-geospatial format
# (parquet, xlsx) is simply not in this set, so it never reaches ensure_geo.
GEO_FORMATS = {"geojson", "shapefile", "shp"}


def _write_shapefile_zip(gdf: gpd.GeoDataFrame, zip_path: str) -> None:
    """Write a shapefile bundle to a temp dir and zip it into zip_path."""
    workdir = tempfile.mkdtemp()
    zip_base = zip_path[:-4] if zip_path.lower().endswith(".zip") else zip_path
    try:
        gdf.to_file(os.path.join(workdir, "data.shp"))
        made = shutil.make_archive(zip_base, "zip", workdir)
        if made != zip_path and os.path.exists(made):
            os.replace(made, zip_path)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def publish_representation(
    cfg: TabularPipeline,
    rep: RepresentationModel,
    dataframe: pd.DataFrame,
    *,
    ckan: CkanResource,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    fmt = rep.format.lower()
    if fmt not in GEO_FORMATS:
        raise dg.Failure(
            f"unknown representation format {rep.format!r} "
            f"(implemented: {sorted(GEO_FORMATS)})"
        )
    # Dispatch first, THEN build geometry: it's a requirement of the geospatial
    # formats, not of publishing a representation. A non-geo format added here
    # gets its own branch and skips this.
    gdf = ensure_geo(dataframe, cfg.geometry, f"representation {rep.format}")
    ext = "geojson" if fmt == "geojson" else "zip"

    # Dry-run (default unless ENVIRONMENT=production): write locally, skip CKAN.
    sink = sink_dir()
    if sink:
        os.makedirs(sink, exist_ok=True)
        stem = "__".join(x for x in [cfg.publisher, cfg.department, cfg.dataset] if x)
        out = os.path.join(sink, f"{stem}.{ext}")
        if fmt == "geojson":
            gdf.to_file(out, driver="GeoJSON")
        else:
            _write_shapefile_zip(gdf, out)
        if context is not None:
            context.log.info(f"[dry-run] wrote {out} (skipped CKAN)")
        return

    fd, tmp = tempfile.mkstemp(suffix=f".{ext}")
    os.close(fd)
    try:
        from wprdc_etl.resources import frame_fingerprint

        # Fingerprinted from the frame, not the file: a shapefile's DBF header
        # carries its write date, so its bytes change every day regardless.
        fingerprint = frame_fingerprint(gdf, fmt)
        if fmt == "geojson":
            gdf.to_file(tmp, driver="GeoJSON")
            changed = ckan.publish_file(
                rep.resource_id, tmp, "data.geojson", fingerprint=fingerprint
            )
        else:
            _write_shapefile_zip(gdf, tmp)
            changed = ckan.publish_file(
                rep.resource_id, tmp, "data.zip", fingerprint=fingerprint
            )
        if context is not None:
            context.log.info(
                f"uploaded {fmt} to {rep.resource_id}"
                if changed
                else f"unchanged: {rep.resource_id} already holds this {fmt}"
            )
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
