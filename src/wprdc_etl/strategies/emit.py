"""Emit strategies: publish additional representations of a dataset.

The primary representation (CSV/tabular) is handled by the load strategy and
goes to CKAN's DataStore. Additional representations declared in a dataset's
`representations:` are geospatial files (GeoJSON, zipped Shapefile) built from
the same canonical validated frame and published as their own CKAN file
resources — one publish asset per representation.

Geometry: if the canonical frame is already a GeoDataFrame it's used directly;
otherwise a representation names lat/lng columns and point geometry is built
from them (assumed EPSG:4326). These publish as file resources (publish_file),
not DataStore, so they pair with snapshot datasets rather than incremental.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import dagster as dg

from wprdc_etl.runtime import sink_dir


def _ensure_geo(dataframe, rep):
    import geopandas as gpd

    if isinstance(dataframe, gpd.GeoDataFrame):
        return dataframe
    if rep.lat and rep.lng:
        missing = [c for c in (rep.lat, rep.lng) if c not in dataframe.columns]
        if missing:
            raise dg.Failure(
                f"representation {rep.format}: lat/lng columns {missing} not found"
            )
        geom = gpd.points_from_xy(dataframe[rep.lng], dataframe[rep.lat])
        return gpd.GeoDataFrame(dataframe.copy(), geometry=geom, crs="EPSG:4326")
    raise dg.Failure(
        f"representation {rep.format} needs geometry: the frame isn't geospatial "
        "and no lat/lng columns were configured"
    )


def _write_shapefile_zip(gdf, zip_path):
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


def publish_representation(cfg, rep, dataframe, *, ckan, context=None) -> None:
    gdf = _ensure_geo(dataframe, rep)
    fmt = rep.format.lower()
    if fmt not in ("geojson", "shapefile", "shp"):
        raise dg.Failure(f"unknown representation format {rep.format!r}")
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
        if fmt == "geojson":
            gdf.to_file(tmp, driver="GeoJSON")
            ckan.publish_file(rep.resource_id, tmp, "data.geojson")
        else:
            _write_shapefile_zip(gdf, tmp)
            ckan.publish_file(rep.resource_id, tmp, "data.zip")
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
