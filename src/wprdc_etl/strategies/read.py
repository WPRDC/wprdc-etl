"""Read strategies: parse a landed object into a dataframe, dispatched by the
file's format (derived from the manifest's recorded filename).

CSV and JSON are implemented. Geospatial readers are registered as stubs — they
return a GeoDataFrame, which changes the type flowing through transform/validate,
so they're filled in with the geo work rather than here.

Readers take a LOCAL FILE PATH. The landed object is downloaded to a temp file
first, because that's the interface geospatial formats need (shapefiles are
multi-file bundles; geopandas wants a real path, not a buffer). For CSV this
costs one local disk write, which is cheap and keeps the interface uniform.
"""

from __future__ import annotations

import os
import tempfile


def _read_csv(path):
    import pandas as pd

    return pd.read_csv(path)


def _read_json(path):
    import pandas as pd

    # Refine orient / lines when the first JSON source appears.
    return pd.read_json(path)


def _read_geo(path):
    """Read a geospatial file into a GeoDataFrame.

    Handles GeoJSON and zipped shapefiles. A shapefile is a multi-file bundle
    (.shp/.shx/.dbf/.prj), so sources ship it as a .zip and geopandas reads it
    in place via the zip:// virtual filesystem.
    """
    import geopandas as gpd

    if path.lower().endswith(".zip"):
        return gpd.read_file(f"zip://{path}")
    return gpd.read_file(path)


READERS = {
    "csv": _read_csv,
    "json": _read_json,
    "geojson": _read_geo,
    "shp": _read_geo,
    "zip": _read_geo,  # assumed zipped shapefile bundle
}


def get_reader(fmt: str):
    try:
        return READERS[fmt]
    except KeyError:
        raise NotImplementedError(f"no reader for format {fmt!r}")


def read_landed(landing, cfg, partition):
    """Download the landed object for (cfg, partition) to a temp file and parse
    it with the format-appropriate reader. Returns a (Geo)DataFrame.

    The format comes from the manifest's recorded filename, so this no longer
    assumes data.csv — a landed data.geojson dispatches to the geo reader.
    """
    manifest = (
        landing.read_manifest(cfg.publisher, cfg.dataset, partition, cfg.department)
        or {}
    )
    filename = manifest.get("filename", "data.csv")
    ext = os.path.splitext(filename)[1]
    fmt = ext.lstrip(".").lower() or "csv"
    prefix = landing.prefix(cfg.publisher, cfg.dataset, partition, cfg.department)

    fd, tmp = tempfile.mkstemp(suffix=ext)
    os.close(fd)
    try:
        landing.download(f"{prefix}/{filename}", tmp)
        return get_reader(fmt)(tmp)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
