"""Pluggable pipeline strategies.

The TabularPipeline component owns the pipeline *shape* (extract -> land ->
transform/validate -> load). This package owns the swappable *how* of each
stage, selected by config:

    extract.py  — how raw data is acquired (source.type)
    load.py     — how transformed data reaches CKAN (derived from `ingest`)

Transform and Emit strategies land here too in later steps.
"""

from wprdc_etl.strategies.extract import get_extractor
from wprdc_etl.strategies.load import get_loader

__all__ = ["get_extractor", "get_loader"]
