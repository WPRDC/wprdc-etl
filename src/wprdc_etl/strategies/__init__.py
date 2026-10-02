"""Pluggable pipeline strategies.

The TabularPipeline component owns the pipeline *shape* (extract -> land ->
transform/validate -> load). This package owns the swappable *how* of each
stage, selected by config:

    extract.py     — how raw data is acquired (source.type)
    accumulate.py  — how a run's frame folds into the cumulative table
    load.py        — how data reaches CKAN (selected by `publish`)

Transform and Emit strategies land here too in later steps.
"""

from wprdc_etl.strategies.extract import get_extractor
from wprdc_etl.strategies.load import get_loader

__all__ = ["get_extractor", "get_loader"]
