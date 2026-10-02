"""Load a co-located Python module from a dataset's defs folder.

Convention: defs/<publisher>/<department?>/<dataset>/<name>.py
Used for per-dataset escape hatches — transform.py, schema.py, fetch.py.
Returns the module or None if the dataset doesn't provide one.

Lives at the top level (not under components/ or strategies/) so both can
import it without a circular dependency.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    from wprdc_etl.components.models import PipelineConfig


def load_dataset_module(cfg: PipelineConfig, name: str) -> ModuleType | None:
    parts = ["wprdc_etl", "defs", cfg.publisher]
    if cfg.department:
        parts.append(cfg.department)
    parts += [cfg.dataset, name]
    try:
        return importlib.import_module(".".join(parts))
    except ModuleNotFoundError:
        return None
