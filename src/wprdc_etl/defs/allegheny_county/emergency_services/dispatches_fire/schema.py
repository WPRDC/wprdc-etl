"""Pandera schema STUB for allegheny_county / public_safety / dispatches_fire.

911 Fire Dispatches. Source file 911-fire-dispatches.csv. Same column layout
as the EMS feed -- a single legacy `CallSchema` served both -- and a separate
CKAN resource in the same package.

The loader resolves exactly one `schema` module per dataset directory
(`dataset_modules.py`), so a resource that shares another dataset's column
contract gets a re-export rather than a copy. Edit `dispatches_ems/schema.py`;
keep this a re-export unless the two sources actually diverge.
"""

from __future__ import annotations

from ..dispatches_ems.schema import SCHEMA

__all__ = ["SCHEMA"]
