"""Load-time invariants and asset-graph shape for the ingest/publish split.

build_defs never touches its ComponentLoadContext, so these construct a
TabularPipeline directly and pass None — no S3, CKAN, or dg machinery involved.
"""

import dagster as dg
import pandas as pd
import pytest

from wprdc_etl.components.models import CkanModel, SourceModel
from wprdc_etl.components.tabular_pipeline import TabularPipeline, _guard_state_loss


def pipeline(**kw) -> TabularPipeline:
    ckan = kw.pop("ckan", CkanModel(resource_id="r-1"))
    return TabularPipeline(
        publisher="p",
        dataset="d",
        source=SourceModel(type="http", url="https://example.invalid/d.csv"),
        ckan=ckan,
        **kw,
    )


def asset_keys(defs: dg.Definitions) -> set[str]:
    return {
        k.to_user_string() for a in defs.assets or [] for k in a.keys  # type: ignore[union-attr]
    }


def leaf_names(defs: dg.Definitions) -> set[str]:
    return {k.rsplit("/", 1)[-1] for k in asset_keys(defs)}


# --------------------------------------------------------------------------
# Graph shape
# --------------------------------------------------------------------------
def test_snapshot_has_no_accumulated_asset():
    defs = pipeline().build_defs(None)
    assert leaf_names(defs) == {"landed", "validated", "loaded"}


def test_incremental_replace_inserts_accumulated():
    defs = pipeline(
        ingest="incremental",
        publish="replace",
        ckan=CkanModel(resource_id="r-1", primary_key=["id"]),
    ).build_defs(None)
    assert "accumulated" in leaf_names(defs)


def test_loaded_reads_accumulated_when_accumulating():
    """The whole point: publish the cumulative table, not this run's delta."""
    defs = pipeline(
        ingest="incremental",
        publish="replace",
        ckan=CkanModel(resource_id="r-1", primary_key=["id"]),
    ).build_defs(None)
    loaded = next(
        a
        for a in defs.assets  # type: ignore[union-attr]
        if any(k.to_user_string().endswith("/loaded") for k in a.keys)
    )
    upstream = {k.to_user_string() for k in loaded.keys_by_input_name.values()}
    assert upstream == {"p/d/accumulated"}


def test_loaded_reads_validated_without_accumulation():
    defs = pipeline().build_defs(None)
    loaded = next(
        a
        for a in defs.assets  # type: ignore[union-attr]
        if any(k.to_user_string().endswith("/loaded") for k in a.keys)
    )
    upstream = {k.to_user_string() for k in loaded.keys_by_input_name.values()}
    assert upstream == {"p/d/validated"}


# --------------------------------------------------------------------------
# State-loss guard
# --------------------------------------------------------------------------
def test_state_loss_guard_passes_on_first_run():
    """No sidecar yet — nothing to compare against."""
    _guard_state_loss(None, None, "p__d")


def test_state_loss_guard_passes_when_table_grew():
    _guard_state_loss(pd.DataFrame({"id": [1, 2, 3]}), {"rows": 2}, "p__d")


def test_state_loss_guard_passes_when_table_is_unchanged():
    _guard_state_loss(pd.DataFrame({"id": [1, 2]}), {"rows": 2}, "p__d")


def test_state_loss_guard_blocks_a_vanished_table():
    """The dangerous one: prior=None would make the delta look like everything."""
    with pytest.raises(dg.Failure, match="0 rows but 500 were last written"):
        _guard_state_loss(None, {"rows": 500}, "p__d")


def test_state_loss_guard_blocks_a_truncated_table():
    with pytest.raises(dg.Failure, match="Refusing to continue"):
        _guard_state_loss(pd.DataFrame({"id": [1]}), {"rows": 500}, "p__d")


def test_state_loss_guard_tolerates_a_malformed_sidecar():
    _guard_state_loss(None, {"updated_at": "whenever"}, "p__d")


# --------------------------------------------------------------------------
# Invariants
# --------------------------------------------------------------------------
def test_upsert_requires_primary_key():
    with pytest.raises(ValueError, match="requires ckan.primary_key"):
        pipeline(ingest="incremental").build_defs(None)


def test_accumulate_requires_primary_key():
    with pytest.raises(ValueError, match="accumulate requires ckan.primary_key"):
        pipeline(ingest="snapshot", accumulate=True).build_defs(None)


def test_accumulate_with_upsert_is_rejected():
    """Would accumulate twice — the DataStore upsert already keeps prior rows."""
    with pytest.raises(ValueError, match="accumulate twice"):
        pipeline(
            publish="upsert",
            accumulate=True,
            ckan=CkanModel(resource_id="r-1", primary_key=["id"]),
        ).build_defs(None)


def test_unknown_publish_mode_is_rejected():
    with pytest.raises(ValueError, match="unknown publish mode"):
        pipeline(publish="sideways").build_defs(None)


def test_snapshot_without_primary_key_still_builds():
    """The common case must not have acquired a new requirement."""
    assert pipeline().build_defs(None) is not None


def test_incremental_forces_unpartitioned():
    defs = pipeline(
        ingest="incremental",
        partition="daily",
        ckan=CkanModel(resource_id="r-1", primary_key=["id"]),
    ).build_defs(None)
    for a in defs.assets:  # type: ignore[union-attr]
        assert a.partitions_def is None
