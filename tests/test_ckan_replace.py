"""CkanResource publishing: who creates the DataStore table, and when nothing
needs publishing at all.

Left to DataPusher+, a new resource's table gets qsv's inferred types (ids and
codes numeric) while the frame publishes them as text, so the second run is
blocked by the compatibility guard. replace() therefore creates the table from
the frame's dtypes on a first load and after a rebuild.

And a run whose output CKAN already holds writes nothing: the fingerprint of
the last upload is kept on the resource itself (`etl_sha256`), so CKAN is the
record of what it has. These pin the CKAN calls; nothing talks to a portal.
"""

import hashlib
from typing import Any

import pandas as pd
import pytest

from wprdc_etl.resources import FINGERPRINT_FIELD, CkanResource

RID = "513290a6-2bac-4e41-8029-354cbda6a7b7"


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    log: list[tuple[str, dict[str, Any]]] = []

    def fake_action(self: CkanResource, name: str, **kwargs: Any) -> dict:
        log.append((name, kwargs.get("json") or kwargs.get("data") or {}))
        return {}

    monkeypatch.setattr(CkanResource, "_action", fake_action)
    monkeypatch.setenv("WPRDC_CKAN_WRITE", "1")
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("WPRDC_FORCE_PUBLISH", raising=False)
    return log


def _ckan(
    monkeypatch: pytest.MonkeyPatch,
    live: dict[str, str],
    rows: int = 0,
    resource: dict[str, Any] | None = None,
    package: dict[str, Any] | None = None,
) -> CkanResource:
    monkeypatch.setattr(
        CkanResource, "_datastore_state", lambda self, rid: (live, rows)
    )
    monkeypatch.setattr(CkanResource, "resource", lambda self, rid: resource or {})
    monkeypatch.setattr(CkanResource, "package", lambda self, pid: package or {})
    return CkanResource(base_url="http://localhost:5001", api_key="t")


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": pd.array(["1816791851"], dtype="string"),
            "ward": pd.array(["19"], dtype="string"),
            "inactive": [False],
            "latitude": [40.41],
        }
    )


def _sha(frame: pd.DataFrame) -> str:
    return hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()


def _names(calls: list[tuple[str, dict[str, Any]]]) -> list[str]:
    return [name for name, _ in calls]


# -- who creates the table ---------------------------------------------------
def test_first_load_creates_the_table_from_the_frame_before_uploading(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    assert _ckan(monkeypatch, live={}).replace(RID, _frame()) is True
    assert _names(calls) == ["datastore_create", "resource_patch", "datapusher_submit"]
    assert calls[0][1]["fields"] == [
        {"id": "id", "type": "text"},
        {"id": "ward", "type": "text"},
        {"id": "inactive", "type": "bool"},
        {"id": "latitude", "type": "numeric"},
    ]
    assert calls[1][1][FINGERPRINT_FIELD] == _sha(_frame())  # recorded with it


def test_an_existing_table_is_truncated_not_recreated(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    _ckan(monkeypatch, live={"id": "text"}, rows=7).replace(RID, _frame())
    assert _names(calls) == ["resource_patch", "datastore_delete", "datapusher_submit"]
    assert calls[1][1]["filters"] == {}  # rows only — the table stays


def test_a_rebuild_drops_then_recreates_with_the_frames_types(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    _ckan(monkeypatch, live={"id": "numeric"}).replace(RID, _frame(), rebuild=True)
    assert _names(calls) == [
        "resource_patch",
        "datastore_delete",
        "datastore_create",
        "datapusher_submit",
    ]
    assert "filters" not in calls[1][1]  # a real drop
    assert {"id": "id", "type": "text"} in calls[2][1]["fields"]


# -- nothing to publish ------------------------------------------------------
def test_replace_skips_when_ckan_already_holds_this_table(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    ckan = _ckan(
        monkeypatch,
        live={"id": "text"},
        rows=1,
        resource={FINGERPRINT_FIELD: _sha(_frame())},
    )
    assert ckan.replace(RID, _frame()) is False
    assert calls == []  # no upload, no truncate, no DataPusher+ reload


def test_replace_republishes_when_the_datastore_is_short_of_rows(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    """A matching fingerprint with a stale table means the last DataPusher+
    push failed after the upload — skipping would keep it stale for good."""
    ckan = _ckan(
        monkeypatch,
        live={"id": "text"},
        rows=0,
        resource={FINGERPRINT_FIELD: _sha(_frame())},
    )
    assert ckan.replace(RID, _frame()) is True
    assert "datapusher_submit" in _names(calls)


@pytest.mark.parametrize("reason", ["rebuild", "force"])
def test_replace_publishes_unchanged_content_when_told_to(
    monkeypatch: pytest.MonkeyPatch, calls: list, reason: str
) -> None:
    if reason == "force":
        monkeypatch.setenv("WPRDC_FORCE_PUBLISH", "1")
    ckan = _ckan(
        monkeypatch,
        live={"id": "text"},
        rows=1,
        resource={FINGERPRINT_FIELD: _sha(_frame())},
    )
    assert ckan.replace(RID, _frame(), rebuild=reason == "rebuild") is True


def test_publish_file_skips_a_matching_fingerprint(
    monkeypatch: pytest.MonkeyPatch, calls: list, tmp_path
) -> None:
    f = tmp_path / "data.geojson"
    f.write_text("{}")
    sha = hashlib.sha256(b"{}").hexdigest()
    ckan = _ckan(monkeypatch, live={}, resource={FINGERPRINT_FIELD: sha})
    assert ckan.publish_file(RID, str(f), "data.geojson") is False
    assert ckan.publish_file(RID, str(f), "data.geojson", fingerprint="other") is True
    assert calls[0][1][FINGERPRINT_FIELD] == "other"


def test_publish_link_skips_a_resource_already_pointing_there(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    ckan = _ckan(monkeypatch, live={}, resource={"url": "https://x/hub"})
    assert ckan.publish_link(RID, "https://x/hub") is False
    assert ckan.publish_link(RID, "https://x/other") is True
    assert _names(calls) == ["resource_patch"]


def test_patch_package_skips_identical_notes_and_tags(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    live = {"notes": "About.", "tags": [{"name": "b"}, {"name": "a"}]}
    ckan = _ckan(monkeypatch, live={}, package=live)
    assert ckan.patch_package("pkg", notes="About.", tags=["a", "b"]) is False
    assert ckan.patch_package("pkg", notes="About.", tags=None) is False
    assert ckan.patch_package("pkg", notes="Changed.", tags=["a", "b"]) is True
    assert _names(calls) == ["package_patch"]
