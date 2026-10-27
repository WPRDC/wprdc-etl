"""The `wprdc/rebuild` run tag: a one-run `ckan.rebuild` with no commit."""

from types import SimpleNamespace
from typing import Any

import pytest

from wprdc_etl.strategies.load import REBUILD_TAG, wants_rebuild


def _cfg(rebuild: bool = False) -> Any:
    return SimpleNamespace(ckan=SimpleNamespace(rebuild=rebuild))


def _context(tags: dict[str, str]) -> Any:
    logged: list[str] = []
    return SimpleNamespace(
        run=SimpleNamespace(tags=tags),
        log=SimpleNamespace(warning=logged.append),
        logged=logged,
    )


@pytest.mark.parametrize("value", ["true", "True", "1", "yes", " true "])
def test_the_tag_rebuilds_that_run(value: str) -> None:
    context = _context({REBUILD_TAG: value})
    assert wants_rebuild(_cfg(), context) is True
    assert context.logged  # says why it is dropping the table


@pytest.mark.parametrize("tags", [{}, {REBUILD_TAG: "false"}, {REBUILD_TAG: ""}])
def test_no_tag_no_rebuild(tags: dict[str, str]) -> None:
    assert wants_rebuild(_cfg(), _context(tags)) is False


def test_the_yaml_flag_still_rebuilds() -> None:
    assert wants_rebuild(_cfg(rebuild=True), None) is True
    assert wants_rebuild(_cfg(rebuild=True), _context({})) is True


def test_no_context_no_tag() -> None:
    assert wants_rebuild(_cfg(), None) is False
