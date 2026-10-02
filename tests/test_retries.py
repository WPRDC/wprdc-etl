"""Retries for steps that talk to the outside world.

Network steps (landing, publishing, mirrors, metadata, PostGIS layers) carry a
production-only RetryPolicy, so a transient failure — an ArcGIS export still
building, a 5xx — retries instead of failing the run and paging Slack.
Failures a retry can't fix are raised with `allow_retries=False`, so they alert
at once instead of after minutes of backoff.

`is_production` is patched rather than ENVIRONMENT set: CLAUDE.md forbids
setting ENVIRONMENT=production in tests.
"""

import dagster as dg
import pytest

from wprdc_etl.components._common import network_retry_policy
from wprdc_etl.strategies.extract import resolve_arcgis_distribution
from wprdc_etl.strategies.load import _guard_replace


def test_no_retries_outside_production(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dev failure should surface in seconds, not after minutes of backoff."""
    monkeypatch.setattr("wprdc_etl.runtime.is_production", lambda: False)
    assert network_retry_policy() is None


def test_production_retries_with_jittered_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("wprdc_etl.runtime.is_production", lambda: True)
    policy = network_retry_policy()
    assert policy is not None
    assert policy.max_retries == 3
    assert policy.backoff == dg.Backoff.EXPONENTIAL
    assert policy.jitter == dg.Jitter.PLUS_MINUS


def _attempts(error: Exception) -> int:
    """Run one asset that always raises `error` under a 3-retry policy, and
    return how many times it ran."""
    calls = []

    @dg.asset(retry_policy=dg.RetryPolicy(max_retries=3, delay=0))
    def flaky() -> None:
        calls.append(1)
        raise error

    result = dg.materialize([flaky], raise_on_error=False)
    assert not result.success
    return len(calls)


def test_a_transient_failure_is_retried() -> None:
    assert _attempts(dg.Failure("ArcGIS is still generating this export")) == 4


def test_a_permanent_failure_is_not() -> None:
    assert _attempts(dg.Failure("no such title", allow_retries=False)) == 1


def test_a_title_gone_from_the_catalogue_is_permanent() -> None:
    with pytest.raises(dg.Failure) as exc:
        resolve_arcgis_distribution([{"title": "Other"}], "Missing", "geojson")
    assert exc.value.allow_retries is False


def test_the_replace_column_guard_is_permanent() -> None:
    """A column change is still there a minute later; it needs a person."""
    report = {
        "live_exists": True,
        "added": ["new_col"],
        "removed": [],
        "type_changes": {},
    }
    with pytest.raises(dg.Failure, match="replace aborted") as exc:
        _guard_replace(report, rebuild=False, context=None)
    assert exc.value.allow_retries is False
