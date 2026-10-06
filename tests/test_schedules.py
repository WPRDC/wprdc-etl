"""Schedules: when datasets run, and that a scheduled run gets a partition.

Two things went wrong before. Every partitioned job had a plain
ScheduleDefinition, which requests NO partition — so every scheduled run would
have failed in production (bin/run hid it by choosing the partition itself).
And ~100 GIS layers all fired Monday at 07:00 / 07:30, in business hours.
Weekly datasets now run on Sunday 03:00-08:59 and monthly ones on the 1st
03:00-07:59 Eastern, on a 3-minute grid (scripts/schedule_slots.py), through
build_schedule_from_partitioned_job.
"""

import datetime as dt
import pathlib
import sys

import dagster as dg
import pytest
import yaml

from wprdc_etl.components._common import partitioned_schedule_fields

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import schedule_slots as slots  # noqa: E402

SCHEDULED = slots.scheduled()  # {key: (cadence, cron)}


def _time(cron: str) -> tuple[int, int]:
    minute, hour = cron.split()[:2]
    return int(hour), int(minute)


def test_every_schedule_fits_its_partitioning() -> None:
    bad = []
    for key, (cadence, cron) in SCHEDULED.items():
        try:
            partitioned_schedule_fields(cadence, cron)
        except ValueError as e:
            bad.append(f"{key}: {e}")
    assert not bad, bad


@pytest.mark.parametrize("cadence", ["weekly", "monthly"])
def test_runs_are_spread_outside_business_hours(cadence: str) -> None:
    crons = [cron for c, cron in SCHEDULED.values() if c == cadence]
    assert crons, f"no {cadence} datasets found"
    day_field = 4 if cadence == "weekly" else 2
    day = "0" if cadence == "weekly" else "1"  # Sunday / the 1st
    assert {c.split()[day_field] for c in crons} == {day}
    first, last = slots.WINDOWS[cadence]
    times = [_time(c) for c in crons]
    assert all(first <= h <= last for h, _ in times), "a run is outside its window"
    assert all(m % slots.GRID_MINUTES == 0 for _, m in times)
    assert len(set(times)) == len(times), "two datasets share a minute"


def test_regenerating_a_dataset_keeps_its_slot() -> None:
    key, (cadence, cron) = next(
        (k, v) for k, v in SCHEDULED.items() if v[0] == "weekly"
    )
    publisher, *middle, dataset = key.split("/")
    department = middle[0] if middle else None
    assert slots.schedule_for(publisher, department, dataset, cadence) == cron


def test_a_new_dataset_gets_a_free_slot() -> None:
    cron = slots.schedule_for("city_of_pittsburgh", "gis", "_not_a_dataset", "weekly")
    taken = {c for cad, c in SCHEDULED.values() if cad == "weekly"}
    assert cron not in taken
    partitioned_schedule_fields("weekly", cron)  # and it fits


@pytest.mark.parametrize(
    "cadence,cron",
    [("weekly", "0 7 1 * *"), ("monthly", "0 7 * * 0"), ("weekly", "*/5 7 * * 0")],
)
def test_a_cron_that_does_not_fit_the_cadence_is_rejected(
    cadence: str, cron: str
) -> None:
    with pytest.raises(ValueError):
        partitioned_schedule_fields(cadence, cron)


@pytest.mark.parametrize(
    "schedule",
    [
        "city_of_pittsburgh__water_features__schedule",
        "allegheny_county__gis__parcels__schedule",
    ],
)
def test_a_scheduled_tick_requests_a_partition(schedule: str) -> None:
    """The bug this file exists for: a tick with no partition key."""
    import wprdc_etl.definitions as d

    sched = d.defs.get_schedule_def(schedule)
    assert sched.execution_timezone == "America/New_York"
    with dg.instance_for_test() as instance:
        context = dg.build_schedule_context(
            instance=instance,
            scheduled_execution_time=dt.datetime(
                2026, 11, 8, 15, tzinfo=dt.timezone.utc
            ),
        )
        result = sched.evaluate_tick(context)
    keys = [r.partition_key for r in result.run_requests or []]
    assert keys and all(keys), f"{schedule} requested {keys}"


@pytest.mark.parametrize("production", [True, False])
def test_schedules_start_running_only_in_production(
    monkeypatch: pytest.MonkeyPatch, production: bool
) -> None:
    """Dagster creates schedules stopped; production would run nothing."""
    from types import SimpleNamespace

    from wprdc_etl.components._common import partitions_for, schedule_or_sensor

    monkeypatch.setattr("wprdc_etl.runtime.is_production", lambda: production)
    want = (
        dg.DefaultScheduleStatus.RUNNING
        if production
        else dg.DefaultScheduleStatus.STOPPED
    )

    @dg.asset(partitions_def=partitions_for("weekly"))
    def weekly() -> None: ...

    job = dg.define_asset_job("weekly_job", selection=[weekly])
    cfg = SimpleNamespace(schedule="0 3 * * 0", partition="weekly")
    (partitioned,), _ = schedule_or_sensor(cfg, "x", job, partitions_for("weekly"))
    cfg = SimpleNamespace(schedule="0 3 * * 0", partition="none")
    (plain,), _ = schedule_or_sensor(cfg, "y", job, None)
    assert partitioned.default_status == want
    assert plain.default_status == want


def test_paused_holds_production_schedules_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first-boot brake: production, but nothing starts until switched on."""
    from wprdc_etl.components._common import default_schedule_status

    monkeypatch.setattr("wprdc_etl.runtime.is_production", lambda: True)
    monkeypatch.setenv("WPRDC_SCHEDULES_PAUSED", "1")
    assert default_schedule_status() == dg.DefaultScheduleStatus.STOPPED
    monkeypatch.setenv("WPRDC_SCHEDULES_PAUSED", "")
    assert default_schedule_status() == dg.DefaultScheduleStatus.RUNNING
