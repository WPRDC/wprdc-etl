"""Schedules: when datasets run, and that a scheduled run gets a partition.

Two things went wrong before. Every partitioned job had a plain
ScheduleDefinition, which requests NO partition — so every scheduled run would
have failed in production (bin/run hid it by choosing the partition itself).
And ~100 GIS layers all fired Monday at 07:00 / 07:30. Weekly datasets now run
on Sunday, monthly on the 1st, at odd minutes between 03:00 and 08:59 Eastern
(scripts/schedule_slots.py), through build_schedule_from_partitioned_job.
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
def test_runs_are_spread_over_odd_minutes_in_the_window(cadence: str) -> None:
    crons = [cron for c, cron in SCHEDULED.values() if c == cadence]
    assert crons, f"no {cadence} datasets found"
    day_field = 4 if cadence == "weekly" else 2
    day = "0" if cadence == "weekly" else "1"  # Sunday / the 1st
    assert {c.split()[day_field] for c in crons} == {day}
    times = [_time(c) for c in crons]
    assert all(slots.FIRST_HOUR <= h <= slots.LAST_HOUR for h, _ in times)
    assert all(m % 5 for _, m in times), "a run landed on a :x0/:x5 mark"
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
