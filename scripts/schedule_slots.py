"""When each scheduled dataset runs: a stable, odd-minute slot per dataset.

Weekly datasets run on SUNDAY, monthly ones on the 1st, all between 03:00 and
08:59 Eastern on a minute that isn't a multiple of 5 — so nothing lands on the
:00/:15/:30 marks everyone else's cron uses, and ~100 ArcGIS layers don't
all ask the Hub for exports in the same minute.

  * Sunday because a weekly partition runs Sunday-to-Saturday and a schedule
    runs the latest COMPLETE one: a Sunday run lands the week that just ended.
  * From 03:00 because US clocks change at 02:00 on a Sunday; a slot in that
    hour can be skipped or run twice.

The first layout spaces a cadence's datasets evenly (spread). A dataset added
later gets a hash of its key, moved to the next free slot if taken, so it
never reshuffles the others. An existing defs.yaml keeps
whatever schedule it already has — regenerating a dataset doesn't move it.
"""

from __future__ import annotations

import hashlib
import pathlib

import yaml

DEFS = pathlib.Path(__file__).resolve().parent.parent / "src" / "wprdc_etl" / "defs"

FIRST_HOUR, LAST_HOUR = 3, 8  # 03:00 through 08:59
SLOTS = [
    (hour, minute)
    for hour in range(FIRST_HOUR, LAST_HOUR + 1)
    for minute in range(60)
    if minute % 5
]
DAY_NAMES = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]


def cron_for(cadence: str, slot: tuple[int, int]) -> str:
    """The cron for `slot` at this cadence: weekly on Sunday, monthly on the 1st."""
    hour, minute = slot
    if cadence == "weekly":
        return f"{minute} {hour} * * 0"
    if cadence == "monthly":
        return f"{minute} {hour} 1 * *"
    raise ValueError(f"no slot scheme for {cadence!r} partitions")


def describe(cron: str) -> str:
    """Human form of a slot cron, for the comment beside it."""
    minute, hour, dom, _, dow = cron.split()
    when = f"{int(hour):02d}:{int(minute):02d} ET"
    if dow != "*":
        return f"{DAY_NAMES[int(dow)]} {when}"
    return f"day {dom} of the month, {when}"


def _key(publisher: str, department: str | None, dataset: str) -> str:
    return "/".join(x for x in (publisher, department, dataset) if x)


def _slot(cron: str) -> tuple[int, int] | None:
    parts = cron.split()
    return (int(parts[1]), int(parts[0])) if len(parts) == 5 else None


def scheduled() -> dict[str, tuple[str, str]]:
    """{dataset key: (cadence, cron)} for every defs.yaml with a schedule."""
    out = {}
    for path in DEFS.rglob("defs.yaml"):
        attrs = (yaml.safe_load(path.read_text()) or {}).get("attributes") or {}
        if attrs.get("schedule"):
            key = _key(attrs["publisher"], attrs.get("department"), attrs["dataset"])
            out[key] = (attrs.get("partition", "daily"), attrs["schedule"])
    return out


def spread(keys: list[str]) -> dict[str, tuple[int, int]]:
    """Evenly spaced slots for `keys`, in key order.

    For laying out a whole cadence at once (the first assignment). Hashing
    clusters — 102 hashed slots came out 1 to 12 minutes apart — where even
    spacing puts them ~3.5 minutes apart. Later additions go through
    assign(), which fills a free slot without moving anything.
    """
    keys = sorted(keys)
    if len(keys) > len(SLOTS):
        raise RuntimeError("more datasets than schedule slots")
    step = len(SLOTS) / max(1, len(keys))
    return {k: SLOTS[int(i * step)] for i, k in enumerate(keys)}


def assign(keys: list[str], taken: set[tuple[int, int]]) -> dict[str, tuple[int, int]]:
    """A free slot for each key: its hash slot, else the next free one."""
    taken = set(taken)
    out = {}
    for key in sorted(keys):
        i = int(hashlib.sha256(key.encode()).hexdigest(), 16) % len(SLOTS)
        while SLOTS[i] in taken:
            i = (i + 1) % len(SLOTS)
            if len(taken) >= len(SLOTS):
                raise RuntimeError("no free schedule slots left")
        taken.add(SLOTS[i])
        out[key] = SLOTS[i]
    return out


def schedule_for(
    publisher: str, department: str | None, dataset: str, cadence: str
) -> str:
    """The cron a generator should write for this dataset.

    Its existing schedule if it already has a defs.yaml with one in this
    scheme's shape; otherwise a new slot, clear of every other dataset at the
    same cadence.
    """
    key = _key(publisher, department, dataset)
    existing = scheduled()
    if key in existing and existing[key][1] in _scheme_crons(cadence):
        return existing[key][1]
    taken = {
        _slot(cron)
        for k, (c, cron) in existing.items()
        if c == cadence and k != key and cron in _scheme_crons(cadence)
    }
    return cron_for(cadence, assign([key], taken)[key])


def _scheme_crons(cadence: str) -> set[str]:
    return {cron_for(cadence, s) for s in SLOTS}
