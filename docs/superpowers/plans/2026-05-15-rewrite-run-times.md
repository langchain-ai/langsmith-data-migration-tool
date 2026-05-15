# Rewrite Run Times To Satisfy 24h Platform Window — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Always rewrite the timestamps of migrated experiment runs (and their parent experiment session) so the newest one lands at "now" on the destination, while preserving the exact time offsets between every run. This is a required workaround — not a user-facing option — for the destination's `POST /runs/batch` 24-hour timestamp restriction.

**Architecture:** A pure-function utility module (`utils/time_shift.py`) parses/formats the two timestamp encodings used by LangSmith (ISO-8601 for `start_time`/`end_time`/`events[].time`, and the compact `YYYYMMDDTHHMMSSffffffZ` form embedded in `dotted_order`), computes one `timedelta` per experiment using `now - max(end_time, start_time)`, and applies it uniformly to every timestamp-bearing field on the experiment session and every run beneath it. The delta is persisted per-experiment in the state manager (`time_shift_seconds`) so resumed attempts replay later batches with the same shift their already-created sibling runs got — preserving the relative-offset invariant across resume boundaries.

**Tech Stack:** Python 3.x, `datetime` (stdlib), pytest + respx for tests, existing dataclass-based state manager, existing `ExperimentMigrator` / `MigrationOrchestrator`.

---

## Background

### Why this change is required (not optional)

`POST /runs/batch` rejects runs whose timestamps fall outside a 24-hour window of "now". This is binding when migrating experiment data with datasets:

- The dataset → experiment flow lives in `langsmith_migrator/core/migrators/orchestrator.py:246` (`_migrate_experiments_for_datasets`).
- For each experiment it calls `ExperimentMigrator.migrate_runs_streaming` (`langsmith_migrator/core/migrators/experiment.py:325`), which streams runs via `POST /runs/query` and replays them via `POST /runs/batch` (`langsmith_migrator/core/migrators/experiment.py:602` `_create_runs_batch`).
- Historical experiments routinely have run timestamps months or years old. Without rewriting, every `POST /runs/batch` call fails outright and the experiment ends up with zero migrated runs.

Therefore this behavior is **always on whenever an experiment's runs are being migrated**. There is no CLI flag and no config knob. The change is internal to the migration path.

### What gets shifted

Each run carries timestamps in four places:

- `start_time` — ISO-8601 (e.g. `"2026-02-03T00:35:19.695988+00:00"`, or with `Z`).
- `end_time` — same format; may be absent for in-flight runs.
- `dotted_order` — a `.`-separated chain like `20260203T003519695988Z{uuid}.20260203T003519695988Z{uuid}` where each segment encodes that ancestor run's `start_time` in `YYYYMMDDTHHMMSSffffffZ` (microsecond precision, no separators). The current code already regenerates the UUID portion (`langsmith_migrator/core/migrators/experiment.py:23-73`); we add timestamp shifting on top.
- `events[].time` — optional ISO-8601 timestamp on each event entry.

The parent experiment session carries `start_time` and `end_time` (ISO-8601) which `create_experiment` forwards verbatim in the `POST /sessions` payload (`langsmith_migrator/core/migrators/experiment.py:303-311`). We shift those too — otherwise the experiment row would show a date months old in the UI while all its child runs are stamped "today", and the relationship between `experiment.end_time` and the newest child run's `end_time` would become inconsistent.

### Anchor strategy

Per experiment, pick the anchor in this order:

1. `experiment.end_time` if present (the typical case for completed experiments).
2. Otherwise `experiment.start_time`.
3. Otherwise no shift is possible — log a warning and proceed without shifting. (The platform may then reject the runs; user can re-attempt once the experiment has timestamps.)

`delta = now_utc - anchor`. Apply that single delta to the experiment session and every run beneath it. Because every `dotted_order` segment encodes a `start_time` from the same experiment, and every `start_time` shifts by the same delta, every chain stays internally consistent. Because the anchor is `end_time`, the *newest* run lands at "now" and every earlier run/event lands in the past, comfortably inside the 24h window for any experiment shorter than 24h of wall-clock duration.

### Why the delta is persisted

Run IDs are deterministic (`uuid5(...)` — see `langsmith_migrator/core/migrators/experiment.py:13-17`). On retry/resume, run IDs match exactly; if the run already exists on the destination, the 409 is treated as replay success (`langsmith_migrator/core/migrators/experiment.py:642-648`). But if we recomputed `delta` on each attempt, *new* runs replayed in the second attempt would be shifted by a different amount than their already-created siblings from the first attempt — breaking the "same offsets between runs" guarantee within one experiment.

Solution: on the very first attempt for a given experiment we compute the delta, store it as `time_shift_seconds: float` in that experiment's state-item metadata, and reuse it on every subsequent attempt. The trade-off: if a resume happens more than ~24h after the first attempt, the cached delta will push new runs back outside the 24h window and the platform will reject them. That's an acceptable failure mode — the user can `langsmith-migrator clean` and restart fresh.

---

## File Structure

- Create: `langsmith_migrator/utils/time_shift.py` — pure functions: ISO parse/format, dotted-timestamp parse/format, `compute_delta`, `shift_iso`, `shift_dotted_order`, `shift_events`, `shift_run_payload`, `shift_experiment_payload`.
- Create: `tests/unit/test_time_shift.py` — exhaustive unit tests for the pure helpers.
- Modify: `langsmith_migrator/core/migrators/experiment.py` — accept and apply `time_delta` in `create_experiment` and `migrate_runs_streaming`; import the new helpers.
- Modify: `langsmith_migrator/core/migrators/orchestrator.py` — compute the delta on first encounter (`_resolve_experiment_delta`), persist it on the state item, reuse on resume, forward into `create_experiment` and `migrate_runs_streaming`.
- Modify: `tests/test_experiment_migrator.py` — new unit tests for migrator shifting; create the file if it does not exist.
- Modify: `tests/functional/test_experiment_time_shift.py` — end-to-end respx test asserting offsets are preserved and newest run lands inside the 24h window.
- Modify: `CHANGELOG.md` — note the behavioral change.
- Modify: `CLAUDE.md` — short note under "Key Design Patterns" so future readers know runs are always time-shifted.

No CLI flags. No config additions. The shift is invisible to users beyond the resulting timestamps in the destination.

---

## Task 1: Add `time_shift` pure utility module — ISO parse/format

**Files:**
- Create: `langsmith_migrator/utils/time_shift.py`
- Test: `tests/unit/test_time_shift.py`

- [ ] **Step 1.1: Write failing tests for ISO parse/format round-trip**

Create `tests/unit/test_time_shift.py`:

```python
"""Unit tests for time_shift utility."""

from datetime import datetime, timedelta, timezone

import pytest

from langsmith_migrator.utils.time_shift import (
    parse_iso,
    format_iso,
    parse_dotted_timestamp,
    format_dotted_timestamp,
    compute_delta,
    shift_iso,
    shift_dotted_order,
    shift_events,
    shift_run_payload,
    shift_experiment_payload,
)


class TestIsoParseFormat:
    def test_parses_z_suffix(self):
        dt = parse_iso("2026-02-03T00:35:19.695988Z")
        assert dt == datetime(2026, 2, 3, 0, 35, 19, 695988, tzinfo=timezone.utc)

    def test_parses_offset(self):
        dt = parse_iso("2026-02-03T00:35:19.695988+00:00")
        assert dt == datetime(2026, 2, 3, 0, 35, 19, 695988, tzinfo=timezone.utc)

    def test_parses_without_microseconds(self):
        dt = parse_iso("2026-02-03T00:35:19Z")
        assert dt == datetime(2026, 2, 3, 0, 35, 19, 0, tzinfo=timezone.utc)

    def test_format_uses_offset_notation(self):
        dt = datetime(2026, 2, 3, 0, 35, 19, 695988, tzinfo=timezone.utc)
        assert format_iso(dt) == "2026-02-03T00:35:19.695988+00:00"

    def test_round_trip(self):
        original = "2026-02-03T00:35:19.695988+00:00"
        assert format_iso(parse_iso(original)) == original

    def test_none_returns_none(self):
        assert parse_iso(None) is None
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_time_shift.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'langsmith_migrator.utils.time_shift'`.

- [ ] **Step 1.3: Implement parse_iso / format_iso**

Create `langsmith_migrator/utils/time_shift.py`:

```python
"""Pure helpers for shifting timestamps on migrated runs.

Migrated experiments and their runs are always shifted forward by a per-
experiment delta so that the newest timestamp lands at "now" on the
destination. This is a required workaround for the destination's
`POST /runs/batch` 24-hour timestamp window; without the shift, historical
experiments would be rejected on replay.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse a LangSmith ISO-8601 timestamp into an aware UTC datetime."""
    if value is None:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso(dt: datetime) -> str:
    """Format an aware datetime as `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`."""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")
```

- [ ] **Step 1.4: Run ISO tests to verify they pass**

Run: `uv run pytest tests/unit/test_time_shift.py::TestIsoParseFormat -v`
Expected: PASS (6 tests).

- [ ] **Step 1.5: Commit**

```bash
git add langsmith_migrator/utils/time_shift.py tests/unit/test_time_shift.py
git commit -m "Add time_shift module with ISO parse/format helpers"
```

---

## Task 2: dotted_order timestamp parse/format

**Files:**
- Modify: `langsmith_migrator/utils/time_shift.py`
- Modify: `tests/unit/test_time_shift.py`

- [ ] **Step 2.1: Write failing tests**

Append to `tests/unit/test_time_shift.py`:

```python
class TestDottedTimestamp:
    def test_parses_dotted_timestamp(self):
        dt = parse_dotted_timestamp("20260203T003519695988Z")
        assert dt == datetime(2026, 2, 3, 0, 35, 19, 695988, tzinfo=timezone.utc)

    def test_format_dotted_timestamp(self):
        dt = datetime(2026, 2, 3, 0, 35, 19, 695988, tzinfo=timezone.utc)
        assert format_dotted_timestamp(dt) == "20260203T003519695988Z"

    def test_dotted_round_trip(self):
        original = "20260203T003519695988Z"
        assert format_dotted_timestamp(parse_dotted_timestamp(original)) == original

    def test_rejects_malformed(self):
        with pytest.raises(ValueError):
            parse_dotted_timestamp("not-a-timestamp")
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_time_shift.py::TestDottedTimestamp -v`
Expected: FAIL with missing attribute.

- [ ] **Step 2.3: Implement parse_dotted_timestamp / format_dotted_timestamp**

Append to `langsmith_migrator/utils/time_shift.py`:

```python
_DOTTED_FORMAT = "%Y%m%dT%H%M%S%fZ"


def parse_dotted_timestamp(value: str) -> datetime:
    """Parse a dotted_order timestamp segment (`YYYYMMDDTHHMMSSffffffZ`)."""
    dt = datetime.strptime(value, _DOTTED_FORMAT)
    return dt.replace(tzinfo=timezone.utc)


def format_dotted_timestamp(dt: datetime) -> str:
    """Format an aware datetime in the `dotted_order` segment encoding."""
    return dt.astimezone(timezone.utc).strftime(_DOTTED_FORMAT)
```

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_time_shift.py::TestDottedTimestamp -v`
Expected: PASS (4 tests).

- [ ] **Step 2.5: Commit**

```bash
git add langsmith_migrator/utils/time_shift.py tests/unit/test_time_shift.py
git commit -m "Add dotted_order timestamp parse/format helpers"
```

---

## Task 3: `compute_delta` and `shift_iso`

**Files:**
- Modify: `langsmith_migrator/utils/time_shift.py`
- Modify: `tests/unit/test_time_shift.py`

- [ ] **Step 3.1: Write failing tests**

Append to `tests/unit/test_time_shift.py`:

```python
class TestComputeDelta:
    def test_prefers_end_time_over_start_time(self):
        now = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        delta = compute_delta(
            end_time="2026-02-03T01:00:00+00:00",
            start_time="2026-02-03T00:00:00+00:00",
            now=now,
        )
        # Anchor is end_time, so delta = now - end_time
        assert delta == now - datetime(2026, 2, 3, 1, 0, 0, tzinfo=timezone.utc)

    def test_falls_back_to_start_time_when_end_missing(self):
        now = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        delta = compute_delta(
            end_time=None,
            start_time="2026-02-03T00:00:00+00:00",
            now=now,
        )
        assert delta == now - datetime(2026, 2, 3, 0, 0, 0, tzinfo=timezone.utc)

    def test_returns_none_when_both_missing(self):
        assert compute_delta(end_time=None, start_time=None) is None

    def test_uses_real_now_when_omitted(self):
        delta = compute_delta(
            end_time="2026-02-03T00:00:00+00:00", start_time=None,
        )
        assert delta is not None and delta.total_seconds() > 0


class TestShiftIso:
    def test_shifts_iso_by_delta(self):
        result = shift_iso("2026-02-03T00:00:00+00:00", timedelta(days=1))
        assert result == "2026-02-04T00:00:00.000000+00:00"

    def test_none_passthrough(self):
        assert shift_iso(None, timedelta(days=1)) is None
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_time_shift.py::TestComputeDelta tests/unit/test_time_shift.py::TestShiftIso -v`
Expected: FAIL with `ImportError` / missing attribute.

- [ ] **Step 3.3: Implement compute_delta and shift_iso**

Append to `langsmith_migrator/utils/time_shift.py`:

```python
def compute_delta(
    *,
    end_time: Optional[str],
    start_time: Optional[str],
    now: Optional[datetime] = None,
) -> Optional[timedelta]:
    """Return the delta needed to anchor the experiment's newest timestamp at `now`.

    Anchor selection (in order):
      1. `end_time` — typical for completed experiments. Anchoring on the
         experiment's end shifts the newest run to "now" and everything else
         into the past, comfortably inside the 24h replay window.
      2. `start_time` — used when end_time is absent (in-flight experiments).
         The oldest run lands at "now", later runs land slightly in the future.

    Returns None when neither anchor exists; callers should warn and skip
    shifting in that case.
    """
    anchor = parse_iso(end_time) or parse_iso(start_time)
    if anchor is None:
        return None
    reference = now if now is not None else datetime.now(timezone.utc)
    return reference - anchor


def shift_iso(value: Optional[str], delta: timedelta) -> Optional[str]:
    """Apply `delta` to an ISO-8601 string, preserving aware-UTC formatting."""
    parsed = parse_iso(value)
    if parsed is None:
        return None
    return format_iso(parsed + delta)
```

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_time_shift.py::TestComputeDelta tests/unit/test_time_shift.py::TestShiftIso -v`
Expected: PASS (6 tests).

- [ ] **Step 3.5: Commit**

```bash
git add langsmith_migrator/utils/time_shift.py tests/unit/test_time_shift.py
git commit -m "Add compute_delta with end_time/start_time anchor fallback"
```

---

## Task 4: `shift_dotted_order`

**Files:**
- Modify: `langsmith_migrator/utils/time_shift.py`
- Modify: `tests/unit/test_time_shift.py`

- [ ] **Step 4.1: Write failing tests**

Append to `tests/unit/test_time_shift.py`:

```python
class TestShiftDottedOrder:
    def test_shifts_single_segment(self):
        result = shift_dotted_order(
            "20260203T000000000000Z" + "c9ba7a73-985a-4104-aad7-7e3c4fd27a5f",
            timedelta(days=1),
        )
        assert result == (
            "20260204T000000000000Z" + "c9ba7a73-985a-4104-aad7-7e3c4fd27a5f"
        )

    def test_shifts_all_segments(self):
        original = (
            "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa."
            "20260203T000005000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        shifted = shift_dotted_order(original, timedelta(days=1))
        assert shifted == (
            "20260204T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa."
            "20260204T000005000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )

    def test_none_returns_none(self):
        assert shift_dotted_order(None, timedelta(days=1)) is None

    def test_empty_string_returns_empty(self):
        assert shift_dotted_order("", timedelta(days=1)) == ""

    def test_unparseable_segment_left_as_is(self):
        weird = "notatimestampuuid-1234"
        assert shift_dotted_order(weird, timedelta(days=1)) == weird

    def test_zero_delta_is_identity(self):
        original = (
            "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa."
            "20260203T000005000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        assert shift_dotted_order(original, timedelta(0)) == original
```

- [ ] **Step 4.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_time_shift.py::TestShiftDottedOrder -v`
Expected: FAIL with missing attribute.

- [ ] **Step 4.3: Implement shift_dotted_order**

Append to `langsmith_migrator/utils/time_shift.py`:

```python
def shift_dotted_order(
    dotted_order: Optional[str],
    delta: timedelta,
) -> Optional[str]:
    """Shift every timestamp segment of a `dotted_order` chain by `delta`.

    Each segment has the form `YYYYMMDDTHHMMSSffffffZ{uuid}`. Segments are
    joined by `.`. UUIDs are preserved verbatim. Segments that don't match
    the expected shape are passed through unchanged so this stays safe to
    apply to malformed historical data.
    """
    if dotted_order is None:
        return None
    if dotted_order == "":
        return ""

    new_parts = []
    for part in dotted_order.split("."):
        z_idx = part.rfind("Z")
        if z_idx == -1 or z_idx == len(part) - 1 or z_idx < 19:
            new_parts.append(part)
            continue
        timestamp_segment = part[: z_idx + 1]
        suffix = part[z_idx + 1 :]
        try:
            parsed = parse_dotted_timestamp(timestamp_segment)
        except ValueError:
            new_parts.append(part)
            continue
        new_parts.append(format_dotted_timestamp(parsed + delta) + suffix)
    return ".".join(new_parts)
```

- [ ] **Step 4.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_time_shift.py::TestShiftDottedOrder -v`
Expected: PASS (6 tests).

- [ ] **Step 4.5: Commit**

```bash
git add langsmith_migrator/utils/time_shift.py tests/unit/test_time_shift.py
git commit -m "Add shift_dotted_order helper"
```

---

## Task 5: `shift_events`, `shift_run_payload`, `shift_experiment_payload`

**Files:**
- Modify: `langsmith_migrator/utils/time_shift.py`
- Modify: `tests/unit/test_time_shift.py`

- [ ] **Step 5.1: Write failing tests**

Append to `tests/unit/test_time_shift.py`:

```python
class TestShiftEvents:
    def test_shifts_event_times(self):
        events = [
            {"name": "start", "time": "2026-02-03T00:00:00+00:00"},
            {"name": "end", "time": "2026-02-03T00:00:05+00:00"},
        ]
        shifted = shift_events(events, timedelta(days=1))
        assert shifted == [
            {"name": "start", "time": "2026-02-04T00:00:00.000000+00:00"},
            {"name": "end", "time": "2026-02-04T00:00:05.000000+00:00"},
        ]

    def test_events_without_time_passthrough(self):
        events = [{"name": "noted"}]
        assert shift_events(events, timedelta(days=1)) == [{"name": "noted"}]

    def test_empty_or_none(self):
        assert shift_events([], timedelta(days=1)) == []
        assert shift_events(None, timedelta(days=1)) is None

    def test_does_not_mutate_input(self):
        events = [{"name": "start", "time": "2026-02-03T00:00:00+00:00"}]
        shift_events(events, timedelta(days=1))
        assert events[0]["time"] == "2026-02-03T00:00:00+00:00"


class TestShiftRunPayload:
    def test_shifts_all_timestamp_fields(self):
        run = {
            "id": "abc",
            "name": "n",
            "run_type": "chain",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T00:00:05+00:00",
            "dotted_order": (
                "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa."
                "20260203T000005000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            ),
            "events": [{"name": "start", "time": "2026-02-03T00:00:00+00:00"}],
            "inputs": {"x": 1},
        }
        shifted = shift_run_payload(run, timedelta(days=1))
        assert shifted["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert shifted["end_time"] == "2026-02-04T00:00:05.000000+00:00"
        assert shifted["dotted_order"].startswith("20260204T000000000000Z")
        assert shifted["events"][0]["time"] == "2026-02-04T00:00:00.000000+00:00"
        assert shifted["inputs"] == {"x": 1}

    def test_missing_fields_ok(self):
        run = {"id": "abc", "name": "n", "run_type": "chain"}
        shifted = shift_run_payload(run, timedelta(days=1))
        assert "start_time" not in shifted
        assert "end_time" not in shifted

    def test_does_not_mutate_input(self):
        run = {
            "id": "abc",
            "start_time": "2026-02-03T00:00:00+00:00",
            "events": [{"name": "x", "time": "2026-02-03T00:00:00+00:00"}],
        }
        shift_run_payload(run, timedelta(days=1))
        assert run["start_time"] == "2026-02-03T00:00:00+00:00"
        assert run["events"][0]["time"] == "2026-02-03T00:00:00+00:00"


class TestShiftExperimentPayload:
    def test_shifts_start_and_end(self):
        exp = {
            "name": "e",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
        }
        shifted = shift_experiment_payload(exp, timedelta(days=1))
        assert shifted["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert shifted["end_time"] == "2026-02-04T01:00:00.000000+00:00"

    def test_missing_end_time_ok(self):
        exp = {"name": "e", "start_time": "2026-02-03T00:00:00+00:00"}
        shifted = shift_experiment_payload(exp, timedelta(days=1))
        assert shifted["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert "end_time" not in shifted
```

- [ ] **Step 5.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_time_shift.py::TestShiftEvents tests/unit/test_time_shift.py::TestShiftRunPayload tests/unit/test_time_shift.py::TestShiftExperimentPayload -v`
Expected: FAIL with missing attribute.

- [ ] **Step 5.3: Implement the three helpers**

Append to `langsmith_migrator/utils/time_shift.py`:

```python
def shift_events(
    events: Optional[list],
    delta: timedelta,
) -> Optional[list]:
    """Return a deep-copied list with each event's `time` shifted."""
    if events is None:
        return None
    new_events = []
    for event in events:
        if not isinstance(event, dict) or "time" not in event:
            new_events.append(copy.deepcopy(event))
            continue
        new_event = copy.deepcopy(event)
        new_event["time"] = shift_iso(event.get("time"), delta)
        new_events.append(new_event)
    return new_events


def shift_run_payload(run: Dict[str, Any], delta: timedelta) -> Dict[str, Any]:
    """Return a copy of `run` with start_time, end_time, dotted_order, and
    events[].time each shifted by `delta`. Missing fields are passed through
    untouched. The input dict is not mutated.
    """
    shifted = dict(run)
    if "start_time" in run:
        shifted["start_time"] = shift_iso(run.get("start_time"), delta)
    if "end_time" in run:
        shifted["end_time"] = shift_iso(run.get("end_time"), delta)
    if "dotted_order" in run:
        shifted["dotted_order"] = shift_dotted_order(run.get("dotted_order"), delta)
    if "events" in run:
        shifted["events"] = shift_events(run.get("events"), delta)
    return shifted


def shift_experiment_payload(
    experiment: Dict[str, Any],
    delta: timedelta,
) -> Dict[str, Any]:
    """Return a copy of `experiment` with start_time/end_time shifted."""
    shifted = dict(experiment)
    if "start_time" in experiment:
        shifted["start_time"] = shift_iso(experiment.get("start_time"), delta)
    if "end_time" in experiment:
        shifted["end_time"] = shift_iso(experiment.get("end_time"), delta)
    return shifted
```

- [ ] **Step 5.4: Run all unit tests to verify they pass**

Run: `uv run pytest tests/unit/test_time_shift.py -v`
Expected: PASS for all sub-tests across the six classes.

- [ ] **Step 5.5: Commit**

```bash
git add langsmith_migrator/utils/time_shift.py tests/unit/test_time_shift.py
git commit -m "Add shift_events, shift_run_payload, shift_experiment_payload helpers"
```

---

## Task 6: Apply shift in `ExperimentMigrator.create_experiment`

**Files:**
- Modify: `langsmith_migrator/core/migrators/experiment.py:1-10` (imports)
- Modify: `langsmith_migrator/core/migrators/experiment.py:282-323` (`create_experiment`)
- Test: `tests/test_experiment_migrator.py` (create if absent)

- [ ] **Step 6.1: Check whether tests/test_experiment_migrator.py exists**

Run: `ls tests/test_experiment_migrator.py 2>/dev/null || echo "missing"`

If missing, create the file with `"""Tests for ExperimentMigrator."""` as its first line.

- [ ] **Step 6.2: Write failing test for shifted experiment creation**

Append to `tests/test_experiment_migrator.py`:

```python
from datetime import timedelta
from unittest.mock import Mock

from langsmith_migrator.core.migrators.experiment import ExperimentMigrator
from langsmith_migrator.utils.config import Config


def _make_migrator():
    config = Config(
        source_api_key="s",
        dest_api_key="d",
        source_url="https://s.test",
        dest_url="https://d.test",
    )
    config.migration.skip_existing = False
    source = Mock()
    dest = Mock()
    return ExperimentMigrator(source, dest, None, config)


class TestCreateExperimentTimeShift:
    def test_applies_delta_to_payload(self):
        migrator = _make_migrator()
        migrator.find_existing_experiment = Mock(return_value=None)
        migrator.dest.post = Mock(return_value={"id": "new-exp-id"})

        experiment = {
            "id": "src-exp",
            "name": "exp",
            "description": "",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
            "extra": None,
            "trace_tier": None,
        }
        migrator.create_experiment(
            experiment, "dest-dataset-id", time_delta=timedelta(days=1)
        )

        sent_payload = migrator.dest.post.call_args[0][1]
        assert sent_payload["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert sent_payload["end_time"] == "2026-02-04T01:00:00.000000+00:00"

    def test_no_delta_preserves_original_times(self):
        migrator = _make_migrator()
        migrator.find_existing_experiment = Mock(return_value=None)
        migrator.dest.post = Mock(return_value={"id": "new-exp-id"})

        experiment = {
            "id": "src-exp",
            "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
        }
        migrator.create_experiment(experiment, "dest-dataset-id", time_delta=None)

        sent_payload = migrator.dest.post.call_args[0][1]
        assert sent_payload["start_time"] == "2026-02-03T00:00:00+00:00"
        assert sent_payload["end_time"] == "2026-02-03T01:00:00+00:00"
```

- [ ] **Step 6.3: Run tests to verify they fail**

Run: `uv run pytest tests/test_experiment_migrator.py::TestCreateExperimentTimeShift -v`
Expected: FAIL with `TypeError: create_experiment() got an unexpected keyword argument 'time_delta'`.

- [ ] **Step 6.4: Add imports**

In `langsmith_migrator/core/migrators/experiment.py`, replace the existing import block at the top of the file:

```python
"""Experiment migration logic."""

from datetime import timedelta
from typing import Dict, List, Any, Optional, Tuple
import copy
import uuid

from .base import BaseMigrator
from ...utils.time_shift import shift_experiment_payload, shift_run_payload
```

- [ ] **Step 6.5: Extend create_experiment to accept and apply a delta**

In `langsmith_migrator/core/migrators/experiment.py:282`, replace the entire `create_experiment` method:

```python
    def create_experiment(
        self,
        experiment: Dict[str, Any],
        new_dataset_id: str,
        time_delta: Optional[timedelta] = None,
    ) -> str:
        """Create or update experiment in destination, optionally shifting times."""
        existing_id = self.find_existing_experiment(experiment["name"], new_dataset_id)

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Experiment '{experiment['name']}' already exists, skipping", "warning")
                return existing_id
            else:
                self.log(f"Experiment '{experiment['name']}' exists, updating...", "info")
                self.update_experiment(existing_id, experiment)
                return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create experiment: {experiment['name']}")
            return f"dry-run-{experiment['id']}"

        source_view = (
            shift_experiment_payload(experiment, time_delta)
            if time_delta is not None
            else experiment
        )

        extra = self._ensure_evaluator_types(source_view.get("extra"))

        payload = {
            "name": source_view["name"],
            "description": source_view.get("description") or None,
            "reference_dataset_id": new_dataset_id,
            "start_time": source_view.get("start_time"),
            "end_time": source_view.get("end_time"),
            "extra": extra,
            "trace_tier": source_view.get("trace_tier"),
        }

        response = self.dest.post("/sessions", payload)

        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(f"Invalid response creating experiment: expected dict, got {type(response)}")
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(f"Invalid response creating experiment: missing 'id' field. Response: {response}")

        return response["id"]
```

- [ ] **Step 6.6: Run tests to verify they pass**

Run: `uv run pytest tests/test_experiment_migrator.py::TestCreateExperimentTimeShift -v`
Expected: PASS (2 tests).

- [ ] **Step 6.7: Commit**

```bash
git add langsmith_migrator/core/migrators/experiment.py tests/test_experiment_migrator.py
git commit -m "Apply optional time delta in create_experiment"
```

---

## Task 7: Apply shift in `ExperimentMigrator.migrate_runs_streaming`

**Files:**
- Modify: `langsmith_migrator/core/migrators/experiment.py:325-600`
- Test: `tests/test_experiment_migrator.py`

- [ ] **Step 7.1: Write failing tests**

Append to `tests/test_experiment_migrator.py`:

```python
class TestMigrateRunsStreamingTimeShift:
    def test_runs_get_shifted_timestamps(self):
        migrator = _make_migrator()

        source_runs = [
            {
                "id": "run-a",
                "name": "a",
                "run_type": "chain",
                "session_id": "src-exp",
                "start_time": "2026-02-03T00:00:00+00:00",
                "end_time": "2026-02-03T00:00:05+00:00",
                "dotted_order": (
                    "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                ),
                "trace_id": "run-a",
                "events": [{"name": "begin", "time": "2026-02-03T00:00:00+00:00"}],
            }
        ]

        migrator.source.post = Mock(
            return_value={"runs": source_runs, "cursors": {"next": None}}
        )

        captured_batches = []

        def fake_batch_post(path, payload):
            captured_batches.append(payload)
            return {"errors": []}

        migrator.dest.post = Mock(side_effect=fake_batch_post)

        migrator.migrate_runs_streaming(
            ["src-exp"],
            {
                "experiments": {"src-exp": "dest-exp"},
                "examples": {},
            },
            time_deltas={"src-exp": timedelta(days=1)},
        )

        batch_payload = captured_batches[0]
        sent_run = batch_payload["post"][0]
        assert sent_run["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert sent_run["end_time"] == "2026-02-04T00:00:05.000000+00:00"
        assert sent_run["dotted_order"].startswith("20260204T000000000000Z")
        assert sent_run["events"][0]["time"] == "2026-02-04T00:00:00.000000+00:00"

    def test_no_delta_passes_original_times(self):
        migrator = _make_migrator()
        source_runs = [
            {
                "id": "run-a",
                "name": "a",
                "run_type": "chain",
                "session_id": "src-exp",
                "start_time": "2026-02-03T00:00:00+00:00",
                "end_time": "2026-02-03T00:00:05+00:00",
                "dotted_order": (
                    "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                ),
                "trace_id": "run-a",
            }
        ]
        migrator.source.post = Mock(
            return_value={"runs": source_runs, "cursors": {"next": None}}
        )
        captured = []

        def fake_post(path, payload):
            captured.append(payload)
            return {"errors": []}

        migrator.dest.post = Mock(side_effect=fake_post)

        migrator.migrate_runs_streaming(
            ["src-exp"],
            {
                "experiments": {"src-exp": "dest-exp"},
                "examples": {},
            },
        )
        sent = captured[0]["post"][0]
        assert sent["start_time"] == "2026-02-03T00:00:00+00:00"
```

- [ ] **Step 7.2: Run tests to verify they fail**

Run: `uv run pytest tests/test_experiment_migrator.py::TestMigrateRunsStreamingTimeShift -v`
Expected: FAIL with `TypeError: migrate_runs_streaming() got an unexpected keyword argument 'time_deltas'`.

- [ ] **Step 7.3: Update method signature and apply shift**

In `langsmith_migrator/core/migrators/experiment.py:325`, replace the signature and docstring (lines 325-340):

```python
    def migrate_runs_streaming(
        self,
        experiment_ids: List[str],
        id_mappings: Dict[str, Dict[str, str]],
        time_deltas: Optional[Dict[str, timedelta]] = None,
    ) -> Tuple[int, Dict[str, str], int]:
        """
        Migrate runs for experiments using streaming.

        Args:
            experiment_ids: List of source experiment IDs to migrate runs for
            id_mappings: Dict containing "experiments" and "examples" mappings
            time_deltas: Mapping of source experiment ID -> timedelta to apply
                to start_time, end_time, dotted_order, and events[].time on
                every run in that experiment. Required for migrating historical
                experiments because POST /runs/batch rejects timestamps outside
                a 24h window of now. When None or missing for an experiment,
                runs are sent with their original timestamps (which will fail
                on the destination for any historical experiment).

        Returns:
            Tuple of (total_runs_migrated, run_id_mapping, failed_run_count)
        """
```

Inside the per-experiment loop, right after the existing line `for exp_idx, experiment_id in enumerate(experiment_ids, 1):` (around line 361), add:

```python
            experiment_delta = (time_deltas or {}).get(experiment_id)
```

In the inner per-run loop, immediately before the existing line `# Remove None values to avoid API validation errors (422)` (around line 489), add:

```python
                    if experiment_delta is not None:
                        migrated_run = shift_run_payload(migrated_run, experiment_delta)
```

(The shift runs *after* `_regenerate_dotted_order` — that function rewrites UUIDs while keeping timestamps; the shift then rewrites the timestamps without disturbing the new UUIDs.)

- [ ] **Step 7.4: Run tests to verify they pass**

Run: `uv run pytest tests/test_experiment_migrator.py::TestMigrateRunsStreamingTimeShift -v`
Expected: PASS (2 tests).

- [ ] **Step 7.5: Run the full experiment migrator test file**

Run: `uv run pytest tests/test_experiment_migrator.py -v`
Expected: All tests pass.

- [ ] **Step 7.6: Commit**

```bash
git add langsmith_migrator/core/migrators/experiment.py tests/test_experiment_migrator.py
git commit -m "Apply optional time delta to runs in migrate_runs_streaming"
```

---

## Task 8: Compute and persist delta in the orchestrator

**Files:**
- Modify: `langsmith_migrator/core/migrators/orchestrator.py:1-25` (imports)
- Modify: `langsmith_migrator/core/migrators/orchestrator.py:366-514` (`_resolve_experiment_item`)
- Test: `tests/test_experiment_migrator.py`

- [ ] **Step 8.1: Write failing tests**

Append to `tests/test_experiment_migrator.py`:

```python
from pathlib import Path

from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.state import StateManager


def _orchestrator(tmp_path: Path):
    config = Config(
        source_api_key="s", dest_api_key="d",
        source_url="https://s.test", dest_url="https://d.test",
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    state_manager = StateManager(state_dir=str(state_dir))
    return MigrationOrchestrator(config, state_manager), state_manager


class TestOrchestratorDeltaPersistence:
    def test_computes_and_persists_delta_on_first_attempt(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        item = state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="create_experiment",
            workspace_pair={"source": None, "dest": None},
            metadata={"source_dataset_id": "src-ds", "dest_dataset_id": "dst-ds"},
        )

        experiment_payload = {
            "id": "src-exp", "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
        }

        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        ok, _ = orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )
        assert ok is True

        # The delta passed to create_experiment and migrate_runs_streaming is the
        # same and is anchored on experiment.end_time.
        passed_to_create = exp_mig.create_experiment.call_args.kwargs["time_delta"]
        passed_to_runs = exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"]
        assert passed_to_create is not None
        assert passed_to_runs["src-exp"] == passed_to_create

        stored = state.get_item(item.id).metadata["time_shift_seconds"]
        assert isinstance(stored, (int, float))
        assert stored > 0

    def test_resume_reuses_persisted_delta(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="migrate_runs",
            workspace_pair={"source": None, "dest": None},
            metadata={
                "source_dataset_id": "src-ds",
                "dest_dataset_id": "dst-ds",
                "destination_experiment_id": "dest-exp",
                "time_shift_seconds": 86400.0,
            },
        )

        experiment_payload = {
            "id": "src-exp", "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
        }
        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )

        passed = exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"]
        assert passed["src-exp"] == timedelta(seconds=86400.0)

    def test_missing_anchor_skips_shift(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="create_experiment",
            workspace_pair={"source": None, "dest": None},
            metadata={"source_dataset_id": "src-ds", "dest_dataset_id": "dst-ds"},
        )

        experiment_payload = {"id": "src-exp", "name": "exp"}
        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )

        assert exp_mig.create_experiment.call_args.kwargs["time_delta"] is None
        assert exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"] is None
```

- [ ] **Step 8.2: Run tests to verify they fail**

Run: `uv run pytest tests/test_experiment_migrator.py::TestOrchestratorDeltaPersistence -v`
Expected: FAIL — orchestrator doesn't yet compute, persist, or forward `time_shift_seconds`.

- [ ] **Step 8.3: Add imports**

At the top of `langsmith_migrator/core/migrators/orchestrator.py`, add:

```python
from datetime import timedelta

from ...utils.time_shift import compute_delta
```

- [ ] **Step 8.4: Add helper method `_resolve_experiment_delta`**

Place this method just above `_resolve_experiment_item` in `langsmith_migrator/core/migrators/orchestrator.py` (around line 366):

```python
    def _resolve_experiment_delta(
        self,
        experiment: Dict[str, Any],
        item,
    ) -> Optional[timedelta]:
        """Return the timedelta to apply to this experiment's timestamps.

        On first encounter we compute `now - max(end_time, start_time)` and
        persist it as `time_shift_seconds` on the experiment's state item.
        On resume we reuse the persisted value so later batches receive the
        same shift their already-created sibling runs got — preserving the
        relative-offset invariant across resume boundaries.

        Returns None when the experiment has neither end_time nor start_time
        (no anchor available). Callers should log and proceed unshifted; the
        platform will then enforce the 24h window on /runs/batch.
        """
        stored = item.metadata.get("time_shift_seconds") if item else None
        if stored is not None:
            return timedelta(seconds=float(stored))

        delta = compute_delta(
            end_time=experiment.get("end_time"),
            start_time=experiment.get("start_time"),
        )
        if delta is None:
            self.console.print(
                f"[yellow]Experiment {experiment.get('name') or experiment.get('id')} "
                "has no start_time or end_time; cannot anchor runs to now. "
                "Runs older than 24h will be rejected by the destination.[/yellow]"
            )
            return None

        if item is not None:
            self.state.update_item_checkpoint(
                item.id,
                metadata={"time_shift_seconds": delta.total_seconds()},
            )
            self.state_manager.save()
        return delta
```

- [ ] **Step 8.5: Forward the delta from `_resolve_experiment_item`**

Inside `_resolve_experiment_item` (around line 383), capture the delta at the top of the `try` block and forward it to both calls. The relevant edits:

Right after `dest_experiment_id = item.destination_id or item.metadata.get("destination_experiment_id")` (around line 381):

```python
        time_delta = self._resolve_experiment_delta(experiment, item)
        time_deltas = (
            {source_experiment_id: time_delta} if time_delta is not None else None
        )
```

In the `create_experiment` call (around line 398), add `time_delta=time_delta`:

```python
                dest_experiment_id = experiment_migrator.create_experiment(
                    experiment,
                    dest_dataset_id,
                    time_delta=time_delta,
                )
```

In the `migrate_runs_streaming` call (around line 416), add `time_deltas=time_deltas`:

```python
                total_runs, _, failed_run_count = experiment_migrator.migrate_runs_streaming(
                    [source_experiment_id],
                    {
                        "experiments": {source_experiment_id: dest_experiment_id},
                        "examples": self.state.id_mappings.get("examples", {}),
                    },
                    time_deltas=time_deltas,
                )
```

(Feedback handling and error paths in the same method are unchanged.)

- [ ] **Step 8.6: Run tests to verify they pass**

Run: `uv run pytest tests/test_experiment_migrator.py::TestOrchestratorDeltaPersistence -v`
Expected: PASS (3 tests).

- [ ] **Step 8.7: Run the full test suite**

Run: `uv run pytest -q`
Expected: All previously-passing tests plus the new ones pass.

- [ ] **Step 8.8: Commit**

```bash
git add langsmith_migrator/core/migrators/orchestrator.py tests/test_experiment_migrator.py
git commit -m "Compute and persist per-experiment time delta in orchestrator"
```

---

## Task 9: End-to-end integration test with respx

**Files:**
- Create: `tests/functional/test_experiment_time_shift.py`

- [ ] **Step 9.1: Confirm `tests/functional/` exists**

Run: `ls tests/functional/`
Expected: directory listing.

- [ ] **Step 9.2: Write end-to-end test**

Create `tests/functional/test_experiment_time_shift.py`:

```python
"""End-to-end: dataset+experiment migration always rewrites run timestamps so
that the newest one is at "now", while preserving relative offsets between runs.
This guards the 24h POST /runs/batch platform restriction."""

from datetime import datetime, timedelta, timezone
import json

import pytest
import respx
from httpx import Response

from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.config import Config
from langsmith_migrator.utils.state import StateManager
from langsmith_migrator.utils.time_shift import parse_iso


SOURCE = "https://source.test/api/v1"
DEST = "https://dest.test/api/v1"


def _config():
    return Config(
        source_api_key="s", dest_api_key="d",
        source_url="https://source.test", dest_url="https://dest.test",
    )


@respx.mock
def test_runs_shifted_consistently_preserves_offsets(tmp_path):
    dataset_id = "ds-1"
    exp_id = "exp-1"

    respx.get(f"{SOURCE}/datasets/{dataset_id}").mock(
        return_value=Response(200, json={
            "id": dataset_id, "name": "DS", "description": "",
            "data_type": "kv", "inputs_schema_definition": {},
            "outputs_schema_definition": {}, "externally_managed": False,
            "transformations": [],
        })
    )
    respx.get(f"{SOURCE}/datasets/{dataset_id}/examples").mock(
        return_value=Response(200, json=[])
    )
    respx.post(f"{DEST}/datasets").mock(
        return_value=Response(200, json={"id": "new-ds-1"})
    )
    respx.get(f"{SOURCE}/sessions", params={"reference_dataset": dataset_id}).mock(
        return_value=Response(200, json=[{"id": exp_id, "name": "EXP"}])
    )
    respx.get(f"{SOURCE}/sessions/{exp_id}").mock(
        return_value=Response(200, json={
            "id": exp_id, "name": "EXP",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time":   "2026-02-03T00:10:00+00:00",
            "extra": None,
        })
    )
    respx.get(f"{DEST}/sessions", params={"reference_dataset": "new-ds-1"}).mock(
        return_value=Response(200, json=[])
    )

    captured_session_payload = {}

    def session_creator(request):
        captured_session_payload.update(json.loads(request.content))
        return Response(200, json={"id": "new-exp-1"})

    respx.post(f"{DEST}/sessions").mock(side_effect=session_creator)

    runs = [
        {
            "id": "run-a", "name": "a", "run_type": "chain", "session_id": exp_id,
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time":   "2026-02-03T00:00:05+00:00",
            "dotted_order": (
                "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            ),
            "trace_id": "run-a",
        },
        {
            "id": "run-b", "name": "b", "run_type": "chain", "session_id": exp_id,
            "start_time": "2026-02-03T00:00:30+00:00",
            "end_time":   "2026-02-03T00:00:35+00:00",
            "dotted_order": (
                "20260203T000030000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            ),
            "trace_id": "run-b",
        },
    ]
    respx.post(f"{SOURCE}/runs/query").mock(
        return_value=Response(200, json={"runs": runs, "cursors": {"next": None}})
    )

    captured_batches = []

    def batch_creator(request):
        captured_batches.append(json.loads(request.content))
        return Response(200, json={"errors": []})

    respx.post(f"{DEST}/runs/batch").mock(side_effect=batch_creator)
    respx.get(f"{SOURCE}/feedback").mock(return_value=Response(200, json=[]))

    state_manager = StateManager(state_dir=str(tmp_path / "state"))
    orchestrator = MigrationOrchestrator(_config(), state_manager)
    orchestrator.migrate_datasets_parallel(
        [dataset_id], include_examples=True, include_experiments=True,
    )

    posted = captured_batches[0]["post"]
    a = next(r for r in posted if r["name"] == "a")
    b = next(r for r in posted if r["name"] == "b")

    # Relative offsets preserved.
    assert parse_iso(b["start_time"]) - parse_iso(a["start_time"]) == timedelta(seconds=30)
    assert parse_iso(a["end_time"]) - parse_iso(a["start_time"]) == timedelta(seconds=5)

    # dotted_order timestamps line up with start_time after the shift.
    a_seg = a["dotted_order"].split(".")[-1]
    a_dotted_part = a_seg[: a_seg.rfind("Z") + 1]
    assert a_dotted_part[:8] == parse_iso(a["start_time"]).strftime("%Y%m%d")

    # Anchor is experiment.end_time, so the newest run end ≈ now.
    now = datetime.now(timezone.utc)
    newest_end = max(parse_iso(a["end_time"]), parse_iso(b["end_time"]))
    # newest_end - now should be small (sub-minute on a healthy machine).
    assert abs((newest_end - now).total_seconds()) < 120

    # And all runs are inside the 24h window.
    earliest_start = min(parse_iso(a["start_time"]), parse_iso(b["start_time"]))
    assert (now - earliest_start) < timedelta(hours=24)

    # Experiment session payload is shifted to align with the runs.
    assert parse_iso(captured_session_payload["start_time"]).date() == now.date()
    assert parse_iso(captured_session_payload["end_time"]).date() == now.date()
```

- [ ] **Step 9.3: Run the test**

Run: `uv run pytest tests/functional/test_experiment_time_shift.py -v`
Expected: PASS.

- [ ] **Step 9.4: Commit**

```bash
git add tests/functional/test_experiment_time_shift.py
git commit -m "Add end-to-end test for run-time rewriting and 24h window"
```

---

## Task 10: Update CHANGELOG and CLAUDE.md

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `CLAUDE.md`

- [ ] **Step 10.1: Add CHANGELOG entry**

Open `CHANGELOG.md`. Under the topmost `## [Unreleased]` (or create one if absent), add:

```markdown
### Changed
- Experiment run migration now rewrites every run's `start_time`, `end_time`,
  `dotted_order` timestamps, and `events[].time` (plus the parent experiment's
  `start_time`/`end_time`) by a per-experiment delta so the newest timestamp
  lands at "now" on the destination. This works around the destination's
  `POST /runs/batch` 24-hour timestamp window; without it, historical
  experiments would have zero runs migrated. Relative offsets between runs
  within an experiment are preserved exactly. The delta is persisted in
  migration state per experiment so resume replays use the same shift as the
  initial attempt.
```

- [ ] **Step 10.2: Note the behavior in CLAUDE.md**

In `CLAUDE.md`, under "Key Design Patterns", append:

```markdown
5. **Run timestamps anchored to "now" on migration**: When migrating experiment
   data (datasets `--include-experiments` or `migrate-all`), every run is
   shifted by `now - max(experiment.end_time, experiment.start_time)` so it
   fits the destination's 24h `POST /runs/batch` window. Offsets between
   runs in the same experiment are preserved. The shift is persisted per
   experiment in state so resumes stay consistent.
```

- [ ] **Step 10.3: Commit**

```bash
git add CHANGELOG.md CLAUDE.md
git commit -m "Document run-time rewriting behavior"
```

---

## Task 11: Final verification and PR

- [ ] **Step 11.1: Run the full test suite with coverage**

Run: `uv run pytest --cov -q`
Expected: All tests pass. Coverage on `langsmith_migrator/utils/time_shift.py` should be near 100%.

- [ ] **Step 11.2: Smoke-check the CLI**

Run: `uv run langsmith-migrator datasets --help`
Expected: Help output unchanged (no new flag) and includes `--include-experiments` as before.

- [ ] **Step 11.3: Open the PR**

```bash
gh pr create \
  --base main \
  --head feature/rewrite-run-times \
  --title "Rewrite run timestamps to satisfy /runs/batch 24h window" \
  --body "$(cat <<'EOF'
## Summary
- Always rewrites every migrated run's `start_time`, `end_time`, `dotted_order`
  timestamps, and `events[].time` (plus the parent experiment's
  `start_time`/`end_time`) by a per-experiment delta = `now - max(end_time, start_time)`.
- Required workaround: the destination's `POST /runs/batch` rejects runs with
  timestamps outside a 24-hour window of now, so historical experiments would
  otherwise migrate with zero runs.
- Per-experiment delta is persisted in state (`time_shift_seconds`) so resumed
  migrations apply the same shift to the remaining runs, preserving the
  relative-offset invariant across resume boundaries — required because run
  IDs are deterministic and partial replays would otherwise mix shifts within
  a single experiment.

## Test plan
- [ ] Unit tests for parse/format/shift helpers in `tests/unit/test_time_shift.py`
- [ ] `ExperimentMigrator.create_experiment` shifts experiment timestamps
- [ ] `ExperimentMigrator.migrate_runs_streaming` shifts all four run fields
- [ ] Orchestrator computes, persists, and reuses delta on resume; skips on missing anchor
- [ ] End-to-end respx test asserting offsets preserved and newest run within 24h of now
- [ ] Full pytest suite passes

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.
