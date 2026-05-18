"""Pure helpers for shifting timestamps on migrated runs.

Migrated experiments and their runs are always shifted forward by a per-
experiment delta so that the newest timestamp lands at "now" on the
destination. This is a required workaround for the destination's
`POST /runs/batch` 24-hour timestamp window; without the shift, historical
experiments would be rejected on replay.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse a LangSmith ISO-8601 timestamp into an aware UTC datetime."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso(dt: datetime) -> str:
    """Format an aware datetime as `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`."""
    if dt.tzinfo is None:
        raise ValueError(f"format_iso requires a timezone-aware datetime, got {dt!r}")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


_DOTTED_FORMAT = "%Y%m%dT%H%M%S%fZ"


def parse_dotted_timestamp(value: str) -> datetime:
    """Parse a dotted_order timestamp segment (`YYYYMMDDTHHMMSSffffffZ`)."""
    dt = datetime.strptime(value, _DOTTED_FORMAT)
    return dt.replace(tzinfo=timezone.utc)


def format_dotted_timestamp(dt: datetime) -> str:
    """Format an aware datetime in the `dotted_order` segment encoding."""
    if dt.tzinfo is None:
        raise ValueError(f"format_dotted_timestamp requires a timezone-aware datetime, got {dt!r}")
    return dt.astimezone(timezone.utc).strftime(_DOTTED_FORMAT)


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
      2. `start_time` — used when end_time is absent or unparseable
         (in-flight experiments or partially-corrupt data). The oldest run
         lands at "now", later runs land slightly in the future.

    Each candidate is parsed independently; a malformed `end_time` still
    falls back to `start_time`. Returns None when no candidate yields a
    usable timestamp; callers should warn and skip shifting in that case.
    """
    anchor: Optional[datetime] = None
    for candidate in (end_time, start_time):
        try:
            parsed = parse_iso(candidate)
        except ValueError:
            continue
        if parsed is not None:
            anchor = parsed
            break
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


def shift_events(
    events: Optional[list],
    delta: timedelta,
) -> Optional[list]:
    """Return a new list with each event's `time` shifted.

    Events without a `time` field are passed through by reference (no copy).
    Time-bearing events get a shallow copy with the `time` value replaced —
    sufficient because we only overwrite a single string field.
    """
    if events is None:
        return None
    new_events = []
    for event in events:
        if not isinstance(event, dict) or "time" not in event:
            new_events.append(event)
            continue
        new_event = dict(event)
        new_event["time"] = shift_iso(event["time"], delta)
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
