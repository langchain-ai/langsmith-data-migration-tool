"""Pure helpers for shifting timestamps on migrated runs.

Migrated experiments and their runs are always shifted forward by a per-
experiment delta so that the newest timestamp lands at "now" on the
destination. This is a required workaround for the destination's
`POST /runs/batch` 24-hour timestamp window; without the shift, historical
experiments would be rejected on replay.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


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


def shift_dotted_order(*args, **kwargs):
    raise NotImplementedError


def shift_events(*args, **kwargs):
    raise NotImplementedError


def shift_run_payload(*args, **kwargs):
    raise NotImplementedError


def shift_experiment_payload(*args, **kwargs):
    raise NotImplementedError
