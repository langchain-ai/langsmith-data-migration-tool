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


def parse_dotted_timestamp(*args, **kwargs):
    raise NotImplementedError


def format_dotted_timestamp(*args, **kwargs):
    raise NotImplementedError


def compute_delta(*args, **kwargs):
    raise NotImplementedError


def shift_iso(*args, **kwargs):
    raise NotImplementedError


def shift_dotted_order(*args, **kwargs):
    raise NotImplementedError


def shift_events(*args, **kwargs):
    raise NotImplementedError


def shift_run_payload(*args, **kwargs):
    raise NotImplementedError


def shift_experiment_payload(*args, **kwargs):
    raise NotImplementedError
