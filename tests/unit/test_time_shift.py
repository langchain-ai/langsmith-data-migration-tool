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
