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
    shift_dotted_order,  # noqa: F401
    shift_events,  # noqa: F401
    shift_run_payload,  # noqa: F401
    shift_experiment_payload,  # noqa: F401
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

    def test_format_rejects_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            format_iso(datetime(2026, 2, 3, 0, 35, 19))


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

    def test_format_rejects_naive_datetime(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            format_dotted_timestamp(datetime(2026, 2, 3, 0, 35, 19, 695988))


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
