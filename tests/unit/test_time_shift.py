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

    def test_empty_or_whitespace_returns_none(self):
        assert parse_iso("") is None
        assert parse_iso("   ") is None

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
