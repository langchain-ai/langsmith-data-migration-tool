"""End-to-end: dataset+experiment migration always rewrites run timestamps so
that the newest one is at "now", while preserving relative offsets between runs.
This guards the 24h POST /runs/batch platform restriction.

The API client uses `requests` (not httpx), so we monkeypatch EnhancedAPIClient
methods rather than using respx (which is httpx-only).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.config import Config
from langsmith_migrator.utils.state import StateManager
from langsmith_migrator.utils.time_shift import parse_iso


DATASET_ID = "ds-1"
EXP_ID = "exp-1"

# Source experiment timestamps — ancient, well outside the 24h window.
# EXP_END matches the last run's end_time so the delta anchors the newest
# run exactly at "now" (which is what the platform requires).
EXP_START = "2026-02-03T00:00:00+00:00"
EXP_END = "2026-02-03T00:00:35+00:00"  # == run-b end_time

# Two runs: "a" starts at T+0, "b" starts at T+30s, both end 5s after start
RUN_A = {
    "id": "run-a",
    "name": "a",
    "run_type": "chain",
    "session_id": EXP_ID,
    "start_time": "2026-02-03T00:00:00+00:00",
    "end_time": "2026-02-03T00:00:05+00:00",
    "dotted_order": "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "trace_id": "run-a",
}
RUN_B = {
    "id": "run-b",
    "name": "b",
    "run_type": "chain",
    "session_id": EXP_ID,
    "start_time": "2026-02-03T00:00:30+00:00",
    "end_time": "2026-02-03T00:00:35+00:00",
    "dotted_order": "20260203T000030000000Zbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "trace_id": "run-b",
}

DATASET_PAYLOAD = {
    "id": DATASET_ID,
    "name": "DS",
    "description": "",
    "data_type": "kv",
    "inputs_schema_definition": {},
    "outputs_schema_definition": {},
    "externally_managed": False,
    "transformations": [],
}

EXPERIMENT_PAYLOAD = {
    "id": EXP_ID,
    "name": "EXP",
    "start_time": EXP_START,
    "end_time": EXP_END,
    "extra": None,
}


def _make_config():
    return Config(
        source_api_key="s",
        dest_api_key="d",
        source_url="https://source.test",
        dest_url="https://dest.test",
    )


def test_runs_shifted_consistently_preserves_offsets(tmp_path: Path):
    """
    Verify all 5 assertions:
      1. Relative offsets between runs are preserved.
      2. dotted_order timestamps align with start_time after the shift.
      3. The newest run ends at approximately now.
      4. All runs are within the 24h window.
      5. The experiment session payload is also shifted.
    """
    captured_session_payload = {}
    captured_batches = []

    # -----------------------------------------------------------------------
    # Build per-method dispatch tables for source and destination clients
    # -----------------------------------------------------------------------

    def source_get(endpoint, params=None):
        """Mock source GET."""
        if endpoint == f"/datasets/{DATASET_ID}":
            return DATASET_PAYLOAD
        if endpoint == f"/sessions/{EXP_ID}":
            return EXPERIMENT_PAYLOAD
        # Feedback pagination (list_feedback_for_session)
        if endpoint == "/feedback":
            return []
        raise AssertionError(f"Unexpected source GET: {endpoint!r} params={params}")

    def source_get_paginated(endpoint, params=None, page_size=100):
        """Mock source GET paginated."""
        if endpoint == "/sessions" and (params or {}).get("reference_dataset") == DATASET_ID:
            return [{"id": EXP_ID, "name": "EXP"}]
        if endpoint == "/examples":
            return []
        raise AssertionError(f"Unexpected source get_paginated: {endpoint!r} params={params}")

    def source_post(endpoint, data):
        """Mock source POST."""
        if endpoint == "/runs/query":
            # Only return runs on first call; cursor=None signals end of pages
            return {"runs": [RUN_A, RUN_B], "cursors": {"next": None}}
        raise AssertionError(f"Unexpected source POST: {endpoint!r}")

    def dest_get(endpoint, params=None):
        """Mock destination GET."""
        # find_existing_dataset checks /datasets with name param
        if endpoint == "/datasets":
            return []  # no existing dataset
        raise AssertionError(f"Unexpected dest GET: {endpoint!r} params={params}")

    def dest_get_paginated(endpoint, params=None, page_size=100):
        """Mock destination GET paginated."""
        if endpoint == "/sessions":
            return []  # no existing experiment
        if endpoint == "/examples":
            return []  # no existing examples (upsert check)
        raise AssertionError(f"Unexpected dest get_paginated: {endpoint!r} params={params}")

    def dest_post(endpoint, data):
        """Mock destination POST."""
        if endpoint == "/datasets":
            return {"id": "new-ds-1"}
        if endpoint == "/sessions":
            captured_session_payload.update(data)
            return {"id": "new-exp-1"}
        if endpoint == "/runs/batch":
            captured_batches.append(data)
            return {"errors": []}
        raise AssertionError(f"Unexpected dest POST: {endpoint!r}")

    # -----------------------------------------------------------------------
    # Construct orchestrator and patch both clients
    # -----------------------------------------------------------------------
    state_manager = StateManager(state_dir=tmp_path / "state")
    config = _make_config()
    orchestrator = MigrationOrchestrator(config, state_manager)

    # Patch source client
    orchestrator.source_client.get = source_get
    orchestrator.source_client.get_paginated = source_get_paginated
    orchestrator.source_client.post = source_post

    # Patch dest client
    orchestrator.dest_client.get = dest_get
    orchestrator.dest_client.get_paginated = dest_get_paginated
    orchestrator.dest_client.post = dest_post

    # Suppress rate-limit sleep so the test runs fast
    orchestrator.source_client.rate_limit_delay = 0
    orchestrator.dest_client.rate_limit_delay = 0

    # -----------------------------------------------------------------------
    # Exercise
    # -----------------------------------------------------------------------
    orchestrator.migrate_datasets_parallel(
        [DATASET_ID],
        include_examples=True,
        include_experiments=True,
    )

    # -----------------------------------------------------------------------
    # At least one /runs/batch call must have been made
    # -----------------------------------------------------------------------
    assert captured_batches, "No /runs/batch call was captured"

    posted = captured_batches[0]["post"]
    assert len(posted) == 2, f"Expected 2 posted runs, got {len(posted)}: {posted}"

    a = next(r for r in posted if r["name"] == "a")
    b = next(r for r in posted if r["name"] == "b")

    # -----------------------------------------------------------------------
    # Assertion 1: Relative offsets preserved
    # -----------------------------------------------------------------------
    a_start = parse_iso(a["start_time"])
    b_start = parse_iso(b["start_time"])
    a_end = parse_iso(a["end_time"])

    offset_ab = b_start - a_start
    assert offset_ab == timedelta(seconds=30), (
        f"Expected 30s offset between run-a and run-b start times, got {offset_ab}"
    )
    duration_a = a_end - a_start
    assert duration_a == timedelta(seconds=5), (
        f"Expected 5s duration for run-a, got {duration_a}"
    )

    # -----------------------------------------------------------------------
    # Assertion 2: dotted_order timestamp aligns with start_time
    # -----------------------------------------------------------------------
    # dotted_order for a root run is a single segment: {timestamp}Z{uuid}
    a_dotted = a["dotted_order"]
    z_idx = a_dotted.rfind("Z")
    a_dotted_ts = a_dotted[: z_idx + 1]  # e.g. "20260203T000000000000Z" shifted
    # The date portion (first 8 chars: YYYYMMDD) must match the shifted start_time date
    assert a_dotted_ts[:8] == a_start.strftime("%Y%m%d"), (
        f"dotted_order date {a_dotted_ts[:8]!r} doesn't match start_time date "
        f"{a_start.strftime('%Y%m%d')!r}"
    )

    # -----------------------------------------------------------------------
    # Assertion 3: Newest run end is approximately "now"
    # -----------------------------------------------------------------------
    now = datetime.now(timezone.utc)
    b_end = parse_iso(b["end_time"])
    newest_end = max(a_end, b_end)
    skew = abs((newest_end - now).total_seconds())
    assert skew < 120, (
        f"Newest run end {newest_end} is {skew:.1f}s from now — expected < 120s"
    )

    # -----------------------------------------------------------------------
    # Assertion 4: All runs within 24h window
    # -----------------------------------------------------------------------
    earliest_start = min(a_start, b_start)
    age = (now - earliest_start).total_seconds()
    assert age < 24 * 3600, (
        f"Earliest run start {earliest_start} is {age / 3600:.2f}h old — must be < 24h"
    )

    # -----------------------------------------------------------------------
    # Assertion 5: Experiment session payload is also shifted
    # -----------------------------------------------------------------------
    assert captured_session_payload, "No /sessions POST payload was captured"
    sess_start = parse_iso(captured_session_payload["start_time"])
    sess_end = parse_iso(captured_session_payload["end_time"])
    assert sess_start.date() == now.date(), (
        f"Experiment start_time date {sess_start.date()} != today {now.date()}"
    )
    assert sess_end.date() == now.date(), (
        f"Experiment end_time date {sess_end.date()} != today {now.date()}"
    )
