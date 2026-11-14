"""The trace migrator against instances that serve the v2 run query."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators.trace import TraceMigrator
from langsmith_migrator.core.run_query import to_v1_run
from langsmith_migrator.core.trace_domain import Window

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
WINDOW = Window(NOW - timedelta(days=1), NOW)
V2 = "/api/v2/runs/query"


def _client(tier: str) -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    client.runs_v2_supported = True
    client.get.return_value = {"id": "p", "name": "proj", "trace_tier": tier}
    return client


def _item(rid: str) -> dict:
    return {
        "id": rid,
        "trace_id": rid,
        "start_time": "2026-08-25T10:00:00Z",
        "project_id": "src",
        "dotted_order": f"20260825T100000000000Z{rid}",
    }


def _migrator(sample_config, state=None, *, source_tier="longlived", dest_tier="longlived"):
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        m = TraceMigrator(
            _client(source_tier),
            _client(dest_tier),
            state,
            sample_config,
            range_start=NOW - timedelta(days=30),
            range_end=NOW,
        )
    m.dest_ls_client = Mock()
    return m


def test_long_lived_project_on_v2_keeps_every_run(sample_config):
    m = _migrator(sample_config)
    m.source.post.return_value = {"items": [_item("a"), _item("b")], "next_cursor": None}

    runs = m.slice_runs(m.source, "src", WINDOW)

    assert [r["id"] for r in runs] == ["a", "b"]
    endpoint, body = m.source.post.call_args.args
    assert endpoint == V2
    assert body["project_ids"] == ["src"]
    assert body["selects"] == ["ID", "TRACE_ID", "START_TIME"]  # no per-run tier on v2
    assert "trace_filter" in body and "min_start_time" in body


def test_short_lived_project_on_v2_is_not_queried(sample_config):
    m = _migrator(sample_config, source_tier="shortlived")

    assert m.slice_runs(m.source, "src", WINDOW) == []
    m.source.post.assert_not_called()


def test_tier_filter_off_ignores_the_project_tier(sample_config):
    m = _migrator(sample_config, dest_tier="shortlived")
    m.dest.post.return_value = {"items": [_item("a")], "next_cursor": None}

    assert m.long_lived_run_ids(m.dest, "dst", WINDOW, tier_filter=False) == {"a"}


def test_skipped_short_lived_project_is_reported_once(sample_config):
    m = _migrator(sample_config, source_tier="shortlived")
    source, target = {"id": "src", "name": "proj"}, {"id": "dst"}

    first = m.prepare(source, target, WINDOW, set)
    second = m.prepare(source, target, WINDOW, set)

    assert [issue[1] for issue in first.issues] == ["shortlived_project_not_migrated"]
    assert first.issues[0][0] == "degraded"
    assert second.issues == []


def test_v2_attachments_become_v1_s3_urls():
    run = to_v1_run({**_item("a"), "attachments": {"report.pdf": "https://blob.test/x"}})

    assert run["s3_urls"] == {"attachment.report.pdf": {"presigned_url": "https://blob.test/x"}}
    assert "attachments" not in run  # on a run payload that key means downloaded bytes


def test_canary_reads_the_run_back_through_the_v2_query(sample_config):
    m = _migrator(sample_config)
    m.ingest = Mock(return_value=[])
    m._scratch_session = Mock(return_value={"id": "scratch"})
    stamp = m.resolved_range_start()
    m.dest.post.side_effect = lambda endpoint, body: {
        "items": [{"id": body["ids"][0], "start_time": stamp.isoformat(), "project_id": "scratch"}],
        "next_cursor": None,
    }

    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.canary()

    endpoint, body = m.dest.post.call_args.args
    assert endpoint == V2
    assert body["project_ids"] == ["scratch"]
    assert body["min_start_time"] < stamp.isoformat()
    assert not any("/runs/" in str(c.args[0]) for c in m.dest.get.call_args_list)


def test_missing_run_diagnosis_skips_the_lookup_v2_cannot_do(sample_config):
    m = _migrator(sample_config)

    code, _ = m._diagnose_missing("dst", {"a", "b"})

    assert code == "run_not_ingested"
    m.dest.get.assert_not_called()
