"""Run queries use the v2 endpoint where an instance has it, and v1 otherwise."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError, EnhancedAPIClient, NotFoundError
from langsmith_migrator.core.migrators import ExperimentMigrator
from langsmith_migrator.core.run_query import REPLAY_SELECT, RunQuery, to_v1_run

ROOT = "11111111-1111-1111-1111-111111111111"
CHILD = "22222222-2222-2222-2222-222222222222"
V2 = "/api/v2/runs/query"


def _client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.base_url = "https://example.test/api/v1"
    client.session = Mock()
    client.session.headers = {}
    client.get.return_value = {"id": "exp-src", "start_time": "2026-03-01T00:00:00Z"}
    return client


def _v2_item(run_id: str = ROOT, dotted: str | None = None) -> dict:
    return {
        "id": run_id,
        "name": "run",
        "run_type": "chain",
        "project_id": "exp-src",
        "trace_id": ROOT,
        "start_time": "2026-03-01T00:00:01Z",
        "dotted_order": dotted or f"20260301T000001000000Z{run_id}",
        "manifest": {"k": "v"},
    }


def _v2_server(*pages):
    """POST handler for a server with the v2 run query; the first call is the probe."""
    remaining = list(pages)

    def post(endpoint, data=None):
        assert endpoint == V2, f"deprecated endpoint used: {endpoint}"
        if data.get("page_size") == 1 and data.get("selects") == ["ID"]:
            return {"items": [], "next_cursor": None}
        return remaining.pop(0)

    return post


def _v1_server(response):
    def post(endpoint, data=None):
        if endpoint == V2:
            raise NotFoundError(f"Not found: {endpoint}", status_code=404)
        return response

    return post


@pytest.fixture
def migrator(sample_config, migration_state):
    return ExperimentMigrator(_client(), _client(), migration_state, sample_config)


def test_to_v1_run_restores_the_field_names_migrators_read():
    child = to_v1_run(
        {
            **_v2_item(CHILD, f"20260301T000001000000Z{ROOT}.20260301T000002000000Z{CHILD}"),
            "parent_run_ids": [ROOT],
        }
    )
    assert child["session_id"] == "exp-src"
    assert child["parent_run_id"] == ROOT
    assert child["serialized"] == {"k": "v"}
    assert "parent_run_id" not in to_v1_run(_v2_item())


def test_v1_body_is_translated_for_the_v2_endpoint(migrator):
    migrator.source.post.side_effect = _v2_server({"items": [_v2_item()], "next_cursor": "c2"})

    runs, cursor = RunQuery(migrator).page(
        "source",
        {
            "session": ["exp-src"],
            "id": [ROOT],
            "select": ["id", "start_time", "trace_tier", "session_id"],
            "limit": 5000,
            "start_time": "2026-02-01T00:00:00+00:00",
            "trace_filter": "eq(is_root, true)",
            "cursor": "c1",
            "skip_pagination": False,
        },
    )

    assert cursor == "c2"
    assert runs[0]["session_id"] == "exp-src"
    assert migrator.source.post.call_args.args[1] == {
        "project_ids": ["exp-src"],
        "ids": [ROOT],
        "selects": ["ID", "START_TIME", "PROJECT_ID"],  # no v2 equivalent for trace_tier
        "page_size": 1000,
        "min_start_time": "2026-02-01T00:00:00+00:00",
        "trace_filter": "eq(is_root, true)",
        "cursor": "c1",
    }


def test_v2_query_without_a_start_time_is_bounded_by_the_project_start(migrator):
    migrator.source.post.side_effect = _v2_server({"items": [], "next_cursor": None})

    RunQuery(migrator).page("source", {"session": ["exp-src"]})

    body = migrator.source.post.call_args.args[1]
    assert body["min_start_time"].startswith("2026-02-28T00:00:00")  # a day before the project
    assert set(body["selects"]) >= {"INPUTS", "OUTPUTS", "MANIFEST", "PARENT_RUN_IDS"}
    assert len(body["selects"]) == len(REPLAY_SELECT)


def test_server_without_v2_gets_the_v1_request_unchanged(migrator):
    migrator.source.post.side_effect = _v1_server(
        {"runs": [{"id": ROOT}], "cursors": {"next": "c2"}}
    )
    body = {"session": ["exp-src"], "skip_pagination": False}
    query = RunQuery(migrator)

    assert query.page("source", body) == ([{"id": ROOT}], "c2")
    query.page("source", body)

    calls = migrator.source.post.call_args_list
    assert [c.args[0] for c in calls] == [V2, "/runs/query", "/runs/query"]  # probed once
    assert calls[1].args[1] == body


def test_missing_project_is_not_mistaken_for_a_server_without_v2(migrator):
    migrator.source.post.side_effect = NotFoundError("project not found", status_code=404)
    migrator.source.get.side_effect = NotFoundError("project not found", status_code=404)

    with pytest.raises(NotFoundError):
        RunQuery(migrator).uses_v2("source", "exp-gone")
    assert "runs_v2_supported" not in migrator.source.__dict__


def test_no_deprecated_fallback_refuses_the_v1_run_query(migrator):
    migrator.config.migration.no_deprecated_fallback = True
    migrator.source.post.side_effect = _v1_server({"runs": []})

    with pytest.raises(APIError, match="MIGRATION_NO_DEPRECATED_FALLBACK"):
        RunQuery(migrator).page("source", {"session": ["exp-src"]})
    assert [c.args[0] for c in migrator.source.post.call_args_list] == [V2]


def test_experiment_runs_replay_from_the_v2_endpoint(migrator, migration_state):
    child = _v2_item(CHILD, f"20260301T000001000000Z{ROOT}.20260301T000002000000Z{CHILD}")
    migrator.source.post.side_effect = _v2_server(
        {"items": [child, _v2_item()], "next_cursor": None}
    )
    migrator.dest.post.return_value = {}
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_runs"
    )

    total, mapping, failed = migrator.migrate_runs_streaming(
        ["exp-src"], {"experiments": {"exp-src": "exp-dst"}, "examples": {}}
    )

    assert (total, failed) == (2, 0)
    posted = {r["name"] + r["id"]: r for r in migrator.dest.post.call_args.args[1]["post"]}
    parent, kid = posted["run" + mapping[ROOT]], posted["run" + mapping[CHILD]]
    assert kid["parent_run_id"] == parent["id"]
    assert kid["session_id"] == "exp-dst"
    assert kid["serialized"] == {"k": "v"}
    assert migration_state.get_item("experiment_exp-src").metadata["run_cursor_api"] == "v2"


def test_saved_v1_cursor_is_dropped_when_the_source_now_serves_v2(migrator, migration_state):
    migrator.source.post.side_effect = _v2_server({"items": [], "next_cursor": None})
    migration_state.ensure_item(
        "experiment_exp-src",
        "experiment",
        "exp-1",
        "exp-src",
        stage="migrate_runs",
        metadata={"run_cursor": "v1-cursor"},
    )

    migrator.migrate_runs_streaming(
        ["exp-src"], {"experiments": {"exp-src": "exp-dst"}, "examples": {}}
    )

    assert "cursor" not in migrator.source.post.call_args.args[1]


def test_empty_fields_come_back_the_way_v1_returns_them(migrator):
    """Content checks compare a v1 source with a v2 destination, so empties must agree."""
    item = {**_v2_item(), "manifest": {}}
    migrator.source.post.side_effect = _v2_server({"items": [item], "next_cursor": None})

    runs, _ = RunQuery(migrator).page(
        "source", {"session": ["exp-src"], "select": ["id", "events", "serialized"]}
    )

    assert runs[0]["events"] == []
    assert runs[0]["serialized"] is None
