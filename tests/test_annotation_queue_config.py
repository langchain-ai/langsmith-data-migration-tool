"""Tests for copying an annotation queue's rubric, feedback configs and reviewers."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError, EnhancedAPIClient
from langsmith_migrator.core.migrators import AnnotationQueueMigrator

CONFIG = {"type": "categorical", "categories": [{"value": 0, "label": "bad"}, {"value": 1, "label": "good"}]}

SOURCE_QUEUE = {
    "id": "src-q",
    "name": "Review",
    "rubric_instructions": "Label carefully",
    "reviewer_access_mode": "assigned",
    "rubric_items": [
        {
            "feedback_key": "quality",
            "description": "Overall quality",
            "is_required": True,
            "feedback_config_id": "src-config",
            "feedback_config": {
                "id": "src-config",
                "feedback_key": "quality",
                "feedback_config": CONFIG,
                "is_lower_score_better": False,
                "tenant_id": "src-ws",
            },
        }
    ],
    "assigned_reviewers": [
        {"id": "src-identity-1", "email": "Reviewer.One@example.com", "name": "One"},
        {"id": "src-identity-2", "email": "gone@example.com", "name": "Gone"},
    ],
}


def _client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


@pytest.fixture
def migrator(sample_config):
    source, dest = _client(), _client()
    m = AnnotationQueueMigrator(source, dest, None, sample_config)
    # The queue list leaves out the rubric; only the single-queue read has it.
    source.get.side_effect = lambda endpoint, params=None: dict(SOURCE_QUEUE)
    dest.get.side_effect = lambda endpoint, params=None: []

    def paginated(endpoint, **kwargs):
        if endpoint == "/workspaces/current/members/active":
            return iter([{"id": "dst-identity-1", "email": "reviewer.one@example.com"}])
        return iter([])

    dest.get_paginated.side_effect = paginated
    dest.post.side_effect = lambda endpoint, body: {"id": "dst-q"}
    return m


def _posts(migrator, endpoint):
    return [c.args[1] for c in migrator.dest.post.call_args_list if c.args[0] == endpoint]


def test_create_copies_rubric_access_mode_and_reviewers(migrator):
    listed = {"id": "src-q", "name": "Review"}

    assert migrator.create_queue(listed) == "dst-q"

    [queue] = _posts(migrator, "/annotation-queues")
    assert queue["rubric_instructions"] == "Label carefully"
    assert queue["reviewer_access_mode"] == "assigned"
    assert queue["rubric_items"] == [
        {"feedback_key": "quality", "description": "Overall quality", "is_required": True}
    ]
    assert _posts(migrator, "/feedback-configs") == [
        {"feedback_key": "quality", "feedback_config": CONFIG, "is_lower_score_better": False}
    ]
    assert _posts(migrator, "/annotation-queues/dst-q/reviewers") == [
        {"identity_id": "dst-identity-1"}
    ]


def test_existing_feedback_config_is_not_recreated(migrator):
    migrator.dest.get.side_effect = lambda endpoint, params=None: (
        [{"feedback_key": "quality"}] if endpoint == "/feedback-configs" else []
    )

    migrator.create_queue({"id": "src-q", "name": "Review"})

    assert _posts(migrator, "/feedback-configs") == []


def test_update_copies_rubric_access_mode_and_reviewers(migrator):
    def paginated(endpoint, **kwargs):
        if endpoint == "/annotation-queues":
            return iter([{"id": "dst-existing", "name": "Review"}])
        return iter([{"id": "dst-identity-1", "email": "reviewer.one@example.com"}])

    migrator.dest.get_paginated.side_effect = paginated

    assert migrator.create_queue({"id": "src-q", "name": "Review"}) == "dst-existing"

    patch = migrator.dest.patch.call_args.args
    assert patch[0] == "/annotation-queues/dst-existing"
    assert patch[1]["reviewer_access_mode"] == "assigned"
    assert patch[1]["rubric_items"][0]["feedback_key"] == "quality"
    assert "feedback_config_id" not in patch[1]["rubric_items"][0]
    assert _posts(migrator, "/annotation-queues/dst-existing/reviewers") == [
        {"identity_id": "dst-identity-1"}
    ]


def test_unreadable_queue_keeps_the_listed_fields(migrator):
    migrator.source.get.side_effect = APIError("forbidden", status_code=403)

    assert migrator.create_queue({"id": "src-q", "name": "Review"}) == "dst-q"

    [queue] = _posts(migrator, "/annotation-queues")
    assert queue["rubric_items"] == []
    assert queue["reviewer_access_mode"] == "any"
