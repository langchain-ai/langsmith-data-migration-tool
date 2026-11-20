"""Tests for copying feedback configs to the destination."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError, EnhancedAPIClient
from langsmith_migrator.core.migrators import FeedbackConfigMigrator
from langsmith_migrator.core.migrators.feedback_config import config_definition

SCALE_0_5 = {"type": "continuous", "min": 0, "max": 5}
SCALE_0_1 = {"type": "continuous", "min": 0, "max": 1}


def _client(configs) -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    client.get_paginated.side_effect = lambda *a, **k: iter(configs)
    return client


def _config(key, definition, **extra):
    return {"feedback_key": key, "feedback_config": definition, "is_lower_score_better": False, **extra}


@pytest.fixture
def make(sample_config):
    def build(source, dest):
        return FeedbackConfigMigrator(_client(source), _client(dest), None, sample_config)
    return build


def _created(migrator):
    return [c.args[1] for c in migrator.dest.post.call_args_list]


def test_copies_configs_the_destination_lacks(make):
    migrator = make(
        [_config("care_axis", SCALE_0_5), _config("quality", SCALE_0_1), _config("gone", SCALE_0_1, is_deleted=True)],
        [_config("quality", {**SCALE_0_1, "categories": None, "allows_multiple_categories": False})],
    )

    result = migrator.migrate_all()

    assert result == {"created": 1, "existing": 1, "failed": 0, "errors": []}
    assert _created(migrator) == [
        {"feedback_key": "care_axis", "feedback_config": SCALE_0_5, "is_lower_score_better": False}
    ]


def test_same_key_with_another_scale_is_copied(make):
    migrator = make([_config("quality", SCALE_0_5)], [_config("quality", SCALE_0_1)])

    assert migrator.migrate_all()["created"] == 1


def test_enforced_config_falls_back_to_plain_when_refused(make):
    migrator = make([_config("quality", SCALE_0_5, is_enforced=True)], [])
    migrator.dest.post.side_effect = [APIError("forbidden", status_code=403), {}]

    result = migrator.migrate_all()

    assert result["created"] == 1
    assert [body.get("is_enforced") for body in _created(migrator)] == [True, None]


def test_failed_create_is_reported(make):
    migrator = make([_config("quality", SCALE_0_5)], [])
    migrator.dest.post.side_effect = APIError("bad config", status_code=422)

    result = migrator.migrate_all()

    assert result["failed"] == 1
    assert result["errors"] == [("quality", "bad config")]


def test_legacy_string_definition_is_read():
    assert config_definition('{"type": "continuous", "min": 0, "max": 5}') == SCALE_0_5


def test_first_config_is_created_first_and_the_enforced_one_last(make):
    """The first created config becomes the key's default, and enforcing a key
    makes the destination refuse its other scales, so order matters."""
    scale_0_10 = {"type": "continuous", "min": 0, "max": 10}
    migrator = make(
        [  # newest first, as the source lists them
            _config("quality", scale_0_10, is_enforced=True, modified_at="2026-03-01"),
            _config("quality", SCALE_0_5, modified_at="2026-02-01"),
            _config("quality", SCALE_0_1, is_first=True, modified_at="2026-01-01"),
        ],
        [],
    )

    migrator.migrate_all()

    assert [(b["feedback_config"]["max"], b.get("is_enforced")) for b in _created(migrator)] == [
        (1, None),
        (5, None),
        (10, True),
    ]
