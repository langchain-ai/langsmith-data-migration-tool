"""Chart same-instance mode decision tests."""

import pytest

from langsmith_migrator.utils.chart_mode import (
    normalize_deployment_url,
    should_reuse_chart_ids,
    workspace_pair_allows_same_instance,
)
from langsmith_migrator.utils.config import Config


def _config(
    *,
    source_url: str = "https://same.example",
    dest_url: str = "https://same.example/api/v1",
    source_key: str = "shared-key",
    dest_key: str = "shared-key",
) -> Config:
    return Config(
        source_api_key=source_key,
        dest_api_key=dest_key,
        source_url=source_url,
        dest_url=dest_url,
    )


@pytest.mark.parametrize(
    ("source_url", "dest_url", "expected"),
    [
        ("https://same.example", "https://same.example/", True),
        ("https://same.example", "https://same.example/api/v1", True),
        ("https://same.example/api/v2/", "https://same.example", True),
        ("https://SAME.example/api/v1/", "https://same.example/api/v2", True),
        ("https://source.example", "https://dest.example", False),
    ],
)
def test_normalize_deployment_url_permutations(source_url, dest_url, expected):
    """Deployment comparison should ignore API suffixes, trailing slashes, and case."""

    assert (
        normalize_deployment_url(source_url) == normalize_deployment_url(dest_url)
    ) is expected


@pytest.mark.parametrize(
    ("source_ws", "dest_ws", "expected"),
    [
        (None, None, True),
        ("", "", True),
        ("shared-ws", "shared-ws", True),
        ("src-ws", "dst-ws", False),
        ("src-ws", None, False),
        (None, "dst-ws", False),
        ("src-ws", "", False),
        ("", "dst-ws", False),
    ],
)
def test_workspace_pair_allows_same_instance_permutations(
    source_ws,
    dest_ws,
    expected,
):
    """Workspace scoping must only allow ID reuse for identical or unscoped pairs."""

    assert workspace_pair_allows_same_instance(source_ws, dest_ws) is expected


@pytest.mark.parametrize(
    (
        "source_url",
        "dest_url",
        "source_key",
        "dest_key",
        "source_ws",
        "dest_ws",
        "expected",
    ),
    [
        (
            "https://same.example",
            "https://same.example/api/v1",
            "shared-key",
            "shared-key",
            None,
            None,
            True,
        ),
        (
            "https://same.example",
            "https://same.example/api/v1",
            "shared-key",
            "shared-key",
            "shared-ws",
            "shared-ws",
            True,
        ),
        (
            "https://same.example",
            "https://same.example/api/v1",
            "shared-key",
            "shared-key",
            "src-ws",
            "dst-ws",
            False,
        ),
        (
            "https://same.example",
            "https://same.example/api/v1",
            "src-key",
            "dst-key",
            "shared-ws",
            "shared-ws",
            True,
        ),
        (
            "https://same.example",
            "https://same.example/api/v1",
            "src-key",
            "dst-key",
            "src-ws",
            "dst-ws",
            False,
        ),
        (
            "https://same.example",
            "https://same.example/api/v1",
            "src-key",
            "dst-key",
            None,
            None,
            False,
        ),
        (
            "https://source.example",
            "https://dest.example",
            "shared-key",
            "shared-key",
            "shared-ws",
            "shared-ws",
            False,
        ),
    ],
)
def test_should_reuse_chart_ids_permutations(
    source_url,
    dest_url,
    source_key,
    dest_key,
    source_ws,
    dest_ws,
    expected,
):
    """Chart ID reuse is safe only for same deployment plus same key or workspace."""

    assert (
        should_reuse_chart_ids(
            _config(
                source_url=source_url,
                dest_url=dest_url,
                source_key=source_key,
                dest_key=dest_key,
            ),
            source_ws,
            dest_ws,
        )
        is expected
    )
