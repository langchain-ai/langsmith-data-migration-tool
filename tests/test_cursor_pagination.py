"""Tests for CursorPaginationHelper."""

from unittest.mock import Mock

from langsmith_migrator.utils.pagination import CursorPaginationHelper


def test_paginate_single_page():
    """Single page with no next_cursor should yield all items and stop."""
    fetch_fn = Mock(return_value={
        "items": [{"id": "1"}, {"id": "2"}],
        "next_cursor": None,
    })

    results = list(CursorPaginationHelper.paginate(fetch_fn, "/test"))

    assert len(results) == 2
    assert results[0]["id"] == "1"
    assert results[1]["id"] == "2"
    fetch_fn.assert_called_once()


def test_paginate_multi_page():
    """Multiple pages should follow next_cursor until it is absent."""
    call_cursors = []

    def tracking_fetch(endpoint, params):
        call_cursors.append(params.get("cursor"))
        if len(call_cursors) == 1:
            return {"items": [{"id": "1"}], "next_cursor": "cursor1"}
        elif len(call_cursors) == 2:
            return {"items": [{"id": "2"}], "next_cursor": "cursor2"}
        else:
            return {"items": [{"id": "3"}], "next_cursor": None}

    results = list(CursorPaginationHelper.paginate(tracking_fetch, "/test"))

    assert len(results) == 3
    assert [r["id"] for r in results] == ["1", "2", "3"]
    assert call_cursors == [None, "cursor1", "cursor2"]


def test_paginate_empty_response():
    """Empty items list should stop immediately."""
    fetch_fn = Mock(return_value={"items": [], "next_cursor": None})

    results = list(CursorPaginationHelper.paginate(fetch_fn, "/test"))

    assert len(results) == 0
    fetch_fn.assert_called_once()


def test_paginate_deduplicates_by_id():
    """Duplicate IDs across pages should be deduplicated."""
    responses = [
        {"items": [{"id": "1"}, {"id": "2"}], "next_cursor": "c1"},
        {"items": [{"id": "2"}, {"id": "3"}], "next_cursor": None},
    ]
    fetch_fn = Mock(side_effect=responses)

    results = list(CursorPaginationHelper.paginate(fetch_fn, "/test"))

    assert len(results) == 3
    assert [r["id"] for r in results] == ["1", "2", "3"]


def test_paginate_stops_on_error():
    """An exception from fetch_fn should stop iteration."""
    fetch_fn = Mock(side_effect=Exception("API error"))

    results = list(CursorPaginationHelper.paginate(fetch_fn, "/test"))

    assert len(results) == 0
