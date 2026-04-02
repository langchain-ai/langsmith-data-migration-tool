"""Tests for PaginationHelper."""

import pytest

from langsmith_migrator.utils.pagination import PaginationHelper
from langsmith_migrator.utils.retry import APIError
from langsmith_migrator.core.api_client import NotFoundError


class TestPaginationHelper:
    """Tests for PaginationHelper.paginate."""

    def test_single_page(self):
        def fetch_fn(endpoint, params):
            return [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert len(items) == 2
        assert items[0]["id"] == "1"

    def test_multiple_pages(self):
        pages = [
            [{"id": "1"}, {"id": "2"}],
            [{"id": "3"}, {"id": "4"}],
            [{"id": "5"}],
        ]
        call_count = 0

        def fetch_fn(endpoint, params):
            nonlocal call_count
            page = pages[call_count]
            call_count += 1
            return page

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=2))
        assert len(items) == 5
        assert [i["id"] for i in items] == ["1", "2", "3", "4", "5"]

    def test_empty_response_stops(self):
        def fetch_fn(endpoint, params):
            return []

        items = list(PaginationHelper.paginate(fetch_fn, "/test"))
        assert items == []

    def test_dict_response_with_items_key(self):
        def fetch_fn(endpoint, params):
            return {"items": [{"id": "1"}], "total": 1}

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert len(items) == 1

    def test_dict_response_with_data_key(self):
        def fetch_fn(endpoint, params):
            return {"data": [{"id": "1"}]}

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert len(items) == 1

    def test_dict_response_with_results_key(self):
        def fetch_fn(endpoint, params):
            return {"results": [{"id": "1"}]}

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert len(items) == 1

    def test_dict_response_with_no_known_key_returns_empty(self):
        def fetch_fn(endpoint, params):
            return {"unknown_key": [{"id": "1"}]}

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert items == []

    def test_dict_response_with_null_items_returns_empty(self):
        def fetch_fn(endpoint, params):
            return {"items": None}

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=10))
        assert items == []

    def test_duplicate_detection(self):
        pages = [
            [{"id": "1"}, {"id": "2"}],
            [{"id": "2"}, {"id": "3"}],
            [{"id": "4"}],
        ]
        call_count = 0

        def fetch_fn(endpoint, params):
            nonlocal call_count
            page = pages[call_count]
            call_count += 1
            return page

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=2))
        assert [i["id"] for i in items] == ["1", "2", "3", "4"]

    def test_all_duplicates_stops_pagination(self):
        def fetch_fn(endpoint, params):
            return [{"id": "1"}, {"id": "2"}]

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=2))
        assert len(items) == 2

    def test_not_found_error_stops_gracefully(self):
        call_count = 0

        def fetch_fn(endpoint, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [{"id": "1"}, {"id": "2"}]
            raise NotFoundError("not found", status_code=404)

        items = list(PaginationHelper.paginate(fetch_fn, "/test", page_size=2))
        assert len(items) == 2

    def test_api_error_propagates(self):
        def fetch_fn(endpoint, params):
            raise APIError("server error", status_code=500)

        with pytest.raises(APIError, match="server error"):
            list(PaginationHelper.paginate(fetch_fn, "/test"))

    def test_generic_exception_propagates(self):
        def fetch_fn(endpoint, params):
            raise RuntimeError("unexpected")

        with pytest.raises(RuntimeError, match="unexpected"):
            list(PaginationHelper.paginate(fetch_fn, "/test"))

    def test_params_not_mutated(self):
        original_params = {"filter": "active"}

        def fetch_fn(endpoint, params):
            return [{"id": "1"}]

        list(PaginationHelper.paginate(fetch_fn, "/test", params=original_params, page_size=10))
        assert original_params == {"filter": "active"}

    def test_none_params_handled(self):
        def fetch_fn(endpoint, params):
            return [{"id": "1"}]

        items = list(PaginationHelper.paginate(fetch_fn, "/test", params=None, page_size=10))
        assert len(items) == 1


class TestExtractItems:
    """Tests for PaginationHelper._extract_items."""

    def test_list_response(self):
        assert PaginationHelper._extract_items([1, 2, 3]) == [1, 2, 3]

    def test_dict_with_items(self):
        assert PaginationHelper._extract_items({"items": [1, 2]}) == [1, 2]

    def test_dict_with_data(self):
        assert PaginationHelper._extract_items({"data": [1]}) == [1]

    def test_dict_with_results(self):
        assert PaginationHelper._extract_items({"results": [1]}) == [1]

    def test_items_takes_precedence_over_data(self):
        assert PaginationHelper._extract_items({"items": [1], "data": [2]}) == [1]

    def test_dict_with_null_items(self):
        assert PaginationHelper._extract_items({"items": None}) == []

    def test_dict_with_no_known_keys(self):
        assert PaginationHelper._extract_items({"foo": [1]}) == []

    def test_non_list_non_dict(self):
        assert PaginationHelper._extract_items("string") == []
        assert PaginationHelper._extract_items(42) == []
        assert PaginationHelper._extract_items(None) == []

    def test_empty_list(self):
        assert PaginationHelper._extract_items([]) == []

    def test_empty_dict(self):
        assert PaginationHelper._extract_items({}) == []
