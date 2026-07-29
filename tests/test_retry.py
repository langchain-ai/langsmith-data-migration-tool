"""Tests for the retry_on_failure decorator's retry policy."""

from __future__ import annotations

import pytest
import requests

from langsmith_migrator.utils.retry import (
    APIError,
    AuthenticationError,
    ConflictError,
    RateLimitError,
    UpstreamRejectionError,
    retry_on_failure,
)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Keep backoff from actually sleeping."""
    monkeypatch.setattr("time.sleep", lambda _seconds: None)


def _counting_raiser(exc: Exception):
    """Return a decorated callable that always raises, plus its call counter."""
    calls = {"count": 0}

    @retry_on_failure(max_retries=3)
    def call():
        calls["count"] += 1
        raise exc

    return call, calls


@pytest.mark.parametrize(
    "exc",
    [
        UpstreamRejectionError("proxy said no", status_code=403),
        UpstreamRejectionError("proxy said no", status_code=401),
        RateLimitError("slow down"),
        APIError("server blew up", status_code=503),
        requests.exceptions.ConnectionError("dns failed"),
        requests.exceptions.ReadTimeout("too slow"),
    ],
)
def test_transient_failures_are_retried(exc):
    call, calls = _counting_raiser(exc)

    with pytest.raises(type(exc)):
        call()

    assert calls["count"] == 3


@pytest.mark.parametrize(
    "exc",
    [
        AuthenticationError("bad key", status_code=401),
        AuthenticationError("no permission", status_code=403),
        ConflictError("already exists"),
        APIError("bad request", status_code=400),
        APIError("no status code at all"),
    ],
)
def test_terminal_failures_are_not_retried(exc):
    call, calls = _counting_raiser(exc)

    with pytest.raises(type(exc)):
        call()

    assert calls["count"] == 1


def test_upstream_rejection_succeeds_on_a_later_attempt():
    """The whole point: a transient edge refusal should not lose the work."""
    calls = {"count": 0}

    @retry_on_failure(max_retries=3)
    def call():
        calls["count"] += 1
        if calls["count"] < 3:
            raise UpstreamRejectionError("proxy said no", status_code=403)
        return {"ok": True}

    assert call() == {"ok": True}
    assert calls["count"] == 3


def test_upstream_rejection_is_not_caught_by_the_auth_clause():
    """Guards the subclassing: it must derive from APIError, not AuthenticationError."""
    assert issubclass(UpstreamRejectionError, APIError)
    assert not issubclass(UpstreamRejectionError, AuthenticationError)


def test_rate_limit_honors_retry_after(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", slept.append)
    call, _calls = _counting_raiser(RateLimitError("slow down", retry_after=7.0))

    with pytest.raises(RateLimitError):
        call()

    assert slept[0] == 7.0


def test_backoff_is_capped(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", slept.append)
    call, _calls = _counting_raiser(RateLimitError("slow down", retry_after=9999.0))

    with pytest.raises(RateLimitError):
        call()

    assert all(delay <= 60.0 for delay in slept)
