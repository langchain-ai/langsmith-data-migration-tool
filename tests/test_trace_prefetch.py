"""Look-ahead retrieval must not weaken the ordering guarantee.

Concurrent preparation is only safe because committing stays serial and
oldest-first: that is what makes "a failure leaves no gap" true, and what lets
a clean window print a watermark. These tests pin that, not the speedup.
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators.trace import SlicePrepared, TraceMigrator
from langsmith_migrator.core.trace_domain import Reconciliation, plan_slice

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _client():
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _migrator(sample_config, **kwargs):
    kwargs.setdefault("range_start", NOW - timedelta(days=6))
    kwargs.setdefault("range_end", NOW)
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        m = TraceMigrator(_client(), _client(), None, sample_config, **kwargs)
    m.dest_ls_client = Mock()
    m.resolve_dest_session = Mock(return_value={"id": "dst", "trace_tier": "longlived"})
    m.ensure_long_lived = Mock()
    m.restore_tier = Mock()
    return m


def _empty(window):
    return SlicePrepared(window, "src", "dst", plan_slice(set(), set()), [])


def _labels(n_windows):
    """Window labels in the order they must be committed."""
    from langsmith_migrator.core.trace_domain import iter_windows

    return [w.label() for w in iter_windows(NOW - timedelta(days=n_windows), NOW, 24.0)]


def _drive(m, n_windows=6):
    """Walk n windows of one day each, recording commit order."""
    m._resolved_start = NOW - timedelta(days=n_windows)
    m._resolved_end = NOW
    m.window_hours = 24.0
    return m.migrate_session({"id": "src", "name": "p", "trace_tier": "longlived"})


# --------------------------------------------------------------------------
# Ordering
# --------------------------------------------------------------------------
def test_commits_run_in_window_order_however_preparation_finishes(sample_config):
    """The slowest window to prepare must still commit in its own place."""
    m = _migrator(sample_config, prefetch_windows=4)
    labels = _labels(6)
    order = []

    def prepare(_s, _d, window):
        # earliest windows made slowest, so completion order fights commit order
        time.sleep(0.04 * (len(labels) - labels.index(window.label())))
        return _empty(window)

    m.prepare_slice = Mock(side_effect=prepare)
    m.commit_slice = Mock(
        side_effect=lambda p: (
            order.append(p.window.label())
            or Reconciliation("src", "dst", p.window.label(), 0, 0, 0, 0, 0)
        )
    )
    _drive(m)
    assert order == labels


def test_preparation_is_actually_concurrent(sample_config):
    m = _migrator(sample_config, prefetch_windows=4)
    live, peak = [], []
    lock = threading.Lock()

    def prepare(_s, _d, window):
        with lock:
            live.append(1)
            peak.append(len(live))
        time.sleep(0.05)
        with lock:
            live.pop()
        return _empty(window)

    m.prepare_slice = Mock(side_effect=prepare)
    m.commit_slice = Mock(
        side_effect=lambda p: Reconciliation("src", "dst", p.window.label(), 0, 0, 0, 0, 0)
    )
    _drive(m)
    assert max(peak) > 1, "windows were prepared one at a time"


def test_look_ahead_is_bounded_by_prefetch_windows(sample_config):
    """Unbounded look-ahead would buffer the whole range in memory.

    The bound is prefetch_windows in flight plus the one being committed: the
    next window is submitted before the commit so readers stay busy during it.
    """
    m = _migrator(sample_config, prefetch_windows=2)
    prepared, committed = [], []

    def prepare(_s, _d, window):
        prepared.append(window.label())
        return _empty(window)

    def commit(p):
        # never more than prefetch_windows ahead of what has been committed
        assert len(prepared) - len(committed) <= 3, (len(prepared), len(committed))
        committed.append(p.window.label())
        return Reconciliation("src", "dst", p.window.label(), 0, 0, 0, 0, 0)

    m.prepare_slice = Mock(side_effect=prepare)
    m.commit_slice = Mock(side_effect=commit)
    _drive(m, n_windows=8)
    assert len(committed) == 8


# --------------------------------------------------------------------------
# Failure leaves a clean prefix, not a hole
# --------------------------------------------------------------------------
def test_a_failure_commits_every_earlier_window_and_nothing_after(sample_config):
    m = _migrator(sample_config, prefetch_windows=4)
    committed = []

    def commit(p):
        if len(committed) == 2:
            raise RuntimeError("ingest died")
        committed.append(p.window.label())
        return Reconciliation("src", "dst", p.window.label(), 0, 0, 0, 0, 0)

    m.prepare_slice = Mock(side_effect=lambda s, d, w: _empty(w))
    m.commit_slice = Mock(side_effect=commit)
    with pytest.raises(RuntimeError, match="ingest died"):
        _drive(m, n_windows=6)
    assert len(committed) == 2  # a prefix, in order, with no gap after it


def test_a_preparation_failure_surfaces_at_its_own_window(sample_config):
    """A worker exception must not be swallowed, nor reordered past its window."""
    m = _migrator(sample_config, prefetch_windows=4)
    labels = _labels(6)
    committed = []

    def prepare(_s, _d, window):
        # keyed to the window, not to arrival order: with N readers, which
        # window starts third is not deterministic
        if window.label() == labels[2]:
            raise RuntimeError("source query blew up")
        return _empty(window)

    m.prepare_slice = Mock(side_effect=prepare)
    m.commit_slice = Mock(
        side_effect=lambda p: (
            committed.append(p.window.label())
            or Reconciliation("src", "dst", p.window.label(), 0, 0, 0, 0, 0)
        )
    )
    with pytest.raises(RuntimeError, match="source query blew up"):
        _drive(m, n_windows=6)
    # the two earlier windows are committed; the walk stops at the one that failed
    assert committed == labels[:2]


def test_a_raised_tier_is_still_accounted_for_when_a_prefetched_walk_fails(sample_config):
    """A raise left in place silently changes a project's retention.

    An unfinished walk deliberately keeps the tier raised so a re-run can still
    ingest long-lived - but it must say so. That accounting lives in a
    ``finally`` and has to survive the executor.
    """
    m = _migrator(sample_config, prefetch_windows=4, restore_session_tier=True)
    dest = {"id": "dst", "trace_tier": "shortlived"}
    m.resolve_dest_session = Mock(return_value=dest)
    # the real one mutates the dict on a genuine raise; that is the signal
    # _settle_tier reads to mean "we raised this"
    m.ensure_long_lived = Mock(side_effect=lambda s: s.update(trace_tier="longlived"))
    m.record_issue = Mock()
    m.prepare_slice = Mock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        _drive(m)
    m.ensure_long_lived.assert_called_once()
    codes = [c.args[1] for c in m.record_issue.call_args_list]
    assert "dest_tier_left_raised" in codes
    m.restore_tier.assert_not_called()


# --------------------------------------------------------------------------
# Serial parity
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# The parallel half must not touch shared state
# --------------------------------------------------------------------------


def test_ingest_refuses_to_run_off_the_main_thread(sample_config):
    """Serial, ordered ingest is load-bearing; a stray thread must not slip in."""
    m = _migrator(sample_config, compress_level=None)
    m.dest_ls_client.multipart_ingest.return_value = None
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(1) as pool:
        with pytest.raises(AssertionError, match="main thread"):
            pool.submit(m.ingest, [{"id": "a", "trace_id": "t"}]).result()
