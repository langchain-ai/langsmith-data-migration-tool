"""The resume hint must redo about one ingest batch, not the whole span."""

import datetime as dt
from unittest.mock import patch

import langsmith_migrator.cli.main as cli_main
from langsmith_migrator.core.trace_domain import Reconciliation


def _watermark(verified_runs, span_hours, batch_runs):
    """Render the hint for a clean project and return (text, earliest, latest)."""
    latest = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)
    earliest = latest - dt.timedelta(hours=span_hours)
    report = Reconciliation(
        "S",
        "D",
        "",
        verified_runs,
        verified_runs,
        0,
        0,
        0,
        earliest=earliest.isoformat(),
        latest=latest.isoformat(),
        verified_runs=verified_runs,
    )
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(report, batch_runs)
    return "\n".join(printed), earliest, latest


def _resume_instant(text):
    """The hint now carries an absolute stamp, which cannot drift."""
    return dt.datetime.fromisoformat(text.split("--since ")[1].split()[0])


def test_the_overlap_is_one_batch_not_the_whole_span():
    # 1000 runs spread over 10h, batches of 100 -> one batch occupied ~1h
    text, earliest, latest = _watermark(1000, span_hours=10, batch_runs=100)
    assert "one ingest batch" in text
    resume = _resume_instant(text)
    redone_hours = (latest - resume).total_seconds() / 3600
    assert 0.9 < redone_hours < 1.1, f"redid {redone_hours}h, expected ~1h"
    assert resume > earliest, "must not re-walk the whole verified span"


def test_a_span_smaller_than_one_batch_resumes_at_the_earliest():
    # Redoing everything *is* one batch here, so there is nothing to trim.
    text, earliest, _ = _watermark(50, span_hours=10, batch_runs=100)
    assert "the whole span" in text
    assert _resume_instant(text) == earliest


def test_a_project_with_blocked_runs_claims_no_watermark():
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(Reconciliation("S", "D", "", 2, 1, 0, 0, 1), 100)
    text = "\n".join(printed)
    assert "no verified watermark" in text
    assert "--since" not in text


def test_window_durations_read_as_wall_clock():
    """The pre-flight prints the window in units an operator thinks in."""
    h = cli_main._human_duration  # takes hours, since --window does
    assert h(0.24) == "14m24s"
    assert h(2.4) == "2h24m"
    assert h(24.0) == "1d"
    assert h(60.0) == "2d12h"
    assert h(2 + 5 / 60 + 4 / 3600) == "2h5m4s"
    assert h(1 / 3600) == "1s"
    assert h(0) == "0s"
