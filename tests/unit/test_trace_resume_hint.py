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
        "S", "D", "", verified_runs, verified_runs, 0, 0, 0,
        earliest=earliest.isoformat(), latest=latest.isoformat(), verified_runs=verified_runs,
    )
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(report, batch_runs)
    return "\n".join(printed), earliest, latest


def _resume_instant(text):
    """The hint now carries an absolute stamp, which cannot drift."""
    return dt.datetime.fromisoformat(text.split("--max-age-stamp ")[1].split()[0])


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


def test_a_denser_project_gets_a_smaller_overlap():
    sparse = _resume_instant(_watermark(1000, span_hours=10, batch_runs=100)[0])
    dense = _resume_instant(_watermark(10_000, span_hours=10, batch_runs=100)[0])
    assert dense > sparse, "10x the run density should mean 10x less time redone"


def test_a_project_with_blocked_runs_claims_no_watermark():
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(Reconciliation("S", "D", "", 2, 1, 0, 0, 1), 100)
    text = "\n".join(printed)
    assert "no verified watermark" in text
    assert "--max-age-stamp" not in text


def test_a_dry_run_explains_nothing_rather_than_blaming_fidelity():
    """The suppression reason must match reality.

    With --dry-run or --no-verify there is nothing verified; saying "degraded
    or blocked runs" would send an operator hunting for problems that the very
    same line reports as zero.
    """
    clean_but_unverified = Reconciliation("S", "D", "", 2, 2, 0, 0, 0)
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(clean_but_unverified, 100, verified=False)
    assert printed == []


def test_a_verified_run_with_blocked_runs_does_explain_itself():
    printed = []
    with patch.object(cli_main.console, "print", lambda *a, **k: printed.append(str(a[0]))):
        cli_main._print_trace_watermark(Reconciliation("S", "D", "", 2, 1, 0, 0, 1), 100, verified=True)
    assert "degraded or blocked runs" in "\n".join(printed)


def test_window_durations_read_as_wall_clock():
    """The pre-flight prints the window in units an operator thinks in."""
    h = cli_main._human_duration
    assert h(0.01) == "14m24s"
    assert h(0.1) == "2h24m"
    assert h(1.0) == "1d"
    assert h(2.5) == "2d12h"
    assert h((2 * 3600 + 5 * 60 + 4) / 86400) == "2h5m4s"   # the example asked for
    assert h(1 / 86400) == "1s"
    assert h(0) == "0s"
