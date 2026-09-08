"""Tests for the reconciliation comparison between raw totals and rated ledger totals."""

from decimal import Decimal

from ledger.reconcile import reconcile


def _raw(**totals: str) -> dict[tuple[str, str], Decimal]:
    return {tuple(key.split("/")): Decimal(value) for key, value in totals.items()}


def test_reconcile_matches() -> None:
    raw = _raw(**{"acct-1/api_calls": "1200", "acct-2/storage_gb": "40"})
    ledger = _raw(**{"acct-1/api_calls": "1200", "acct-2/storage_gb": "40"})

    report = reconcile(raw, ledger, Decimal("0"))

    assert len(report) == 2
    assert all(row["status"] == "ok" for row in report)
    assert all(row["delta"] == Decimal("0") for row in report)


def test_reconcile_flags_shortfall() -> None:
    raw = _raw(**{"acct-1/api_calls": "1200"})
    ledger = _raw(**{"acct-1/api_calls": "1150"})

    report = reconcile(raw, ledger, Decimal("0"))

    assert len(report) == 1
    row = report[0]
    assert row["account_id"] == "acct-1"
    assert row["meter"] == "api_calls"
    assert row["status"] == "shortfall"
    assert row["delta"] == Decimal("-50")


def test_reconcile_ignores_within_tolerance() -> None:
    raw = _raw(**{"acct-3/egress_gb": "1000"})
    ledger = _raw(**{"acct-3/egress_gb": "999.5"})

    report = reconcile(raw, ledger, Decimal("1"))

    assert report[0]["status"] == "ok"
    assert report[0]["delta"] == Decimal("-0.5")


def test_reconcile_reports_missing_ledger_line() -> None:
    raw = _raw(**{"acct-4/api_calls": "300", "acct-4/storage_gb": "12"})
    ledger = _raw(**{"acct-4/api_calls": "300"})

    report = reconcile(raw, ledger, Decimal("0"))
    by_meter = {row["meter"]: row for row in report}

    assert by_meter["api_calls"]["status"] == "ok"
    assert by_meter["storage_gb"]["status"] == "shortfall"
    assert by_meter["storage_gb"]["ledger_quantity"] == Decimal("0")


def test_reconcile_reports_unexpected_ledger_line() -> None:
    raw = _raw(**{"acct-5/api_calls": "10"})
    ledger = _raw(**{"acct-5/api_calls": "10", "acct-5/egress_gb": "4"})

    report = reconcile(raw, ledger, Decimal("0"))
    by_meter = {row["meter"]: row for row in report}

    assert by_meter["egress_gb"]["status"] == "overage"
    assert by_meter["egress_gb"]["raw_quantity"] == Decimal("0")
