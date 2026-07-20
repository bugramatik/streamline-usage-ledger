"""Reconcile raw event totals against rated ledger totals for one day.

Both sides are summed per (account_id, meter). The raw side comes from the
hourly usage buckets written by the aggregation job, the ledger side from the
lines the rating service wrote. Any difference above the tolerance is reported
as a shortfall or an overage so a disputed invoice line can be traced back.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

from ledger.db import connect

TOLERANCE = Decimal("0.0001")


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = datetime.fromtimestamp(start.timestamp() + 86400, tz=timezone.utc)
    return start, end


def load_raw_totals(conn, day: date) -> dict[tuple[str, str], Decimal]:
    """Sum the aggregated usage buckets for the day, per account and meter."""
    start, end = _day_bounds(day)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id, meter, SUM(quantity)
            FROM usage_buckets
            WHERE hour >= %s AND hour < %s
            GROUP BY account_id, meter
            """,
            (start, end),
        )
        rows = cur.fetchall()
    return {(account, meter): Decimal(str(qty or 0)) for account, meter, qty in rows}


def load_ledger_totals(conn, day: date) -> dict[tuple[str, str], Decimal]:
    """Sum the rated ledger lines for the day, per account and meter."""
    start, end = _day_bounds(day)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id, meter, SUM(quantity)
            FROM ledger_lines
            WHERE hour >= %s AND hour < %s
            GROUP BY account_id, meter
            """,
            (start, end),
        )
        rows = cur.fetchall()
    return {(account, meter): Decimal(str(qty or 0)) for account, meter, qty in rows}


def reconcile(
    raw: dict[tuple[str, str], Decimal],
    ledger: dict[tuple[str, str], Decimal],
    tolerance: Decimal = TOLERANCE,
) -> list[dict]:
    """Compare the two totals and return one entry per account and meter.

    A key present on only one side is reported with the missing side at zero,
    which is how a bucket that never got rated shows up.
    """
    keys = sorted(set(raw) | set(ledger))
    report: list[dict] = []
    for account_id, meter in keys:
        raw_qty = raw.get((account_id, meter), Decimal("0"))
        ledger_qty = ledger.get((account_id, meter), Decimal("0"))
        delta = ledger_qty - raw_qty
        if abs(delta) <= tolerance:
            status = "ok"
        elif delta < 0:
            status = "shortfall"
        else:
            status = "overage"
        report.append(
            {
                "account_id": account_id,
                "meter": meter,
                "raw_quantity": str(raw_qty),
                "ledger_quantity": str(ledger_qty),
                "delta": str(delta),
                "status": status,
            }
        )
    return report


def summarize(report: list[dict]) -> dict:
    """Count the entries by status for the console line and the dashboard."""
    counts = {"ok": 0, "shortfall": 0, "overage": 0}
    for entry in report:
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    total = len(report)
    matched = counts["ok"]
    return {
        "entries": total,
        "ok": matched,
        "shortfall": counts["shortfall"],
        "overage": counts["overage"],
        "matched_pct": round(100.0 * matched / total, 2) if total else 100.0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile raw usage against the rated ledger.")
    parser.add_argument("--day", required=True, help="UTC day to reconcile, YYYY-MM-DD")
    parser.add_argument("--tolerance", default=str(TOLERANCE), help="absolute quantity tolerance")
    parser.add_argument("--out", help="write the JSON report here instead of stdout")
    args = parser.parse_args(argv)

    try:
        day = date.fromisoformat(args.day)
    except ValueError:
        print(f"invalid --day {args.day!r}, expected YYYY-MM-DD", file=sys.stderr)
        return 2

    tolerance = Decimal(args.tolerance)
    conn = connect()
    try:
        raw = load_raw_totals(conn, day)
        ledger = load_ledger_totals(conn, day)
    finally:
        conn.close()

    report = reconcile(raw, ledger, tolerance)
    summary = summarize(report)
    payload = {"day": day.isoformat(), "summary": summary, "entries": report}
    text = json.dumps(payload, indent=2)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
    else:
        print(text)

    print(
        f"{day.isoformat()}: {summary['ok']}/{summary['entries']} matched, "
        f"{summary['shortfall']} shortfall, {summary['overage']} overage",
        file=sys.stderr,
    )
    return 0 if summary["shortfall"] == 0 and summary["overage"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
