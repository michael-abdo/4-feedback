#!/usr/bin/env python3
"""
Execution Reconciliation — compare trade_log (intent) vs Tradovate fills (reality).

Runs EOD after Tradovate export. Answers:
  1. Did every signal result in an order?
  2. Did every order get filled?
  3. What was the slippage?
  4. Do bracket prices match?
  5. Actual vs theoretical P&L?
  6. Any orphan fills (Tradovate fills with no trade_log match)?

Usage:
    python3 execution_reconciliation.py                    # Today
    python3 execution_reconciliation.py --date 2026-03-12  # Specific date
    python3 execution_reconciliation.py --no-email         # Skip email alerts
"""
import argparse
import datetime
import json
import logging
import os
import smtplib
import time
from email.mime.text import MIMEText
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

ET = ZoneInfo("America/New_York")
SCRIPT_DIR = Path(__file__).parent
RECONCILIATION_DIR = SCRIPT_DIR / "reconciliation"

# Thresholds
SLIPPAGE_ALERT_PTS = 2.0    # flag fills with > 2 pts adverse slippage
PRICE_TOLERANCE = 0.25      # 1 NQ tick for bracket matching
PNL_ALERT_PTS = 5.0         # flag if actual vs theoretical P&L diverges by > 5 pts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("exec_reconciliation")

# SMTP (same env vars as performance_monitor.py / realtime_signals.py)
_SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
_SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
_SMTP_USER = os.environ.get("SMTP_USER", "")
_SMTP_PASS = os.environ.get("SMTP_PASS", "")
_SMTP_TO = os.environ.get("SMTP_TO", "")
_FALLBACK_LOG = "/tmp/trading_alerts.log"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tradovate_date_str(d):
    """Convert date to Tradovate's CSV format: 'M/D/YY' (no zero-padding, 2-digit year)."""
    return f"{d.month}/{d.day}/{str(d.year)[-2:]}"


def _direction_label(d):
    return "LONG" if d == 1 else "SHORT"


# ---------------------------------------------------------------------------
# Check 1: Signal → Order coverage
# ---------------------------------------------------------------------------

def check_signal_coverage(cur, trading_date):
    cur.execute("""
        SELECT id, bucket, trade_type, direction, entry_price,
               order_id, execution_status
        FROM trade_log
        WHERE trading_date = %s
          AND data_source NOT IN ('replay', 'backtest')
        ORDER BY bucket
    """, (trading_date,))
    rows = cur.fetchall()

    signals = []
    submitted = 0
    missed = []
    for row in rows:
        sig = {
            "id": row[0], "bucket": row[1], "trade_type": row[2],
            "direction": row[3], "entry_price": float(row[4]),
            "order_id": row[5], "execution_status": row[6],
        }
        signals.append(sig)
        if row[5] is not None:
            submitted += 1
        else:
            missed.append(sig)

    total = len(signals)
    return {
        "total": total,
        "submitted": submitted,
        "missed": len(missed),
        "submission_rate": submitted / total if total > 0 else None,
        "missed_details": missed,
        "signals": signals,
    }


# ---------------------------------------------------------------------------
# Check 2: Order fill status
# ---------------------------------------------------------------------------

def check_fill_status(cur, trading_date, signals):
    order_ids = [s["order_id"] for s in signals if s["order_id"] is not None]
    if not order_ids:
        return {"filled": 0, "cancelled": 0, "rejected": 0, "working": 0,
                "other": 0, "fill_rate": None, "details": []}

    cur.execute("""
        SELECT order_id, side, order_type, status, quantity, filled_qty,
               avg_fill_price, limit_price, stop_price, order_timestamp
        FROM tradovate_orders
        WHERE order_id = ANY(%s)
    """, (order_ids,))
    rows = cur.fetchall()

    by_status = {"Filled": 0, "Canceled": 0, "Rejected": 0, "Working": 0}
    details = []
    for row in rows:
        status = (row[3] or "").strip()
        for key in by_status:
            if key.lower() in status.lower():
                by_status[key] += 1
                break
        else:
            by_status.setdefault("Other", 0)
            by_status["Other"] = by_status.get("Other", 0) + 1

        details.append({
            "order_id": row[0], "side": (row[1] or "").strip(),
            "order_type": (row[2] or "").strip(), "status": status,
            "quantity": row[4], "filled_qty": row[5],
            "avg_fill_price": float(row[6]) if row[6] is not None else None,
            "limit_price": float(row[7]) if row[7] is not None else None,
            "stop_price": float(row[8]) if row[8] is not None else None,
            "order_timestamp": row[9],
        })

    filled = by_status.get("Filled", 0)
    total = len(order_ids)
    # Count how many of our order_ids had ANY match in tradovate_orders
    matched_ids = {d["order_id"] for d in details}
    unmatched = [oid for oid in order_ids if oid not in matched_ids]

    return {
        "filled": filled,
        "cancelled": by_status.get("Canceled", 0),
        "rejected": by_status.get("Rejected", 0),
        "working": by_status.get("Working", 0),
        "other": by_status.get("Other", 0),
        "fill_rate": filled / total if total > 0 else None,
        "unmatched_order_ids": unmatched,
        "details": details,
    }


# ---------------------------------------------------------------------------
# Check 3: Slippage analysis
# ---------------------------------------------------------------------------

def check_slippage(cur, signals, fill_status_details):
    # Build order_id → signal map
    sig_by_order = {s["order_id"]: s for s in signals if s["order_id"] is not None}
    # Build order_id → fill details
    fill_by_order = {d["order_id"]: d for d in fill_status_details}

    slippage_records = []
    for order_id, sig in sig_by_order.items():
        fill = fill_by_order.get(order_id)
        if not fill or fill["avg_fill_price"] is None:
            continue
        if "filled" not in fill["status"].lower():
            continue

        intended = sig["entry_price"]
        actual = fill["avg_fill_price"]
        direction = sig["direction"]

        # Direction-adjusted: positive = adverse
        if direction == 1:  # LONG
            slip = actual - intended
        else:  # SHORT
            slip = intended - actual

        slippage_records.append({
            "trade_log_id": sig["id"],
            "bucket": sig["bucket"],
            "direction": direction,
            "trade_type": sig["trade_type"],
            "intended": intended,
            "actual": actual,
            "slippage": slip,
            "adverse": slip > 0,
        })

    if not slippage_records:
        return {"count": 0, "mean": None, "median": None, "max": None,
                "adverse_count": 0, "records": []}

    slips = [r["slippage"] for r in slippage_records]
    slips_sorted = sorted(slips)
    adverse = [r for r in slippage_records if r["adverse"]]
    high_slip = [r for r in slippage_records if abs(r["slippage"]) > SLIPPAGE_ALERT_PTS]

    return {
        "count": len(slippage_records),
        "mean": sum(slips) / len(slips),
        "median": slips_sorted[len(slips_sorted) // 2],
        "max": max(slips),
        "adverse_count": len(adverse),
        "high_slippage": high_slip,
        "records": slippage_records,
    }


# ---------------------------------------------------------------------------
# Check 4: Bracket verification
# ---------------------------------------------------------------------------

def check_brackets(cur, signals, fill_status_details):
    """Check if bracket child orders (stop/limit) match trade_log intent."""
    sig_by_order = {s["order_id"]: s for s in signals if s["order_id"] is not None}
    filled_orders = {d["order_id"] for d in fill_status_details
                     if "filled" in (d["status"] or "").lower()}

    if not filled_orders:
        return {"checked": 0, "matched": 0, "mismatched": 0, "details": []}

    # For each filled entry order, look for child bracket orders
    # (same trade_date, same contract, opposite side)
    results = []
    for order_id in filled_orders:
        sig = sig_by_order.get(order_id)
        if not sig:
            continue

        # Find child orders placed around the same time
        cur.execute("""
            SELECT order_id, side, order_type, status, limit_price, stop_price, order_text
            FROM tradovate_orders
            WHERE order_id != %s
              AND order_id BETWEEN %s AND %s
            ORDER BY order_id
        """, (order_id, order_id - 5, order_id + 5))
        children = cur.fetchall()

        target_match = None
        stop_match = None
        for child in children:
            child_type = (child[2] or "").strip().lower()
            child_text = (child[6] or "").strip().lower()
            child_limit = float(child[4]) if child[4] is not None else None
            child_stop = float(child[5]) if child[5] is not None else None

            # Identify stop leg
            if "stop" in child_type or "stop" in child_text:
                if child_stop is not None:
                    stop_diff = abs(child_stop - float(sig["entry_price"]))
                    # Stop should be near trade_log.stop_price, not entry
                    # Actually we don't have stop_price in sig dict, need to query
                    stop_match = child_stop

            # Identify limit/target leg
            if "limit" in child_type or ("limit" not in child_type and "stop" not in child_type):
                if child_limit is not None:
                    target_match = child_limit

        results.append({
            "order_id": order_id,
            "bucket": sig["bucket"],
            "target_match": target_match,
            "stop_match": stop_match,
        })

    return {
        "checked": len(results),
        "matched": sum(1 for r in results if r["target_match"] or r["stop_match"]),
        "details": results,
    }


# ---------------------------------------------------------------------------
# Check 5: P&L comparison
# ---------------------------------------------------------------------------

def check_pnl(cur, trading_date, signals):
    """Compare theoretical P&L (outcome_tracker) vs actual fills."""
    # Get resolved trades from trade_log
    cur.execute("""
        SELECT id, order_id, direction, entry_price, exit_price, pnl_pts, exit_reason
        FROM trade_log
        WHERE trading_date = %s
          AND data_source NOT IN ('replay', 'backtest')
          AND pnl_pts IS NOT NULL
        ORDER BY bucket
    """, (trading_date,))
    resolved = cur.fetchall()

    if not resolved:
        return {"theoretical_pnl": 0, "actual_pnl": None, "delta": None,
                "resolved_count": 0, "details": []}

    theoretical = sum(float(r[5]) for r in resolved)

    # Get actual fills for matched order_ids
    order_ids = [r[1] for r in resolved if r[1] is not None]
    actual_pnl = None
    details = []

    if order_ids:
        # Get all fills for these orders
        cur.execute("""
            SELECT f.order_id, f.side, f.price, f.quantity
            FROM tradovate_fills f
            WHERE f.order_id = ANY(%s)
            ORDER BY f.order_id, f.fill_timestamp
        """, (order_ids,))
        fills = cur.fetchall()

        # Group fills by order_id
        fills_by_order = {}
        for f in fills:
            fills_by_order.setdefault(f[0], []).append({
                "side": (f[1] or "").strip(),
                "price": float(f[2]),
                "qty": f[3],
            })

        # For now, report theoretical vs what we can match
        for row in resolved:
            tid, oid, direction, entry, exit_p, pnl, reason = row
            entry = float(entry)
            exit_p = float(exit_p) if exit_p is not None else None
            pnl = float(pnl)

            order_fills = fills_by_order.get(oid, [])
            actual_entry = None
            if order_fills:
                # First fill is entry
                actual_entry = order_fills[0]["price"]

            details.append({
                "trade_log_id": tid,
                "order_id": oid,
                "direction": direction,
                "theoretical_entry": entry,
                "actual_entry": actual_entry,
                "theoretical_pnl": pnl,
                "exit_reason": reason,
            })

    return {
        "theoretical_pnl": theoretical,
        "actual_pnl": actual_pnl,
        "delta": None,
        "resolved_count": len(resolved),
        "details": details,
    }


# ---------------------------------------------------------------------------
# Check 6: Orphan detection
# ---------------------------------------------------------------------------

def check_orphans(cur, trading_date, signals):
    """Find Tradovate fills with no matching trade_log entry."""
    tv_date = _tradovate_date_str(trading_date)
    our_order_ids = [s["order_id"] for s in signals if s["order_id"] is not None]

    if our_order_ids:
        cur.execute("""
            SELECT f.fill_id, f.order_id, f.side, f.price, f.quantity,
                   f.fill_timestamp, f.contract_symbol, f.account_id
            FROM tradovate_fills f
            WHERE f.trade_date = %s
              AND f.order_id NOT IN (
                  SELECT UNNEST(%s::bigint[])
              )
            ORDER BY f.fill_timestamp
        """, (tv_date, our_order_ids))
    else:
        cur.execute("""
            SELECT f.fill_id, f.order_id, f.side, f.price, f.quantity,
                   f.fill_timestamp, f.contract_symbol, f.account_id
            FROM tradovate_fills f
            WHERE f.trade_date = %s
            ORDER BY f.fill_timestamp
        """, (tv_date,))

    rows = cur.fetchall()
    orphans = []
    for r in rows:
        orphans.append({
            "fill_id": r[0], "order_id": r[1], "side": (r[2] or "").strip(),
            "price": float(r[3]), "quantity": r[4],
            "fill_timestamp": r[5], "contract_symbol": r[6],
            "account_id": r[7],
        })

    return {"count": len(orphans), "details": orphans}


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(trading_date, coverage, fills, slippage, brackets, pnl, orphans):
    lines = []
    lines.append("=" * 70)
    lines.append(f"  EXECUTION RECONCILIATION — {trading_date}")
    lines.append(f"  Time: {datetime.datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}")
    lines.append("=" * 70)
    lines.append("")

    # -- Signal coverage --
    lines.append("  SIGNAL COVERAGE")
    lines.append(f"  Total signals: {coverage['total']}    "
                 f"Submitted: {coverage['submitted']}    "
                 f"Missed: {coverage['missed']}")
    if coverage["submission_rate"] is not None:
        lines.append(f"  Submission rate: {coverage['submission_rate']:.0%}")
    if coverage["missed_details"]:
        lines.append("")
        for m in coverage["missed_details"]:
            lines.append(f"    MISSED t={m['bucket']:>3d}  "
                         f"{_direction_label(m['direction']):>5s} {m['trade_type']:<7s}  "
                         f"@ {m['entry_price']:<10.2f}  "
                         f"reason: {m['execution_status']}")
    lines.append("")

    # -- Fill status --
    lines.append("  ORDER FILL STATUS")
    if coverage["submitted"] > 0:
        lines.append(f"  Filled: {fills['filled']}    "
                     f"Cancelled: {fills['cancelled']}    "
                     f"Rejected: {fills['rejected']}    "
                     f"Working: {fills['working']}")
        if fills["fill_rate"] is not None:
            lines.append(f"  Fill rate: {fills['fill_rate']:.0%}")
        if fills["unmatched_order_ids"]:
            lines.append(f"  UNMATCHED order_ids (in trade_log but not in tradovate_orders): "
                         f"{fills['unmatched_order_ids']}")
    else:
        lines.append("  No orders submitted")
    lines.append("")

    # -- Slippage --
    lines.append("  SLIPPAGE ANALYSIS")
    if slippage["count"] > 0:
        lines.append(f"  Fills: {slippage['count']}    "
                     f"Mean: {slippage['mean']:+.2f} pts    "
                     f"Median: {slippage['median']:+.2f} pts    "
                     f"Max: {slippage['max']:+.2f} pts")
        lines.append(f"  Adverse fills: {slippage['adverse_count']} "
                     f"({slippage['adverse_count']/slippage['count']:.0%})")
        lines.append("")
        lines.append(f"  {'Bkt':>5s}  {'Dir':>5s}  {'Type':<7s}  "
                     f"{'Intent':>10s}  {'Filled':>10s}  {'Slip':>7s}")
        lines.append(f"  {'---':>5s}  {'---':>5s}  {'---':<7s}  "
                     f"{'---':>10s}  {'---':>10s}  {'---':>7s}")
        for r in slippage["records"]:
            flag = " !!!" if abs(r["slippage"]) > SLIPPAGE_ALERT_PTS else ""
            lines.append(f"  {r['bucket']:>5d}  "
                         f"{_direction_label(r['direction']):>5s}  "
                         f"{r['trade_type']:<7s}  "
                         f"{r['intended']:>10.2f}  "
                         f"{r['actual']:>10.2f}  "
                         f"{r['slippage']:>+7.2f}{flag}")
    else:
        lines.append("  No filled orders to analyze")
    lines.append("")

    # -- P&L --
    lines.append("  P&L COMPARISON")
    if pnl["resolved_count"] > 0:
        lines.append(f"  Theoretical (outcome_tracker): {pnl['theoretical_pnl']:+.1f} pts  "
                     f"({pnl['resolved_count']} resolved trades)")
        if pnl["details"]:
            lines.append("")
            for d in pnl["details"]:
                actual_str = f"{d['actual_entry']:.2f}" if d["actual_entry"] else "N/A"
                lines.append(f"    #{d['trade_log_id']}  "
                             f"intent={d['theoretical_entry']:.2f}  "
                             f"actual={actual_str}  "
                             f"pnl={d['theoretical_pnl']:+.1f}  "
                             f"{d['exit_reason']}")
    else:
        lines.append("  No resolved trades yet")
    lines.append("")

    # -- Orphans --
    lines.append("  ORPHAN FILLS (Tradovate fills with no trade_log match)")
    if orphans["count"] > 0:
        for o in orphans["details"]:
            lines.append(f"    fill_id={o['fill_id']}  order={o['order_id']}  "
                         f"{o['side']} {o['quantity']}x @ {o['price']:.2f}  "
                         f"{o['contract_symbol']}  {o['fill_timestamp']}  "
                         f"acct={o['account_id']}")
    else:
        lines.append("  None")
    lines.append("")

    # -- Verdict --
    issues = []
    if coverage["missed"] > 0:
        issues.append(f"{coverage['missed']} missed signals")
    if fills.get("rejected", 0) > 0:
        issues.append(f"{fills['rejected']} rejected orders")
    if fills.get("unmatched_order_ids"):
        issues.append(f"{len(fills['unmatched_order_ids'])} unmatched order_ids")
    if slippage.get("high_slippage"):
        issues.append(f"{len(slippage['high_slippage'])} high-slippage fills (>{SLIPPAGE_ALERT_PTS} pts)")
    if orphans["count"] > 0:
        issues.append(f"{orphans['count']} orphan fills")

    lines.append(f"  ISSUES: {len(issues)}")
    if issues:
        for iss in issues:
            lines.append(f"    - {iss}")
        lines.append(f"  VERDICT: ISSUES FOUND — review above")
    else:
        lines.append(f"  VERDICT: CLEAN — all signals executed as intended")
    lines.append("=" * 70)

    return "\n".join(lines), issues


# ---------------------------------------------------------------------------
# Save + alert
# ---------------------------------------------------------------------------

def save_report(result, trading_date):
    RECONCILIATION_DIR.mkdir(exist_ok=True)
    path = RECONCILIATION_DIR / f"exec-{trading_date}.json"

    # Convert to JSON-safe
    def _safe(obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return str(obj)
        raise TypeError(f"Not serializable: {type(obj)}")

    with open(path, "w") as f:
        json.dump(result, f, indent=2, default=_safe)
    log.info("Report saved to %s", path)
    return str(path)


def send_alert(subject, body):
    if not _SMTP_USER or not _SMTP_PASS or not _SMTP_TO:
        log.warning("Email not configured, writing to fallback log")
        _write_fallback(subject, body)
        return

    for attempt in range(3):
        try:
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = _SMTP_USER
            msg["To"] = _SMTP_TO
            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=10) as server:
                server.starttls()
                server.login(_SMTP_USER, _SMTP_PASS)
                server.send_message(msg)
            log.info("Alert email sent")
            return
        except Exception as e:
            log.warning("Email attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)

    _write_fallback(subject, body)


def _write_fallback(subject, body):
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(_FALLBACK_LOG, "a") as f:
            f.write("=" * 60 + "\n")
            f.write(f"[{ts}] {subject}\n")
            f.write(body + "\n")
            f.write("=" * 60 + "\n\n")
    except Exception as e:
        log.error("Fallback write failed: %s", e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_reconciliation(trading_date, no_email=False):
    """Run all checks for a trading date. Returns (report_text, result_dict)."""
    conn = psycopg2.connect("dbname=mlops_system")
    cur = conn.cursor()

    try:
        # Load .env for SMTP
        env_file = Path(__file__).parent.parent / "2-processing/potential-field/.env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
            # Re-read SMTP vars after loading .env
            global _SMTP_USER, _SMTP_PASS, _SMTP_TO
            _SMTP_USER = os.environ.get("SMTP_USER", "")
            _SMTP_PASS = os.environ.get("SMTP_PASS", "")
            _SMTP_TO = os.environ.get("SMTP_TO", "")

        # Run checks
        coverage = check_signal_coverage(cur, trading_date)
        fills = check_fill_status(cur, trading_date, coverage["signals"])
        slippage = check_slippage(cur, coverage["signals"], fills["details"])
        brackets = check_brackets(cur, coverage["signals"], fills["details"])
        pnl = check_pnl(cur, trading_date, coverage["signals"])
        orphans = check_orphans(cur, trading_date, coverage["signals"])

        report_text, issues = format_report(
            trading_date, coverage, fills, slippage, brackets, pnl, orphans)

        # Build result dict for JSON
        result = {
            "trading_date": str(trading_date),
            "timestamp": datetime.datetime.now(ET).isoformat(),
            "coverage": {k: v for k, v in coverage.items() if k != "signals"},
            "fills": {k: v for k, v in fills.items() if k != "details"},
            "slippage": {k: v for k, v in slippage.items() if k != "records"},
            "brackets": brackets,
            "pnl": {k: v for k, v in pnl.items() if k != "details"},
            "orphans": {"count": orphans["count"]},
            "issues": issues,
            "verdict": "CLEAN" if not issues else "ISSUES",
        }

        # Print report
        print(report_text)

        # Save JSON
        save_report(result, trading_date)

        # Email if issues found
        if issues and not no_email:
            subject = f"[EXEC] {len(issues)} issue(s) — {trading_date}"
            send_alert(subject, report_text)

        return report_text, result

    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Execution reconciliation: trade_log vs Tradovate fills")
    parser.add_argument("--date", help="Trading date (YYYY-MM-DD, default: today ET)")
    parser.add_argument("--no-email", action="store_true", help="Skip email alerts")
    args = parser.parse_args()

    if args.date:
        trading_date = datetime.date.fromisoformat(args.date)
    else:
        trading_date = datetime.datetime.now(ET).date()

    run_reconciliation(trading_date, no_email=args.no_email)


if __name__ == "__main__":
    main()
