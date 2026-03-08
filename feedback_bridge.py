#!/usr/bin/env python3
"""
Feedback Bridge — EOD closed-loop between Tradovate fills and the conviction engine.

Run once at end of day. Pulls the day's fills, FIFO-matches closed trades, matches
to conviction signals, updates trade_log with actual P&L, recalibrates modifier
weights, and saves a versioned snapshot for rollback.

Flow:
    Tradovate API → SQLite fills → FIFO match → closed trades
    → match to trade_log signals (PostgreSQL) → update actual P&L
    → recalibrate modifier weights → version snapshot → live_weights.json

Usage:
    python3 feedback_bridge.py                  # EOD run (pull + match + recalibrate)
    python3 feedback_bridge.py --recalibrate    # Recalibrate only (no fill pull)
    python3 feedback_bridge.py --history        # Show weight version history
    python3 feedback_bridge.py --rollback 3     # Rollback to version 3
    python3 feedback_bridge.py --diff 3 5       # Compare versions 3 and 5
    python3 feedback_bridge.py --reset          # Clear processed state
"""

import sys
import json
import logging
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque
from typing import List, Dict, Tuple

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np

# ============================================================================
# Paths
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
REPORTS_DIR = SCRIPT_DIR / "reports"
REPORTS_SCRIPTS = REPORTS_DIR / "scripts"

sys.path.insert(0, str(REPORTS_SCRIPTS))

TRADING_DB = REPORTS_SCRIPTS / "trading.db"
POSTGRES_DSN = "dbname=mlops_system"

# Outputs
WEIGHT_FILE = SCRIPT_DIR / "live_weights.json"
HISTORY_FILE = SCRIPT_DIR / "weights_history.json"
STATE_FILE = SCRIPT_DIR / ".bridge_state.json"

# ============================================================================
# Constants
# ============================================================================

MARKET_OPEN_HOUR, MARKET_OPEN_MIN = 8, 30

BUCKET_WINDOW = 10      # ±10 min for signal matching
PRICE_WINDOW = 5.0      # ±5 pts for signal matching

TICK_VALUE = {"NQ": 5.0, "MNQ": 0.5, "ES": 12.5, "MES": 1.25}
TICK_SIZE = {"NQ": 0.25, "MNQ": 0.25, "ES": 0.25, "MES": 0.25}

DEFAULT_WEIGHTS = {
    "ke_lt_pe_near": 10, "ke_gt_pe_far": 10, "anti_aligned_far": 15,
    "dominant_well": 5, "wide_well_near": 5, "f_growing_far": 10,
    "mtf_aligned": 5, "accelerating": 3,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bridge")


# ============================================================================
# Version tracking
# ============================================================================

def load_history() -> list:
    if HISTORY_FILE.exists():
        return json.loads(HISTORY_FILE.read_text())
    return []


def save_history(history: list):
    HISTORY_FILE.write_text(json.dumps(history, indent=2))


def next_version(history: list) -> int:
    if not history:
        return 1
    return max(h["version"] for h in history) + 1


def save_version(weights: dict, metrics: dict, prev_weights: dict) -> dict:
    """Save a versioned weight snapshot. Returns the version record."""
    history = load_history()
    ver = next_version(history)

    # Compute diff from previous
    diff = {}
    all_keys = set(list(weights.keys()) + list(prev_weights.keys()))
    for k in sorted(all_keys):
        old = prev_weights.get(k, 0)
        new = weights.get(k, 0)
        if old != new:
            diff[k] = {"old": old, "new": new}

    record = {
        "version": ver,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "timestamp": datetime.now().isoformat(),
        "weights": weights,
        "metrics": metrics,
        "diff": diff,
    }

    history.append(record)
    save_history(history)

    # Write live weights
    WEIGHT_FILE.write_text(json.dumps(weights, indent=2))

    log.info("Saved weight version %d → %s", ver, WEIGHT_FILE)
    return record


def rollback_to(version: int):
    """Restore weights from a specific version."""
    history = load_history()
    match = [h for h in history if h["version"] == version]
    if not match:
        print(f"Version {version} not found. Use --history to see available versions.")
        return

    record = match[0]
    WEIGHT_FILE.write_text(json.dumps(record["weights"], indent=2))

    print(f"Rolled back to version {version} ({record['date']})")
    print(f"  Win rate: {record['metrics'].get('win_rate', '?'):.1f}%")
    print(f"  Avg P&L: {record['metrics'].get('avg_pnl', '?'):+.1f} pts")
    print(f"  Weights: {WEIGHT_FILE}")


def show_history():
    """Print version history table."""
    history = load_history()
    if not history:
        print("No weight versions yet. Run an EOD feedback pass first.")
        return

    print()
    print("=" * 80)
    print("  WEIGHT VERSION HISTORY")
    print("=" * 80)
    print("  {:<4} {:<12} {:>6} {:>7} {:>9} {:>8}  {}".format(
        "Ver", "Date", "N", "Win%", "Avg P&L", "Trades", "Changes"))
    print("  " + "-" * 74)

    for h in history:
        m = h["metrics"]
        n_changes = len(h.get("diff", {}))
        changes = ", ".join(f"{k}:{d['old']}→{d['new']}" for k, d in list(h.get("diff", {}).items())[:3])
        if n_changes > 3:
            changes += f" +{n_changes - 3} more"

        print("  {:<4} {:<12} {:>6} {:>6.1f}% {:>+8.1f} {:>8}  {}".format(
            h["version"], h["date"], m.get("n_signals", "?"),
            m.get("win_rate", 0), m.get("avg_pnl", 0),
            m.get("n_today", 0), changes or "(initial)"))

    # Show current active version
    if WEIGHT_FILE.exists():
        current = json.loads(WEIGHT_FILE.read_text())
        latest = history[-1]
        if current == latest["weights"]:
            print(f"\n  Active: v{latest['version']} (latest)")
        else:
            print(f"\n  Active: custom (not matching any version)")
    print()


def show_diff(v1: int, v2: int):
    """Compare two weight versions side by side."""
    history = load_history()
    h1 = next((h for h in history if h["version"] == v1), None)
    h2 = next((h for h in history if h["version"] == v2), None)

    if not h1 or not h2:
        print(f"Version(s) not found. Available: {[h['version'] for h in history]}")
        return

    print()
    print(f"  DIFF: v{v1} ({h1['date']}) → v{v2} ({h2['date']})")
    print("  " + "-" * 55)

    all_keys = sorted(set(list(h1["weights"].keys()) + list(h2["weights"].keys())))
    print("  {:<22} {:>8} {:>8} {:>8}".format("Modifier", f"v{v1}", f"v{v2}", "Delta"))
    print("  " + "-" * 50)

    for k in all_keys:
        w1 = h1["weights"].get(k, 0)
        w2 = h2["weights"].get(k, 0)
        marker = " *" if w1 != w2 else ""
        print("  {:<22} {:>8} {:>8} {:>+8}{}".format(k, w1, w2, w2 - w1, marker))

    m1, m2 = h1["metrics"], h2["metrics"]
    print()
    print("  {:<22} {:>8} {:>8} {:>8}".format("Metric", f"v{v1}", f"v{v2}", "Delta"))
    print("  " + "-" * 50)
    for key, label in [("win_rate", "Win %"), ("avg_pnl", "Avg P&L"), ("total_pnl", "Total P&L"),
                        ("drift_win_rate", "Drift Win %"), ("bounce_win_rate", "Bounce Win %"),
                        ("cal_error", "Cal Error")]:
        a = m1.get(key, 0)
        b = m2.get(key, 0)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            print("  {:<22} {:>8.1f} {:>8.1f} {:>+8.1f}".format(label, a, b, b - a))
    print()


# ============================================================================
# State persistence
# ============================================================================

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"processed_pairs": []}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ============================================================================
# 1. Pull fills from Tradovate API → SQLite
# ============================================================================

def pull_fills():
    """Pull today's fills from Tradovate into trading.db."""
    try:
        from tradovate_auth import load_token, check_token, load_credentials, refresh_token
        from tradovate_export import init_db, export_account, load_accounts
    except ImportError as e:
        log.error("Cannot import reports scripts: %s", e)
        return False

    token = load_token()
    if token:
        valid, _, _ = check_token(token)
        if not valid:
            token = None

    if not token:
        creds = load_credentials()
        if not creds:
            log.error("No credentials — run: cd reports/scripts && python3 tradovate_auth.py --env live")
            return False
        ok = refresh_token(
            creds["username"], creds["password"], "live",
            creds.get("device_id"), creds.get("password_encoded"), creds.get("challenge"),
        )
        if not ok:
            return False
        token = load_token()

    conn = init_db(TRADING_DB)
    accounts = load_accounts()
    if not accounts:
        log.error("No accounts in reports/scripts/.tradovate_accounts")
        conn.close()
        return False

    today = datetime.now()
    start = (today - timedelta(days=1)).strftime("%m/%d/%Y")
    end = (today + timedelta(days=1)).strftime("%m/%d/%Y")

    total_fills = 0
    for acct in accounts:
        try:
            fc, _ = export_account(conn, token, acct, start, end, -360)
            total_fills += fc
        except Exception as e:
            log.warning("Export failed for %s: %s", acct, e)

    conn.close()
    log.info("Pulled %d fills from %d accounts", total_fills, len(accounts))
    return True


# ============================================================================
# 2. FIFO match fills → closed trades
# ============================================================================

def _root(symbol: str) -> str:
    s = symbol.strip().upper()
    for r in sorted(TICK_VALUE.keys(), key=len, reverse=True):
        if s.startswith(r):
            return r
    return "".join(c for c in s if c.isalpha())


def fifo_match() -> List[dict]:
    """FIFO-match all fills in trading.db. Returns closed trades."""
    if not TRADING_DB.exists():
        return []

    conn = sqlite3.connect(TRADING_DB)
    conn.row_factory = sqlite3.Row
    fills = pd.read_sql_query("SELECT * FROM fills ORDER BY fill_timestamp, fill_id", conn)
    conn.close()

    if fills.empty:
        return []

    for fmt in ["%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"]:
        try:
            fills["ts"] = pd.to_datetime(fills["fill_timestamp"], format=fmt)
            break
        except Exception:
            continue
    else:
        fills["ts"] = pd.to_datetime(fills["fill_timestamp"], errors="coerce")

    fills = fills.sort_values(["ts", "fill_id"]).dropna(subset=["ts"])

    open_buys: Dict[str, deque] = {}
    open_sells: Dict[str, deque] = {}
    closed = []

    for _, f in fills.iterrows():
        sym = f["contract_symbol"]
        if sym not in open_buys:
            open_buys[sym] = deque()
            open_sells[sym] = deque()

        lot = {"fill_id": int(f["fill_id"]), "ts": f["ts"],
               "price": float(f["price"]), "qty": int(f["quantity"])}
        root = _root(sym)
        tv = TICK_VALUE.get(root, 5.0)
        ts_val = TICK_SIZE.get(root, 0.25)

        if f["side"] == "Sell":
            remaining = lot["qty"]
            while remaining > 0 and open_buys[sym]:
                buy = open_buys[sym][0]
                mq = min(remaining, buy["qty"])

                closed.append({
                    "contract": sym, "root": root, "direction": 1,
                    "qty": mq, "entry_price": buy["price"], "exit_price": lot["price"],
                    "entry_time": buy["ts"], "exit_time": lot["ts"],
                    "entry_fill_id": buy["fill_id"], "exit_fill_id": lot["fill_id"],
                    "pnl_pts": lot["price"] - buy["price"],
                    "pnl_dollars": (lot["price"] - buy["price"]) / ts_val * tv * mq,
                })

                remaining -= mq
                if mq >= buy["qty"]:
                    open_buys[sym].popleft()
                else:
                    open_buys[sym][0] = {**buy, "qty": buy["qty"] - mq}

            if remaining > 0:
                open_sells[sym].append({**lot, "qty": remaining})

        elif f["side"] == "Buy":
            remaining = lot["qty"]
            while remaining > 0 and open_sells[sym]:
                sell = open_sells[sym][0]
                mq = min(remaining, sell["qty"])

                closed.append({
                    "contract": sym, "root": root, "direction": -1,
                    "qty": mq, "entry_price": sell["price"], "exit_price": lot["price"],
                    "entry_time": sell["ts"], "exit_time": lot["ts"],
                    "entry_fill_id": sell["fill_id"], "exit_fill_id": lot["fill_id"],
                    "pnl_pts": sell["price"] - lot["price"],
                    "pnl_dollars": (sell["price"] - lot["price"]) / ts_val * tv * mq,
                })

                remaining -= mq
                if mq >= sell["qty"]:
                    open_sells[sym].popleft()
                else:
                    open_sells[sym][0] = {**sell, "qty": sell["qty"] - mq}

            if remaining > 0:
                open_buys[sym].append({**lot, "qty": remaining})

    return closed


# ============================================================================
# 3. Match closed trades to conviction signals
# ============================================================================

def _ts_to_bucket(ts) -> int:
    if isinstance(ts, str):
        ts = pd.Timestamp(ts)
    market_open = ts.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)
    return int(round((ts - market_open).total_seconds() / 60))


def _classify_exit(trade: dict, signal: dict) -> str:
    ep = trade["exit_price"]
    d = trade["direction"]
    tgt = signal.get("target_price")
    stp = signal.get("stop_price")

    if tgt is not None:
        if (d == 1 and ep >= tgt) or (d == -1 and ep <= tgt):
            return "target"
    if stp is not None:
        if (d == 1 and ep <= stp) or (d == -1 and ep >= stp):
            return "stop"

    try:
        dur = (trade["exit_time"] - trade["entry_time"]).total_seconds() / 60
        if dur >= 55:
            return "timeout"
    except Exception:
        pass

    return "closed"


def match_and_update(closed_trades: List[dict], processed_pairs: set) -> Tuple[int, int, int, List[dict]]:
    """
    Match new closed trades to trade_log signals, update PostgreSQL.
    Returns (matched, discretionary, skipped, summaries).
    """
    new_trades = [t for t in closed_trades
                  if (t["entry_fill_id"], t["exit_fill_id"]) not in processed_pairs]

    if not new_trades:
        return 0, 0, len(closed_trades), []

    pg = psycopg2.connect(POSTGRES_DSN)
    cur = pg.cursor(cursor_factory=psycopg2.extras.DictCursor)

    matched_count = 0
    disc_count = 0
    summaries = []

    for t in new_trades:
        entry_date = t["entry_time"]
        trading_date = entry_date.strftime("%Y-%m-%d") if isinstance(entry_date, pd.Timestamp) else str(entry_date)[:10]
        bucket = _ts_to_bucket(t["entry_time"])

        cur.execute("""
            SELECT id, trading_date, bucket, trade_type, direction, conviction_pct,
                   zone, distance, entry_price, target_price, stop_price, modifiers_json
            FROM trade_log
            WHERE trading_date = %s AND direction = %s
              AND ABS(bucket - %s) <= %s AND ABS(entry_price - %s) <= %s
              AND exit_price IS NULL
            ORDER BY ABS(bucket - %s), ABS(entry_price - %s)
            LIMIT 1
        """, (trading_date, t["direction"], bucket, BUCKET_WINDOW,
              t["entry_price"], PRICE_WINDOW, bucket, t["entry_price"]))

        row = cur.fetchone()

        if row:
            exit_reason = _classify_exit(t, dict(row))
            cur.execute("""
                UPDATE trade_log
                SET exit_price = %s, exit_reason = %s, pnl_pts = %s, execution_status = 'filled'
                WHERE id = %s AND exit_price IS NULL
            """, (float(t["exit_price"]), exit_reason, float(t["pnl_pts"]), row["id"]))

            if cur.rowcount > 0:
                matched_count += 1
                summaries.append({
                    "type": "signal", "signal_id": row["id"],
                    "trade_type": row["trade_type"], "conviction": float(row["conviction_pct"]),
                    "direction": t["direction"], "entry": t["entry_price"],
                    "exit": t["exit_price"], "pnl_pts": t["pnl_pts"],
                    "exit_reason": exit_reason,
                })
        else:
            cur.execute("""
                INSERT INTO trade_log (
                    trading_date, bucket, trade_type, direction,
                    conviction_pct, zone, distance,
                    entry_price, target_price, stop_price,
                    exit_price, exit_reason, pnl_pts,
                    modifiers_json, execution_status
                ) VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)
            """, (
                trading_date, bucket, "discretionary", t["direction"],
                0, "unknown", 0,
                float(t["entry_price"]), None, None,
                float(t["exit_price"]), "closed", float(t["pnl_pts"]),
                json.dumps({}), "filled",
            ))
            disc_count += 1
            summaries.append({
                "type": "discretionary", "signal_id": None,
                "trade_type": "discretionary", "conviction": 0,
                "direction": t["direction"], "entry": t["entry_price"],
                "exit": t["exit_price"], "pnl_pts": t["pnl_pts"],
                "exit_reason": "closed",
            })

        processed_pairs.add((t["entry_fill_id"], t["exit_fill_id"]))

    pg.commit()
    pg.close()

    return matched_count, disc_count, len(closed_trades) - len(new_trades), summaries


# ============================================================================
# 4. Recalibrate + version
# ============================================================================

def recalibrate(n_today: int = 0) -> dict:
    """
    Recalibrate modifier weights from all resolved signals.
    Saves a versioned snapshot for rollback.
    Returns new weights.
    """
    pg = psycopg2.connect(POSTGRES_DSN)
    cur = pg.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("""
        SELECT trade_type, direction, conviction_pct, zone, distance,
               entry_price, exit_price, pnl_pts, modifiers_json
        FROM trade_log
        WHERE pnl_pts IS NOT NULL AND trade_type != 'discretionary'
        ORDER BY trading_date, bucket
    """)
    rows = cur.fetchall()
    pg.close()

    signals = [{
        "trade_type": r["trade_type"], "direction": r["direction"],
        "conviction": float(r["conviction_pct"]), "zone": r["zone"],
        "distance": float(r["distance"]), "pnl": float(r["pnl_pts"]),
        "modifiers": r["modifiers_json"] if isinstance(r["modifiers_json"], dict) else {},
    } for r in rows]

    if len(signals) < 20:
        log.info("Only %d resolved signals — need 20+ for recalibration", len(signals))
        return {}

    # Previous weights (for diff)
    prev_weights = dict(DEFAULT_WEIGHTS)
    if WEIGHT_FILE.exists():
        prev_weights.update(json.loads(WEIGHT_FILE.read_text()))

    new_weights = dict(DEFAULT_WEIGHTS)

    # --- Modifier lift ---
    all_mods = set()
    for s in signals:
        if isinstance(s["modifiers"], dict):
            all_mods.update(s["modifiers"].keys())

    lift_rows = []
    for mod in sorted(all_mods):
        w = [s for s in signals if isinstance(s["modifiers"], dict) and s["modifiers"].get(mod, 0) != 0]
        wo = [s for s in signals if not (isinstance(s["modifiers"], dict) and s["modifiers"].get(mod, 0) != 0)]
        if len(w) < 5 or len(wo) < 5:
            continue

        wr_w = sum(1 for s in w if s["pnl"] > 0) / len(w) * 100
        wr_wo = sum(1 for s in wo if s["pnl"] > 0) / len(wo) * 100
        lift = wr_w - wr_wo
        suggested = max(0, min(20, round(lift / 2)))
        new_weights[mod] = suggested

        lift_rows.append({"modifier": mod, "win_with": wr_w, "win_without": wr_wo,
                          "lift": lift, "n": len(w), "suggested": suggested})

    # --- Calibration ---
    cal_rows = []
    cal_errors = []
    for lo, hi in [(55,60),(60,65),(65,70),(70,75),(75,80),(80,85),(85,90),(90,101)]:
        sub = [s for s in signals if lo <= s["conviction"] < hi]
        if len(sub) < 3:
            continue
        pred = (lo + hi) / 2
        actual = sum(1 for s in sub if s["pnl"] > 0) / len(sub) * 100
        err = actual - pred
        cal_errors.append(abs(err))
        cal_rows.append({"bin": f"{lo}-{hi}%", "predicted": pred, "actual": actual,
                         "error": err, "n": len(sub)})

    # --- Stats ---
    total_pnl = sum(s["pnl"] for s in signals)
    wins = sum(1 for s in signals if s["pnl"] > 0)
    win_rate = wins / len(signals) * 100

    drift = [s for s in signals if s["trade_type"] == "drift"]
    bounce = [s for s in signals if s["trade_type"] == "bounce"]
    drift_wr = (sum(1 for s in drift if s["pnl"] > 0) / len(drift) * 100) if drift else 0
    bounce_wr = (sum(1 for s in bounce if s["pnl"] > 0) / len(bounce) * 100) if bounce else 0

    metrics = {
        "n_signals": len(signals),
        "n_today": n_today,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 1),
        "avg_pnl": round(total_pnl / len(signals), 1),
        "drift_win_rate": round(drift_wr, 1),
        "drift_n": len(drift),
        "bounce_win_rate": round(bounce_wr, 1),
        "bounce_n": len(bounce),
        "cal_error": round(np.mean(cal_errors), 1) if cal_errors else 0,
    }

    # Save versioned snapshot
    record = save_version(new_weights, metrics, prev_weights)

    # --- Print report ---
    print()
    print("=" * 64)
    print(f"  RECALIBRATION — v{record['version']} ({record['date']})")
    print("=" * 64)
    print(f"  {len(signals)} signals | {win_rate:.1f}% win | {total_pnl:+.1f} pts | {total_pnl/len(signals):+.1f}/trade")
    print(f"  DRIFT: {drift_wr:.1f}% ({len(drift)}) | BOUNCE: {bounce_wr:.1f}% ({len(bounce)})")
    if n_today:
        print(f"  Today: {n_today} new trades matched")
    print()

    if lift_rows:
        print("  MODIFIER LIFT")
        print("  {:<22} {:>7} {:>7} {:>7} {:>5} {:>5}".format(
            "Modifier", "Win%+", "Win%-", "Lift", "N", "Wt"))
        print("  " + "-" * 58)
        for r in sorted(lift_rows, key=lambda x: -abs(x["lift"])):
            print("  {:<22} {:>6.1f}% {:>6.1f}% {:>+6.1f} {:>5} {:>5}".format(
                r["modifier"], r["win_with"], r["win_without"], r["lift"], r["n"], r["suggested"]))

    if cal_rows:
        print()
        print("  CALIBRATION (mean error: {:.1f}%)".format(np.mean(cal_errors)))
        print("  {:<10} {:>7} {:>7} {:>7} {:>5}".format("Bin", "Pred", "Actual", "Error", "N"))
        print("  " + "-" * 42)
        for r in cal_rows:
            print("  {:<10} {:>6.1f}% {:>6.1f}% {:>+6.1f} {:>5}".format(
                r["bin"], r["predicted"], r["actual"], r["error"], r["n"]))

    if record["diff"]:
        print()
        print("  WEIGHT CHANGES (from previous)")
        for k, d in record["diff"].items():
            print(f"    {k}: {d['old']} → {d['new']}")

    print()
    print(f"  Version {record['version']} saved")
    print(f"  Weights  → {WEIGHT_FILE}")
    print(f"  History  → {HISTORY_FILE}")
    print()

    return new_weights


# ============================================================================
# EOD run
# ============================================================================

def run_eod():
    """Single end-of-day feedback pass."""
    today = datetime.now().strftime("%Y-%m-%d")

    print()
    print("=" * 64)
    print(f"  EOD FEEDBACK — {today}")
    print("=" * 64)
    print()

    # 1. Pull fills
    log.info("Pulling fills from Tradovate...")
    pull_fills()

    # 2. FIFO match
    log.info("FIFO matching...")
    closed = fifo_match()
    if not closed:
        log.info("No closed trades found")
        print("  No trades to process. Skipping recalibration.")
        return

    # 3. Match to signals + update
    state = load_state()
    processed = set(tuple(p) for p in state.get("processed_pairs", []))
    matched, disc, skipped, summaries = match_and_update(closed, processed)

    if matched + disc == 0:
        log.info("No new trades (all %d already processed)", len(closed))
        print("  All trades already processed. Skipping recalibration.")
        return

    # Print today's trades
    print(f"  {matched} matched to signals, {disc} discretionary, {skipped} previously processed")
    print()

    today_pnl = 0
    for s in summaries:
        d = "LONG" if s["direction"] == 1 else "SHORT"
        tag = f"#{s['signal_id']} {s['trade_type']} {s['conviction']:.0f}%" if s["signal_id"] else "discretionary"
        print(f"  {d} {s['entry']:.2f} → {s['exit']:.2f} = {s['pnl_pts']:+.2f} pts [{tag}] {s['exit_reason']}")
        today_pnl += s["pnl_pts"]

    print(f"\n  Day P&L: {today_pnl:+.2f} pts ({len(summaries)} trades)")

    # Save state
    state["processed_pairs"] = [list(p) for p in processed]
    save_state(state)

    # 4. Recalibrate with version tracking
    recalibrate(n_today=matched + disc)


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="EOD feedback bridge: Tradovate fills → conviction recalibration"
    )
    parser.add_argument("--recalibrate", action="store_true", help="Recalibrate only (no fill pull)")
    parser.add_argument("--history", action="store_true", help="Show weight version history")
    parser.add_argument("--rollback", type=int, metavar="VER", help="Rollback to weight version N")
    parser.add_argument("--diff", type=int, nargs=2, metavar=("V1", "V2"), help="Compare two versions")
    parser.add_argument("--reset", action="store_true", help="Clear processed state")
    args = parser.parse_args()

    if args.history:
        show_history()
        return

    if args.rollback is not None:
        rollback_to(args.rollback)
        return

    if args.diff:
        show_diff(args.diff[0], args.diff[1])
        return

    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        log.info("State cleared — will reprocess all fills")

    if args.recalibrate:
        recalibrate()
        return

    # Default: full EOD run
    run_eod()


if __name__ == "__main__":
    main()
