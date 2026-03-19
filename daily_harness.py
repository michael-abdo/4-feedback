#!/usr/bin/env python3
"""
Daily Feedback Harness — strategy-agnostic live-vs-backtest comparison + similar day finder.

Runs after market close (~16:35 ET). Two capabilities:
  A) Compare today's live metrics to backtest per-day distribution → green/yellow/red
  B) Find historically similar days by independent features → validate model behavior

Usage:
    python3 daily_harness.py                          # Today, all active strategies
    python3 daily_harness.py --date 2026-03-18        # Specific date
    python3 daily_harness.py --strategy bounce_drift  # Single strategy
    python3 daily_harness.py --no-email               # Skip alerts
    python3 daily_harness.py --backfill 30            # Run for last 30 days
"""
import sys
import os
import argparse
import datetime
import json
import logging
import math
from collections import defaultdict

import numpy as np
import psycopg2

# Path setup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PF_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "2-processing", "potential-field")
sys.path.insert(0, PF_DIR)
sys.path.insert(0, SCRIPT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONN = "dbname=mlops_system"

# Z-score thresholds for traffic light
Z_YELLOW = 1.5
Z_RED = 2.5

# Similar day search
TOP_K = 10
FEATURE_WEIGHTS = {
    "range_pts": 2.0,
    "intraday_vol": 2.0,
    "dow_sin": 0.5,
    "dow_cos": 0.5,
    "dte": 1.0,
}

# NQ quarterly expiry: 3rd Friday of Mar, Jun, Sep, Dec
EXPIRY_MONTHS = [3, 6, 9, 12]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _third_friday(year, month):
    """Return the 3rd Friday of the given month."""
    import calendar
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [d for d in c.itermonthdays2(year, month) if d[0] != 0 and d[1] == 4]
    return datetime.date(year, month, fridays[2][0])


def _days_to_expiry(dt):
    """Days to nearest NQ quarterly expiry (3rd Friday of Mar/Jun/Sep/Dec)."""
    if isinstance(dt, str):
        dt = datetime.date.fromisoformat(dt)
    candidates = []
    for ym_offset in range(-1, 5):
        y = dt.year + (dt.month + ym_offset * 3 - 1) // 12
        m = ((dt.month + ym_offset * 3 - 1) % 12) + 1
        if m in EXPIRY_MONTHS:
            try:
                exp = _third_friday(y, m)
                candidates.append(exp)
            except (ValueError, IndexError):
                pass
    if not candidates:
        return 30  # fallback
    diffs = [(exp - dt).days for exp in candidates if (exp - dt).days >= 0]
    return min(diffs) if diffs else min(abs((exp - dt).days) for exp in candidates)


def _z_score(live_val, mean, std):
    if std == 0 or std is None:
        return 0.0
    return (live_val - mean) / std


def _status(z):
    az = abs(z)
    if az >= Z_RED:
        return "red"
    if az >= Z_YELLOW:
        return "yellow"
    return "green"


def _percentile(val, sorted_vals):
    """Compute percentile of val within sorted_vals."""
    if not sorted_vals:
        return 50.0
    below = sum(1 for v in sorted_vals if v <= val)
    return round(below / len(sorted_vals) * 100, 1)


def _worst_status(*statuses):
    order = {"red": 2, "yellow": 1, "green": 0}
    worst = max(statuses, key=lambda s: order.get(s, 0))
    return worst


# ---------------------------------------------------------------------------
# Strategy Discovery
# ---------------------------------------------------------------------------
def get_active_strategies(conn):
    """Get all promoted (active) strategies from strategy_promotions."""
    cur = conn.cursor()
    cur.execute("""
        SELECT sp.strategy_name, sv.version_tag, sv.run_id,
               sv.conviction_threshold, sv.timeout_buckets, sv.extra_config
        FROM strategy_promotions sp
        JOIN strategy_versions sv ON sv.id = sp.strategy_version_id
        WHERE sp.is_active = TRUE
    """)
    rows = cur.fetchall()
    cur.close()
    strategies = []
    for r in rows:
        strategy_name = r[0]
        version_tag = r[1]
        # Build list of all strategy_id values that belong to this strategy.
        # trade_log.strategy_id is inconsistently tagged — sometimes the
        # strategy_name ('bounce_drift'), sometimes the version_tag
        # ('v5-recalibrated'), sometimes sub-versions ('v4-baseline').
        strategy_ids = {strategy_name, version_tag}
        # Also find any strategy_id in trade_log that matches either name
        cur2 = conn.cursor()
        cur2.execute("""
            SELECT DISTINCT strategy_id FROM trade_log
            WHERE strategy_id IN (%s, %s)
               OR strategy_version LIKE %s
        """, (strategy_name, version_tag, f"%{version_tag}%"))
        for row2 in cur2.fetchall():
            strategy_ids.add(row2[0])
        cur2.close()

        strategies.append({
            "strategy_id": strategy_name,
            "strategy_ids": list(strategy_ids),  # all matching IDs
            "version_tag": version_tag,
            "run_id": str(r[2]) if r[2] else None,
            "conviction_threshold": float(r[3]) if r[3] else 85.0,
            "timeout_buckets": int(r[4]) if r[4] else 90,
            "extra_config": r[5] or {},
        })
    return strategies


# ---------------------------------------------------------------------------
# Capability A: Live vs Backtest Comparison
# ---------------------------------------------------------------------------
def compute_live_metrics(conn, trading_date, strategy_ids):
    """Compute today's metrics from trade_log (executed trades only)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_type, conviction_pct, pnl_pts, exit_reason, bucket,
               COALESCE(timeout_buckets, 90) as timeout_used
        FROM trade_log
        WHERE trading_date = %s AND strategy_id = ANY(%s)
          AND data_source NOT IN ('replay')
          AND pnl_pts IS NOT NULL
          AND (execution_status IN ('accepted', 'demo', 'filled')
               OR execution_status IS NULL)
    """, (trading_date, strategy_ids))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return {"n_trades": 0, "has_data": False}

    pnls = [r[2] for r in rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    exit_reasons = defaultdict(int)
    for r in rows:
        exit_reasons[r[3] or "unknown"] += 1

    total_wins = sum(wins) if wins else 0
    total_losses = abs(sum(losses)) if losses else 0

    return {
        "has_data": True,
        "n_trades": len(rows),
        "win_rate": round(len(wins) / len(rows), 3) if rows else 0,
        "avg_pnl": round(float(np.mean(pnls)), 1),
        "total_pnl": round(sum(pnls), 1),
        "profit_factor": round(total_wins / total_losses, 2) if total_losses > 0 else None,
        "exit_reasons": dict(exit_reasons),
        "target_pct": round(exit_reasons.get("target", 0) / len(rows), 3),
        "stop_pct": round(exit_reasons.get("stop", 0) / len(rows), 3),
        "timeout_pct": round(exit_reasons.get("timeout", 0) / len(rows), 3),
        "avg_conviction": round(float(np.mean([r[1] for r in rows])), 1),
    }


def compute_backtest_distribution(conn, trading_date, strategy_ids):
    """Compute per-day distribution from all historical data (backtest + executed live)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT trading_date,
               COUNT(*) as n,
               AVG(CASE WHEN pnl_pts > 0 THEN 1.0 ELSE 0.0 END) as wr,
               AVG(pnl_pts) as avg_pnl,
               SUM(pnl_pts) as total_pnl,
               SUM(CASE WHEN exit_reason='target' THEN 1 ELSE 0 END)::float / COUNT(*) as target_pct,
               SUM(CASE WHEN exit_reason='stop' THEN 1 ELSE 0 END)::float / COUNT(*) as stop_pct
        FROM trade_log
        WHERE strategy_id = ANY(%s)
          AND data_source NOT IN ('replay')
          AND pnl_pts IS NOT NULL
          AND trading_date < %s
          AND (execution_status IN ('accepted', 'demo', 'filled')
               OR execution_status IS NULL)
        GROUP BY trading_date
        HAVING COUNT(*) > 0
    """, (strategy_ids, trading_date))
    rows = cur.fetchall()
    cur.close()

    if len(rows) < 30:
        return None  # insufficient data

    dist = {
        "n_days": len(rows),
        "n_trades": {"vals": sorted([r[1] for r in rows])},
        "win_rate": {"vals": sorted([float(r[2]) for r in rows])},
        "avg_pnl": {"vals": sorted([float(r[3]) for r in rows])},
        "total_pnl": {"vals": sorted([float(r[4]) for r in rows])},
        "target_pct": {"vals": sorted([float(r[5]) for r in rows])},
        "stop_pct": {"vals": sorted([float(r[6]) for r in rows])},
    }

    for key in ["n_trades", "win_rate", "avg_pnl", "total_pnl", "target_pct", "stop_pct"]:
        vals = dist[key]["vals"]
        dist[key]["mean"] = round(float(np.mean(vals)), 3)
        dist[key]["std"] = round(float(np.std(vals)), 3)
        dist[key]["p5"] = round(float(np.percentile(vals, 5)), 3)
        dist[key]["p25"] = round(float(np.percentile(vals, 25)), 3)
        dist[key]["p50"] = round(float(np.percentile(vals, 50)), 3)
        dist[key]["p75"] = round(float(np.percentile(vals, 75)), 3)
        dist[key]["p95"] = round(float(np.percentile(vals, 95)), 3)
        del dist[key]["vals"]  # don't store raw arrays in DB

    return dist


def compare_live_to_backtest(live, bt_dist):
    """Compare live metrics to backtest distribution. Return comparison dict."""
    if not live.get("has_data") or bt_dist is None:
        return {"status": "grey", "reason": "no data"}

    comparison = {}
    metrics = [
        ("n_trades", live["n_trades"]),
        ("win_rate", live["win_rate"]),
        ("avg_pnl", live["avg_pnl"]),
        ("target_pct", live["target_pct"]),
        ("stop_pct", live["stop_pct"]),
    ]

    statuses = []
    for name, live_val in metrics:
        bt = bt_dist.get(name, {})
        mean = bt.get("mean", 0)
        std = bt.get("std", 1)
        z = _z_score(live_val, mean, std)
        st = _status(z)
        statuses.append(st)
        comparison[name] = {
            "live": live_val,
            "bt_mean": mean,
            "bt_std": std,
            "z": round(z, 2),
            "status": st,
        }

    # Special case: 0 trades when backtest expects 2+
    if live["n_trades"] == 0 and bt_dist.get("n_trades", {}).get("mean", 0) >= 2:
        comparison["n_trades"]["status"] = "red"
        statuses.append("red")

    comparison["overall_status"] = _worst_status(*statuses)
    return comparison


# ---------------------------------------------------------------------------
# Capability B: Similar Day Finder
# ---------------------------------------------------------------------------
def compute_day_features(conn, trading_date):
    """Compute independent market features for a given date."""
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(price) - MIN(price) as range_pts,
               STDDEV(price) as intraday_vol,
               COUNT(*) as n_buckets
        FROM materialized_futures
        WHERE day_content_hash IN (
            SELECT DISTINCT ON (td) day_content_hash
            FROM (SELECT day_content_hash,
                         (bucket_time AT TIME ZONE 'UTC')::date AS td
                  FROM materialized_futures
                  WHERE day_content_hash NOT LIKE 'eth-%%') sub
            WHERE td = %s
            ORDER BY td, day_content_hash
        )
    """, (trading_date,))
    row = cur.fetchone()
    cur.close()

    if row is None or row[0] is None:
        return None

    dt = trading_date if isinstance(trading_date, datetime.date) else datetime.date.fromisoformat(str(trading_date))
    dow = dt.weekday()  # 0=Mon, 4=Fri

    return {
        "range_pts": float(row[0]),
        "intraday_vol": float(row[1]) if row[1] else 0.0,
        "n_buckets": int(row[2]),
        "dow": dow,
        "dow_sin": math.sin(2 * math.pi * dow / 5),
        "dow_cos": math.cos(2 * math.pi * dow / 5),
        "dte": _days_to_expiry(dt),
    }


def compute_all_historical_features(conn, before_date):
    """Compute features for all historical days. Returns list of (date, features_dict)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT sub.td,
               MAX(mf.price) - MIN(mf.price) as range_pts,
               STDDEV(mf.price) as intraday_vol,
               COUNT(*) as n_buckets
        FROM (
            SELECT DISTINCT ON (td) day_content_hash, td
            FROM (SELECT day_content_hash,
                         (bucket_time AT TIME ZONE 'UTC')::date AS td
                  FROM materialized_futures
                  WHERE day_content_hash NOT LIKE 'eth-%%') inner_sub
            ORDER BY td, day_content_hash
        ) sub
        JOIN materialized_futures mf ON mf.day_content_hash = sub.day_content_hash
        WHERE sub.td < %s
        GROUP BY sub.td
        HAVING COUNT(*) > 100
    """, (before_date,))
    rows = cur.fetchall()
    cur.close()

    results = []
    for r in rows:
        dt = r[0]
        dow = dt.weekday()
        results.append((dt, {
            "range_pts": float(r[1]) if r[1] else 0,
            "intraday_vol": float(r[2]) if r[2] else 0,
            "dow_sin": math.sin(2 * math.pi * dow / 5),
            "dow_cos": math.cos(2 * math.pi * dow / 5),
            "dte": _days_to_expiry(dt),
        }))
    return results


def find_similar_days(conn, trading_date, strategy_ids, top_k=TOP_K):
    """Find top-K historically similar days by independent market features."""
    today_features = compute_day_features(conn, trading_date)
    if today_features is None:
        return None, None

    hist = compute_all_historical_features(conn, trading_date)
    if len(hist) < 30:
        return today_features, None

    # Z-score normalize
    feature_keys = list(FEATURE_WEIGHTS.keys())
    all_vals = {k: [] for k in feature_keys}
    for _, feat in hist:
        for k in feature_keys:
            all_vals[k].append(feat.get(k, 0))

    means = {k: np.mean(all_vals[k]) for k in feature_keys}
    stds = {k: np.std(all_vals[k]) for k in feature_keys}

    def _normalize(feat):
        return {k: (feat.get(k, 0) - means[k]) / max(stds[k], 1e-10) for k in feature_keys}

    today_norm = _normalize(today_features)

    # Compute distances
    distances = []
    for dt, feat in hist:
        hist_norm = _normalize(feat)
        dist = 0
        for k in feature_keys:
            dist += FEATURE_WEIGHTS[k] * (today_norm[k] - hist_norm[k]) ** 2
        distances.append((dt, math.sqrt(dist), feat))

    distances.sort(key=lambda x: x[1])
    top_days = distances[:top_k]

    # Fetch backtest results for similar days
    if top_days:
        cur = conn.cursor()
        date_list = [d[0] for d in top_days]
        cur.execute("""
            SELECT trading_date,
                   COUNT(*) as n_trades,
                   AVG(CASE WHEN pnl_pts > 0 THEN 1.0 ELSE 0.0 END) as win_rate,
                   AVG(pnl_pts) as avg_pnl,
                   SUM(pnl_pts) as total_pnl
            FROM trade_log
            WHERE strategy_id = ANY(%s)
              AND data_source NOT IN ('replay')
              AND pnl_pts IS NOT NULL
              AND trading_date = ANY(%s)
              AND (execution_status IN ('accepted', 'demo', 'filled')
                   OR execution_status IS NULL)
            GROUP BY trading_date
        """, (strategy_ids, date_list))
        bt_rows = {r[0]: {"n_trades": r[1], "win_rate": round(float(r[2]), 3),
                          "avg_pnl": round(float(r[3]), 1), "total_pnl": round(float(r[4]), 1)}
                   for r in cur.fetchall()}
        cur.close()
    else:
        bt_rows = {}

    similar = []
    for dt, dist, feat in top_days:
        entry = {
            "trading_date": str(dt),
            "distance": round(dist, 3),
            "features": {k: round(v, 2) for k, v in feat.items()},
            "backtest": bt_rows.get(dt, {"n_trades": 0, "note": "no backtest data"}),
        }
        similar.append(entry)

    # Summary
    bt_available = [s["backtest"] for s in similar if s["backtest"].get("n_trades", 0) > 0]
    summary = {}
    if bt_available:
        summary = {
            "avg_n_trades": round(np.mean([b["n_trades"] for b in bt_available]), 1),
            "avg_win_rate": round(np.mean([b["win_rate"] for b in bt_available]), 3),
            "avg_pnl": round(np.mean([b["avg_pnl"] for b in bt_available]), 1),
            "n_similar_with_data": len(bt_available),
        }

    return today_features, {"similar_days": similar, "summary": summary}


# ---------------------------------------------------------------------------
# Main harness
# ---------------------------------------------------------------------------
def run_harness(trading_date, strategy_filter=None, send_emails=True):
    """Run the full harness for a given date."""
    conn = psycopg2.connect(DB_CONN)

    strategies = get_active_strategies(conn)
    if strategy_filter:
        strategies = [s for s in strategies if s["strategy_id"] == strategy_filter]

    if not strategies:
        log.warning("No active strategies found (filter=%s)", strategy_filter)
        conn.close()
        return

    for strat in strategies:
        sid = strat["strategy_id"]
        sids = strat["strategy_ids"]
        log.info("Processing %s for %s (matching IDs: %s)...", sid, trading_date, sids)

        # Capability A
        live = compute_live_metrics(conn, trading_date, sids)
        bt_dist = compute_backtest_distribution(conn, trading_date, sids)
        comparison = compare_live_to_backtest(live, bt_dist)

        # Capability B
        day_features, similar_result = find_similar_days(conn, trading_date, sids)

        # Build verdict
        a_status = comparison.get("overall_status", "grey")
        b_status = "green"
        action_items = []

        if live.get("has_data") and similar_result and similar_result.get("summary"):
            summary = similar_result["summary"]
            if summary.get("avg_pnl") and live["avg_pnl"] != 0:
                pnl_ratio = float(live["total_pnl"]) / max(abs(float(summary["avg_pnl"])), 0.1)
                if abs(pnl_ratio) > 3:
                    b_status = "yellow"
                    action_items.append(
                        f"YELLOW: live total PnL {live['total_pnl']:+.0f} vs similar-day avg "
                        f"{summary['avg_pnl']:+.1f} (ratio {pnl_ratio:.1f}x)")

        # Red flags from comparison
        for metric, data in comparison.items():
            if isinstance(data, dict) and data.get("status") == "red":
                action_items.append(
                    f"RED: {metric} = {data['live']} (z={data['z']:+.1f}, "
                    f"expected {data['bt_mean']:.2f} ± {data['bt_std']:.2f})")

        overall = _worst_status(a_status, b_status)

        verdict = {
            "overall_status": overall,
            "strategy": sid,
            "version_tag": strat["version_tag"],
            "live_vs_backtest": {
                "status": a_status,
                "summary": (f"{live['n_trades']} trades, "
                            f"{live.get('win_rate', 0)*100:.0f}% WR, "
                            f"{live.get('total_pnl', 0):+.0f} pts"
                            if live.get("has_data") else "no trades"),
            },
            "similar_days": {
                "status": b_status,
                "top_match": (similar_result["similar_days"][0]["trading_date"]
                              if similar_result and similar_result.get("similar_days")
                              else None),
                "summary": similar_result.get("summary") if similar_result else None,
            },
            "action_items": action_items,
        }

        # Persist to DB
        cur = conn.cursor()
        # Remove raw vals from bt_dist to keep JSONB small
        bt_dist_clean = {}
        if bt_dist:
            bt_dist_clean = {k: {kk: vv for kk, vv in v.items() if kk != "vals"}
                             if isinstance(v, dict) else v
                             for k, v in bt_dist.items()}

        cur.execute("""
            INSERT INTO daily_feedback
                (trading_date, strategy_id, live_metrics, backtest_dist,
                 comparison, status, day_features, similar_days, verdict)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trading_date, strategy_id)
            DO UPDATE SET
                run_timestamp = NOW(),
                live_metrics = EXCLUDED.live_metrics,
                backtest_dist = EXCLUDED.backtest_dist,
                comparison = EXCLUDED.comparison,
                status = EXCLUDED.status,
                day_features = EXCLUDED.day_features,
                similar_days = EXCLUDED.similar_days,
                verdict = EXCLUDED.verdict
        """, (
            trading_date, sid,
            json.dumps(live, default=str),
            json.dumps(bt_dist_clean, default=str),
            json.dumps(comparison, default=str),
            overall,
            json.dumps(day_features, default=str) if day_features else "{}",
            json.dumps(similar_result.get("similar_days", []), default=str) if similar_result else "[]",
            json.dumps(verdict, default=str),
        ))
        conn.commit()
        cur.close()

        log.info("[%s] %s → %s | %d trades, %s",
                 sid, trading_date, overall.upper(),
                 live.get("n_trades", 0),
                 "; ".join(action_items) if action_items else "all clear")

        # Email on red
        if overall == "red" and send_emails:
            try:
                from execution_reconciliation import send_alert
                subject = f"[HARNESS] RED — {sid} {trading_date}"
                body = json.dumps(verdict, indent=2, default=str)
                send_alert(subject, body)
            except Exception as e:
                log.warning("Failed to send alert: %s", e)

    conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily feedback harness")
    parser.add_argument("--date", type=str, default=None,
                        help="Trading date (YYYY-MM-DD, default: today ET)")
    parser.add_argument("--strategy", type=str, default=None,
                        help="Filter to single strategy_id")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email alerts")
    parser.add_argument("--backfill", type=int, default=None,
                        help="Run for last N trading days")
    args = parser.parse_args()

    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    today = datetime.datetime.now(ET).date()

    if args.backfill:
        # Run for last N calendar days (skips weekends via no-data)
        for i in range(args.backfill, 0, -1):
            dt = today - datetime.timedelta(days=i)
            if dt.weekday() >= 5:  # skip weekends
                continue
            run_harness(dt, args.strategy, send_emails=False)
    else:
        dt = datetime.date.fromisoformat(args.date) if args.date else today
        run_harness(dt, args.strategy, send_emails=not args.no_email)
