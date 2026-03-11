#!/usr/bin/env python3
"""
EOD Reconciliation — replay the day's signals and compare to live.

Detects divergence between the backtest/replay engine and the live signal system.
Runs after market close (~16:15 ET). Three steps:

  1. Replay: re-run ConvictionEngine on today's materialized data → trade_log (data_source='replay')
  2. Outcome resolution: resolve any open trades with known price data
  3. Compare: diff replay signals vs live signals for the same day

Usage:
    python3 eod_reconciliation.py                    # Today
    python3 eod_reconciliation.py --date 2026-03-10  # Specific date
    python3 eod_reconciliation.py --date 2026-03-10 --no-write  # Dry run (no DB writes)
"""
import sys
import os
import argparse
import datetime
import json
import logging
from zoneinfo import ZoneInfo

# Path setup — we need access to potential-field modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PF_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "2-processing", "potential-field")
sys.path.insert(0, PF_DIR)
import confpath  # noqa: F401

import psycopg2
import psycopg2.extras

from strategies.bounce_drift.realtime_signals import (
    build_config,
    load_params,
    fetch_day_hash,
    load_day_for_engine,
    log_signal_to_db,
    format_alert,
    SignalDedup,
    ConcurrencyTracker,
)
from strategies.bounce_drift.conviction_engine import ConvictionEngine
from scripts.outcome_tracker import (
    fetch_open_trades,
    fetch_futures_prices,
    resolve_trade,
    update_trade_outcome,
    load_promoted_config,
    DEFAULT_TIMEOUT_BUCKETS,
)

ET = ZoneInfo("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("eod_reconciliation")


# ============================================================================
# Step 1: Replay
# ============================================================================

def run_replay_for_date(conn, trading_date, write_to_db=True):
    """Replay signals for a date using the same engine as live.

    Tags signals with data_source='replay' so they don't mix with live.
    Returns list of replay signal dicts.
    """
    cur = conn.cursor()

    # Build config from promoted strategy (same as live)
    class FakeArgs:
        min_conviction = None
        source = None
        ab_test = False

    config = build_config(FakeArgs(), cur)
    run_id = config["run_id"]
    params = load_params(cur, run_id)

    log.info("Replay params: %s", ", ".join(f"{k}={v:.4f}" for k, v in params.items()))

    day_hash = fetch_day_hash(cur, trading_date)
    if day_hash is None:
        log.warning("No materialized data for %s — skipping replay", trading_date)
        return []

    day_data = load_day_for_engine(cur, trading_date, day_hash)
    if day_data is None:
        log.warning("load_day_for_engine returned None for %s", trading_date)
        return []

    log.info("Replaying %s (hash=%s, %d buckets)",
             trading_date, day_hash[:16], day_data.get("num_buckets", 0))

    ce = ConvictionEngine(params)
    dedup = SignalDedup(window_minutes=config["dedup_gap_min"])
    conc = ConcurrencyTracker(cap=config["concurrency_cap"],
                               timeout_buckets=config["timeout_buckets"])

    replay_signals = []
    min_conv = config["conviction_threshold"]
    allowed_types = set(config["trade_types"])
    strategy_id = config["version_tag"]

    for bucket_idx, signal in ce.replay_day(day_data, every_n=5, min_conviction=min_conv):
        if signal is None:
            continue
        if signal.trade_type.value not in allowed_types:
            continue

        synth_time = datetime.datetime(2026, 1, 1, 9, 30, tzinfo=ET) + \
            datetime.timedelta(minutes=bucket_idx)

        if dedup.is_duplicate(signal, now=synth_time):
            continue
        if not conc.can_open(signal.well_price, bucket_idx):
            continue

        dedup.record(signal, now=synth_time)
        conc.record_open(signal.well_price, bucket_idx)

        sig_dict = {
            "bucket": bucket_idx,
            "trade_type": signal.trade_type.value,
            "direction": signal.direction,
            "conviction_pct": signal.conviction_pct,
            "zone": signal.zone.value,
            "distance": signal.distance,
            "entry_price": signal.entry_price,
            "target_price": signal.target_price,
            "stop_price": signal.stop_price,
            "well_price": signal.well_price,
            "modifiers": signal.modifiers,
        }
        replay_signals.append(sig_dict)

        if write_to_db:
            log_signal_to_db(
                cur, signal, trading_date, bucket_idx,
                strategy_id=strategy_id, data_source="replay", session="RTH"
            )
            conn.commit()

    log.info("Replay produced %d signals for %s", len(replay_signals), trading_date)
    return replay_signals


# ============================================================================
# Step 2: Resolve open trades
# ============================================================================

def resolve_open_trades(conn, trading_date):
    """Resolve any open trades for the given date."""
    cur = conn.cursor()

    promoted = load_promoted_config(cur)
    timeout = promoted["timeout_buckets"] if promoted else DEFAULT_TIMEOUT_BUCKETS

    # Fetch only open trades for this date
    cur.execute("""
        SELECT id, trading_date, bucket, trade_type, direction,
               entry_price, target_price, stop_price, well_price,
               conviction_pct, zone, distance, created_at
        FROM trade_log
        WHERE exit_price IS NULL AND trading_date = %s
        ORDER BY bucket
    """, (trading_date,))

    columns = [
        "id", "trading_date", "bucket", "trade_type", "direction",
        "entry_price", "target_price", "stop_price", "well_price",
        "conviction_pct", "zone", "distance", "created_at",
    ]
    open_trades = [dict(zip(columns, row)) for row in cur.fetchall()]

    if not open_trades:
        log.info("No open trades to resolve for %s", trading_date)
        return 0

    futures_prices = fetch_futures_prices(cur, trading_date)
    resolved = 0

    for trade in open_trades:
        outcome = resolve_trade(trade, futures_prices, timeout)
        if outcome is not None:
            update_trade_outcome(cur, trade["id"], outcome)
            resolved += 1
            log.info("Resolved #%d: %s %s @ %.0f → %s @ %.1f (%.1f pts)",
                     trade["id"], trade["trade_type"],
                     "LONG" if trade["direction"] == 1 else "SHORT",
                     float(trade["entry_price"]),
                     outcome["exit_reason"], outcome["exit_price"],
                     outcome["pnl_pts"])

    conn.commit()
    log.info("Resolved %d / %d open trades for %s", resolved, len(open_trades), trading_date)
    return resolved


# ============================================================================
# Step 3: Compare
# ============================================================================

def fetch_signals_for_date(conn, trading_date, data_source):
    """Fetch all signals for a date and source."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT id, bucket, trade_type, direction, conviction_pct,
               zone, distance, entry_price, target_price, stop_price,
               well_price, modifiers_json, exit_price, exit_reason, pnl_pts
        FROM trade_log
        WHERE trading_date = %s AND data_source = %s
        ORDER BY bucket
    """, (trading_date, data_source))
    return [dict(r) for r in cur.fetchall()]


def compare_signals(live_signals, replay_signals, trading_date):
    """Compare live vs replay signals and print a reconciliation report.

    Returns dict with comparison metrics.
    """
    print()
    print("=" * 70)
    print(f"  EOD RECONCILIATION — {trading_date}")
    print("=" * 70)
    print()

    # Summary counts
    print(f"  Live signals:   {len(live_signals)}")
    print(f"  Replay signals: {len(replay_signals)}")
    print()

    if not live_signals and not replay_signals:
        print("  No signals from either source. Nothing to compare.")
        return {"live_count": 0, "replay_count": 0, "matched": 0,
                "live_only": 0, "replay_only": 0, "divergences": []}

    # Match signals: same bucket (±2), same direction, same trade_type
    BUCKET_TOLERANCE = 2
    matched = []
    live_unmatched = list(range(len(live_signals)))
    replay_unmatched = list(range(len(replay_signals)))

    for li, ls in enumerate(live_signals):
        best_ri = None
        best_bucket_diff = 999
        for ri in replay_unmatched:
            rs = replay_signals[ri]
            if (ls["direction"] == rs["direction"] and
                ls["trade_type"] == rs["trade_type"] and
                abs(ls["bucket"] - rs["bucket"]) <= BUCKET_TOLERANCE):
                bd = abs(ls["bucket"] - rs["bucket"])
                if bd < best_bucket_diff:
                    best_bucket_diff = bd
                    best_ri = ri

        if best_ri is not None:
            matched.append((li, best_ri))
            live_unmatched.remove(li)
            replay_unmatched.remove(best_ri)

    # Print matched signals
    if matched:
        print("  MATCHED SIGNALS ({})".format(len(matched)))
        print("  {:<6} {:<8} {:<6} {:>8} {:>8} {:>8} {:>6} {:>6}".format(
            "Bkt", "Type", "Dir", "Entry", "Conv%L", "Conv%R", "ΔConv", "ΔEntry"))
        print("  " + "-" * 66)

        for li, ri in matched:
            ls, rs = live_signals[li], replay_signals[ri]
            d_conv = float(rs["conviction_pct"]) - float(ls["conviction_pct"])
            d_entry = float(rs["entry_price"]) - float(ls["entry_price"])
            dir_str = "LONG" if ls["direction"] == 1 else "SHORT"
            print("  {:<6} {:<8} {:<6} {:>8.1f} {:>7.1f}% {:>7.1f}% {:>+5.1f} {:>+6.1f}".format(
                ls["bucket"], ls["trade_type"], dir_str,
                float(ls["entry_price"]),
                float(ls["conviction_pct"]), float(rs["conviction_pct"]),
                d_conv, d_entry))
        print()

    # Print live-only (missed by replay)
    divergences = []
    if live_unmatched:
        print("  LIVE-ONLY (replay missed these — potential UNDERDETECTION):")
        for li in live_unmatched:
            ls = live_signals[li]
            dir_str = "LONG" if ls["direction"] == 1 else "SHORT"
            print("    t={:<4} {} {:<8} @ {:.1f}  conv={:.0f}%  zone={}".format(
                ls["bucket"], dir_str, ls["trade_type"],
                float(ls["entry_price"]), float(ls["conviction_pct"]),
                ls["zone"]))
            divergences.append({
                "type": "live_only", "bucket": ls["bucket"],
                "trade_type": ls["trade_type"], "direction": ls["direction"],
                "entry": float(ls["entry_price"]),
                "conviction": float(ls["conviction_pct"]),
            })
        print()

    if replay_unmatched:
        print("  REPLAY-ONLY (live missed these — potential OVERFILTERING):")
        for ri in replay_unmatched:
            rs = replay_signals[ri]
            dir_str = "LONG" if rs["direction"] == 1 else "SHORT"
            print("    t={:<4} {} {:<8} @ {:.1f}  conv={:.0f}%  zone={}".format(
                rs["bucket"], dir_str, rs["trade_type"],
                float(rs["entry_price"]), float(rs["conviction_pct"]),
                rs["zone"]))
            divergences.append({
                "type": "replay_only", "bucket": rs["bucket"],
                "trade_type": rs["trade_type"], "direction": rs["direction"],
                "entry": float(rs["entry_price"]),
                "conviction": float(rs["conviction_pct"]),
            })
        print()

    # Conviction drift for matched pairs
    if matched:
        conv_diffs = [float(replay_signals[ri]["conviction_pct"]) - float(live_signals[li]["conviction_pct"])
                      for li, ri in matched]
        entry_diffs = [float(replay_signals[ri]["entry_price"]) - float(live_signals[li]["entry_price"])
                       for li, ri in matched]

        print("  CONVICTION DRIFT (replay - live):")
        print(f"    Mean: {sum(conv_diffs)/len(conv_diffs):+.2f}%")
        print(f"    Max:  {max(conv_diffs, key=abs):+.2f}%")
        print(f"    StdDev: {_std(conv_diffs):.2f}%")
        print()
        print("  ENTRY PRICE DRIFT:")
        print(f"    Mean: {sum(entry_diffs)/len(entry_diffs):+.2f} pts")
        print(f"    Max:  {max(entry_diffs, key=abs):+.2f} pts")
        print()

    # P&L comparison (for resolved signals)
    live_resolved = [s for s in live_signals if s["pnl_pts"] is not None]
    replay_resolved = [s for s in replay_signals if s["pnl_pts"] is not None]

    if live_resolved:
        live_pnl = sum(float(s["pnl_pts"]) for s in live_resolved)
        live_wins = sum(1 for s in live_resolved if float(s["pnl_pts"]) > 0)
        print(f"  LIVE P&L:   {live_pnl:+.1f} pts ({len(live_resolved)} resolved, "
              f"{live_wins}/{len(live_resolved)} wins)")
    if replay_resolved:
        replay_pnl = sum(float(s["pnl_pts"]) for s in replay_resolved)
        replay_wins = sum(1 for s in replay_resolved if float(s["pnl_pts"]) > 0)
        print(f"  REPLAY P&L: {replay_pnl:+.1f} pts ({len(replay_resolved)} resolved, "
              f"{replay_wins}/{len(replay_resolved)} wins)")

    # Verdict
    match_rate = len(matched) / max(len(live_signals), len(replay_signals), 1) * 100
    print()
    if match_rate == 100 and not divergences:
        print("  VERDICT: PERFECT MATCH — live and replay produced identical signals")
    elif match_rate >= 80:
        print(f"  VERDICT: MOSTLY ALIGNED ({match_rate:.0f}% match) — {len(divergences)} divergence(s)")
    elif len(live_signals) == 0 and len(replay_signals) == 0:
        print("  VERDICT: NO SIGNALS — quiet day")
    else:
        print(f"  VERDICT: DIVERGENT ({match_rate:.0f}% match) — INVESTIGATE")
        print(f"           {len(live_unmatched)} live-only, {len(replay_unmatched)} replay-only")
    print("=" * 70)
    print()

    return {
        "trading_date": str(trading_date),
        "live_count": len(live_signals),
        "replay_count": len(replay_signals),
        "matched": len(matched),
        "live_only": len(live_unmatched),
        "replay_only": len(replay_unmatched),
        "match_rate": match_rate,
        "divergences": divergences,
    }


def _std(vals):
    if len(vals) < 2:
        return 0.0
    mean = sum(vals) / len(vals)
    return (sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5


# ============================================================================
# Save report
# ============================================================================

def save_report(result, trading_date):
    """Persist reconciliation report to 4-feedback/reconciliation/."""
    report_dir = os.path.join(SCRIPT_DIR, "reconciliation")
    os.makedirs(report_dir, exist_ok=True)

    report_path = os.path.join(report_dir, f"{trading_date}.json")
    with open(report_path, "w") as f:
        json.dump(result, f, indent=2, default=str)

    log.info("Report saved: %s", report_path)
    return report_path


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="EOD reconciliation: replay day signals and compare to live"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Trading date (YYYY-MM-DD). Default: today."
    )
    parser.add_argument(
        "--no-write", action="store_true",
        help="Dry run — don't write replay signals to DB"
    )
    parser.add_argument(
        "--skip-resolve", action="store_true",
        help="Skip outcome resolution step"
    )
    args = parser.parse_args()

    if args.date:
        trading_date = args.date
    else:
        trading_date = datetime.datetime.now(ET).strftime("%Y-%m-%d")

    print()
    print("=" * 70)
    print(f"  EOD RECONCILIATION STARTING — {trading_date}")
    print(f"  Time: {datetime.datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}")
    print("=" * 70)

    conn = psycopg2.connect("dbname=mlops_system")

    try:
        # Step 0: Clean any previous replay for this date
        if not args.no_write:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM trade_log WHERE trading_date = %s AND data_source = 'replay'",
                (trading_date,)
            )
            deleted = cur.rowcount
            conn.commit()
            if deleted:
                log.info("Cleared %d previous replay signals for %s", deleted, trading_date)

        # Step 1: Replay
        log.info("STEP 1: Replaying signals...")
        replay_signals = run_replay_for_date(conn, trading_date, write_to_db=not args.no_write)

        # Step 2: Resolve open trades
        if not args.skip_resolve:
            log.info("STEP 2: Resolving open trades...")
            resolve_open_trades(conn, trading_date)

        # Step 3: Compare
        log.info("STEP 3: Comparing live vs replay...")

        # Fetch from DB (live = anything that's not 'replay')
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT id, bucket, trade_type, direction, conviction_pct,
                   zone, distance, entry_price, target_price, stop_price,
                   well_price, modifiers_json, exit_price, exit_reason, pnl_pts
            FROM trade_log
            WHERE trading_date = %s AND data_source != 'replay'
            ORDER BY bucket
        """, (trading_date,))
        live_signals = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT id, bucket, trade_type, direction, conviction_pct,
                   zone, distance, entry_price, target_price, stop_price,
                   well_price, modifiers_json, exit_price, exit_reason, pnl_pts
            FROM trade_log
            WHERE trading_date = %s AND data_source = 'replay'
            ORDER BY bucket
        """, (trading_date,))
        replay_from_db = [dict(r) for r in cur.fetchall()]

        result = compare_signals(live_signals, replay_from_db, trading_date)

        # Save report
        save_report(result, trading_date)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
