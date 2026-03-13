#!/usr/bin/env python3
"""
Tradovate EOD Export Scheduler

Long-running process managed by PM2. Runs the tradovate export after market close
each trading day (5:30 PM CT, Mon-Fri), then sleeps until the next run.

Auth is handled by AuthManager (shared with execution system) — no separate
token refresh step needed.

Usage:
    pm2 start tradovate_scheduler.py --name tradovate-eod --interpreter python3
    pm2 start tradovate_scheduler.py --name tradovate-eod --interpreter python3 -- --run-now
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).parent
EXPORT_SCRIPT = SCRIPT_DIR / "tradovate_export.py"

CT_TZ = ZoneInfo("America/Chicago")

# Run at 5:30 PM CT (after 5 PM market daily close, gives 30min buffer)
RUN_HOUR = 17
RUN_MINUTE = 30

# Only run Mon-Fri (weekday 0=Mon, 4=Fri)
TRADING_DAYS = {0, 1, 2, 3, 4}

# How many days to look back on each export
EXPORT_LOOKBACK_DAYS = 3


def now_ct() -> datetime:
    return datetime.now(CT_TZ)


def next_run_time() -> datetime:
    """Calculate the next scheduled run time (5:30 PM CT on a trading day)."""
    ct_now = now_ct()
    candidate = ct_now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)

    # If we already passed today's run time, start from tomorrow
    if ct_now >= candidate:
        candidate += timedelta(days=1)

    # Skip weekends
    while candidate.weekday() not in TRADING_DAYS:
        candidate += timedelta(days=1)

    return candidate


def run_export() -> bool:
    """Run the tradovate export. Returns True on success."""
    print(f"[{now_ct().isoformat()}] Running Tradovate EOD export (last {EXPORT_LOOKBACK_DAYS} days)...")
    result = subprocess.run(
        [sys.executable, str(EXPORT_SCRIPT), "--since", str(EXPORT_LOOKBACK_DAYS)],
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        text=True,
        timeout=300,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"Export FAILED: {result.stderr}", file=sys.stderr)
        return False
    print("Export completed successfully")
    return True


def main():
    parser = argparse.ArgumentParser(description="Tradovate EOD scheduler (PM2-managed)")
    parser.add_argument("--run-now", action="store_true", help="Run one export immediately, then enter schedule loop")
    parser.add_argument("--once", action="store_true", help="Run one export and exit (no loop)")
    args = parser.parse_args()

    print(f"[{now_ct().isoformat()}] Tradovate EOD Scheduler starting")
    print(f"  Schedule: {RUN_HOUR}:{RUN_MINUTE:02d} CT, Mon-Fri")
    print(f"  Lookback: {EXPORT_LOOKBACK_DAYS} days")
    print(f"  Auth: AuthManager (shared with execution system)")
    print(f"  Export script: {EXPORT_SCRIPT}")

    if args.once:
        success = run_export()
        sys.exit(0 if success else 1)

    if args.run_now:
        print("\n--- Immediate run requested ---")
        run_export()
        print("--- Immediate run complete ---\n")

    while True:
        target = next_run_time()
        ct_now = now_ct()
        wait_seconds = (target - ct_now).total_seconds()

        print(f"\n[{ct_now.isoformat()}] Next run: {target.isoformat()} ({wait_seconds/3600:.1f} hours)")
        sys.stdout.flush()

        # Sleep in 5-minute chunks so PM2 logs stay responsive
        while now_ct() < target:
            remaining = (target - now_ct()).total_seconds()
            sleep_chunk = min(300, max(1, remaining))
            time.sleep(sleep_chunk)

        # Run the export
        print(f"\n{'='*60}")
        print(f"[{now_ct().isoformat()}] Scheduled EOD export starting")
        print(f"{'='*60}")

        success = run_export()

        if success:
            print(f"[{now_ct().isoformat()}] EOD export completed successfully")
        else:
            print(f"[{now_ct().isoformat()}] EOD export FAILED - will retry next cycle")

        sys.stdout.flush()


if __name__ == "__main__":
    main()
