#!/usr/bin/env python3
"""
Tradovate Report Export Script

Pulls Fills and Orders from Tradovate API and imports into PostgreSQL (mlops_system).
Auto-refreshes token if expired.

Usage:
    python3 tradovate_export.py                    # Export all accounts
    python3 tradovate_export.py --account "APEX123"
    python3 tradovate_export.py --since 7          # Last 7 days
    python3 tradovate_export.py --dry-run          # Fetch from API but don't write to DB
"""

import sys
import requests
import argparse
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras

# Add execution system to path for shared AuthManager
_EXEC_SRC = Path(__file__).parent.parent.parent / "3-execution/tradovate-execution/tradovate_api/src"
if str(_EXEC_SRC) not in sys.path:
    sys.path.insert(0, str(_EXEC_SRC))
# AuthManager also needs the shared config dir importable
_EXEC_ROOT = Path(__file__).parent.parent.parent / "3-execution/tradovate-execution"
if str(_EXEC_ROOT) not in sys.path:
    sys.path.insert(0, str(_EXEC_ROOT))

from auth_manager import AuthManager, AuthenticationError

# API endpoints
# Reports use demo endpoint for APEX demo accounts
REPORT_API_URL = "https://rpt-demo.tradovateapi.com/v1/reports/requestreport"
ACCOUNT_LIST_URL = "https://demo.tradovateapi.com/v1/account/list"

# Default paths (same directory as script)
SCRIPT_DIR = Path(__file__).parent

# Tick values by product root (from report_generator.py)
TICK_VALUE_BY_ROOT = {
    "NQ": 5.0, "MNQ": 0.5, "ES": 12.5, "MES": 1.25,
    "YM": 5.0, "MYM": 0.5, "RTY": 5.0, "M2K": 0.5,
    "GC": 10.0, "SI": 25.0, "CL": 10.0, "NG": 10.0,
    "ZB": 31.25, "ZN": 15.625,
}

DEFAULT_TICK_SIZE_BY_ROOT = {
    "NQ": 0.25, "MNQ": 0.25, "ES": 0.25, "MES": 0.25,
    "YM": 1.0, "MYM": 1.0, "RTY": 0.1, "M2K": 0.1,
    "GC": 0.1, "SI": 0.005, "CL": 0.01, "NG": 0.001,
}


# ============================================================================
# Database Schema (PostgreSQL, tradovate_ prefixed)
# ============================================================================

SQL_CREATE_ACCOUNTS = """
CREATE TABLE IF NOT EXISTS tradovate_accounts (
    account_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

SQL_CREATE_CONTRACTS = """
CREATE TABLE IF NOT EXISTS tradovate_contracts (
    contract_symbol TEXT PRIMARY KEY,
    product_root TEXT NOT NULL,
    product_description TEXT,
    price_format DOUBLE PRECISION,
    price_format_type DOUBLE PRECISION,
    tick_size DOUBLE PRECISION NOT NULL,
    tick_value DOUBLE PRECISION
);
"""

SQL_CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS tradovate_orders (
    order_id BIGINT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES tradovate_accounts(account_id),
    contract_symbol TEXT NOT NULL REFERENCES tradovate_contracts(contract_symbol),
    side TEXT NOT NULL,
    order_type TEXT,
    quantity INTEGER NOT NULL,
    limit_price DOUBLE PRECISION,
    stop_price DOUBLE PRECISION,
    status TEXT NOT NULL,
    filled_qty INTEGER,
    avg_fill_price DOUBLE PRECISION,
    fill_time TEXT,
    order_timestamp TEXT NOT NULL,
    trade_date TEXT,
    order_text TEXT,
    notional_value DOUBLE PRECISION,
    currency TEXT DEFAULT 'USD'
);
"""

SQL_CREATE_FILLS = """
CREATE TABLE IF NOT EXISTS tradovate_fills (
    fill_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES tradovate_orders(order_id),
    account_id TEXT NOT NULL REFERENCES tradovate_accounts(account_id),
    contract_symbol TEXT NOT NULL REFERENCES tradovate_contracts(contract_symbol),
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    fill_timestamp TEXT NOT NULL,
    trade_date TEXT,
    commission DOUBLE PRECISION,
    price_format DOUBLE PRECISION,
    price_format_type DOUBLE PRECISION,
    tick_size DOUBLE PRECISION
);
"""

SQL_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tv_fills_timestamp ON tradovate_fills(fill_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_tv_fills_contract ON tradovate_fills(contract_symbol);",
    "CREATE INDEX IF NOT EXISTS idx_tv_fills_account ON tradovate_fills(account_id);",
    "CREATE INDEX IF NOT EXISTS idx_tv_orders_timestamp ON tradovate_orders(order_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_tv_orders_account ON tradovate_orders(account_id);",
]


# ============================================================================
# Database Functions
# ============================================================================

def init_db(conn):
    """Initialize tables and indexes."""
    cur = conn.cursor()
    cur.execute(SQL_CREATE_ACCOUNTS)
    cur.execute(SQL_CREATE_CONTRACTS)
    cur.execute(SQL_CREATE_ORDERS)
    cur.execute(SQL_CREATE_FILLS)
    for idx_sql in SQL_CREATE_INDEXES:
        cur.execute(idx_sql)
    conn.commit()
    cur.close()


def find_col(df: pd.DataFrame, candidates) -> Optional[str]:
    """Find column by trying multiple name variations."""
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in cols:
            return cols[name.lower()]
    return None


def root_from_contract(contract: str) -> str:
    """Extract root symbol from futures contract (e.g., 'NQH6' -> 'NQ')."""
    c = (contract or "").strip().upper()
    for root in sorted(TICK_VALUE_BY_ROOT.keys(), key=len, reverse=True):
        if c.startswith(root):
            return root
    root = ""
    for ch in c:
        if ch.isalpha():
            root += ch
        else:
            break
    return root or c


def norm_side(x: str) -> str:
    """Normalize buy/sell side."""
    s = str(x).strip().lower()
    if s in {"b", "buy"}:
        return "Buy"
    if s in {"s", "sell"}:
        return "Sell"
    return s.capitalize()


# ============================================================================
# API Functions
# ============================================================================

def get_report(
    token: str,
    report_name: str,
    account: str,
    start_date: str,
    end_date: str,
    start_time: str = "00:00:00",
    end_time: str = "00:00:00",
    timezone: int = -360,
) -> str:
    """Request a report from Tradovate API. Returns CSV data as string."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Referer": "https://trader.tradovate.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    payload = {
        "name": report_name,
        "params": [
            {"name": "startDate", "value": start_date},
            {"name": "endDate", "value": end_date},
            {"name": "startTime", "value": start_time},
            {"name": "endTime", "value": end_time},
            {"name": "account", "value": account}
        ],
        "representationType": "csv",
        "timezone": timezone
    }

    print(f"  Fetching {report_name}...")

    response = requests.post(REPORT_API_URL, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"API returned status {response.status_code}: {response.text}")

    result = response.json()
    if "data" not in result:
        raise Exception(f"Unexpected response format: {result}")

    return result["data"]


def fetch_accounts_from_api(token: str) -> list:
    """Fetch all active accounts from Tradovate API. Returns list of account names."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(ACCOUNT_LIST_URL, headers=headers)
        if response.status_code != 200:
            print(f"  Warning: Account list API returned {response.status_code}")
            return []

        accounts = response.json()
        active_accounts = [
            acc["name"] for acc in accounts
            if acc.get("active") and not acc.get("closed")
        ]
        return active_accounts

    except Exception as e:
        print(f"  Warning: Failed to fetch accounts from API: {e}")
        return []


# ============================================================================
# Import Functions
# ============================================================================

def import_fills(conn, csv_data: str, dry_run: bool = False) -> int:
    """Import fills from CSV data into database. Returns count of fills imported."""
    df = pd.read_csv(io.StringIO(csv_data))

    if df.empty:
        print("    No fills to import")
        return 0

    col_id = find_col(df, ["Fill ID", "FillID", "fillId", "_id"])
    col_order = find_col(df, ["Order ID", "OrderID", "orderId", "_orderId"])
    col_account = find_col(df, ["Account", "_accountId"])
    col_contract = find_col(df, ["Contract", "Symbol"])
    col_product = find_col(df, ["Product"])
    col_desc = find_col(df, ["Product Description"])
    col_side = find_col(df, ["B/S", "Side", "Buy/Sell"])
    col_qty = find_col(df, ["Quantity", "Qty"])
    col_price = find_col(df, ["Price", "Fill Price"])
    col_ts = find_col(df, ["Timestamp", "_timestamp"])
    col_date = find_col(df, ["Date", "_tradeDate"])
    col_commission = find_col(df, ["commission"])
    col_pf = find_col(df, ["_priceFormat"])
    col_pft = find_col(df, ["_priceFormatType"])
    col_tick = find_col(df, ["_tickSize"])

    if dry_run:
        print(f"    [DRY RUN] Would import {len(df)} fills")
        for _, row in df.head(5).iterrows():
            print(f"      fill_id={int(row[col_id])}, "
                  f"contract={row[col_contract]}, "
                  f"side={row[col_side]}, "
                  f"qty={int(row[col_qty])}, "
                  f"price={float(row[col_price])}, "
                  f"ts={row[col_ts]}")
        if len(df) > 5:
            print(f"      ... and {len(df) - 5} more")
        return len(df)

    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        fill_id = int(row[col_id])
        order_id = int(row[col_order])
        account_id = str(row[col_account]).strip()
        contract_symbol = str(row[col_contract]).strip()
        product_root = str(row[col_product]).strip() if col_product and pd.notna(row.get(col_product)) else root_from_contract(contract_symbol)
        product_desc = str(row[col_desc]).strip() if col_desc and pd.notna(row.get(col_desc)) else None
        side = norm_side(row[col_side])
        quantity = int(row[col_qty])
        price = float(row[col_price])
        fill_timestamp = str(row[col_ts])
        trade_date = str(row[col_date]) if col_date and pd.notna(row.get(col_date)) else None
        commission = float(row[col_commission]) if col_commission and pd.notna(row.get(col_commission)) else None
        pf = float(row[col_pf]) if col_pf and pd.notna(row.get(col_pf)) else None
        pft = float(row[col_pft]) if col_pft and pd.notna(row.get(col_pft)) else None
        tick_size = float(row[col_tick]) if col_tick and pd.notna(row.get(col_tick)) else DEFAULT_TICK_SIZE_BY_ROOT.get(product_root, 0.25)
        tick_value = TICK_VALUE_BY_ROOT.get(product_root)

        # Upsert account
        cur.execute(
            "INSERT INTO tradovate_accounts (account_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (account_id,))

        # Upsert contract
        cur.execute(
            """INSERT INTO tradovate_contracts
               (contract_symbol, product_root, product_description, price_format,
                price_format_type, tick_size, tick_value)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (contract_symbol) DO UPDATE SET
                 product_root = EXCLUDED.product_root,
                 product_description = EXCLUDED.product_description,
                 price_format = EXCLUDED.price_format,
                 price_format_type = EXCLUDED.price_format_type,
                 tick_size = EXCLUDED.tick_size,
                 tick_value = EXCLUDED.tick_value""",
            (contract_symbol, product_root, product_desc, pf, pft, tick_size, tick_value))

        # Upsert fill
        cur.execute(
            """INSERT INTO tradovate_fills
               (fill_id, order_id, account_id, contract_symbol, side, quantity, price,
                fill_timestamp, trade_date, commission, price_format, price_format_type, tick_size)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (fill_id) DO UPDATE SET
                 price = EXCLUDED.price,
                 commission = EXCLUDED.commission""",
            (fill_id, order_id, account_id, contract_symbol, side, quantity, price,
             fill_timestamp, trade_date, commission, pf, pft, tick_size))
        inserted += 1

    conn.commit()
    cur.close()
    return inserted


def import_orders(conn, csv_data: str, dry_run: bool = False) -> int:
    """Import orders from CSV data into database. Returns count of orders imported."""
    df = pd.read_csv(io.StringIO(csv_data))

    if df.empty:
        print("    No orders to import")
        return 0

    col_id = find_col(df, ["orderId", "Order ID", "OrderID"])
    col_account = find_col(df, ["Account"])
    col_contract = find_col(df, ["Contract", "Symbol"])
    col_product = find_col(df, ["Product"])
    col_desc = find_col(df, ["Product Description"])
    col_side = find_col(df, ["B/S", "Side"])
    col_type = find_col(df, ["Type"])
    col_qty = find_col(df, ["Quantity", "Qty"])
    col_limit = find_col(df, ["Limit Price", "decimalLimit"])
    col_stop = find_col(df, ["Stop Price", "decimalStop"])
    col_status = find_col(df, ["Status"])
    col_filled_qty = find_col(df, ["filledQty", "Filled Qty"])
    col_avg_price = find_col(df, ["avgPrice", "Avg Fill Price", "decimalFillAvg"])
    col_fill_time = find_col(df, ["Fill Time"])
    col_ts = find_col(df, ["Timestamp"])
    col_date = find_col(df, ["Date"])
    col_text = find_col(df, ["Text"])
    col_notional = find_col(df, ["Notional Value"])
    col_currency = find_col(df, ["Currency"])
    col_pf = find_col(df, ["_priceFormat"])
    col_pft = find_col(df, ["_priceFormatType"])
    col_tick = find_col(df, ["_tickSize"])

    if dry_run:
        print(f"    [DRY RUN] Would import {len(df)} orders")
        for _, row in df.head(5).iterrows():
            print(f"      order_id={int(row[col_id])}, "
                  f"contract={row[col_contract]}, "
                  f"side={row[col_side]}, "
                  f"status={row[col_status]}, "
                  f"qty={int(row[col_qty])}, "
                  f"ts={row[col_ts]}")
        if len(df) > 5:
            print(f"      ... and {len(df) - 5} more")
        return len(df)

    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        order_id = int(row[col_id])
        account_id = str(row[col_account]).strip()
        contract_symbol = str(row[col_contract]).strip()
        product_root = str(row[col_product]).strip() if col_product and pd.notna(row.get(col_product)) else root_from_contract(contract_symbol)
        product_desc = str(row[col_desc]).strip() if col_desc and pd.notna(row.get(col_desc)) else None
        side = norm_side(row[col_side])
        order_type = str(row[col_type]).strip() if col_type and pd.notna(row.get(col_type)) else None
        quantity = int(row[col_qty])
        limit_price = float(row[col_limit]) if col_limit and pd.notna(row.get(col_limit)) else None
        stop_price = float(row[col_stop]) if col_stop and pd.notna(row.get(col_stop)) else None
        status = str(row[col_status]).strip()
        filled_qty = int(row[col_filled_qty]) if col_filled_qty and pd.notna(row.get(col_filled_qty)) else None
        avg_price = float(row[col_avg_price]) if col_avg_price and pd.notna(row.get(col_avg_price)) else None
        fill_time = str(row[col_fill_time]) if col_fill_time and pd.notna(row.get(col_fill_time)) else None
        order_timestamp = str(row[col_ts])
        trade_date = str(row[col_date]) if col_date and pd.notna(row.get(col_date)) else None
        order_text = str(row[col_text]).strip() if col_text and pd.notna(row.get(col_text)) else None

        notional = None
        if col_notional and pd.notna(row.get(col_notional)):
            notional_str = str(row[col_notional]).replace(",", "").replace("$", "").replace('"', '')
            try:
                notional = float(notional_str)
            except ValueError:
                pass

        currency = str(row[col_currency]).strip() if col_currency and pd.notna(row.get(col_currency)) else "USD"

        pf = float(row[col_pf]) if col_pf and pd.notna(row.get(col_pf)) else None
        pft = float(row[col_pft]) if col_pft and pd.notna(row.get(col_pft)) else None
        tick_size = float(row[col_tick]) if col_tick and pd.notna(row.get(col_tick)) else DEFAULT_TICK_SIZE_BY_ROOT.get(product_root, 0.25)
        tick_value = TICK_VALUE_BY_ROOT.get(product_root)

        # Upsert account
        cur.execute(
            "INSERT INTO tradovate_accounts (account_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (account_id,))

        # Upsert contract
        cur.execute(
            """INSERT INTO tradovate_contracts
               (contract_symbol, product_root, product_description, price_format,
                price_format_type, tick_size, tick_value)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (contract_symbol) DO UPDATE SET
                 product_root = EXCLUDED.product_root,
                 product_description = EXCLUDED.product_description,
                 price_format = EXCLUDED.price_format,
                 price_format_type = EXCLUDED.price_format_type,
                 tick_size = EXCLUDED.tick_size,
                 tick_value = EXCLUDED.tick_value""",
            (contract_symbol, product_root, product_desc, pf, pft, tick_size, tick_value))

        # Upsert order
        cur.execute(
            """INSERT INTO tradovate_orders
               (order_id, account_id, contract_symbol, side, order_type, quantity,
                limit_price, stop_price, status, filled_qty, avg_fill_price, fill_time,
                order_timestamp, trade_date, order_text, notional_value, currency)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (order_id) DO UPDATE SET
                 status = EXCLUDED.status,
                 filled_qty = EXCLUDED.filled_qty,
                 avg_fill_price = EXCLUDED.avg_fill_price,
                 fill_time = EXCLUDED.fill_time""",
            (order_id, account_id, contract_symbol, side, order_type, quantity,
             limit_price, stop_price, status, filled_qty, avg_price, fill_time,
             order_timestamp, trade_date, order_text, notional, currency))
        inserted += 1

    conn.commit()
    cur.close()
    return inserted


def export_account(conn, token: str, account: str,
                   start_date: str, end_date: str, timezone: int,
                   dry_run: bool = False) -> tuple:
    """Export fills and orders for a single account. Returns (fills_count, orders_count)."""
    # Orders first (fills reference orders via FK)
    orders_csv = get_report(
        token=token,
        report_name="Orders",
        account=account,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone
    )
    orders_count = import_orders(conn, orders_csv, dry_run=dry_run)

    # Then fills
    fills_csv = get_report(
        token=token,
        report_name="Fills",
        account=account,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone
    )
    fills_count = import_fills(conn, fills_csv, dry_run=dry_run)

    return fills_count, orders_count


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pull Fills & Orders from Tradovate API into PostgreSQL (mlops_system)"
    )
    parser.add_argument("--token", help="Bearer token (overrides AuthManager)")
    parser.add_argument("--account", help="Account ID (default: auto-discover from API)")
    parser.add_argument("--since", type=int, default=14,
                        help="Export last N days (default: 14)")
    parser.add_argument("--start-date", help="Start date MM/DD/YYYY (overrides --since)")
    parser.add_argument("--end-date", help="End date MM/DD/YYYY (default: tomorrow)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch from API and show what would be imported, but don't write to DB")
    parser.add_argument("--timezone", type=int, default=-360,
                        help="Timezone offset in minutes (default: -360 for CST)")

    args = parser.parse_args()

    # Unified auth via AuthManager (same as execution system)
    # Fallback chain: .tokens.json (disk cache) → renew → fresh API auth
    if args.token:
        token = args.token
        print(f"Token: CLI override")
    else:
        try:
            # Load credentials from .env if present (same as realtime_signals)
            env_file = Path(__file__).parent.parent.parent / "2-processing/potential-field/.env"
            if env_file.exists():
                for line in env_file.read_text().splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())

            auth = AuthManager(environment="demo")
            token = auth.get_access_token()
            token_info = auth.get_token()
            remaining = int(token_info.seconds_until_expiry) if token_info else 0
            print(f"Token (AuthManager): valid, {remaining // 60}m remaining")
        except AuthenticationError as e:
            print(f"ERROR: AuthManager failed: {e}")
            return 1

    # Determine accounts to process
    if args.account:
        accounts = [args.account]
    else:
        print("Fetching accounts from API...")
        accounts = fetch_accounts_from_api(token)
        if accounts:
            print(f"  Found {len(accounts)} active accounts")
        else:
            print("  ERROR: No accounts found from API")
            return 1

    # Default dates: last N days (default 14) to tomorrow
    today = datetime.now()
    tomorrow = today + timedelta(days=1)

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (today - timedelta(days=args.since)).strftime("%m/%d/%Y")

    end_date = args.end_date or tomorrow.strftime("%m/%d/%Y")

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"Tradovate Export [{mode}]")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Accounts: {len(accounts)} — {', '.join(accounts)}")
    print(f"  Database: mlops_system (PostgreSQL)")
    print()

    try:
        conn = psycopg2.connect("dbname=mlops_system")

        if not args.dry_run:
            init_db(conn)
            print("Database tables initialized")
        else:
            print("[DRY RUN] Skipping table creation")
        print()

        total_fills = 0
        total_orders = 0

        for account in accounts:
            print(f"[{account}]")
            try:
                fills_count, orders_count = export_account(
                    conn, token, account, start_date, end_date, args.timezone,
                    dry_run=args.dry_run
                )
                print(f"    {fills_count} fills, {orders_count} orders")
                total_fills += fills_count
                total_orders += orders_count
            except Exception as e:
                print(f"    ERROR: {e}")
                conn.rollback()

        conn.close()

        print()
        print(f"{'[DRY RUN] ' if args.dry_run else ''}Done: {total_fills} fills + {total_orders} orders across {len(accounts)} accounts")

        return 0

    except Exception as e:
        print(f"FAILED: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
