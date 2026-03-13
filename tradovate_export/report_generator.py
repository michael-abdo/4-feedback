#!/usr/bin/env python3
"""
Trading database management and performance report generation.

Workflow:
1. Initialize database: python report_generator.py --init-db
2. Import fills:        python report_generator.py --import-fills Fills.csv
3. Import orders:       python report_generator.py --import-orders Orders.csv
4. Generate report:     python report_generator.py --report --out Performance.csv

Output columns:
symbol,_priceFormat,_priceFormatType,_tickSize,buyFillId,sellFillId,qty,buyPrice,sellPrice,pnl,boughtTimestamp,soldTimestamp,duration

Notes:
- FIFO lot matching per Contract
- Long: buyFillId=entry buy, sellFillId=exit sell
- Short: sellFillId=entry sell, buyFillId=cover buy
- PnL = (sellPrice - buyPrice)/tickSize * tickValue * qty
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

import pandas as pd


# Extend as needed
TICK_VALUE_BY_ROOT = {
    "NQ": 5.0,
    "MNQ": 0.5,
    "ES": 12.5,
    "MES": 1.25,
    "YM": 5.0,
    "MYM": 0.5,
    "RTY": 5.0,
    "M2K": 0.5,
    "GC": 10.0,   # 0.10 tick -> $10
    "SI": 25.0,   # 0.005 tick -> $25
    "CL": 10.0,   # 0.01 tick -> $10
    "NG": 10.0,   # 0.001 tick -> $10
    "ZB": 31.25,  # 1/32 tick -> $31.25 (tickSize varies by export)
    "ZN": 15.625,
}

DEFAULT_TICK_SIZE_BY_ROOT = {
    "NQ": 0.25,
    "MNQ": 0.25,
    "ES": 0.25,
    "MES": 0.25,
    "YM": 1.0,
    "MYM": 1.0,
    "RTY": 0.1,
    "M2K": 0.1,
    "GC": 0.1,
    "SI": 0.005,
    "CL": 0.01,
    "NG": 0.001,
}

# Default database path (same directory as this script)
DEFAULT_DB_PATH = Path(__file__).parent / "trading.db"


def get_db_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Create and return a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# SQL Schema Definitions
SQL_CREATE_ACCOUNTS = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SQL_CREATE_CONTRACTS = """
CREATE TABLE IF NOT EXISTS contracts (
    contract_symbol TEXT PRIMARY KEY,
    product_root TEXT NOT NULL,
    product_description TEXT,
    price_format REAL,
    price_format_type REAL,
    tick_size REAL NOT NULL,
    tick_value REAL
);
"""

SQL_CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    contract_symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT,
    quantity INTEGER NOT NULL,
    limit_price REAL,
    stop_price REAL,
    status TEXT NOT NULL,
    filled_qty INTEGER,
    avg_fill_price REAL,
    fill_time TEXT,
    order_timestamp TEXT NOT NULL,
    trade_date TEXT,
    order_text TEXT,
    notional_value REAL,
    currency TEXT DEFAULT 'USD',
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (contract_symbol) REFERENCES contracts(contract_symbol)
);
"""

SQL_CREATE_FILLS = """
CREATE TABLE IF NOT EXISTS fills (
    fill_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    contract_symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    fill_timestamp TEXT NOT NULL,
    trade_date TEXT,
    commission REAL,
    price_format REAL,
    price_format_type REAL,
    tick_size REAL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (contract_symbol) REFERENCES contracts(contract_symbol)
);
"""

SQL_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fills_timestamp ON fills(fill_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_fills_contract ON fills(contract_symbol);",
    "CREATE INDEX IF NOT EXISTS idx_fills_account ON fills(account_id);",
    "CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(order_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);",
    "CREATE INDEX IF NOT EXISTS idx_orders_contract ON orders(contract_symbol);",
    "CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id);",
]


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize database schema. Creates tables and indexes if they don't exist."""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute(SQL_CREATE_ACCOUNTS)
    cursor.execute(SQL_CREATE_CONTRACTS)
    cursor.execute(SQL_CREATE_ORDERS)
    cursor.execute(SQL_CREATE_FILLS)

    # Create indexes
    for idx_sql in SQL_CREATE_INDEXES:
        cursor.execute(idx_sql)

    conn.commit()
    conn.close()
    print(f"Database initialized at: {db_path}")


def extract_accounts_from_df(df: pd.DataFrame, account_col: str) -> List[str]:
    """Extract unique account IDs from a DataFrame."""
    accounts = df[account_col].dropna().astype(str).str.strip().unique().tolist()
    return [a for a in accounts if a]


def extract_contracts_from_df(
    df: pd.DataFrame,
    contract_col: str,
    product_col: str,
    desc_col: Optional[str],
    pf_col: Optional[str],
    pft_col: Optional[str],
    tick_col: Optional[str],
) -> List[Dict]:
    """Extract unique contracts with metadata from a DataFrame."""
    contracts = {}
    for _, row in df.iterrows():
        symbol = str(row[contract_col]).strip()
        if not symbol or symbol in contracts:
            continue

        root = str(row[product_col]).strip() if product_col and pd.notna(row[product_col]) else root_from_contract(symbol)
        desc = str(row[desc_col]).strip() if desc_col and pd.notna(row.get(desc_col)) else None

        pf = None
        if pf_col and pd.notna(row.get(pf_col)):
            pf = float(row[pf_col])

        pft = None
        if pft_col and pd.notna(row.get(pft_col)):
            pft = float(row[pft_col])

        tick_size = DEFAULT_TICK_SIZE_BY_ROOT.get(root, 0.25)
        if tick_col and pd.notna(row.get(tick_col)):
            tick_size = float(row[tick_col])

        tick_value = TICK_VALUE_BY_ROOT.get(root)

        contracts[symbol] = {
            "contract_symbol": symbol,
            "product_root": root,
            "product_description": desc,
            "price_format": pf,
            "price_format_type": pft,
            "tick_size": tick_size,
            "tick_value": tick_value,
        }

    return list(contracts.values())


def insert_accounts(conn: sqlite3.Connection, accounts: List[str]) -> int:
    """Insert accounts into database. Returns count of new accounts inserted."""
    cursor = conn.cursor()
    inserted = 0
    for account_id in accounts:
        cursor.execute(
            "INSERT OR IGNORE INTO accounts (account_id) VALUES (?)",
            (account_id,)
        )
        inserted += cursor.rowcount
    conn.commit()
    return inserted


def insert_contracts(conn: sqlite3.Connection, contracts: List[Dict]) -> int:
    """Insert contracts into database. Returns count of new contracts inserted."""
    cursor = conn.cursor()
    inserted = 0
    for c in contracts:
        cursor.execute(
            """INSERT OR REPLACE INTO contracts
               (contract_symbol, product_root, product_description, price_format,
                price_format_type, tick_size, tick_value)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (c["contract_symbol"], c["product_root"], c["product_description"],
             c["price_format"], c["price_format_type"], c["tick_size"], c["tick_value"])
        )
        inserted += cursor.rowcount
    conn.commit()
    return inserted


def import_fills(db_path: Path, csv_path: Path) -> int:
    """Import fills from CSV into database. Returns count of fills imported."""
    df = pd.read_csv(csv_path)

    # Find columns
    col_id = find_col(df, ["Fill ID", "FillID", "fillId", "fill_id"])
    col_order = find_col(df, ["Order ID", "OrderID", "orderId", "_orderId"])
    col_account = find_col(df, ["Account"])
    col_contract = find_col(df, ["Contract", "Symbol"])
    col_product = find_col(df, ["Product"])
    col_desc = find_col(df, ["Product Description"])
    col_side = find_col(df, ["B/S", "Side", "Buy/Sell"])
    col_qty = find_col(df, ["Quantity", "Qty", "Filled Qty"])
    col_price = find_col(df, ["Price", "Fill Price"])
    col_ts = find_col(df, ["_timestamp", "Timestamp"])
    col_date = find_col(df, ["_tradeDate", "Date"])
    col_commission = find_col(df, ["commission"])
    col_pf = find_col(df, ["_priceFormat", "priceFormat"])
    col_pft = find_col(df, ["_priceFormatType", "priceFormatType"])
    col_tick = find_col(df, ["_tickSize", "Tick Size", "tickSize"])

    required = [col_id, col_order, col_account, col_contract, col_side, col_qty, col_price, col_ts]
    if any(c is None for c in required):
        missing = []
        labels = ["Fill ID", "Order ID", "Account", "Contract", "Side", "Quantity", "Price", "Timestamp"]
        for lab, c in zip(labels, required):
            if c is None:
                missing.append(lab)
        raise SystemExit(f"Missing required columns in fills CSV: {missing}. Columns present: {list(df.columns)}")

    conn = get_db_connection(db_path)

    # Extract and insert accounts
    accounts = extract_accounts_from_df(df, col_account)
    insert_accounts(conn, accounts)

    # Extract and insert contracts
    contracts = extract_contracts_from_df(df, col_contract, col_product, col_desc, col_pf, col_pft, col_tick)
    insert_contracts(conn, contracts)

    # Normalize side
    def norm_side(x: str) -> str:
        s = str(x).strip().lower()
        if s in {"b", "buy"}:
            return "Buy"
        if s in {"s", "sell"}:
            return "Sell"
        return s.capitalize()

    # Insert fills
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        fill_id = int(row[col_id])
        order_id = int(row[col_order])
        account_id = str(row[col_account]).strip()
        contract_symbol = str(row[col_contract]).strip()
        side = norm_side(row[col_side])
        quantity = int(row[col_qty])
        price = float(row[col_price])
        fill_timestamp = str(row[col_ts])
        trade_date = str(row[col_date]) if col_date and pd.notna(row.get(col_date)) else None
        commission = float(row[col_commission]) if col_commission and pd.notna(row.get(col_commission)) else None
        pf = float(row[col_pf]) if col_pf and pd.notna(row.get(col_pf)) else None
        pft = float(row[col_pft]) if col_pft and pd.notna(row.get(col_pft)) else None
        tick = float(row[col_tick]) if col_tick and pd.notna(row.get(col_tick)) else None

        cursor.execute(
            """INSERT OR REPLACE INTO fills
               (fill_id, order_id, account_id, contract_symbol, side, quantity, price,
                fill_timestamp, trade_date, commission, price_format, price_format_type, tick_size)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (fill_id, order_id, account_id, contract_symbol, side, quantity, price,
             fill_timestamp, trade_date, commission, pf, pft, tick)
        )
        inserted += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"Imported {inserted} fills from {csv_path}")
    return inserted


def import_orders(db_path: Path, csv_path: Path) -> int:
    """Import orders from CSV into database. Returns count of orders imported."""
    df = pd.read_csv(csv_path)

    # Find columns
    col_id = find_col(df, ["orderId", "Order ID", "OrderID"])
    col_account = find_col(df, ["Account"])
    col_contract = find_col(df, ["Contract", "Symbol"])
    col_product = find_col(df, ["Product"])
    col_desc = find_col(df, ["Product Description"])
    col_side = find_col(df, ["B/S", "Side", "Buy/Sell"])
    col_type = find_col(df, ["Type"])
    col_qty = find_col(df, ["Quantity", "Qty"])
    col_limit = find_col(df, ["Limit Price", "LimitPrice"])
    col_stop = find_col(df, ["Stop Price", "StopPrice"])
    col_status = find_col(df, ["Status"])
    col_filled_qty = find_col(df, ["filledQty", "Filled Qty"])
    col_avg_price = find_col(df, ["avgPrice", "Avg Fill Price"])
    col_fill_time = find_col(df, ["Fill Time"])
    col_ts = find_col(df, ["Timestamp"])
    col_date = find_col(df, ["Date"])
    col_text = find_col(df, ["Text"])
    col_notional = find_col(df, ["Notional Value"])
    col_currency = find_col(df, ["Currency"])
    col_pf = find_col(df, ["_priceFormat", "priceFormat"])
    col_pft = find_col(df, ["_priceFormatType", "priceFormatType"])
    col_tick = find_col(df, ["_tickSize", "Tick Size", "tickSize"])

    required = [col_id, col_account, col_contract, col_side, col_qty, col_status, col_ts]
    if any(c is None for c in required):
        missing = []
        labels = ["Order ID", "Account", "Contract", "Side", "Quantity", "Status", "Timestamp"]
        for lab, c in zip(labels, required):
            if c is None:
                missing.append(lab)
        raise SystemExit(f"Missing required columns in orders CSV: {missing}. Columns present: {list(df.columns)}")

    conn = get_db_connection(db_path)

    # Extract and insert accounts
    accounts = extract_accounts_from_df(df, col_account)
    insert_accounts(conn, accounts)

    # Extract and insert contracts
    contracts = extract_contracts_from_df(df, col_contract, col_product, col_desc, col_pf, col_pft, col_tick)
    insert_contracts(conn, contracts)

    # Normalize side
    def norm_side(x: str) -> str:
        s = str(x).strip().lower()
        if s in {"b", "buy"}:
            return "Buy"
        if s in {"s", "sell"}:
            return "Sell"
        return s.capitalize()

    # Insert orders
    cursor = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        order_id = int(row[col_id])
        account_id = str(row[col_account]).strip()
        contract_symbol = str(row[col_contract]).strip()
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
            notional_str = str(row[col_notional]).replace(",", "").replace("$", "")
            try:
                notional = float(notional_str)
            except ValueError:
                notional = None
        currency = str(row[col_currency]).strip() if col_currency and pd.notna(row.get(col_currency)) else "USD"

        cursor.execute(
            """INSERT OR REPLACE INTO orders
               (order_id, account_id, contract_symbol, side, order_type, quantity,
                limit_price, stop_price, status, filled_qty, avg_fill_price, fill_time,
                order_timestamp, trade_date, order_text, notional_value, currency)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, account_id, contract_symbol, side, order_type, quantity,
             limit_price, stop_price, status, filled_qty, avg_price, fill_time,
             order_timestamp, trade_date, order_text, notional, currency)
        )
        inserted += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"Imported {inserted} orders from {csv_path}")
    return inserted


def query_fills_from_db(db_path: Path) -> pd.DataFrame:
    """Query fills from database and return as DataFrame compatible with report generation."""
    conn = get_db_connection(db_path)
    query = """
        SELECT
            f.fill_id as "Fill ID",
            f.order_id as "Order ID",
            f.account_id as "Account",
            f.contract_symbol as "Contract",
            f.side as "B/S",
            f.quantity as "Quantity",
            f.price as "Price",
            f.fill_timestamp as "Timestamp",
            f.trade_date as "Date",
            f.commission,
            f.price_format as "_priceFormat",
            f.price_format_type as "_priceFormatType",
            f.tick_size as "_tickSize",
            c.product_root as "Product",
            c.product_description as "Product Description"
        FROM fills f
        LEFT JOIN contracts c ON f.contract_symbol = c.contract_symbol
        ORDER BY f.fill_timestamp
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def generate_report_filename(df: pd.DataFrame) -> Path:
    """Generate filename: accountid-performance-startdate-enddate-exporttimestamp.csv
    Saves to performance-reports subdirectory."""
    # Output directory
    output_dir = Path(__file__).parent / "performance-reports"
    output_dir.mkdir(exist_ok=True)

    # Get account ID (use first if multiple)
    account_col = find_col(df, ["Account", "account_id"])
    if account_col:
        accounts = df[account_col].dropna().unique()
        account_id = str(accounts[0]).strip() if len(accounts) > 0 else "unknown"
    else:
        account_id = "unknown"

    # Get date range from timestamps
    ts_col = find_col(df, ["Timestamp", "fill_timestamp"])
    if ts_col:
        timestamps = pd.to_datetime(df[ts_col], errors="coerce").dropna()
        if not timestamps.empty:
            start_date = timestamps.min().strftime("%Y%m%d")
            end_date = timestamps.max().strftime("%Y%m%d")
        else:
            start_date = end_date = "unknown"
    else:
        start_date = end_date = "unknown"

    # Export timestamp
    export_ts = datetime.now().strftime("%Y%m%d%H%M%S")

    filename = f"{account_id}-performance-{start_date}-{end_date}-{export_ts}.csv"
    return output_dir / filename


def calculate_statistics(trades: List[Dict]) -> Dict:
    """Calculate comprehensive trading statistics from matched trades."""
    import numpy as np

    if not trades:
        return {}

    # Sort trades by exit timestamp for run-up/drawdown calculations
    sorted_trades = sorted(trades, key=lambda t: t.get("exit_timestamp", datetime.min))

    # Extract numeric values
    pnls = [t["pnl_numeric"] for t in sorted_trades]
    qtys = [t["qty"] for t in sorted_trades]
    durations = [t["duration_seconds"] for t in sorted_trades]
    commissions = [t.get("commission", 0.0) for t in sorted_trades]
    exit_timestamps = [t.get("exit_timestamp") for t in sorted_trades]

    # Separate winning and losing trades
    winning_trades = [t for t in sorted_trades if t["pnl_numeric"] > 0]
    losing_trades = [t for t in sorted_trades if t["pnl_numeric"] < 0]

    winning_pnls = [t["pnl_numeric"] for t in winning_trades]
    losing_pnls = [t["pnl_numeric"] for t in losing_trades]
    winning_durations = [t["duration_seconds"] for t in winning_trades]
    losing_durations = [t["duration_seconds"] for t in losing_trades]

    # All Trades metrics
    gross_pnl = sum(pnls)
    num_trades = len(sorted_trades)
    num_contracts = sum(qtys)
    avg_trade_time = sum(durations) / num_trades if num_trades > 0 else 0
    longest_trade_time = max(durations) if durations else 0
    pct_profitable = (len(winning_trades) / num_trades * 100) if num_trades > 0 else 0
    expectancy = gross_pnl / num_trades if num_trades > 0 else 0
    total_commission = sum(commissions)
    total_pnl = gross_pnl - total_commission

    # Profit trades metrics
    total_profit = sum(winning_pnls) if winning_pnls else 0
    num_winning_trades = len(winning_trades)
    num_winning_contracts = sum(t["qty"] for t in winning_trades)
    largest_win = max(winning_pnls) if winning_pnls else 0
    avg_winning_trade = np.mean(winning_pnls) if winning_pnls else 0
    std_winning_trade = np.std(winning_pnls, ddof=1) if len(winning_pnls) > 1 else 0
    avg_winning_time = np.mean(winning_durations) if winning_durations else 0
    longest_winning_time = max(winning_durations) if winning_durations else 0

    # Losing trades metrics
    total_loss = sum(losing_pnls) if losing_pnls else 0
    num_losing_trades = len(losing_trades)
    num_losing_contracts = sum(t["qty"] for t in losing_trades)
    largest_loss = min(losing_pnls) if losing_pnls else 0
    avg_losing_trade = np.mean(losing_pnls) if losing_pnls else 0
    std_losing_trade = np.std(losing_pnls, ddof=1) if len(losing_pnls) > 1 else 0
    avg_losing_time = np.mean(losing_durations) if losing_durations else 0
    longest_losing_time = max(losing_durations) if losing_durations else 0

    # Calculate cumulative P/L for run-up and drawdown
    cumulative_pnl = []
    running_total = 0
    for pnl in pnls:
        running_total += pnl
        cumulative_pnl.append(running_total)

    # Max Run-up (maximum gain from any trough to subsequent peak)
    max_runup = 0
    max_runup_from_idx = 0
    max_runup_to_idx = 0
    trough = cumulative_pnl[0] if cumulative_pnl else 0
    trough_idx = 0

    for i, cum_pnl in enumerate(cumulative_pnl):
        if cum_pnl < trough:
            trough = cum_pnl
            trough_idx = i
        runup = cum_pnl - trough
        if runup > max_runup:
            max_runup = runup
            max_runup_from_idx = trough_idx
            max_runup_to_idx = i

    # Max Drawdown (maximum loss from any peak to subsequent trough)
    max_drawdown = 0
    max_drawdown_from_idx = 0
    max_drawdown_to_idx = 0
    peak = cumulative_pnl[0] if cumulative_pnl else 0
    peak_idx = 0

    for i, cum_pnl in enumerate(cumulative_pnl):
        if cum_pnl > peak:
            peak = cum_pnl
            peak_idx = i
        drawdown = cum_pnl - peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            max_drawdown_from_idx = peak_idx
            max_drawdown_to_idx = i

    # Get timestamps for run-up and drawdown
    runup_from_ts = exit_timestamps[max_runup_from_idx] if exit_timestamps and max_runup_from_idx < len(exit_timestamps) else None
    runup_to_ts = exit_timestamps[max_runup_to_idx] if exit_timestamps and max_runup_to_idx < len(exit_timestamps) else None
    drawdown_from_ts = exit_timestamps[max_drawdown_from_idx] if exit_timestamps and max_drawdown_from_idx < len(exit_timestamps) else None
    drawdown_to_ts = exit_timestamps[max_drawdown_to_idx] if exit_timestamps and max_drawdown_to_idx < len(exit_timestamps) else None

    return {
        # All Trades
        "gross_pnl": gross_pnl,
        "num_trades": num_trades,
        "num_contracts": int(num_contracts),
        "avg_trade_time_sec": avg_trade_time,
        "longest_trade_time_sec": longest_trade_time,
        "pct_profitable": pct_profitable,
        "expectancy": expectancy,
        "total_commission": total_commission,
        "total_pnl": total_pnl,
        # Profit Trades
        "total_profit": total_profit,
        "num_winning_trades": num_winning_trades,
        "num_winning_contracts": int(num_winning_contracts),
        "largest_win": largest_win,
        "avg_winning_trade": avg_winning_trade,
        "std_winning_trade": std_winning_trade,
        "avg_winning_time_sec": avg_winning_time,
        "longest_winning_time_sec": longest_winning_time,
        "max_runup": max_runup,
        "max_runup_from": runup_from_ts,
        "max_runup_to": runup_to_ts,
        # Losing Trades
        "total_loss": total_loss,
        "num_losing_trades": num_losing_trades,
        "num_losing_contracts": int(num_losing_contracts),
        "largest_loss": largest_loss,
        "avg_losing_trade": avg_losing_trade,
        "std_losing_trade": std_losing_trade,
        "avg_losing_time_sec": avg_losing_time,
        "longest_losing_time_sec": longest_losing_time,
        "max_drawdown": max_drawdown,
        "max_drawdown_from": drawdown_from_ts,
        "max_drawdown_to": drawdown_to_ts,
    }


def format_duration_long(seconds: float) -> str:
    """Format duration in verbose style like '1min 23sec'."""
    if seconds <= 0:
        return "0sec"
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    if m == 0:
        return f"{s}sec"
    if s == 0:
        return f"{m}min"
    return f"{m}min {s}sec"


def print_statistics(stats: Dict) -> None:
    """Print formatted statistics report."""
    def fmt_money(val):
        if val >= 0:
            return f"${val:,.2f}"
        return f"$({abs(val):,.2f})"

    def fmt_ts(ts):
        if ts is None:
            return "N/A"
        if hasattr(ts, 'strftime'):
            return ts.strftime("%m/%d/%Y %H:%M:%S")
        return str(ts)

    print("\n" + "=" * 50)
    print("ALL TRADES")
    print("=" * 50)
    print(f"{'Gross P/L':<30} {fmt_money(stats['gross_pnl'])}")
    print(f"{'# of Trades':<30} {stats['num_trades']}")
    print(f"{'# of Contracts':<30} {stats['num_contracts']}")
    print(f"{'Avg. Trade Time':<30} {format_duration_long(stats['avg_trade_time_sec'])}")
    print(f"{'Longest Trade Time':<30} {format_duration_long(stats['longest_trade_time_sec'])}")
    print(f"{'% Profitable Trades':<30} {stats['pct_profitable']:.2f}%")
    print(f"{'Expectancy':<30} {fmt_money(stats['expectancy'])}")
    print(f"{'Trade Fees & Comm.':<30} {fmt_money(-stats['total_commission'])}")
    print(f"{'Total P/L':<30} {fmt_money(stats['total_pnl'])}")

    print("\n" + "-" * 50)
    print("PROFIT TRADES")
    print("-" * 50)
    print(f"{'Total Profit':<30} {fmt_money(stats['total_profit'])}")
    print(f"{'# of Winning Trades':<30} {stats['num_winning_trades']}")
    print(f"{'# of Winning Contracts':<30} {stats['num_winning_contracts']}")
    print(f"{'Largest Winning Trade':<30} {fmt_money(stats['largest_win'])}")
    print(f"{'Avg. Winning Trade':<30} {fmt_money(stats['avg_winning_trade'])}")
    print(f"{'Std. Dev. Winning Trade':<30} {fmt_money(stats['std_winning_trade'])}")
    print(f"{'Avg. Winning Trade Time':<30} {format_duration_long(stats['avg_winning_time_sec'])}")
    print(f"{'Longest Winning Trade Time':<30} {format_duration_long(stats['longest_winning_time_sec'])}")
    print(f"{'Max Run-up':<30} {fmt_money(stats['max_runup'])}")
    print(f"{'Max Run-up, from':<30} {fmt_ts(stats['max_runup_from'])}")
    print(f"{'Max Run-up, to':<30} {fmt_ts(stats['max_runup_to'])}")

    print("\n" + "-" * 50)
    print("LOSING TRADES")
    print("-" * 50)
    print(f"{'Total Loss':<30} {fmt_money(stats['total_loss'])}")
    print(f"{'# of Losing Trades':<30} {stats['num_losing_trades']}")
    print(f"{'# of Losing Contracts':<30} {stats['num_losing_contracts']}")
    print(f"{'Largest Losing Trade':<30} {fmt_money(stats['largest_loss'])}")
    print(f"{'Avg. Losing Trade':<30} {fmt_money(stats['avg_losing_trade'])}")
    print(f"{'Std. Dev. Losing Trade':<30} {fmt_money(stats['std_losing_trade'])}")
    print(f"{'Avg. Losing Trade Time':<30} {format_duration_long(stats['avg_losing_time_sec'])}")
    print(f"{'Longest Losing Trade Time':<30} {format_duration_long(stats['longest_losing_time_sec'])}")
    print(f"{'Max Drawdown':<30} {fmt_money(stats['max_drawdown'])}")
    print(f"{'Max Drawdown, from':<30} {fmt_ts(stats['max_drawdown_from'])}")
    print(f"{'Max Drawdown, to':<30} {fmt_ts(stats['max_drawdown_to'])}")
    print("=" * 50 + "\n")


def save_statistics_csv(stats: Dict, df: pd.DataFrame) -> Path:
    """Save statistics to CSV with naming: accountid-statistics-daterange-timestamp.csv"""
    # Output directory
    output_dir = Path(__file__).parent / "performance-reports"
    output_dir.mkdir(exist_ok=True)

    # Get account ID
    account_col = find_col(df, ["Account", "account_id"])
    if account_col:
        accounts = df[account_col].dropna().unique()
        account_id = str(accounts[0]).strip() if len(accounts) > 0 else "unknown"
    else:
        account_id = "unknown"

    # Get date range
    ts_col = find_col(df, ["Timestamp", "fill_timestamp"])
    if ts_col:
        timestamps = pd.to_datetime(df[ts_col], errors="coerce").dropna()
        if not timestamps.empty:
            start_date = timestamps.min().strftime("%Y%m%d")
            end_date = timestamps.max().strftime("%Y%m%d")
        else:
            start_date = end_date = "unknown"
    else:
        start_date = end_date = "unknown"

    # Export timestamp
    export_ts = datetime.now().strftime("%Y%m%d%H%M%S")

    filename = f"{account_id}-statistics-{start_date}-{end_date}-{export_ts}.csv"
    output_path = output_dir / filename

    # Format timestamps for CSV
    def fmt_ts(ts):
        if ts is None:
            return ""
        if hasattr(ts, 'strftime'):
            return ts.strftime("%m/%d/%Y %H:%M:%S")
        return str(ts)

    # Create rows for CSV (metric, value format)
    rows = [
        # All Trades
        {"Category": "All Trades", "Metric": "Gross P/L", "Value": f"${stats['gross_pnl']:,.2f}"},
        {"Category": "All Trades", "Metric": "# of Trades", "Value": stats['num_trades']},
        {"Category": "All Trades", "Metric": "# of Contracts", "Value": stats['num_contracts']},
        {"Category": "All Trades", "Metric": "Avg. Trade Time", "Value": format_duration_long(stats['avg_trade_time_sec'])},
        {"Category": "All Trades", "Metric": "Longest Trade Time", "Value": format_duration_long(stats['longest_trade_time_sec'])},
        {"Category": "All Trades", "Metric": "% Profitable Trades", "Value": f"{stats['pct_profitable']:.2f}%"},
        {"Category": "All Trades", "Metric": "Expectancy", "Value": f"${stats['expectancy']:,.2f}"},
        {"Category": "All Trades", "Metric": "Trade Fees & Comm.", "Value": f"${-stats['total_commission']:,.2f}"},
        {"Category": "All Trades", "Metric": "Total P/L", "Value": f"${stats['total_pnl']:,.2f}"},
        # Profit Trades
        {"Category": "Profit Trades", "Metric": "Total Profit", "Value": f"${stats['total_profit']:,.2f}"},
        {"Category": "Profit Trades", "Metric": "# of Winning Trades", "Value": stats['num_winning_trades']},
        {"Category": "Profit Trades", "Metric": "# of Winning Contracts", "Value": stats['num_winning_contracts']},
        {"Category": "Profit Trades", "Metric": "Largest Winning Trade", "Value": f"${stats['largest_win']:,.2f}"},
        {"Category": "Profit Trades", "Metric": "Avg. Winning Trade", "Value": f"${stats['avg_winning_trade']:,.2f}"},
        {"Category": "Profit Trades", "Metric": "Std. Dev. Winning Trade", "Value": f"${stats['std_winning_trade']:,.2f}"},
        {"Category": "Profit Trades", "Metric": "Avg. Winning Trade Time", "Value": format_duration_long(stats['avg_winning_time_sec'])},
        {"Category": "Profit Trades", "Metric": "Longest Winning Trade Time", "Value": format_duration_long(stats['longest_winning_time_sec'])},
        {"Category": "Profit Trades", "Metric": "Max Run-up", "Value": f"${stats['max_runup']:,.2f}"},
        {"Category": "Profit Trades", "Metric": "Max Run-up, from", "Value": fmt_ts(stats['max_runup_from'])},
        {"Category": "Profit Trades", "Metric": "Max Run-up, to", "Value": fmt_ts(stats['max_runup_to'])},
        # Losing Trades
        {"Category": "Losing Trades", "Metric": "Total Loss", "Value": f"${stats['total_loss']:,.2f}"},
        {"Category": "Losing Trades", "Metric": "# of Losing Trades", "Value": stats['num_losing_trades']},
        {"Category": "Losing Trades", "Metric": "# of Losing Contracts", "Value": stats['num_losing_contracts']},
        {"Category": "Losing Trades", "Metric": "Largest Losing Trade", "Value": f"${stats['largest_loss']:,.2f}"},
        {"Category": "Losing Trades", "Metric": "Avg. Losing Trade", "Value": f"${stats['avg_losing_trade']:,.2f}"},
        {"Category": "Losing Trades", "Metric": "Std. Dev. Losing Trade", "Value": f"${stats['std_losing_trade']:,.2f}"},
        {"Category": "Losing Trades", "Metric": "Avg. Losing Trade Time", "Value": format_duration_long(stats['avg_losing_time_sec'])},
        {"Category": "Losing Trades", "Metric": "Longest Losing Trade Time", "Value": format_duration_long(stats['longest_losing_time_sec'])},
        {"Category": "Losing Trades", "Metric": "Max Drawdown", "Value": f"${stats['max_drawdown']:,.2f}"},
        {"Category": "Losing Trades", "Metric": "Max Drawdown, from", "Value": fmt_ts(stats['max_drawdown_from'])},
        {"Category": "Losing Trades", "Metric": "Max Drawdown, to", "Value": fmt_ts(stats['max_drawdown_to'])},
    ]

    stats_df = pd.DataFrame(rows)
    stats_df.to_csv(output_path, index=False)
    print(f"Wrote statistics to {output_path}")
    return output_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Trading data management: import CSV data to SQLite and generate reports."
    )

    # Database options
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                   help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})")
    p.add_argument("--init-db", action="store_true",
                   help="Initialize database schema (creates tables if not exist)")

    # Import options
    p.add_argument("--import-fills", type=Path, metavar="CSV",
                   help="Import fills from CSV file into database")
    p.add_argument("--import-orders", type=Path, metavar="CSV",
                   help="Import orders from CSV file into database")

    # Report generation options
    p.add_argument("--report", action="store_true",
                   help="Generate performance report from database")
    p.add_argument("--out", default=None,
                   help="Output CSV path (default: accountid-performance-daterange-timestamp.csv)")

    return p.parse_args()


def coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if pd.isna(v):
            continue
        return v
    return None


def root_from_contract(contract: str) -> str:
    """Extract root symbol from futures contract (e.g., 'NQH6' -> 'NQ').

    Futures format: ROOT + MONTH_CODE + YEAR
    Month codes: F,G,H,J,K,M,N,Q,U,V,X,Z (single letter)
    Year: 1-2 digits
    """
    c = (contract or "").strip().upper()
    if not c:
        return c

    # Known roots take precedence
    known_roots = list(TICK_VALUE_BY_ROOT.keys())
    for root in sorted(known_roots, key=len, reverse=True):  # Check longer roots first
        if c.startswith(root):
            return root

    # Fallback: strip trailing month code + year
    # Month codes are single letters, year is 1-2 digits at end
    import re
    match = re.match(r'^([A-Z]+?)([FGHJKMNQUVXZ])(\d{1,2})$', c)
    if match:
        return match.group(1)

    # Last resort: take all alpha chars
    root = ""
    for ch in c:
        if ch.isalpha():
            root += ch
        else:
            break
    return root or c


def fmt_pnl(x: float) -> str:
    if x >= 0:
        return f"${x:,.2f}"
    return f"$({abs(x):,.2f})"


def fmt_ts(dt: pd.Timestamp) -> str:
    # Match Tradovate Performance style: MM/DD/YYYY HH:MM:SS
    return dt.to_pydatetime().strftime("%m/%d/%Y %H:%M:%S")


def fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return ""
    m, s = divmod(seconds, 60)
    if m == 0:
        return f"{s}sec"
    if m == 1:
        return f"1min {s}sec" if s else "1min"
    return f"{m}min {s}sec" if s else f"{m}min"


@dataclass
class Lot:
    fill_id: str
    ts: pd.Timestamp
    price: float
    qty: float
    price_format: Optional[float]
    price_format_type: Optional[float]
    tick_size: Optional[float]


def find_col(df: pd.DataFrame, candidates) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in cols:
            return cols[name.lower()]
    return None


def main():
    args = parse_args()

    # Handle database initialization
    if args.init_db:
        init_db(args.db)
        if not args.import_fills and not args.import_orders and not args.report:
            return

    # Handle fills import
    if args.import_fills:
        if not args.import_fills.exists():
            raise SystemExit(f"Fills CSV not found: {args.import_fills}")
        import_fills(args.db, args.import_fills)
        if not args.import_orders and not args.report:
            return

    # Handle orders import
    if args.import_orders:
        if not args.import_orders.exists():
            raise SystemExit(f"Orders CSV not found: {args.import_orders}")
        import_orders(args.db, args.import_orders)
        if not args.report:
            return

    # Generate performance report from database
    if not args.report:
        return

    if not args.db.exists():
        raise SystemExit(f"Database not found: {args.db}. Run with --init-db first.")
    df = query_fills_from_db(args.db)
    if df.empty:
        raise SystemExit("No fills found in database. Import fills first with --import-fills.")

    col_id = find_col(df, ["Fill ID", "FillID", "fillId", "fill_id"])
    col_side = find_col(df, ["B/S", "Side", "Buy/Sell", "BuySell"])
    col_qty = find_col(df, ["Quantity", "Qty", "Filled Qty", "Fill Qty"])
    col_price = find_col(df, ["Price", "Fill Price", "Avg Fill Price"])
    col_ts = find_col(df, ["Timestamp", "Time", "Fill Time", "Date/Time"])
    col_contract = find_col(df, ["Contract", "Symbol"])
    col_tick = find_col(df, ["_tickSize", "Tick Size", "tickSize"])
    col_pf = find_col(df, ["_priceFormat", "priceFormat"])
    col_pft = find_col(df, ["_priceFormatType", "priceFormatType"])
    col_commission = find_col(df, ["commission"])

    # Build commission lookup by fill_id
    commission_by_fill = {}
    if col_commission:
        for _, row in df.iterrows():
            fill_id = str(row[col_id])
            comm = row[col_commission]
            if pd.notna(comm):
                commission_by_fill[fill_id] = float(comm)

    needed = [col_id, col_side, col_qty, col_price, col_ts, col_contract]
    if any(c is None for c in needed):
        missing = []
        labels = ["Fill ID", "Side", "Quantity", "Price", "Timestamp", "Contract"]
        for lab, c in zip(labels, needed):
            if c is None:
                missing.append(lab)
        raise SystemExit(f"Missing required columns in Fills.csv: {missing}. Columns present: {list(df.columns)}")

    # Parse timestamp
    df[col_ts] = pd.to_datetime(df[col_ts], errors="coerce", utc=False)
    df = df.dropna(subset=[col_ts]).copy()

    # Normalize side
    def norm_side(x: str) -> str:
        s = str(x).strip().lower()
        if s in {"b", "buy"}:
            return "buy"
        if s in {"s", "sell"}:
            return "sell"
        return s

    df["_side_norm"] = df[col_side].map(norm_side)
    df = df[df["_side_norm"].isin(["buy", "sell"])].copy()

    # Numeric parse
    df["_qty"] = pd.to_numeric(df[col_qty], errors="coerce")
    df["_price"] = pd.to_numeric(df[col_price], errors="coerce")
    df = df.dropna(subset=["_qty", "_price"]).copy()

    # Sort by timestamp, then stable by Fill ID to keep determinism
    df["_fillid_str"] = df[col_id].astype(str)
    df = df.sort_values([col_ts, "_fillid_str"], kind="mergesort")

    open_buys: Dict[str, Deque[Lot]] = {}
    open_sells: Dict[str, Deque[Lot]] = {}

    out_rows = []

    def tick_size_for(contract: str, a: Lot, b: Lot) -> float:
        root = root_from_contract(contract)
        ts = coalesce(a.tick_size, b.tick_size, DEFAULT_TICK_SIZE_BY_ROOT.get(root), 0.25)
        return float(ts)

    def pf_for(a: Lot, b: Lot) -> Tuple[float, float]:
        pf = coalesce(a.price_format, b.price_format, -2)
        pft = coalesce(a.price_format_type, b.price_format_type, 0)
        return float(pf), float(pft)

    def tick_value_for(contract: str) -> Optional[float]:
        root = root_from_contract(contract)
        return TICK_VALUE_BY_ROOT.get(root)

    def emit_match(contract: str, buy: Lot, sell: Lot, q: float):
        tick_size = tick_size_for(contract, buy, sell)
        tv = tick_value_for(contract)
        pf, pft = pf_for(buy, sell)

        pnl_numeric = 0.0
        pnl_str = ""
        if tv is not None and tick_size != 0:
            pnl_numeric = (sell.price - buy.price) / tick_size * tv * q
            pnl_str = fmt_pnl(float(pnl_numeric))

        # Get commission for both fills
        buy_comm = commission_by_fill.get(str(buy.fill_id), 0.0)
        sell_comm = commission_by_fill.get(str(sell.fill_id), 0.0)
        total_commission = buy_comm + sell_comm

        dur_sec = int(abs((sell.ts - buy.ts).total_seconds()))
        out_rows.append({
            "symbol": contract,
            "_priceFormat": pf,
            "_priceFormatType": pft,
            "_tickSize": tick_size,
            "buyFillId": str(buy.fill_id),
            "sellFillId": str(sell.fill_id),
            "qty": q,
            "buyPrice": buy.price,
            "sellPrice": sell.price,
            "pnl": pnl_str,
            "pnl_numeric": pnl_numeric,
            "commission": total_commission,
            "boughtTimestamp": fmt_ts(buy.ts),
            "soldTimestamp": fmt_ts(sell.ts),
            "duration": fmt_duration(dur_sec),
            "duration_seconds": dur_sec,
            "exit_timestamp": sell.ts,  # For chronological sorting
        })

    for _, r in df.iterrows():
        contract = str(r[col_contract]).strip()
        side = r["_side_norm"]
        qty = float(r["_qty"])
        price = float(r["_price"])
        ts = r[col_ts]
        fill_id = str(r["_fillid_str"])

        tick_size = None
        if col_tick:
            tick_size = pd.to_numeric(r[col_tick], errors="coerce")
            tick_size = None if pd.isna(tick_size) else float(tick_size)

        pf = None
        if col_pf:
            pf = pd.to_numeric(r[col_pf], errors="coerce")
            pf = None if pd.isna(pf) else float(pf)

        pft = None
        if col_pft:
            pft = pd.to_numeric(r[col_pft], errors="coerce")
            pft = None if pd.isna(pft) else float(pft)

        lot = Lot(
            fill_id=fill_id,
            ts=ts,
            price=price,
            qty=qty,
            price_format=pf,
            price_format_type=pft,
            tick_size=tick_size,
        )

        open_buys.setdefault(contract, deque())
        open_sells.setdefault(contract, deque())

        if side == "sell":
            # Close longs first; else open short
            while lot.qty > 0 and open_buys[contract]:
                b = open_buys[contract][0]
                q = min(lot.qty, b.qty)

                # Long close: buy=entry, sell=exit
                emit_match(contract, buy=b, sell=Lot(lot.fill_id, lot.ts, lot.price, q, lot.price_format, lot.price_format_type, lot.tick_size), q=q)

                lot.qty -= q
                b.qty -= q
                open_buys[contract].popleft()
                if b.qty > 0:
                    open_buys[contract].appendleft(b)

            if lot.qty > 0:
                open_sells[contract].append(lot)

        else:  # buy
            # Cover shorts first; else open long
            while lot.qty > 0 and open_sells[contract]:
                s = open_sells[contract][0]
                q = min(lot.qty, s.qty)

                # Short cover: sell=entry, buy=cover
                # Keep Performance convention: boughtTimestamp from buy (cover), soldTimestamp from sell (entry)
                emit_match(contract, buy=Lot(lot.fill_id, lot.ts, lot.price, q, lot.price_format, lot.price_format_type, lot.tick_size),
                          sell=s, q=q)

                lot.qty -= q
                s.qty -= q
                open_sells[contract].popleft()
                if s.qty > 0:
                    open_sells[contract].appendleft(s)

            if lot.qty > 0:
                open_buys[contract].append(lot)

    # Calculate and print statistics
    if out_rows:
        stats = calculate_statistics(out_rows)
        print_statistics(stats)
        save_statistics_csv(stats, df)

    out = pd.DataFrame(out_rows, columns=[
        "symbol","_priceFormat","_priceFormatType","_tickSize",
        "buyFillId","sellFillId","qty","buyPrice","sellPrice",
        "pnl","pnl_numeric","commission","boughtTimestamp","soldTimestamp",
        "duration","duration_seconds"
    ])

    # Generate output filename
    output_path = args.out if args.out else generate_report_filename(df)
    out.to_csv(output_path, index=False)
    print(f"Wrote {output_path} with {len(out)} rows.")


if __name__ == "__main__":
    main()
