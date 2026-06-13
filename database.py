import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterable, Optional


DB_PATH = os.path.join("state", "jfo_quant.db")


@contextmanager
def connect(db_path: str = DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def initialize_database(db_path: str = DB_PATH):
    with connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS stocks (
                ticker TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS price_history (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS technical_metrics (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                sma20 REAL,
                sma50 REAL,
                sma200 REAL,
                ema20 REAL,
                ema50 REAL,
                rsi REAL,
                macd REAL,
                atr REAL,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS fundamental_metrics (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                revenue_growth REAL,
                eps_growth REAL,
                fcf_growth REAL,
                margins REAL,
                roe REAL,
                roic REAL,
                debt_to_equity REAL,
                forward_pe REAL,
                peg REAL,
                payload_json TEXT,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS stock_scores (
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                technical_score REAL,
                momentum_score REAL,
                volume_score REAL,
                fundamental_score REAL,
                risk_score REAL,
                catalyst_score REAL,
                final_score REAL,
                rating TEXT,
                confidence REAL,
                risk_level TEXT,
                explanation TEXT,
                payload_json TEXT,
                PRIMARY KEY (ticker, date)
            );

            CREATE TABLE IF NOT EXISTS market_regimes (
                date TEXT PRIMARY KEY,
                regime TEXT,
                health_score REAL,
                risk_environment TEXT,
                buy_environment TEXT,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                entry_price REAL,
                score REAL,
                market_regime TEXT,
                sector TEXT,
                why_now TEXT,
                confidence REAL,
                payload_json TEXT,
                UNIQUE(ticker, date, signal_type)
            );

            CREATE TABLE IF NOT EXISTS signal_outcomes (
                signal_id INTEGER PRIMARY KEY,
                return_1w REAL,
                return_1m REAL,
                return_3m REAL,
                return_6m REAL,
                max_drawdown REAL,
                sp500_relative_return REAL,
                updated_at TEXT,
                FOREIGN KEY(signal_id) REFERENCES signals(id)
            );

            CREATE TABLE IF NOT EXISTS options_data (
                ticker TEXT,
                date TEXT,
                expiration TEXT,
                strike REAL,
                option_type TEXT,
                bid REAL,
                ask REAL,
                last_price REAL,
                volume REAL,
                open_interest REAL,
                implied_volatility REAL,
                PRIMARY KEY (ticker, date, expiration, strike, option_type)
            );

            CREATE TABLE IF NOT EXISTS options_metrics (
                ticker TEXT,
                date TEXT,
                expiration TEXT,
                strike REAL,
                option_type TEXT,
                black_scholes_price REAL,
                delta REAL,
                gamma REAL,
                vega REAL,
                theta REAL,
                rho REAL,
                breakeven REAL,
                required_move REAL,
                liquidity_score REAL,
                PRIMARY KEY (ticker, date, expiration, strike, option_type)
            );

            CREATE TABLE IF NOT EXISTS news_events (
                ticker TEXT,
                date TEXT,
                headline TEXT,
                source TEXT,
                summary TEXT,
                bullish_score REAL,
                bearish_score REAL,
                importance_score REAL,
                why_it_matters TEXT
            );

            CREATE TABLE IF NOT EXISTS portfolio_positions (
                ticker TEXT PRIMARY KEY,
                shares REAL,
                cost_basis REAL,
                entry_date TEXT,
                current_value REAL,
                gain_loss REAL
            );

            CREATE TABLE IF NOT EXISTS portfolio_risk_snapshots (
                date TEXT PRIMARY KEY,
                health_score REAL,
                annual_volatility REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                average_correlation REAL,
                diversification_score REAL,
                why_now TEXT,
                payload_json TEXT
            );
            """
        )


def upsert_stock(ticker: str, company_name: Optional[str] = None, sector: Optional[str] = None, industry: Optional[str] = None):
    initialize_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO stocks (ticker, company_name, sector, industry, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                company_name = COALESCE(excluded.company_name, stocks.company_name),
                sector = COALESCE(excluded.sector, stocks.sector),
                industry = COALESCE(excluded.industry, stocks.industry),
                updated_at = excluded.updated_at
            """,
            (ticker, company_name, sector, industry, datetime.now().isoformat()),
        )


def store_price_history(ticker: str, df):
    initialize_database()
    rows = []
    for idx, row in df.iterrows():
        rows.append(
            (
                ticker,
                idx.date().isoformat() if hasattr(idx, "date") else str(idx),
                _safe(row.get("Open")),
                _safe(row.get("High")),
                _safe(row.get("Low")),
                _safe(row.get("Close")),
                _safe(row.get("Volume")),
            )
        )
    with connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO price_history (ticker, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def store_technical_metrics(ticker: str, analyzed):
    initialize_database()
    rows = []
    for idx, row in analyzed.iterrows():
        rows.append(
            (
                ticker,
                idx.date().isoformat() if hasattr(idx, "date") else str(idx),
                _safe(row.get("SMA20")),
                _safe(row.get("SMA50")),
                _safe(row.get("SMA200")),
                _safe(row.get("EMA20")),
                _safe(row.get("EMA50")),
                _safe(row.get("RSI")),
                _safe(row.get("MACD")),
                _safe(row.get("ATR")),
            )
        )
    with connect() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO technical_metrics
            (ticker, date, sma20, sma50, sma200, ema20, ema50, rsi, macd, atr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def store_stock_score(ticker: str, date: str, score_payload: Dict):
    initialize_database()
    categories = score_payload.get("categories", {})
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO stock_scores
            (ticker, date, technical_score, momentum_score, volume_score, fundamental_score,
             risk_score, catalyst_score, final_score, rating, confidence, risk_level, explanation, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                _safe(categories.get("technical")),
                _safe(categories.get("momentum")),
                _safe(categories.get("volume")),
                _safe(categories.get("fundamental")),
                _safe(categories.get("risk")),
                _safe(categories.get("catalyst")),
                _safe(score_payload.get("final_score")),
                score_payload.get("rating"),
                _safe(score_payload.get("confidence")),
                score_payload.get("risk_level"),
                score_payload.get("explanation"),
                json.dumps(score_payload, default=str),
            ),
        )


def store_market_regime(date: str, payload: Dict):
    initialize_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO market_regimes
            (date, regime, health_score, risk_environment, buy_environment, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                date,
                payload.get("regime"),
                _safe(payload.get("health_score")),
                payload.get("risk_environment"),
                payload.get("buy_environment"),
                json.dumps(payload, default=str),
            ),
        )


def store_signal(ticker: str, date: str, signal_type: str, entry_price: float, score: float, market_regime: str, sector: str, why_now: Dict, confidence: float):
    initialize_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO signals
            (ticker, date, signal_type, entry_price, score, market_regime, sector, why_now, confidence, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                signal_type,
                _safe(entry_price),
                _safe(score),
                market_regime,
                sector,
                why_now.get("reason"),
                _safe(confidence),
                json.dumps(why_now, default=str),
            ),
        )



def store_portfolio_snapshot(date: str, payload: Dict):
    initialize_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio_risk_snapshots
            (date, health_score, annual_volatility, sharpe_ratio, max_drawdown,
             average_correlation, diversification_score, why_now, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date,
                _safe(payload.get("portfolio_health", {}).get("score")),
                _safe(payload.get("variance", {}).get("annual_volatility")),
                _safe(payload.get("sharpe", {}).get("sharpe_ratio")),
                _safe(payload.get("maximum_drawdown")),
                _safe(payload.get("correlation", {}).get("average_correlation")),
                _safe(payload.get("diversification", {}).get("score")),
                payload.get("why_now", {}).get("reason"),
                json.dumps(payload, default=str),
            ),
        )

def recent_scores(ticker: str, limit: int = 10):
    initialize_database()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT date, final_score, rating, payload_json
            FROM stock_scores
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (ticker, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def iter_signals_without_outcomes():
    initialize_database()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT s.*
            FROM signals s
            LEFT JOIN signal_outcomes o ON o.signal_id = s.id
            WHERE o.signal_id IS NULL
            """
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_signal_outcome(signal_id: int, payload: Dict):
    initialize_database()
    with connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO signal_outcomes
            (signal_id, return_1w, return_1m, return_3m, return_6m, max_drawdown, sp500_relative_return, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                _safe(payload.get("return_1w")),
                _safe(payload.get("return_1m")),
                _safe(payload.get("return_3m")),
                _safe(payload.get("return_6m")),
                _safe(payload.get("max_drawdown")),
                _safe(payload.get("sp500_relative_return")),
                datetime.now().isoformat(),
            ),
        )


def _safe(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None
