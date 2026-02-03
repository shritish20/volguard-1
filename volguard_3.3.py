"""
VOLGUARD 3.3 - FASTAPI EDITION
Refactored for AWS deployment: Removed Prometheus, Multiprocessing, Paper Trading
Preserved: AI Morning Brief, Event Radar, Market Intelligence
"""

import os
import sys
import time
import json
import sqlite3
import logging
import threading
import traceback
import signal
import atexit
import concurrent.futures
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, Dict, List, Tuple, Any, TypeVar, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
import io
import functools
import requests
import pandas as pd
import numpy as np
import pytz
from scipy.stats import norm
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
import yfinance as yf
from groq import Groq

import upstox_client
from upstox_client.rest import ApiException
from upstox_client import OrderApiV3
from upstox_client.api.history_v3_api import HistoryV3Api
from upstox_client.api.options_api import OptionsApi
from upstox_client.api.login_api import LoginApi
from upstox_client.api.charge_api import ChargeApi
from upstox_client.api.market_holidays_and_timings_api import MarketHolidaysAndTimingsApi
from upstox_client.api.order_api import OrderApi
from upstox_client.api.portfolio_api import PortfolioApi

# FastAPI imports
from fastapi import FastAPI, BackgroundTasks, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

T = TypeVar('T')

# Configuration
class ProductionConfig:
    ENVIRONMENT = os.getenv("VG_ENV", "PRODUCTION")
    DRY_RUN_MODE = os.getenv("VG_DRY_RUN", "FALSE").upper() == "TRUE"
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    UPSTOX_CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
    UPSTOX_CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
    UPSTOX_REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI")
    UPSTOX_REFRESH_TOKEN = os.getenv("UPSTOX_REFRESH_TOKEN")
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    BASE_CAPITAL = int(os.getenv("VG_BASE_CAPITAL", "1000000"))
    MARGIN_SELL_BASE = 125000
    MARGIN_BUY_BASE = 30000
    MAX_CAPITAL_USAGE = 0.80
    DAILY_LOSS_LIMIT = 0.03
    MAX_POSITION_SIZE = 0.25
    MAX_LOSS_PER_TRADE = int(os.getenv("VG_MAX_LOSS_PER_TRADE", "50000"))
    MAX_CAPITAL_PER_TRADE = int(os.getenv("VG_MAX_CAPITAL_PER_TRADE", "300000"))
    MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
    MAX_DRAWDOWN_PCT = float(os.getenv("VG_MAX_DRAWDOWN_PCT", "0.15"))
    MAX_CONTRACTS_PER_INSTRUMENT = 1800
    PRICE_CHANGE_THRESHOLD = 0.10
    IRON_FLY_MIN_WING_WIDTH = 100
    IRON_FLY_MAX_WING_WIDTH = 400
    IRON_FLY_WING_DELTA_TARGET = 0.10
    IRON_FLY_ATM_TOLERANCE = 0.02
    GAMMA_DANGER_DTE = 1
    GEX_STICKY_RATIO = 0.03
    HIGH_VOL_IVP = 75.0
    LOW_VOL_IVP = 25.0
    VOV_CRASH_ZSCORE = 2.5
    VOV_WARNING_ZSCORE = 2.0
    WEIGHT_VOL = 0.40
    WEIGHT_STRUCT = 0.30
    WEIGHT_EDGE = 0.20
    WEIGHT_RISK = 0.10
    FII_STRONG_LONG = 50000
    FII_STRONG_SHORT = -50000
    FII_MODERATE = 20000
    TARGET_PROFIT_PCT = 0.50
    STOP_LOSS_PCT = 1.0
    MAX_SHORT_DELTA = 0.20
    EXIT_DTE = 1
    SLIPPAGE_TOLERANCE = 0.02
    PARTIAL_FILL_TOLERANCE = 0.95
    HEDGE_FILL_TOLERANCE = 0.98
    ORDER_TIMEOUT = 10
    MAX_BID_ASK_SPREAD = 0.05
    POLL_INTERVAL = 0.5
    ANALYSIS_INTERVAL = 1800
    MAX_API_RETRIES = 3
    DASHBOARD_REFRESH_RATE = 1.0
    PRICE_STALENESS_THRESHOLD = 5
    DB_PATH = os.getenv("VG_DB_PATH", "/app/data/volguard.db")
    LOG_DIR = os.getenv("VG_LOG_DIR", "/app/logs")
    LOG_FILE = os.path.join(LOG_DIR, f"volguard_{ENVIRONMENT.lower()}.log")
    LOG_LEVEL = logging.INFO
    MARKET_OPEN = (9, 15)
    MARKET_CLOSE = (15, 30)
    SAFE_ENTRY_START = (9, 0)
    SAFE_EXIT_END = (15, 15)
    MAX_CONSECUTIVE_LOSSES = 3
    COOL_DOWN_PERIOD = 86400
    MAX_SLIPPAGE_EVENTS_PER_DAY = 5
    ANALYTICS_PROCESS_TIMEOUT = 300
    HEARTBEAT_INTERVAL = 30
    WEBSOCKET_RECONNECT_DELAY = 5
    KILL_SWITCH_FILE = os.getenv("VG_KILL_SWITCH_FILE", "/app/data/KILL_SWITCH")
    POSITION_RECONCILE_INTERVAL = 300
    MARGIN_BUFFER = 0.20
    
    # Strike & Wing Configuration
    DEFAULT_STRIKE_INTERVAL = 50
    MIN_STRIKE_OI = 1000
    WING_FACTOR_EXTREME_VOL = 1.4
    WING_FACTOR_HIGH_VOL = 1.1
    WING_FACTOR_LOW_VOL = 0.8
    WING_FACTOR_STANDARD = 1.0
    IVP_THRESHOLD_EXTREME = 80.0
    IVP_THRESHOLD_HIGH = 50.0
    IVP_THRESHOLD_LOW = 20.0
    MIN_WING_INTERVAL_MULTIPLIER = 2
    DELTA_SHORT_WEEKLY = 0.20
    DELTA_SHORT_MONTHLY = 0.16
    DELTA_LONG_HEDGE = 0.05
    DELTA_CREDIT_SHORT = 0.30
    DELTA_CREDIT_LONG = 0.10
    TREND_BULLISH_THRESHOLD = 0.0
    PCR_BULLISH_THRESHOLD = 1.0

    # AI Configuration
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    IST = pytz.timezone('Asia/Kolkata')

    # Event Risk Configuration
    VETO_KEYWORDS = [
        "RBI Monetary Policy", "RBI Policy", "Reserve Bank of India",
        "Repo Rate Decision", "MPC Meeting",
        "FOMC", "Federal Reserve Meeting", "Fed Meeting",
        "Federal Funds Rate Decision"
    ]
    HIGH_IMPACT_KEYWORDS = [
        "GDP", "Gross Domestic Product",
        "NFP", "Non-Farm Payroll",
        "CPI", "Consumer Price Index",
        "Union Budget", "Budget Speech"
    ]
    MEDIUM_IMPACT_KEYWORDS = [
        "PMI", "Manufacturing PMI", "Services PMI",
        "Industrial Production",
        "Retail Sales"
    ]
    EVENT_RISK_DAYS_AHEAD = 7

    # Volatility Thresholds
    VIX_MOMENTUM_BREAKOUT = 5.0
    SKEW_CRASH_FEAR = 3.0
    SKEW_MELT_UP = -1.0
    
    # FII Thresholds
    FII_VERY_HIGH_CONVICTION = 150_000
    FII_HIGH_CONVICTION = 80_000
    FII_MODERATE_CONVICTION = 40_000

    # Risk Exit Constants
    MAX_RISK_EXIT_PCT = 0.80
    PROFIT_TARGET_TOLERANCE = 0.1
    STOP_LOSS_TOLERANCE = 0.1

    # Calendar Configuration
    ECONOMIC_CALENDAR_URL = "https://economic-calendar.tradingview.com/events"
    SQUARE_OFF_TIME_IST = dtime(14, 0)

    # Greeks Configuration
    GREEKS_WS_RECONNECT_DELAY = 1
    GREEKS_WS_MAX_RECONNECT_DELAY = 60
    GREEKS_STALE_THRESHOLD = 60
    MAX_PORTFOLIO_DELTA = 50.0
    THETA_VEGA_RATIO_CRITICAL = 1.0
    THETA_VEGA_RATIO_WARNING = 2.0
    MAX_POSITION_GAMMA = 100.0

    # Correlation Configuration
    CORRELATION_MAX_SAME_EXPIRY = int(os.getenv("VG_MAX_SAME_EXPIRY", "1"))
    CORRELATION_ATM_OVERLAP_PCT = float(os.getenv("VG_ATM_OVERLAP", "0.02"))
    CORRELATION_VEGA_CONCENTRATION = float(os.getenv("VG_VEGA_CONC", "0.70"))
    CORRELATION_THETA_CONCENTRATION = float(os.getenv("VG_THETA_CONC", "0.70"))
    CORRELATION_CAPITAL_PER_EXPIRY = float(os.getenv("VG_CAPITAL_EXPIRY", "0.40"))

    @classmethod
    def validate(cls):
        missing = []
        if not cls.DRY_RUN_MODE:
            if not cls.UPSTOX_ACCESS_TOKEN: missing.append("UPSTOX_ACCESS_TOKEN")
        if not cls.TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
        if missing: 
            raise EnvironmentError(f"Missing: {', '.join(missing)}")

# Logging Setup
os.makedirs(ProductionConfig.LOG_DIR, exist_ok=True)
file_handler = RotatingFileHandler(ProductionConfig.LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=ProductionConfig.LOG_LEVEL,
    format='%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger("VOLGUARD")

if ProductionConfig.DRY_RUN_MODE:
    logger.warning("=" * 80)
    logger.warning("DRY RUN MODE ENABLED - NO REAL TRADES WILL BE EXECUTED")
    logger.warning("=" * 80)

# Retry Decorator
def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,)
):
    """Retry decorator with exponential backoff"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"{func.__name__} attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {delay:.1f}s")
                    time.sleep(delay)
            
            raise RuntimeError(f"{func.__name__} failed unexpectedly")
        return wrapper
    return decorator

# Telegram Alerts
class TelegramAlerter:
    def __init__(self):
        self.bot_token = ProductionConfig.TELEGRAM_BOT_TOKEN
        self.chat_id = ProductionConfig.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.rate_limit_lock = threading.Lock()
        self.last_send_time = 0
        self.min_interval = 1.0

    def send(self, message: str, level: str = "INFO", retry: int = 3):
        emoji_map = {
            "CRITICAL": "🚨", "ERROR": "❌", "WARNING": "⚠️",
            "INFO": "ℹ️", "SUCCESS": "✅", "TRADE": "💰", "SYSTEM": "⚙️"
        }
        prefix = emoji_map.get(level, "📢")
        full_msg = f"{prefix} *VOLGUARD 3.3*\n{message}"

        with self.rate_limit_lock:
            elapsed = time.time() - self.last_send_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)

        for attempt in range(retry):
            try:
                response = requests.post(
                    f"{self.base_url}/sendMessage",
                    json={"chat_id": self.chat_id, "text": full_msg, "parse_mode": "Markdown"},
                    timeout=5
                )
                if response.status_code == 200:
                    with self.rate_limit_lock:
                        self.last_send_time = time.time()
                    return True
            except Exception as e:
                logger.error(f"Telegram send error (attempt {attempt+1}/{retry}): {e}")
                if attempt < retry - 1:
                    time.sleep(2 ** attempt)
        logger.error(f"Failed to send Telegram alert after {retry} attempts: {message}")
        return False

telegram = TelegramAlerter()

# Economic Calendar Engine
@dataclass
class EconomicEvent:
    title: str
    country: str
    event_date: datetime
    impact_level: str
    event_type: str
    forecast: str
    previous: str
    days_until: int
    hours_until: float
    is_veto_event: bool
    suggested_square_off_time: Optional[datetime]

class CalendarEngine:
    @staticmethod
    def fetch_calendar(days_ahead: int = 7) -> List[EconomicEvent]:
        try:
            from_timestamp = int(datetime.now().timestamp())
            to_timestamp = int((datetime.now() + timedelta(days=days_ahead)).timestamp())
            
            params = {
                'from': from_timestamp,
                'to': to_timestamp,
                'countries': 'IN,US',
                'importance': '1,2,3'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                ProductionConfig.ECONOMIC_CALENDAR_URL,
                params=params,
                headers=headers,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.warning(f"Calendar API returned {response.status_code}")
                return []
            
            data = response.json()
            events = []
            
            for item in data.get('result', []):
                event_title = item.get('title', '').strip()
                country = item.get('country', '')
                event_timestamp = item.get('date', 0)
                importance = item.get('importance', 0)
                forecast = item.get('forecast', 'N/A')
                previous = item.get('previous', 'N/A')
                
                if event_timestamp == 0:
                    continue
                
                event_date = datetime.fromtimestamp(event_timestamp, tz=ProductionConfig.IST)
                now = datetime.now(ProductionConfig.IST)
                hours_until = (event_date - now).total_seconds() / 3600
                days_until = int(hours_until / 24)
                
                is_veto = any(keyword in event_title for keyword in ProductionConfig.VETO_KEYWORDS)
                
                if is_veto:
                    event_type = "VETO"
                    impact_level = "CRITICAL"
                elif any(keyword in event_title for keyword in ProductionConfig.HIGH_IMPACT_KEYWORDS):
                    event_type = "HIGH_IMPACT"
                    impact_level = "HIGH"
                elif any(keyword in event_title for keyword in ProductionConfig.MEDIUM_IMPACT_KEYWORDS):
                    event_type = "MEDIUM_IMPACT"
                    impact_level = "MEDIUM"
                else:
                    event_type = "LOW_IMPACT"
                    impact_level = "LOW"
                
                square_off_time = None
                if is_veto and hours_until > 0:
                    if hours_until <= 24:
                        square_off_time = event_date - timedelta(hours=2)
                    elif hours_until <= 48:
                        day_before = event_date.date() - timedelta(days=1)
                        square_off_time = datetime.combine(
                            day_before, 
                            ProductionConfig.SQUARE_OFF_TIME_IST
                        ).replace(tzinfo=ProductionConfig.IST)
                
                events.append(EconomicEvent(
                    title=event_title,
                    country=country,
                    event_date=event_date,
                    impact_level=impact_level,
                    event_type=event_type,
                    forecast=str(forecast),
                    previous=str(previous),
                    days_until=days_until,
                    hours_until=hours_until,
                    is_veto_event=is_veto,
                    suggested_square_off_time=square_off_time
                ))
            
            events.sort(key=lambda x: x.event_date)
            logger.info(f"Fetched {len(events)} events ({sum(1 for e in events if e.is_veto_event)} veto)")
            return events
            
        except Exception as e:
            logger.error(f"Calendar fetch failed: {e}")
            return []
    
    @staticmethod
    def analyze_veto_risk(events: List[EconomicEvent], current_time: datetime) -> Tuple[bool, Optional[str], bool, Optional[float]]:
        veto_events = [e for e in events if e.is_veto_event and e.hours_until > 0]
        
        if not veto_events:
            return False, None, False, None
        
        nearest = min(veto_events, key=lambda x: x.hours_until)
        square_off_needed = False
        if nearest.hours_until <= 48:
            square_off_needed = True
        
        return True, nearest.title, square_off_needed, nearest.hours_until

    @staticmethod
    def calculate_event_impact(events: List[EconomicEvent]) -> Tuple[
        List[EconomicEvent], List[EconomicEvent], bool, Optional[datetime]
    ]:
        veto_events = [e for e in events if e.is_veto_event and e.hours_until > 0]
        high_impact = [e for e in events if e.event_type == "HIGH_IMPACT" and e.hours_until > 0]
        
        square_off_needed = False
        square_off_time = None
        
        if veto_events:
            nearest = min(veto_events, key=lambda x: x.hours_until)
            if nearest.hours_until <= 48:
                square_off_needed = True
                square_off_time = nearest.suggested_square_off_time
        
        return veto_events, high_impact, square_off_needed, square_off_time

# Synchronous Database Writer (Refactored - No Threading)
class DatabaseWriter:
    def __init__(self, db_path: str = ProductionConfig.DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_schema()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")
        conn.commit()
        return conn

    def _init_schema(self):
        conn = self._get_connection()
        schema = """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            strategy_type TEXT,
            expiry_date DATE,
            entry_premium REAL,
            max_risk REAL,
            status TEXT,
            legs_json TEXT,
            exit_reason TEXT,
            final_pnl REAL
        );
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            instrument_key TEXT,
            strike REAL,
            option_type TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            current_price REAL,
            delta REAL,
            status TEXT,
            FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
        );
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            severity TEXT,
            description TEXT,
            action_taken TEXT
        );
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS order_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            order_id TEXT,
            instrument_key TEXT,
            side TEXT,
            qty INTEGER,
            price REAL,
            status TEXT,
            filled_qty INTEGER,
            avg_price REAL,
            message TEXT
        );
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            total_trades INTEGER,
            winning_trades INTEGER,
            losing_trades INTEGER,
            total_pnl REAL,
            peak_capital REAL,
            current_capital REAL,
            drawdown_pct REAL,
            sharpe_ratio REAL
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            trades_executed INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            largest_win REAL DEFAULT 0,
            largest_loss REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_risk_events_timestamp ON risk_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_order_log_timestamp ON order_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);
        """
        try:
            conn.executescript(schema)
            conn.commit()
            logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            raise
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple = ()):
        """Synchronous execution - no queue, no threading"""
        conn = self._get_connection()
        try:
            conn.execute(sql, params)
            conn.commit()
        except Exception as e:
            logger.error(f"DB Execute error: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def executescript(self, sql: str):
        conn = self._get_connection()
        try:
            conn.executescript(sql)
            conn.commit()
        except Exception as e:
            logger.error(f"DB Script error: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_trade(self, trade_id: str, strategy: str, expiry: date, legs: List[Dict], entry_premium: float, max_risk: float):
        self.execute(
            "INSERT INTO trades (trade_id, strategy_type, expiry_date, entry_premium, max_risk, status, legs_json) VALUES (?, ?, ?, ?, ?, 'OPEN', ?)",
            (trade_id, strategy, expiry, entry_premium, max_risk, json.dumps(legs))
        )

    def update_trade_exit(self, trade_id: str, exit_reason: str, final_pnl: float):
        self.execute(
            "UPDATE trades SET status='CLOSED', exit_reason=?, final_pnl=? WHERE trade_id=?",
            (exit_reason, final_pnl, trade_id)
        )

    def log_risk_event(self, event_type: str, severity: str, desc: str, action: str):
        self.execute(
            "INSERT INTO risk_events (event_type, severity, description, action_taken) VALUES (?, ?, ?, ?)",
            (event_type, severity, desc, action)
        )

    def log_order(self, order_id: str, instrument_key: str, side: str, qty: int, price: float, status: str, filled_qty: int = 0, avg_price: float = 0.0, message: str = ""):
        self.execute(
            "INSERT INTO order_log (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message)
        )

    def set_state(self, key: str, value: str):
        self.execute(
            "INSERT OR REPLACE INTO system_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (key, value)
        )

    def get_state(self, key: str) -> Optional[str]:
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM system_state WHERE key = ?", (key,))
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"State read error: {e}")
            return None

    def update_daily_stats(self, trades: int = 0, pnl: float = 0, largest_win: float = 0, largest_loss: float = 0):
        today = date.today()
        self.execute(
            "INSERT INTO daily_stats (date, trades_executed, total_pnl, largest_win, largest_loss) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(date) DO UPDATE SET trades_executed=trades_executed+?, total_pnl=total_pnl+?, "
            "largest_win=MAX(largest_win, ?), largest_loss=MIN(largest_loss, ?)",
            (today, trades, pnl, largest_win, largest_loss, trades, pnl, largest_win, largest_loss)
        )

    def get_daily_stats(self, target_date: date = None) -> Optional[Dict]:
        if not target_date:
            target_date = date.today()
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM daily_stats WHERE date = ?", (target_date,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return {
                    'trades_executed': row['trades_executed'],
                    'total_pnl': row['total_pnl'],
                    'largest_win': row['largest_win'],
                    'largest_loss': row['largest_loss']
                }
            return None
        except Exception as e:
            logger.error(f"Daily stats read error: {e}")
            return None

    def export_trade_journal(self, output_path: str):
        try:
            conn = self._get_connection()
            trades_df = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC", conn)
            risk_events_df = pd.read_sql_query("SELECT * FROM risk_events ORDER BY timestamp DESC", conn)
            conn.close()
            trades_df.to_csv(os.path.join(output_path, "trades.csv"), index=False)
            risk_events_df.to_csv(os.path.join(output_path, "risk_events.csv"), index=False)
            logger.info(f"Trade journal exported to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False

db_writer = DatabaseWriter()

# Circuit Breaker
class CircuitBreaker:
    def __init__(self, db_writer: DatabaseWriter):
        self.db_writer = db_writer
        self.consecutive_losses = self._load_consecutive_losses()
        self.peak_capital = self._load_peak_capital()
        
        self.breaker_triggered, self.breaker_until = self._load_breaker_state()
        
        if self.breaker_triggered and self.breaker_until:
            remaining = (self.breaker_until - datetime.now()).total_seconds() / 3600
            if remaining > 0:
                logger.warning(f"Circuit breaker ACTIVE on startup. Cooldown remaining: {remaining:.1f} hours")
                telegram.send(f"Circuit breaker resumed from DB: {remaining:.1f}h cooldown remaining", "WARNING")
            else:
                logger.info("Circuit breaker cooldown expired during downtime")
                self.breaker_triggered = False
                self.breaker_until = None
        
        self.daily_slippage_events = 0
        self.last_reset_date = date.today()
        self.current_capital = ProductionConfig.BASE_CAPITAL

    def _load_consecutive_losses(self) -> int:
        try:
            value = db_writer.get_state("consecutive_losses")
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"Failed to load consecutive losses: {e}")
            return 0

    def _load_peak_capital(self) -> float:
        try:
            value = db_writer.get_state("peak_capital")
            return float(value) if value else ProductionConfig.BASE_CAPITAL
        except Exception as e:
            logger.error(f"Failed to load peak capital: {e}")
            return ProductionConfig.BASE_CAPITAL

    def _load_breaker_state(self) -> Tuple[bool, Optional[datetime]]:
        try:
            triggered_str = self.db_writer.get_state("breaker_triggered")
            until_str = self.db_writer.get_state("breaker_until")
            
            if triggered_str == "True":
                breaker_until = datetime.fromisoformat(until_str) if until_str else None
                return True, breaker_until
            
            return False, None
        except Exception as e:
            logger.error(f"Failed to load circuit breaker state: {e}")
            return False, None

    def _save_consecutive_losses(self):
        self.db_writer.set_state("consecutive_losses", str(self.consecutive_losses))

    def _save_peak_capital(self):
        self.db_writer.set_state("peak_capital", str(self.peak_capital))

    def _check_daily_reset(self):
        if date.today() > self.last_reset_date:
            self.daily_slippage_events = 0
            self.last_reset_date = date.today()
            logger.info("Circuit breaker daily counters reset")

    def update_capital(self, new_capital: float):
        self.current_capital = new_capital
        if new_capital > self.peak_capital:
            self.peak_capital = new_capital
            self._save_peak_capital()
        drawdown = (self.peak_capital - new_capital) / self.peak_capital
        if drawdown >= ProductionConfig.MAX_DRAWDOWN_PCT:
            self.trigger_breaker("MAX_DRAWDOWN", f"Drawdown: {drawdown*100:.1f}%")
            return False
        return True

    def check_daily_trade_limit(self) -> bool:
        stats = db_writer.get_daily_stats()
        if stats and stats['trades_executed'] >= ProductionConfig.MAX_TRADES_PER_DAY:
            logger.warning(f"Daily trade limit reached: {stats['trades_executed']}/{ProductionConfig.MAX_TRADES_PER_DAY}")
            return False
        return True

    def check_daily_loss_limit(self, current_pnl: float) -> bool:
        loss_pct = abs(current_pnl) / ProductionConfig.BASE_CAPITAL
        if current_pnl < 0 and loss_pct >= ProductionConfig.DAILY_LOSS_LIMIT:
            self.trigger_breaker("DAILY_LOSS_LIMIT", f"Loss: ₹{current_pnl:,.2f} ({loss_pct*100:.1f}%)")
            return False
        return True

    def record_slippage_event(self, slippage_pct: float) -> bool:
        self._check_daily_reset()
        self.daily_slippage_events += 1
        if self.daily_slippage_events >= ProductionConfig.MAX_SLIPPAGE_EVENTS_PER_DAY:
            self.trigger_breaker("EXCESSIVE_SLIPPAGE", f"{self.daily_slippage_events} events today")
            return False
        return True

    def record_trade_result(self, pnl: float) -> bool:
        if pnl < 0:
            self.consecutive_losses += 1
            self._save_consecutive_losses()
            if self.consecutive_losses >= ProductionConfig.MAX_CONSECUTIVE_LOSSES:
                self.trigger_breaker("CONSECUTIVE_LOSSES", f"{self.consecutive_losses} losses")
                return False
        else:
            if self.consecutive_losses > 0:
                logger.info(f"Winning trade after {self.consecutive_losses} losses - resetting counter")
                self.consecutive_losses = 0
                self._save_consecutive_losses()
        return True

    def trigger_breaker(self, reason: str, details: str):
        self.breaker_triggered = True
        self.breaker_until = datetime.now() + timedelta(seconds=ProductionConfig.COOL_DOWN_PERIOD)
        
        self.db_writer.set_state("breaker_triggered", "True")
        self.db_writer.set_state("breaker_until", self.breaker_until.isoformat())
        
        telegram.send(f"🔴 *CIRCUIT BREAKER*\n{reason}: {details}\nCooldown until: {self.breaker_until.strftime('%H:%M:%S')}", "CRITICAL")
        db_writer.log_risk_event("CIRCUIT_BREAKER", "CRITICAL", reason, details)
        logger.critical(f"CIRCUIT BREAKER: {reason} - {details}")

    def is_active(self) -> bool:
        if os.path.exists(ProductionConfig.KILL_SWITCH_FILE):
            logger.critical(f"KILL SWITCH DETECTED: {ProductionConfig.KILL_SWITCH_FILE}")
            self.trigger_breaker("KILL_SWITCH", "Manual emergency stop")
            return True
        if self.breaker_triggered and self.breaker_until:
            if datetime.now() > self.breaker_until:
                logger.info("Circuit breaker cooldown expired - resetting")
                self.breaker_triggered = False
                self.breaker_until = None
                
                self.db_writer.set_state("breaker_triggered", "False")
                self.db_writer.set_state("breaker_until", "")
                
                telegram.send("Circuit breaker cooldown expired - system ready", "SYSTEM")
                return False
        return self.breaker_triggered

circuit_breaker = CircuitBreaker(db_writer)

# Live Greeks Manager
@dataclass
class GreeksData:
    delta: float = 0.0
    theta: float = 0.0
    gamma: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    iv: float = 0.0
    ltp: float = 0.0
    oi: float = 0.0
    timestamp: float = 0.0

class LiveGreeksManager:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.ws = None
        self.subscribed_keys = set()
        self.greeks_cache = {}
        self.lock = threading.RLock()
        self.running = False
        self.thread = None
        self.connected = False
        self.message_count = 0
        self.last_message_time = time.time()
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60

    def start(self):
        if self.running:
            logger.debug("LiveGreeksManager already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True, name="LiveGreeks-WS")
        self.thread.start()
        logger.info("LiveGreeks Manager started")

    def stop(self):
        logger.info("Shutting down LiveGreeks Manager...")
        self.running = False
        
        if self.ws:
            try:
                self.ws.disconnect()
                logger.debug("WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            
        logger.info("LiveGreeks Manager stopped")

    def _ws_loop(self):
        MAX_RECONNECT_ATTEMPTS = 10
        reconnect_count = 0
        
        while self.running:
            try:
                self._connect()
                self.reconnect_delay = 1
                reconnect_count = 0
                
                while self.running and self.connected:
                    time.sleep(5)
                    if time.time() - self.last_message_time > 30:
                        logger.warning("WebSocket stale (no data for 30s), forcing reconnect")
                        break
                        
            except Exception as e:
                logger.error(f"WebSocket loop error: {e}")
                self.connected = False
                reconnect_count += 1
                
                if reconnect_count >= MAX_RECONNECT_ATTEMPTS:
                    logger.critical(f"Greeks WebSocket permanently failed after {reconnect_count} attempts - HALTING SYSTEM")
                    telegram.send(
                        "🚨 CRITICAL: Greeks feed permanently lost - cannot monitor risk safely. Circuit breaker activated.",
                        "CRITICAL"
                    )
                    
                    try:
                        import sys
                        main_module = sys.modules['__main__']
                        cb = getattr(main_module, 'circuit_breaker', None)
                        if cb:
                            cb.trigger_breaker("GREEKS_FEED_FAILURE", f"Failed {reconnect_count} reconnect attempts")
                    except Exception as cb_error:
                        logger.error(f"Failed to trigger circuit breaker: {cb_error}")
                    
                    self.running = False
                    break
            
            if self.running:
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds... (Attempt {reconnect_count}/{MAX_RECONNECT_ATTEMPTS})")
                time.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)

    def _connect(self):
        with self.lock:
            keys = list(self.subscribed_keys) if self.subscribed_keys else []
        
        if not keys:
            logger.warning("No instruments to subscribe, skipping connection")
            return
        
        logger.info(f"Connecting to Option Greeks WebSocket ({len(keys)} instruments)")
        
        try:
            self.ws = upstox_client.MarketDataStreamerV3(
                self.api_client, 
                keys, 
                "option_greeks"
            )
            
            self.ws.on("open", self._on_open)
            self.ws.on("message", self._on_message)
            self.ws.on("error", self._on_error)
            self.ws.on("close", self._on_close)
            
            self.ws.connect()
            
            while self.running and self.connected:
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self.connected = False
            raise

    def _on_open(self):
        logger.info("Live Greeks WebSocket connected")
        self.connected = True
        self.last_message_time = time.time()

    def _on_message(self, msg):
        self.last_message_time = time.time()
        
        try:
            if isinstance(msg, str):
                data = json.loads(msg)
            else:
                data = msg
            
            if not isinstance(data, dict) or 'feeds' not in data:
                return
            
            self.message_count += 1
            
            feeds = data.get('feeds', {})
            for instrument_key, feed_data in feeds.items():
                self._process_instrument(instrument_key, feed_data)
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.debug(f"Message processing error: {e}")

    def _process_instrument(self, instrument_key: str, feed_data: dict):
        try:
            first_level = feed_data.get('firstLevelWithGreeks', {})
            
            ltpc = first_level.get('ltpc', {})
            ltp = float(ltpc.get('ltp', 0) or 0)
            
            greeks_dict = first_level.get('optionGreeks', {})
            
            data = GreeksData()
            data.ltp = ltp
            data.delta = float(greeks_dict.get('delta', 0) or 0)
            data.theta = float(greeks_dict.get('theta', 0) or 0)
            data.gamma = float(greeks_dict.get('gamma', 0) or 0)
            data.vega = float(greeks_dict.get('vega', 0) or 0)
            data.rho = float(greeks_dict.get('rho', 0) or 0)
            data.iv = float(first_level.get('iv', 0) or 0)
            data.oi = float(first_level.get('oi', 0) or 0)
            data.timestamp = time.time()
            
            with self.lock:
                self.greeks_cache[instrument_key] = data
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Data conversion error for {instrument_key}: {e}")
        except Exception as e:
            logger.debug(f"Parse error for {instrument_key}: {e}")

    def _on_error(self, error):
        logger.error(f"WebSocket error: {error}")
        telegram.send(f"⚠️ Greeks WebSocket error: {str(error)[:100]}", "WARNING")
        self.connected = False

    def _on_close(self):
        if self.connected:
            logger.warning("Live Greeks WebSocket disconnected")
        self.connected = False

    def update_subscriptions(self, instrument_keys: List[str]):
        new_keys = set(instrument_keys)
        
        with self.lock:
            if new_keys == self.subscribed_keys:
                return
            self.subscribed_keys = new_keys
        
        logger.info(f"Updated Greek subscriptions: {len(new_keys)} instruments")
        
        if self.ws and self.connected:
            logger.debug("Forcing reconnect to update subscriptions")
            try:
                self.ws.disconnect()
            except Exception as e:
                logger.error(f"Error forcing disconnect: {e}")

    def get_position_greeks(self, instrument_key: str) -> Optional[GreeksData]:
        with self.lock:
            data = self.greeks_cache.get(instrument_key)
            if data and (time.time() - data.timestamp) < 60:
                return data
            return None

    def calculate_position_greeks(self, leg: Dict, trade_id: str) -> Dict[str, float]:
        live = self.get_position_greeks(leg['key'])
        if not live:
            return {}
        
        qty = leg.get('filled_qty', leg.get('qty', 0))
        side_mult = -1 if leg['side'] == 'SELL' else 1
        
        notional = {
            'delta': live.delta * qty * side_mult,
            'theta': live.theta * qty * side_mult,
            'gamma': live.gamma * qty * side_mult,
            'vega': live.vega * qty * side_mult,
            'iv': live.iv,
            'ltp': live.ltp,
            'oi': live.oi
        }
        
        if abs(notional['vega']) > 0.0001:
            raw_ratio = abs(notional['theta']) / abs(notional['vega'])
            notional['theta_vega_ratio'] = raw_ratio / 1000.0
        else:
            notional['theta_vega_ratio'] = 0.0
            
        return notional

    def get_portfolio_greeks(self, legs: List[Dict], trade_id: str) -> Dict[str, float]:
        portfolio = {
            'delta': 0.0,
            'theta': 0.0,
            'gamma': 0.0,
            'vega': 0.0,
            'theta_vega_ratio': 0.0,
            'short_vega_exposure': 0.0,
            'legs_count': len(legs),
            'stale_count': 0
        }
        
        for leg in legs:
            pos = self.calculate_position_greeks(leg, trade_id)
            if not pos:
                portfolio['stale_count'] += 1
                continue
            
            portfolio['delta'] += pos['delta']
            portfolio['theta'] += pos['theta']
            portfolio['gamma'] += pos['gamma']
            portfolio['vega'] += pos['vega']
            
            if leg['side'] == 'SELL' and pos['vega'] > 0:
                portfolio['short_vega_exposure'] += pos['vega']
        
        if abs(portfolio['vega']) > 0.0001:
            raw_ratio = abs(portfolio['theta']) / abs(portfolio['vega'])
            portfolio['theta_vega_ratio'] = raw_ratio / 1000.0
        
        return portfolio

    def check_risk_limits(self, legs: List[Dict], trade_id: str) -> List[str]:
        warnings = []
        port = self.get_portfolio_greeks(legs, trade_id)
        
        if port['stale_count'] > len(legs) / 2:
            warnings.append(f"CRITICAL: {port['stale_count']}/{len(legs)} legs have stale Greeks data")
        
        ratio = port['theta_vega_ratio']
        if ratio > 0:
            if ratio < 1.0:
                msg = f"CRITICAL Theta/Vega Ratio: {ratio:.2f} (Volatility risk exceeds time income)"
                warnings.append(msg)
                telegram.send(f"🚨 {trade_id}: {msg}", "CRITICAL")
            elif ratio < 2.0:
                warnings.append(f"LOW Theta/Vega Ratio: {ratio:.2f} (Monitor volatility closely)")
            elif ratio > 5.0:
                warnings.append(f"STRONG Theta/Vega Ratio: {ratio:.2f} (Excellent time decay)")
            else:
                warnings.append(f"NORMAL Theta/Vega Ratio: {ratio:.2f}")
        
        if abs(port['delta']) > getattr(ProductionConfig, 'MAX_PORTFOLIO_DELTA', 50):
            warnings.append(f"DELTA ALERT: {port['delta']:.1f} (Limit: ±{ProductionConfig.MAX_PORTFOLIO_DELTA})")
        
        if legs:
            expiry = legs[0].get('expiry', date.today())
            if isinstance(expiry, str):
                expiry = datetime.strptime(expiry, "%Y-%m-%d").date()
            days_to_expiry = (expiry - date.today()).days
            
            if days_to_expiry <= ProductionConfig.EXIT_DTE and abs(port['gamma']) > 100:
                msg = f"GAMMA DANGER: {port['gamma']:.1f} with {days_to_expiry} DTE - AUTO-EXIT AT 2 PM"
                warnings.append(msg)
                telegram.send(msg, "CRITICAL")
        
        return warnings

_live_greeks_instance: Optional[LiveGreeksManager] = None

def get_live_greeks_manager(api_client: upstox_client.ApiClient) -> LiveGreeksManager:
    global _live_greeks_instance
    if _live_greeks_instance is None:
        _live_greeks_instance = LiveGreeksManager(api_client)
    return _live_greeks_instance

# Data Classes
@dataclass
class TimeMetrics:
    current_date: date
    weekly_exp: date
    monthly_exp: date
    next_weekly_exp: date
    dte_weekly: int
    dte_monthly: int
    dte_next_weekly: int = 0
    is_expiry_day_weekly: bool = False
    is_expiry_day_monthly: bool = False
    is_past_square_off_time: bool = False
    is_gamma_week: bool = False
    is_gamma_month: bool = False
    days_to_next_weekly: int = 0

@dataclass
class VolMetrics:
    spot: float
    vix: float
    rv7: float
    rv28: float
    rv90: float
    garch7: float
    garch28: float
    park7: float
    park28: float
    vov: float
    vov_zscore: float
    ivp_30d: float
    ivp_90d: float
    ivp_1yr: float
    ma20: float
    atr14: float
    trend_strength: float
    vol_regime: str
    is_fallback: bool
    vix_change_5d: float = 0.0
    vix_momentum: str = "UNKNOWN"

@dataclass
class StructMetrics:
    net_gex: float
    gex_ratio: float
    total_oi_value: float
    gex_regime: str
    pcr: float
    max_pain: float
    skew_25d: float
    oi_regime: str
    lot_size: int
    pcr_atm: float = 1.0
    skew_regime: str = "UNKNOWN"
    gex_weighted: float = 5.0

@dataclass
class EdgeMetrics:
    iv_weekly: float
    vrp_rv_weekly: float
    vrp_garch_weekly: float
    vrp_park_weekly: float
    iv_monthly: float
    vrp_rv_monthly: float
    vrp_garch_monthly: float
    vrp_park_monthly: float
    iv_next_weekly: float = 0.0
    vrp_rv_next_weekly: float = 0.0
    vrp_garch_next_weekly: float = 0.0
    vrp_park_next_weekly: float = 0.0
    term_spread: float = 0.0
    term_regime: str = "FLAT"
    primary_edge: str = "NONE"
    weighted_vrp_weekly: float = 0.0
    weighted_vrp_monthly: float = 0.0
    weighted_vrp_next_weekly: float = 0.0

@dataclass
class ParticipantData:
    fut_long: float
    fut_short: float
    fut_net: float
    call_long: float
    call_short: float
    call_net: float
    put_long: float
    put_short: float
    put_net: float
    stock_net: float

@dataclass
class ExternalMetrics:
    fii: Optional[ParticipantData]
    dii: Optional[ParticipantData]
    pro: Optional[ParticipantData]
    client: Optional[ParticipantData]
    fii_net_change: float
    flow_regime: str
    flow_score: float = 0.0
    option_bias: float = 0.0
    data_date: str = ""
    is_fallback_data: bool = False
    fast_vol: bool = False
    fii_conviction: str = "NEUTRAL"
    fii_direction: str = "NEUTRAL"
    economic_events: List[EconomicEvent] = field(default_factory=list)
    veto_events: List[EconomicEvent] = field(default_factory=list)
    high_impact_events: List[EconomicEvent] = field(default_factory=list)
    veto_square_off_needed: bool = False
    veto_square_off_time: Optional[datetime] = None
    veto_hours_until: Optional[float] = None
    event_risk: str = "LOW"
    has_veto_event: bool = False
    veto_event_name: Optional[str] = None
    upcoming_high_impact: List[EconomicEvent] = field(default_factory=list)

@dataclass
class DynamicWeights:
    vol_weight: float
    struct_weight: float
    edge_weight: float
    rationale: str

@dataclass
class RegimeScore:
    vol_score: float
    struct_score: float
    edge_score: float
    composite: float
    confidence: str
    score_stability: float
    weights_used: DynamicWeights
    score_drivers: List[str] = field(default_factory=list)

@dataclass
class TradingMandate:
    expiry_type: str
    expiry_date: date
    dte: int
    regime_name: str
    strategy_type: str
    allocation_pct: float
    deployment_amount: float
    score: RegimeScore
    rationale: List[str]
    warnings: List[str]
    veto_reasons: List[str] = field(default_factory=list)
    suggested_structure: str = ""
    directional_bias: str = "NEUTRAL"
    wing_protection: str = "STANDARD"
    is_trade_allowed: bool = True
    data_relevance: str = "FRESH"
    square_off_instruction: Optional[str] = None
    max_lots: int = 0
    risk_per_lot: float = 0.0

@dataclass
class PositionFingerprint:
    trade_id: str
    expiry_date: date
    dte: int
    strategy_type: str
    structure: str
    short_call_strike: float
    short_put_strike: float
    atm_center: float
    strike_width: float
    net_delta: float
    net_theta: float
    net_gamma: float
    net_vega: float
    margin_used: float
    max_risk: float
    timestamp: datetime

class PositionCorrelationManager:
    def __init__(self):
        self.active_positions: List[PositionFingerprint] = []
        self.correlation_rules = {
            'max_same_expiry': ProductionConfig.CORRELATION_MAX_SAME_EXPIRY,
            'max_atm_overlap_pct': ProductionConfig.CORRELATION_ATM_OVERLAP_PCT,
            'max_vega_concentration': ProductionConfig.CORRELATION_VEGA_CONCENTRATION,
            'max_theta_concentration': ProductionConfig.CORRELATION_THETA_CONCENTRATION,
            'max_capital_per_expiry': ProductionConfig.CORRELATION_CAPITAL_PER_EXPIRY,
        }
    
    def register_position(self, legs: List[Dict], trade_id: str, 
                         expiry: date, strategy: str, greeks: Dict):
        short_legs = [l for l in legs if l['side'] == 'SELL']
        ce_shorts = [l['strike'] for l in short_legs if l['type'] == 'CE']
        pe_shorts = [l['strike'] for l in short_legs if l['type'] == 'PE']
        
        short_call = min(ce_shorts) if ce_shorts else 0
        short_put = max(pe_shorts) if pe_shorts else 0
        atm_center = (short_call + short_put) / 2 if (short_call and short_put) else 0
        strike_width = abs(short_call - short_put) if (short_call and short_put) else 0
        
        fingerprint = PositionFingerprint(
            trade_id=trade_id,
            expiry_date=expiry,
            dte=(expiry - date.today()).days,
            strategy_type=strategy,
            structure=legs[0].get('structure', 'UNKNOWN'),
            short_call_strike=short_call,
            short_put_strike=short_put,
            atm_center=atm_center,
            strike_width=strike_width,
            net_delta=greeks.get('delta', 0),
            net_theta=greeks.get('theta', 0),
            net_gamma=greeks.get('gamma', 0),
            net_vega=greeks.get('vega', 0),
            margin_used=greeks.get('margin', 0),
            max_risk=greeks.get('max_risk', 0),
            timestamp=datetime.now()
        )
        
        self.active_positions.append(fingerprint)
        logger.info(f"Registered position: {trade_id} | Expiry: {expiry} | ATM: {atm_center:.0f}")
    
    def remove_position(self, trade_id: str):
        self.active_positions = [p for p in self.active_positions if p.trade_id != trade_id]
        logger.info(f"Removed position: {trade_id}")
    
    def check_correlation(self, proposed_legs: List[Dict], proposed_expiry: date,
                         proposed_strategy: str, proposed_greeks: Dict,
                         spot: float) -> Tuple[bool, List[str]]:
        if not self.active_positions:
            return True, []
        
        warnings = []
        blockers = []
        
        short_legs = [l for l in proposed_legs if l['side'] == 'SELL']
        ce_shorts = [l['strike'] for l in short_legs if l['type'] == 'CE']
        pe_shorts = [l['strike'] for l in short_legs if l['type'] == 'PE']
        
        proposed_call = min(ce_shorts) if ce_shorts else 0
        proposed_put = max(pe_shorts) if pe_shorts else 0
        proposed_atm = (proposed_call + proposed_put) / 2 if (proposed_call and proposed_put) else spot
        
        same_expiry_positions = [p for p in self.active_positions if p.expiry_date == proposed_expiry]
        
        if len(same_expiry_positions) >= self.correlation_rules['max_same_expiry']:
            blockers.append(
                f"SAME EXPIRY BLOCK: Already have {len(same_expiry_positions)} "
                f"position(s) on {proposed_expiry}"
            )
        
        for pos in self.active_positions:
            if pos.atm_center == 0:
                continue
            
            atm_diff_pct = abs(proposed_atm - pos.atm_center) / pos.atm_center
            
            if atm_diff_pct < self.correlation_rules['max_atm_overlap_pct']:
                blockers.append(
                    f"ATM OVERLAP: Proposed center {proposed_atm:.0f} is {atm_diff_pct*100:.1f}% "
                    f"from existing position {pos.trade_id} center {pos.atm_center:.0f} "
                    f"(limit: {self.correlation_rules['max_atm_overlap_pct']*100}%)"
                )
        
        portfolio_theta = sum(p.net_theta for p in self.active_positions)
        portfolio_vega = sum(p.net_vega for p in self.active_positions)
        
        proposed_theta = proposed_greeks.get('theta', 0)
        proposed_vega = proposed_greeks.get('vega', 0)
        
        if abs(portfolio_vega) > 0:
            vega_concentration = abs(proposed_vega) / abs(portfolio_vega)
            if vega_concentration > self.correlation_rules['max_vega_concentration']:
                warnings.append(
                    f"VEGA CONCENTRATION: New position vega ({proposed_vega:.0f}) is "
                    f"{vega_concentration*100:.0f}% of portfolio vega ({portfolio_vega:.0f})"
                )
        
        if abs(portfolio_theta) > 0:
            theta_concentration = abs(proposed_theta) / abs(portfolio_theta)
            if theta_concentration > self.correlation_rules['max_theta_concentration']:
                warnings.append(
                    f"THETA CONCENTRATION: New position theta ({proposed_theta:.0f}) is "
                    f"{theta_concentration*100:.0f}% of portfolio theta ({portfolio_theta:.0f})"
                )
        
        same_expiry_capital = sum(p.margin_used for p in same_expiry_positions)
        proposed_margin = proposed_greeks.get('margin', 0)
        total_expiry_capital = same_expiry_capital + proposed_margin
        
        capital_pct = total_expiry_capital / ProductionConfig.BASE_CAPITAL
        
        if capital_pct > self.correlation_rules['max_capital_per_expiry']:
            blockers.append(
                f"CAPITAL CONCENTRATION: {proposed_expiry} would use "
                f"{capital_pct*100:.0f}% of capital (limit: {self.correlation_rules['max_capital_per_expiry']*100}%)"
            )
        
        proposed_structure = proposed_legs[0].get('structure', 'UNKNOWN')
        same_structure_count = sum(1 for p in self.active_positions if p.structure == proposed_structure)
        
        if same_structure_count >= 2:
            warnings.append(
                f"STRUCTURE REPETITION: Already have {same_structure_count} "
                f"{proposed_structure} positions"
            )
        
        is_allowed = len(blockers) == 0
        
        return is_allowed, blockers + warnings
    
    def get_portfolio_summary(self) -> Dict:
        if not self.active_positions:
            return {
                'position_count': 0,
                'total_delta': 0,
                'total_theta': 0,
                'total_vega': 0,
                'total_gamma': 0,
                'total_margin': 0,
                'total_risk': 0,
                'expiries': []
            }
        
        return {
            'position_count': len(self.active_positions),
            'total_delta': sum(p.net_delta for p in self.active_positions),
            'total_theta': sum(p.net_theta for p in self.active_positions),
            'total_vega': sum(p.net_vega for p in self.active_positions),
            'total_gamma': sum(p.net_gamma for p in self.active_positions),
            'total_margin': sum(p.margin_used for p in self.active_positions),
            'total_risk': sum(p.max_risk for p in self.active_positions),
            'expiries': list(set(p.expiry_date for p in self.active_positions)),
            'positions': [
                {
                    'trade_id': p.trade_id,
                    'expiry': str(p.expiry_date),
                    'dte': p.dte,
                    'structure': p.structure,
                    'atm': p.atm_center,
                    'delta': p.net_delta,
                    'theta': p.net_theta,
                    'vega': p.net_vega
                }
                for p in self.active_positions
            ]
        }

# Analytics Engine (Synchronous - No Multiprocessing)
class AnalyticsEngine:
    def __init__(self):
        pass

    def run(self, config: Dict) -> Tuple[str, Any]:
        """Synchronous execution - returns result directly instead of using queue"""
        try:
            api_client = upstox_client.ApiClient()
            api_client.configuration.access_token = config['access_token']
            history_api = HistoryV3Api(api_client)
            options_api = OptionsApi(api_client)
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=400)).strftime("%Y-%m-%d")
            nifty_response = history_api.get_historical_candle_data1(ProductionConfig.NIFTY_KEY, "days", "1", to_date, from_date)
            vix_response = history_api.get_historical_candle_data1(ProductionConfig.VIX_KEY, "days", "1", to_date, from_date)
            nifty_hist = self._parse_candle_response(nifty_response)
            vix_hist = self._parse_candle_response(vix_response)
            market_api = upstox_client.MarketQuoteV3Api(api_client)
            live_prices = market_api.get_ltp(instrument_key=f"{ProductionConfig.NIFTY_KEY},{ProductionConfig.VIX_KEY}")
            weekly, monthly, next_weekly, lot_size = self._get_expiries(options_api)
            weekly_chain = self._get_option_chain(options_api, weekly) if weekly else pd.DataFrame()
            monthly_chain = self._get_option_chain(options_api, monthly) if monthly else pd.DataFrame()
            next_weekly_chain = self._get_option_chain(options_api, next_weekly) if next_weekly else pd.DataFrame()
            
            calendar_engine = CalendarEngine()
            economic_events = calendar_engine.fetch_calendar(ProductionConfig.EVENT_RISK_DAYS_AHEAD)
            
            participant_data, participant_yest, fii_net_change, data_date = self._fetch_participant_data()
            time_metrics = self.get_time_metrics(weekly, monthly, next_weekly)
            vol_metrics = self.get_vol_metrics(nifty_hist, vix_hist, live_prices)
            struct_metrics_weekly = self.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_metrics_monthly = self.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            edge_metrics = self.get_edge_metrics(weekly_chain, monthly_chain, next_weekly_chain, vol_metrics.spot, vol_metrics, time_metrics)
            external_metrics = self.get_external_metrics(nifty_hist, participant_data, participant_yest, fii_net_change, data_date, economic_events)

            result = {
                'timestamp': datetime.now(),
                'time_metrics': time_metrics,
                'vol_metrics': vol_metrics,
                'weekly_chain': weekly_chain,
                'monthly_chain': monthly_chain,
                'next_weekly_chain': next_weekly_chain,
                'lot_size': lot_size,
                'participant_data': participant_data,
                'participant_yest': participant_yest,
                'fii_net_change': fii_net_change,
                'data_date': data_date,
                'external_metrics': external_metrics,
                'edge_metrics': edge_metrics,
                'struct_metrics_weekly': struct_metrics_weekly,
                'struct_metrics_monthly': struct_metrics_monthly
            }
            return ('success', result)
        except Exception as e:
            logger.error(f"Analytics process error: {e}")
            traceback.print_exc()
            return ('error', str(e))

    def _parse_candle_response(self, response):
        if response.status != 'success':
            return pd.DataFrame()
        candles = response.data.candles if hasattr(response.data, 'candles') else []
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df.astype(float).sort_index()

    def _get_expiries(self, options_api: OptionsApi) -> Tuple[Optional[date], Optional[date], Optional[date], int]:
        try:
            response = options_api.get_option_contracts(instrument_key=ProductionConfig.NIFTY_KEY)
            if response.status != 'success':
                return None, None, None, 0
            data = response.data
            if not data:
                return None, None, None, 0
            lot_size = next((int(c.lot_size) for c in data if hasattr(c, 'lot_size')), 0)
            expiry_dates = sorted(list(set([
                (c.expiry.date() if hasattr(c.expiry, 'date') else 
                 datetime.strptime(str(c.expiry).split('T')[0], "%Y-%m-%d").date())
                for c in data if hasattr(c, 'expiry') and c.expiry
            ])))
            valid_dates = [d for d in expiry_dates if d >= date.today()]
            if not valid_dates:
                return None, None, None, lot_size
            weekly = valid_dates[0]
            next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
            current_month = date.today().month
            current_year = date.today().year
            monthly_candidates = [d for d in valid_dates if d.month == current_month and d.year == current_year]
            if not monthly_candidates or monthly_candidates[-1] < date.today():
                next_month = current_month + 1 if current_month < 12 else 1
                next_year = current_year if current_month < 12 else current_year + 1
                monthly_candidates = [d for d in valid_dates if d.month == next_month and d.year == next_year]
            monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
            return weekly, monthly, next_weekly, lot_size
        except Exception as e:
            logger.error(f"Expiries fetch error: {e}")
            return None, None, None, 0

    def _get_option_chain(self, options_api: OptionsApi, expiry_date: date) -> pd.DataFrame:
        try:
            response = options_api.get_put_call_option_chain(
                instrument_key=ProductionConfig.NIFTY_KEY,
                expiry_date=expiry_date.strftime("%Y-%m-%d")
            )
            if response.status != 'success':
                return pd.DataFrame()
            data = response.data
            return pd.DataFrame([{
                'strike': x.strike_price,
                'ce_iv': x.call_options.option_greeks.iv,
                'pe_iv': x.put_options.option_greeks.iv,
                'ce_delta': x.call_options.option_greeks.delta,
                'pe_delta': x.put_options.option_greeks.delta,
                'ce_gamma': x.call_options.option_greeks.gamma,
                'pe_gamma': x.put_options.option_greeks.gamma,
                'ce_oi': x.call_options.market_data.oi,
                'pe_oi': x.put_options.market_data.oi,
                'ce_ltp': x.call_options.market_data.ltp,
                'pe_ltp': x.put_options.market_data.ltp,
                'ce_bid': getattr(x.call_options.market_data, 'bid_price', 0),
                'ce_ask': getattr(x.call_options.market_data, 'ask_price', 0),
                'pe_bid': getattr(x.put_options.market_data, 'bid_price', 0),
                'pe_ask': getattr(x.put_options.market_data, 'ask_price', 0),
                'ce_key': x.call_options.instrument_key,
                'pe_key': x.put_options.instrument_key
            } for x in data])
        except Exception as e:
            logger.error(f"Option chain fetch error: {e}")
            return pd.DataFrame()

    def _fetch_participant_data(self):
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)
        dates = []
        candidate = now
        if candidate.hour < 18:
            candidate -= timedelta(days=1)
        while len(dates) < 2:
            if candidate.weekday() < 5:
                dates.append(candidate)
            candidate -= timedelta(days=1)
        today, yest = dates[0], dates[1]

        def fetch_oi_csv(date_obj):
            date_str = date_obj.strftime('%d%m%Y')
            url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    content = r.content.decode('utf-8')
                    lines = content.splitlines()
                    for idx, line in enumerate(lines[:20]):
                        if "Future Index Long" in line:
                            df = pd.read_csv(io.StringIO(content), skiprows=idx)
                            df.columns = df.columns.str.strip()
                            return df
            except:
                pass
            return None

        df_today = fetch_oi_csv(today)
        df_yest = fetch_oi_csv(yest)
        if df_today is None:
            return None, None, 0.0, today.strftime('%d-%b-%Y')
        today_data = self._process_participant_data(df_today)
        yest_data = self._process_participant_data(df_yest) if df_yest is not None else {}
        fii_net_change = 0.0
        if today_data.get('FII') and yest_data.get('FII'):
            fii_net_change = today_data['FII'].fut_net - yest_data['FII'].fut_net
        return today_data, yest_data, fii_net_change, today.strftime('%d-%b-%Y')

    def _process_participant_data(self, df) -> Dict[str, ParticipantData]:
        data = {}
        for p in ["FII", "DII", "Client", "Pro"]:
            try:
                row = df[df['Client Type'].astype(str).str.contains(p, case=False, na=False)].iloc[0]
                data[p] = ParticipantData(
                    fut_long=float(row['Future Index Long']),
                    fut_short=float(row['Future Index Short']),
                    fut_net=float(row['Future Index Long']) - float(row['Future Index Short']),
                    call_long=float(row['Option Index Call Long']),
                    call_short=float(row['Option Index Call Short']),
                    call_net=float(row['Option Index Call Long']) - float(row['Option Index Call Short']),
                    put_long=float(row['Option Index Put Long']),
                    put_short=float(row['Option Index Put Short']),
                    put_net=float(row['Option Index Put Long']) - float(row['Option Index Put Short']),
                    stock_net=float(row['Future Stock Long']) - float(row['Future Stock Short'])
                )
            except:
                data[p] = None
        return data

    def get_time_metrics(self, weekly, monthly, next_weekly) -> TimeMetrics:
        today = date.today()
        dte_w = (weekly - today).days if weekly else 0
        dte_m = (monthly - today).days if monthly else 0
        dte_nw = (next_weekly - today).days if next_weekly else 0
        
        now = datetime.now(ProductionConfig.IST)
        is_expiry_day_weekly = (dte_w == 0)
        is_expiry_day_monthly = (dte_m == 0)
        is_past_square_off_time = now.time() >= ProductionConfig.SQUARE_OFF_TIME_IST
        
        return TimeMetrics(
            current_date=today,
            weekly_exp=weekly,
            monthly_exp=monthly,
            next_weekly_exp=next_weekly,
            dte_weekly=dte_w,
            dte_monthly=dte_m,
            dte_next_weekly=dte_nw,
            is_expiry_day_weekly=is_expiry_day_weekly,
            is_expiry_day_monthly=is_expiry_day_monthly,
            is_past_square_off_time=is_past_square_off_time,
            is_gamma_week=dte_w <= ProductionConfig.EXIT_DTE + 1,
            is_gamma_month=dte_m <= ProductionConfig.EXIT_DTE + 1,
            days_to_next_weekly=dte_nw
        )

    def get_vol_metrics(self, nifty_hist, vix_hist, live_prices) -> VolMetrics:
        is_fallback = False
        nifty_live = vix_live = spot = 0
        if hasattr(live_prices, 'data'):
            data = live_prices.data
            if ProductionConfig.NIFTY_KEY in data:
                nifty_live = data[ProductionConfig.NIFTY_KEY].last_price
            if ProductionConfig.VIX_KEY in data:
                vix_live = data[ProductionConfig.VIX_KEY].last_price
                spot = nifty_live if nifty_live > 0 else (nifty_hist.iloc[-1]['close'] if not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if not vix_hist.empty else 0)
        if nifty_live <= 0 or vix_live <= 0:
            is_fallback = True
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 7 else 0
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 28 else 0
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 90 else 0

        def fit_garch(horizon):
            try:
                if len(returns) < 100:
                    return 0
                from arch import arch_model
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
                result = model.fit(disp='off', show_warning=False)
                forecast = result.forecast(horizon=horizon, reindex=False)
                return np.sqrt(forecast.variance.values[-1, -1]) * np.sqrt(252)
            except:
                return 0

        garch7 = fit_garch(7) or rv7
        garch28 = fit_garch(28) or rv28
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 7 else 0
        park28 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 28 else 0
        vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
        vov = vix_returns.rolling(30).std().iloc[-1] * np.sqrt(252) * 100 if len(vix_returns) >= 30 else 0
        vov_rolling = vix_returns.rolling(30).std() * np.sqrt(252) * 100 if len(vix_returns) >= 30 else pd.Series()
        vov_mean = vov_rolling.rolling(60).mean().iloc[-1] if len(vov_rolling) >= 60 else 0
        vov_std = vov_rolling.rolling(60).std().iloc[-1] if len(vix_returns) >= 60 else 0
        vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0

        def calc_ivp(window):
            if len(vix_hist) < window:
                return 0.0
            history = vix_hist['close'].tail(window)
            return (history < vix).mean() * 100

        ivp_30d, ivp_90d, ivp_1yr = calc_ivp(30), calc_ivp(90), calc_ivp(252)
        ma20 = nifty_hist['close'].rolling(20).mean().iloc[-1] if len(nifty_hist) >= 20 else 0
        true_range = pd.concat([
            nifty_hist['high'] - nifty_hist['low'],
            (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs(),
            (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()
        ], axis=1).max(axis=1)
        atr14 = true_range.rolling(14).mean().iloc[-1] if len(true_range) >= 14 else 0
        trend_strength = abs(spot - ma20) / atr14 if atr14 > 0 else 0
        
        vix_5d_ago = vix_hist['close'].iloc[-6] if len(vix_hist) >= 6 else vix
        vix_change_5d = vix - vix_5d_ago
        
        if vix_change_5d > ProductionConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "EXPLOSIVE_UP"
        elif vix_change_5d < -ProductionConfig.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "COLLAPSING"
        elif vix_change_5d > 2.0:
            vix_momentum = "RISING"
        elif vix_change_5d < -2.0:
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
            
        vol_regime = "EXPLODING" if vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE else \
                    "RICH" if ivp_1yr > ProductionConfig.HIGH_VOL_IVP else \
                    "CHEAP" if ivp_1yr < ProductionConfig.LOW_VOL_IVP else "FAIR"
        return VolMetrics(
            spot, vix, rv7, rv28, rv90, garch7, garch28,
            park7, park28, vov, vov_zscore,
            ivp_30d, ivp_90d, ivp_1yr,
            ma20, atr14, trend_strength, vol_regime, is_fallback,
            vix_change_5d, vix_momentum
        )

    def get_struct_metrics(self, chain, spot, lot_size) -> StructMetrics:
        if chain.empty or spot == 0:
            return StructMetrics(0, 0, 0, "NEUTRAL", 0, 0, 0, "NEUTRAL", lot_size, 1.0, "UNKNOWN", 5.0)
        subset = chain[(chain['strike'] > spot * 0.90) & (chain['strike'] < spot * 1.10)]
        net_gex = ((subset['ce_gamma'] * subset['ce_oi']).sum() - (subset['pe_gamma'] * subset['pe_oi']).sum()) * spot * lot_size
        total_oi_value = (chain['ce_oi'].sum() + chain['pe_oi'].sum()) * spot * lot_size
        gex_ratio = abs(net_gex) / total_oi_value if total_oi_value > 0 else 0
        gex_regime = "STICKY" if gex_ratio > ProductionConfig.GEX_STICKY_RATIO else \
                    "SLIPPERY" if gex_ratio < ProductionConfig.GEX_STICKY_RATIO * 0.5 else "NEUTRAL"
        pcr = chain['pe_oi'].sum() / chain['ce_oi'].sum() if chain['ce_oi'].sum() > 0 else 1.0
        
        atm_range = chain[(chain['strike'] >= spot * 0.98) & (chain['strike'] <= spot * 1.02)]
        pcr_atm = atm_range['pe_oi'].sum() / atm_range['ce_oi'].sum() if not atm_range.empty and atm_range['ce_oi'].sum() > 0 else pcr
        
        strikes = chain['strike'].values
        losses = [
            np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values) + \
            np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values)
            for s in strikes
        ]
        max_pain = strikes[np.argmin(losses)] if losses else 0
        try:
            ce_25d_idx = (chain['ce_delta'].abs() - 0.25).abs().argsort()[:1]
            pe_25d_idx = (chain['pe_delta'].abs() - 0.25).argsort()[:1]
            skew_25d = chain.iloc[pe_25d_idx]['pe_iv'].values[0] - chain.iloc[ce_25d_idx]['ce_iv'].values[0]
            
            if skew_25d > ProductionConfig.SKEW_CRASH_FEAR:
                skew_regime = "CRASH_FEAR"
            elif skew_25d < ProductionConfig.SKEW_MELT_UP:
                skew_regime = "MELT_UP"
            else:
                skew_regime = "BALANCED"
        except:
            skew_25d = 0
            skew_regime = "UNKNOWN"
            
        oi_regime = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL"
        
        if gex_regime == "STICKY":
            gex_weighted = 8.0
        elif gex_regime == "SLIPPERY":
            gex_weighted = 3.0
        else:
            gex_weighted = 5.0
        
        return StructMetrics(
            net_gex, gex_ratio, total_oi_value, gex_regime,
            pcr, max_pain, skew_25d, oi_regime, lot_size,
            pcr_atm, skew_regime, gex_weighted
        )

    def get_edge_metrics(self, weekly_chain, monthly_chain, next_weekly_chain, spot, vol: VolMetrics, time: TimeMetrics) -> EdgeMetrics:
        def get_atm_iv(chain):
            if chain.empty or spot == 0:
                return 0
            atm_idx = (chain['strike'] - spot).abs().argsort()[:1]
            row = chain.iloc[atm_idx].iloc[0]
            return (row['ce_iv'] + row['pe_iv']) / 2
        iv_weekly = get_atm_iv(weekly_chain)
        iv_monthly = get_atm_iv(monthly_chain)
        iv_next_weekly = get_atm_iv(next_weekly_chain)
        
        vrp_rv_weekly = iv_weekly - vol.rv7
        vrp_garch_weekly = iv_weekly - vol.garch7
        vrp_park_weekly = iv_weekly - vol.park7
        weighted_vrp_weekly = (vrp_garch_weekly * 0.70) + (vrp_park_weekly * 0.15) + (vrp_rv_weekly * 0.15)
        
        vrp_rv_monthly = iv_monthly - vol.rv28
        vrp_garch_monthly = iv_monthly - vol.garch28
        vrp_park_monthly = iv_monthly - vol.garch28
        weighted_vrp_monthly = (vrp_garch_monthly * 0.70) + (vrp_park_monthly * 0.15) + (vrp_rv_monthly * 0.15)
        
        vrp_rv_next_weekly = iv_next_weekly - vol.rv7
        vrp_garch_next_weekly = iv_next_weekly - vol.garch7
        vrp_park_next_weekly = iv_next_weekly - vol.park7
        weighted_vrp_next_weekly = (vrp_garch_next_weekly * 0.70) + (vrp_park_next_weekly * 0.15) + (vrp_rv_next_weekly * 0.15)
        
        if time.is_expiry_day_weekly:
            short_term_iv = iv_next_weekly
            term_spread = iv_monthly - short_term_iv
        else:
            short_term_iv = iv_weekly
            term_spread = iv_monthly - short_term_iv
            
        term_regime = "BACKWARDATION" if term_spread < -1.0 else "CONTANGO" if term_spread > 1.0 else "FLAT"
        primary_edge = "LONG_VOL" if vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP else \
                      "SHORT_GAMMA" if weighted_vrp_weekly > 4.0 and vol.ivp_1yr > 50 else \
                      "SHORT_VEGA" if weighted_vrp_monthly > 3.0 and vol.ivp_1yr > 50 else \
                      "CALENDAR_SPREAD" if term_regime == "BACKWARDATION" and term_spread < -2.0 else \
                      "MEAN_REVERSION" if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP else "NONE"
        return EdgeMetrics(
            iv_weekly, vrp_rv_weekly, vrp_garch_weekly, vrp_park_weekly,
            iv_monthly, vrp_rv_monthly, vrp_garch_monthly, vrp_park_monthly,
            iv_next_weekly, vrp_rv_next_weekly, vrp_garch_next_weekly, vrp_park_next_weekly,
            term_spread, term_regime, primary_edge,
            weighted_vrp_weekly, weighted_vrp_monthly, weighted_vrp_next_weekly
        )

    def get_external_metrics(self, nifty_hist, participant_data, participant_yest, fii_net_change, data_date, economic_events) -> ExternalMetrics:
        fast_vol = False
        if not nifty_hist.empty:
            last_bar = nifty_hist.iloc[-1]
            daily_range_pct = ((last_bar['high'] - last_bar['low']) / last_bar['open']) * 100
            fast_vol = daily_range_pct > 1.8
        
        calendar_engine = CalendarEngine()
        veto_events, high_impact, square_off_needed, square_off_time = calendar_engine.calculate_event_impact(economic_events)
        
        has_veto, veto_name, veto_square_off, hours_until = calendar_engine.analyze_veto_risk(economic_events, datetime.now(ProductionConfig.IST))
        
        flow_regime = "NEUTRAL"
        fii_direction = "NEUTRAL"
        if participant_data and participant_data.get('FII'):
            fii_net = participant_data['FII'].fut_net
            if fii_net > ProductionConfig.FII_STRONG_LONG:
                flow_regime = "STRONG_LONG"
                fii_direction = "BULLISH"
            elif fii_net < ProductionConfig.FII_STRONG_SHORT:
                flow_regime = "STRONG_SHORT"
                fii_direction = "BEARISH"
            elif abs(fii_net) > ProductionConfig.FII_MODERATE:
                flow_regime = "MODERATE_LONG" if fii_net > 0 else "MODERATE_SHORT"
                fii_direction = "MILDLY_BULLISH" if fii_net > 0 else "MILDLY_BEARISH"
        
        event_risk = "LOW"
        if has_veto and hours_until and hours_until <= 24:
            event_risk = "CRITICAL"
        elif has_veto and hours_until and hours_until <= 48:
            event_risk = "HIGH"
        elif len(high_impact) > 0 and high_impact[0].hours_until <= 48:
            event_risk = "MEDIUM"
        
        return ExternalMetrics(
            fii=participant_data.get('FII') if participant_data else None,
            dii=participant_data.get('DII') if participant_data else None,
            pro=participant_data.get('Pro') if participant_data else None,
            client=participant_data.get('Client') if participant_data else None,
            fii_net_change=fii_net_change,
            flow_regime=flow_regime,
            flow_score=0.0,
            option_bias=0.0,
            data_date=data_date,
            is_fallback_data=False,
            fast_vol=fast_vol,
            fii_conviction="NEUTRAL",
            fii_direction=fii_direction,
            economic_events=economic_events,
            veto_events=veto_events,
            high_impact_events=high_impact,
            veto_square_off_needed=square_off_needed,
            veto_square_off_time=square_off_time,
            veto_hours_until=hours_until,
            event_risk=event_risk,
            has_veto_event=has_veto,
            veto_event_name=veto_name,
            upcoming_high_impact=high_impact
        )

# Regime Engine
class RegimeEngineV33:
    def calculate_dynamic_weights(self, vol: VolMetrics, external: ExternalMetrics, 
                                  dte: int) -> DynamicWeights:
        base_vol = 0.40
        base_struct = 0.30
        base_edge = 0.30
        
        rationale = "Base: 40% Vol, 30% Struct, 30% Edge"
        
        if vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE or vol.vix_momentum == "RISING":
            base_vol += 0.10
            base_struct -= 0.05
            base_edge -= 0.05
            rationale = "High Vol Environment: Vol up to 50%, Struct/Edge down"
        elif vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP:
            base_edge += 0.10
            base_vol -= 0.05
            base_struct -= 0.05
            rationale = "Low Vol: Edge up to 40% (limited premium)"
        
        if dte <= ProductionConfig.EXIT_DTE:
            base_vol = 1.0
            base_struct = 0.0
            base_edge = 0.0
            rationale = "EXIT ZONE: Score suppressed (should not trade)"
        elif dte <= 2:
            base_vol += 0.10
            base_struct -= 0.05
            base_edge -= 0.05
            rationale += " | Gamma week: Vol up (risk dominates)"
        
        if external.veto_square_off_needed:
            base_vol = 0.60
            base_struct = 0.20
            base_edge = 0.20
            rationale = "VETO EVENT: Vol risk dominates (60/20/20)"
        
        total = base_vol + base_struct + base_edge
        vol_weight = base_vol / total
        struct_weight = base_struct / total
        edge_weight = base_edge / total
        
        return DynamicWeights(vol_weight, struct_weight, edge_weight, rationale)
    
    def calculate_scores(self, vol: VolMetrics, struct: StructMetrics, 
                        edge: EdgeMetrics, external: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        score_drivers = []
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge.weighted_vrp_monthly
        
        edge_score = 5.0
        
        if weighted_vrp > 4.0:
            edge_score += 3.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Excellent) +3.0")
        elif weighted_vrp > 2.0:
            edge_score += 2.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Good) +2.0")
        elif weighted_vrp > 1.0:
            edge_score += 1.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Moderate) +1.0")
        elif weighted_vrp < 0:
            edge_score -= 3.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Negative) -3.0")
        else:
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Neutral)")
        
        if edge.term_regime == "BACKWARDATION" and edge.term_spread < -2.0:
            edge_score += 1.0
            score_drivers.append(f"Edge: Steep Backwardation ({edge.term_spread:.1f}%) +1.0")
        elif edge.term_regime == "CONTANGO":
            edge_score += 0.5
            score_drivers.append("Edge: Contango +0.5")
        
        edge_score = max(0, min(10, edge_score))
        
        vol_score = 5.0
        
        if vol.vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE:
            vol_score = 0.0
            score_drivers.append(f"Vol: VOV Crash ({vol.vov_zscore:.1f}s) -> ZERO")
        elif vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE:
            vol_score -= 3.0
            score_drivers.append(f"Vol: High VOV ({vol.vov_zscore:.1f}s) -3.0")
        elif vol.vov_zscore < 1.5:
            vol_score += 1.5
            score_drivers.append(f"Vol: Stable VOV ({vol.vov_zscore:.1f}s) +1.5")
        
        if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP:
            if vol.vix_momentum == "FALLING":
                vol_score += 1.5
                score_drivers.append(f"Vol: Rich IVP ({vol.ivp_1yr:.0f}%) + Falling VIX +1.5")
            elif vol.vix_momentum == "RISING":
                vol_score -= 1.0
                score_drivers.append(f"Vol: Rich IVP + Rising VIX -1.0")
            else:
                vol_score += 0.5
                score_drivers.append(f"Vol: Rich IVP ({vol.ivp_1yr:.0f}%) +0.5")
        elif vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP:
            vol_score -= 2.5
            score_drivers.append(f"Vol: Cheap IVP ({vol.ivp_1yr:.0f}%) -2.5")
        else:
            vol_score += 1.0
            score_drivers.append(f"Vol: Fair IVP ({vol.ivp_1yr:.0f}%) +1.0")
        
        if vol.vix_momentum == "EXPLOSIVE_UP":
            vol_score -= 2.0
            score_drivers.append(f"Vol: VIX explosive ({vol.vix_change_5d:+.1f}) -2.0")
        elif vol.vix_momentum == "COLLAPSING":
            vol_score += 1.0
            score_drivers.append(f"Vol: VIX collapsing ({vol.vix_change_5d:+.1f}) +1.0")
        
        vol_score = max(0, min(10, vol_score))
        
        struct_score = 5.0
        
        if struct.gex_regime == "STICKY":
            struct_score += 2.5
            score_drivers.append(f"Struct: Sticky GEX ({struct.gex_ratio:.3%}) +2.5")
        elif struct.gex_regime == "SLIPPERY":
            struct_score -= 1.0
            score_drivers.append("Struct: Slippery GEX -1.0")
        
        if 0.9 < struct.pcr_atm < 1.1:
            struct_score += 1.5
            score_drivers.append(f"Struct: Balanced PCR ATM ({struct.pcr_atm:.2f}) +1.5")
        elif struct.pcr_atm > 1.3 or struct.pcr_atm < 0.7:
            struct_score -= 0.5
            score_drivers.append(f"Struct: Extreme PCR ATM ({struct.pcr_atm:.2f}) -0.5")
        
        if struct.skew_regime == "CRASH_FEAR":
            struct_score -= 1.0
            score_drivers.append(f"Struct: Crash Fear Skew ({struct.skew_25d:+.1f}%) -1.0")
        elif struct.skew_regime == "MELT_UP":
            struct_score -= 0.5
            score_drivers.append("Struct: Melt-Up Skew -0.5")
        else:
            struct_score += 0.5
            score_drivers.append("Struct: Balanced Skew +0.5")
        
        struct_score = max(0, min(10, struct_score))
        
        weights = self.calculate_dynamic_weights(vol, external, dte)
        
        composite = (
            vol_score * weights.vol_weight +
            struct_score * weights.struct_weight +
            edge_score * weights.edge_weight
        )
        
        alt_weights = [(0.30, 0.35, 0.35), (0.50, 0.25, 0.25), (0.35, 0.30, 0.35)]
        alt_scores = [
            vol_score * wv + struct_score * ws + edge_score * we 
            for wv, ws, we in alt_weights
        ]
        score_stability = 1.0 - (np.std(alt_scores) / np.mean(alt_scores)) if np.mean(alt_scores) > 0 else 0.5
        
        confidence = "VERY_HIGH" if composite >= 8.0 and score_stability > 0.85 else \
                    "HIGH" if composite >= 6.5 and score_stability > 0.75 else \
                    "MODERATE" if composite >= 4.0 else "LOW"
        
        score_drivers.append(
            f"Composite: {composite:.2f}/10 "
            f"[V:{vol_score:.1f}*{weights.vol_weight:.0%} "
            f"S:{struct_score:.1f}*{weights.struct_weight:.0%} "
            f"E:{edge_score:.1f}*{weights.edge_weight:.0%}]"
        )
        
        return RegimeScore(
            vol_score, struct_score, edge_score, composite, confidence,
            score_stability, weights, score_drivers
        )
    
    def generate_mandate(self, score: RegimeScore, vol: VolMetrics, 
                        struct: StructMetrics, edge: EdgeMetrics,
                        external: ExternalMetrics, time: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        rationale = []
        warnings = []
        veto_reasons = []
        is_trade_allowed = True
        square_off_instruction = None
        data_relevance = "CURRENT"
        directional_bias = "NEUTRAL"
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge.weighted_vrp_monthly
        
        if dte <= ProductionConfig.EXIT_DTE:
            return TradingMandate(
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                dte=dte,
                regime_name="EXIT_ZONE",
                strategy_type="CASH",
                allocation_pct=0.0,
                deployment_amount=0.0,
                score=score,
                rationale=[f"DTE {dte} - Exit zone active"],
                warnings=[f"DTE {dte} - Exit zone, no new trades"],
                veto_reasons=[f"DTE {dte} - Exit zone, no new trades"],
                is_trade_allowed=False,
                square_off_instruction="Exit all positions at 2 PM",
                data_relevance="EXIT_ZONE"
            )
        
        if external.veto_square_off_needed:
            return TradingMandate(
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                dte=dte,
                regime_name="VETO_EVENT",
                strategy_type="CASH",
                allocation_pct=0.0,
                deployment_amount=0.0,
                score=score,
                rationale=[f"VETO EVENT: {external.veto_event_name}"],
                warnings=[f"RBI/Fed event in {external.veto_hours_until:.1f}h - ALL TRADES BLOCKED"],
                veto_reasons=[f"Veto: {external.veto_event_name}"],
                suggested_structure="NONE",
                directional_bias="NEUTRAL",
                wing_protection="NONE",
                is_trade_allowed=False,
                square_off_instruction=f"Square off all positions before {external.veto_event_name}",
                data_relevance="VETO_ACTIVE"
            )
        
        if time.is_expiry_day_weekly and expiry_type == "WEEKLY":
            is_trade_allowed = False
            veto_reasons.append("Expiry day - absolute no-trade zone")
            warnings.append("EXPIRY DAY - All trading blocked")
            data_relevance = "EXPIRY_DAY_BLOCKED"
        elif time.is_expiry_day_monthly and expiry_type == "MONTHLY":
            is_trade_allowed = False
            veto_reasons.append("Expiry day - absolute no-trade zone")
            warnings.append("EXPIRY DAY - All trading blocked")
            data_relevance = "EXPIRY_DAY_BLOCKED"
        
        if time.is_past_square_off_time:
            warnings.append("Past 2 PM - No new trades")
            is_trade_allowed = False
            veto_reasons.append("Past square-off time (2:00 PM IST)")
        
        if struct.pcr_atm > 1.3:
            directional_bias = "BULLISH"
        elif struct.pcr_atm < 0.7:
            directional_bias = "BEARISH"
        elif external.fii_direction == "BULLISH":
            directional_bias = "MILDLY_BULLISH"
        elif external.fii_direction == "BEARISH":
            directional_bias = "MILDLY_BEARISH"
        
        if score.composite >= 7.5 and score.confidence in ["HIGH", "VERY_HIGH"]:
            
            if dte > 2:
                regime_name = "PREMIUM_HARVEST"
                strategy = "AGGRESSIVE_SHORT"
                suggested = "IRON_CONDOR"
                wing_protection = "Standard (100-200 pts)"
                base_allocation = 60.0
                rationale.append(f"Very High Confidence ({score.confidence}): VRP {weighted_vrp:.2f}%")
                rationale.append(f"Dynamic Weights: {score.weights_used.rationale}")
                
            else:
                regime_name = "GAMMA_WEEK_CAUTION"
                strategy = "MODERATE_SHORT"
                suggested = "IRON_FLY"
                wing_protection = "Wide (150-200 pts)"
                base_allocation = 25.0
                rationale.append(f"Gamma week - reduced size with wider protection")
                rationale.append(f"High VRP ({weighted_vrp:.2f}%) but near expiry")
                warnings.append("GAMMA WEEK - Will exit at DTE=1")
                warnings.append(f"Position will auto-close {dte-1} day(s) from now at 2 PM")
        
        elif score.composite >= 6.0 and score.confidence in ["HIGH", "VERY_HIGH"]:
            
            if dte > 1:
                regime_name = "MODERATE_HARVEST"
                strategy = "MODERATE_SHORT"
                suggested = "IRON_CONDOR"
                wing_protection = "Standard (100-150 pts)"
                base_allocation = 40.0
                rationale.append(f"Moderate Confidence: VRP {weighted_vrp:.2f}%")
                rationale.append(f"Weights: {score.weights_used.rationale}")
                
            else:
                regime_name = "EXIT_ZONE_FAILSAFE"
                strategy = "CASH"
                suggested = "NONE"
                wing_protection = "N/A"
                base_allocation = 0.0
                is_trade_allowed = False
                veto_reasons.append(f"DTE {dte} - Exit zone failsafe")
                rationale.append("FAILSAFE: Should have been caught by early veto")
                warnings.append(f"CRITICAL: DTE {dte} - No trading allowed")
        
        elif score.composite >= 4.0:
            
            if dte <= ProductionConfig.EXIT_DTE:
                regime_name = "EXIT_ZONE"
                strategy = "CASH"
                suggested = "NONE"
                wing_protection = "N/A"
                base_allocation = 0.0
                is_trade_allowed = False
                veto_reasons.append(f"DTE {dte} - Exit zone")
                rationale.append("Exit zone active")
                warnings.append(f"DTE {dte} - No trading")
            else:
                if struct.pcr > 1.3 and vol.trend_strength > 0.5:
                    directional_bias = "BULLISH"
                    suggested = "BULL_PUT_SPREAD"
                elif struct.pcr < 0.7 and vol.trend_strength > 0.5:
                    directional_bias = "BEARISH"
                    suggested = "BEAR_CALL_SPREAD"
                else:
                    directional_bias = "NEUTRAL"
                    suggested = "CREDIT_SPREAD"
                
                regime_name = "DEFENSIVE"
                strategy = "DEFENSIVE"
                base_allocation = 20.0
                wing_protection = "Wide (150-250 pts)"
                rationale.append("Defensive Posture - lower conviction")
                rationale.append(f"Directional bias: {directional_bias}")
                warnings.append("LOWER CONVICTION - Reduce size")
        
        else:
            regime_name = "CASH"
            strategy = "CASH"
            suggested = "NONE"
            wing_protection = "N/A"
            base_allocation = 0.0
            is_trade_allowed = False
            rationale.append("Regime Unfavorable: Cash is a position")
            rationale.append(f"Composite score too low: {score.composite:.2f}/10")
            veto_reasons.append("Low composite score")
        
        allocation = base_allocation
        
        if vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE:
            warnings.append(f"HIGH VOL-OF-VOL ({vol.vov_zscore:.2f}s) - Size reduced 30%")
            allocation *= 0.7
        
        if vol.vix_momentum == "EXPLOSIVE_UP":
            warnings.append(f"VIX EXPLOSIVE ({vol.vix:.1f}) - Size reduced 40%")
            allocation *= 0.6
        
        if score.score_stability < 0.75:
            warnings.append(f"LOW SCORE STABILITY ({score.score_stability:.2f}) - Size reduced 20%")
            allocation *= 0.8
        
        if external.high_impact_events:
            high_impact_count = len(external.high_impact_events)
            warnings.append(f"{high_impact_count} HIGH IMPACT EVENT(S) THIS WEEK")
            allocation *= 0.85
        
        try:
            daily_regime = db_writer.get_state("daily_risk_regime")
            if daily_regime:
                ai_data = json.loads(daily_regime)
                
                if ai_data.get("date") == str(date.today()):
                    risk_score = ai_data.get("risk_score", 5)
                    
                    if risk_score >= 8:
                        strategy = "DEFENSIVE"
                        suggested = "CASH_ONLY"
                        allocation = 0.0
                        is_trade_allowed = False
                        warnings.append(f"AI BRAKE: Extreme Risk ({risk_score}/10) - HALTED")
                        rationale.append(f"AI Reason: {ai_data.get('reason', 'High Risk')}")
                        veto_reasons.append("AI Morning Brief: Extreme market risk detected")
                    
                    elif risk_score >= 6:
                        allocation = allocation * 0.5
                        warnings.append(f"AI CAUTION: Risk ({risk_score}/10) - Sizing Halved")
                        rationale.append("AI Brief: Elevated risk - reduced sizing")
        except Exception as e:
            logger.error(f"AI Override Failed: {e}")
        
        allocation = max(0, min(100, allocation))
        
        if dte <= ProductionConfig.EXIT_DTE:
            if allocation > 0:
                logger.critical(f"DEFENSIVE FAILSAFE: Allocation {allocation:.1f}% forced to zero (DTE={dte})")
                telegram.send(f"Defensive Allocation Override\nDTE: {dte}\nOriginal allocation: {allocation:.1f}%\nForced to: 0%", "CRITICAL")
            allocation = 0.0
            deployment_amount = 0.0
        else:
            deployment_amount = ProductionConfig.BASE_CAPITAL * (allocation / 100.0)
        
        if deployment_amount > ProductionConfig.MAX_CAPITAL_PER_TRADE:
            deployment_amount = ProductionConfig.MAX_CAPITAL_PER_TRADE
            warnings.append(f"Capital capped at ₹{ProductionConfig.MAX_CAPITAL_PER_TRADE:,.0f}")
        
        if strategy == "AGGRESSIVE_SHORT":
            risk_per_lot = ProductionConfig.MARGIN_SELL_BASE
        elif strategy == "MODERATE_SHORT":
            risk_per_lot = ProductionConfig.MARGIN_SELL_BASE * 0.8
        elif strategy == "DEFENSIVE":
            risk_per_lot = ProductionConfig.MARGIN_SELL_BASE * 0.6
        else:
            risk_per_lot = 0
            
        max_lots = int(deployment_amount / risk_per_lot) if risk_per_lot > 0 else 0
        
        square_off_instruction = None
        if external.has_veto_event and external.veto_hours_until:
            if external.veto_hours_until <= 48:
                square_off_instruction = (
                    f"Square off by {external.veto_hours_until:.1f}h before "
                    f"{external.veto_event_name}"
                )
        
        return TradingMandate(
            expiry_type=expiry_type,
            expiry_date=expiry_date,
            dte=dte,
            regime_name=regime_name,
            strategy_type=strategy,
            allocation_pct=allocation,
            deployment_amount=deployment_amount,
            score=score,
            rationale=rationale,
            warnings=warnings,
            veto_reasons=veto_reasons,
            suggested_structure=suggested,
            directional_bias=directional_bias,
            wing_protection=wing_protection,
            is_trade_allowed=is_trade_allowed,
            data_relevance=data_relevance,
            square_off_instruction=square_off_instruction,
            max_lots=max_lots,
            risk_per_lot=risk_per_lot
        )

# Strategy Factory
class StrategyFactory:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client

    def _discover_strike_interval(self, df: pd.DataFrame) -> int:
        if df.empty or len(df) < 2:
            return ProductionConfig.DEFAULT_STRIKE_INTERVAL
        strikes = sorted(df['strike'].unique())
        diffs = np.diff(strikes)
        valid_diffs = diffs[diffs > 0]
        if len(valid_diffs) == 0:
            return ProductionConfig.DEFAULT_STRIKE_INTERVAL
        try:
            return int(pd.Series(valid_diffs).mode().iloc[0])
        except:
            return ProductionConfig.DEFAULT_STRIKE_INTERVAL

    def _find_professional_atm(self, df: pd.DataFrame, spot: float) -> Optional[Dict]:
        interval = self._discover_strike_interval(df)
        closest = int(spot / interval + 0.5) * interval
        candidates = [closest, closest + interval, closest - interval]
        best_strike, min_skew, best_cost = None, float('inf'), 0.0
        for strike in candidates:
            ce = df[(df['strike'] == strike) & (df['ce_oi'] > ProductionConfig.MIN_STRIKE_OI)]
            pe = df[(df['strike'] == strike) & (df['pe_oi'] > ProductionConfig.MIN_STRIKE_OI)]
            if ce.empty or pe.empty: continue
            ce_ltp, pe_ltp = ce.iloc[0]['ce_ltp'], pe.iloc[0]['pe_ltp']
            if ce_ltp <= 0.1 or pe_ltp <= 0.1: continue
            skew = abs(ce_ltp - pe_ltp)
            if skew < min_skew:
                min_skew, best_strike, best_cost = skew, strike, ce_ltp + pe_ltp
        if not best_strike:
            logger.warning(f"Using Geometric ATM {closest} (Liquidity Low)")
            return {'strike': closest, 'straddle_cost': 0.0, 'interval': interval}
        logger.info(f"Pro ATM: {best_strike} (Skew: ₹{min_skew:.1f}) | Interval: {interval}")
        return {'strike': best_strike, 'straddle_cost': best_cost, 'interval': interval}

    def _calculate_pro_wing_width(self, straddle_cost: float, vol_metrics: VolMetrics, interval: int) -> int:
        if vol_metrics.ivp_1yr > ProductionConfig.IVP_THRESHOLD_EXTREME:
            factor = ProductionConfig.WING_FACTOR_EXTREME_VOL
        elif vol_metrics.ivp_1yr > ProductionConfig.IVP_THRESHOLD_HIGH:
            factor = ProductionConfig.WING_FACTOR_HIGH_VOL
        elif vol_metrics.ivp_1yr < ProductionConfig.IVP_THRESHOLD_LOW:
            factor = ProductionConfig.WING_FACTOR_LOW_VOL
        else:
            factor = ProductionConfig.WING_FACTOR_STANDARD
        target = straddle_cost * factor
        rounded = int(target / interval + 0.5) * interval
        min_width = interval * ProductionConfig.MIN_WING_INTERVAL_MULTIPLIER
        final = max(min_width, rounded)
        logger.info(f"Wing Width: Target {target:.1f} -> Rounded {final} (Factor {factor})")
        return final

    def _get_leg_details(self, df: pd.DataFrame, strike: float, type_: str) -> Optional[Dict]:
        rows = df[(df['strike'] - strike).abs() < 0.1]
        if rows.empty: return None
        row = rows.iloc[0]
        pref = type_.lower()
        ltp = row[f'{pref}_ltp']
        if ltp <= 0: return None
        return {
            'key': row[f'{pref}_key'],
            'strike': row['strike'],
            'ltp': ltp,
            'delta': row[f'{pref}_delta'],
            'type': type_,
            'bid': row[f'{pref}_bid'],
            'ask': row[f'{pref}_ask']
        }

    def _find_leg_by_delta(self, df: pd.DataFrame, type_: str, target_delta: float) -> Optional[Dict]:
        target, col_delta = abs(target_delta), f"{type_.lower()}_delta"
        df = df.copy()
        df = df[(df[f'{type_.lower()}_oi'] > ProductionConfig.MIN_STRIKE_OI) & (df[f'{type_.lower()}_ltp'] > 0.5)]
        df['delta_diff'] = (df[col_delta].abs() - target).abs()
        for _, row in df.sort_values('delta_diff').head(3).iterrows():
            bid, ask, ltp = row[f'{type_.lower()}_bid'], row[f'{type_.lower()}_ask'], row[f'{type_.lower()}_ltp']
            if ltp <= 0 or ask <= 0: continue
            if (ask - bid) / ltp > ProductionConfig.MAX_BID_ASK_SPREAD: continue
            return self._get_leg_details(df, row['strike'], type_)
        return None

    def _calculate_defined_risk(self, legs: List[Dict], qty: int) -> float:
        if not legs: return 0.0
        
        premiums = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
        debits   = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'BUY')
        net_credit = premiums - debits
        
        ce_legs = sorted([l for l in legs if l['type'] == 'CE'], key=lambda x: x['strike'])
        pe_legs = sorted([l for l in legs if l['type'] == 'PE'], key=lambda x: x['strike'])
        
        call_risk = 0.0
        put_risk = 0.0
        
        if len(ce_legs) >= 2:
            shorts = [l for l in ce_legs if l['side'] == 'SELL']
            longs = [l for l in ce_legs if l['side'] == 'BUY']
            if shorts and longs:
                width = longs[-1]['strike'] - shorts[0]['strike']
                call_risk = width * qty

        if len(pe_legs) >= 2:
            shorts = [l for l in pe_legs if l['side'] == 'SELL']
            longs = [l for l in pe_legs if l['side'] == 'BUY']
            if shorts and longs:
                width = shorts[-1]['strike'] - longs[0]['strike']
                put_risk = width * qty
        
        max_structural_risk = max(call_risk, put_risk)
        max_loss = max(0, max_structural_risk - net_credit)
        
        logger.info(f"Risk Calc: CallRisk={call_risk:.0f}, PutRisk={put_risk:.0f}, Credit={net_credit:.0f} -> MaxLoss={max_loss:.0f}")
        return max_loss

    def generate(self, mandate: TradingMandate, chain: pd.DataFrame, lot_size: int, vol_metrics: VolMetrics, spot: float, struct_metrics: StructMetrics) -> Tuple[List[Dict], float]:
        if mandate.max_lots == 0 or chain.empty: return [], 0.0
        qty = mandate.max_lots * lot_size
        legs = []

        if mandate.suggested_structure == "IRON_FLY":
            logger.info(f"Constructing Iron Fly | DTE={mandate.dte} | Spot={spot:.2f}")
            atm_data = self._find_professional_atm(chain, spot)
            if not atm_data: return [], 0.0
            atm_strike, straddle_cost, interval = atm_data['strike'], atm_data['straddle_cost'], atm_data['interval']
            wing_width = self._calculate_pro_wing_width(straddle_cost, vol_metrics, interval)
            upper_wing, lower_wing = atm_strike + wing_width, atm_strike - wing_width
            atm_call = self._get_leg_details(chain, atm_strike, 'CE')
            atm_put  = self._get_leg_details(chain, atm_strike, 'PE')
            wing_call = self._get_leg_details(chain, upper_wing, 'CE')
            wing_put  = self._get_leg_details(chain, lower_wing, 'PE')
            if not all([atm_call, atm_put, wing_call, wing_put]):
                logger.error("Iron Fly incomplete: Missing liquid strikes"); return [], 0.0
            legs = [
                {**atm_call, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**atm_put,  'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**wing_call,'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'},
                {**wing_put, 'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'}
            ]

        elif mandate.suggested_structure == "IRON_CONDOR":
            logger.info(f"Constructing Iron Condor | DTE={mandate.dte}")
            short_delta = ProductionConfig.DELTA_SHORT_MONTHLY if mandate.expiry_type == "MONTHLY" else ProductionConfig.DELTA_SHORT_WEEKLY
            legs = [
                self._find_leg_by_delta(chain, 'CE', short_delta),
                self._find_leg_by_delta(chain, 'PE', short_delta),
                self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_LONG_HEDGE),
                self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_LONG_HEDGE)
            ]
            if not all(legs): logger.error("Iron Condor incomplete"); return [], 0.0
            legs = [{**l, 'side': 'SELL' if idx < 2 else 'BUY', 'role': 'CORE' if idx < 2 else 'HEDGE', 'qty': qty, 'structure': 'IRON_CONDOR'} for idx, l in enumerate(legs)]

        elif mandate.suggested_structure in ["CREDIT_SPREAD", "BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"]:
            is_uptrend   = vol_metrics.spot > vol_metrics.ma20 * (1 + ProductionConfig.TREND_BULLISH_THRESHOLD/100)
            
            if is_uptrend or mandate.directional_bias in ["BULLISH", "MILDLY_BULLISH"]:
                logger.info("Direction: BULLISH. Deploying BULL PUT SPREAD.")
                short = self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_CREDIT_SHORT)
                long  = self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_CREDIT_LONG)
                if not all([short, long]): return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BULL_PUT_SPREAD'},
                    {**long,  'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'BULL_PUT_SPREAD'}
                ]
            else:
                logger.info("Direction: BEARISH. Deploying BEAR CALL SPREAD.")
                short = self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_CREDIT_SHORT)
                long  = self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_CREDIT_LONG)
                if not all([short, long]): return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BEAR_CALL_SPREAD'},
                    {**long,  'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'BEAR_CALL_SPREAD'}
                ]

        if not legs: return [], 0.0
        for leg in legs:
            if leg['ltp'] <= 0:
                logger.error(f"Invalid Leg Price: {leg['strike']} = {leg['ltp']}")
                return [], 0.0

        max_risk = self._calculate_defined_risk(legs, qty)
        if max_risk > ProductionConfig.MAX_LOSS_PER_TRADE:
            logger.critical(f"Trade Rejected: Max Risk ₹{max_risk:,.2f} > Limit ₹{ProductionConfig.MAX_LOSS_PER_TRADE:,.2f}")
            telegram.send(f"Trade Rejected: Risk ₹{max_risk:,.0f} exceeds limit", "WARNING")
            return [], 0.0
        return legs, max_risk

# Execution Engine
class ExecutionEngine:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.order_updates = {}
        self.update_lock = threading.Lock()
        self.price_cache = {}
        self.price_cache_lock = threading.Lock()
        self.websocket_connected = False
        self._setup_portfolio_stream()

    def _setup_portfolio_stream(self):
        try:
            self.portfolio_streamer = upstox_client.PortfolioDataStreamer(
                self.api_client,
                order_update=True,
                position_update=True,
                holding_update=False,
                gtt_update=True
            )

            def on_message(message):
                with self.update_lock:
                    if 'order_updates' in message:
                        for update in message['order_updates']:
                            order_id = update.get('order_id')
                            if order_id:
                                self.order_updates[order_id] = update
                                logger.debug(f"WebSocket order update: {order_id} -> {update.get('status')}")

            def on_open():
                self.websocket_connected = True
                logger.info("Portfolio Stream Connected")

            def on_error(error):
                self.websocket_connected = False
                logger.error(f"Portfolio Stream Error: {error}")

            def on_close():
                self.websocket_connected = False
                logger.warning("Portfolio Stream Closed")

            self.portfolio_streamer.on("message", on_message)
            self.portfolio_streamer.on("open", on_open)
            self.portfolio_streamer.on("error", on_error)
            self.portfolio_streamer.on("close", on_close)
            self.portfolio_streamer.auto_reconnect(True, 10, 5)
            threading.Thread(target=self.portfolio_streamer.connect, daemon=True, name="Portfolio-WS").start()
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to setup portfolio stream: {e}")
            self.websocket_connected = False

    @retry_with_backoff(max_retries=3, base_delay=1.0, exceptions=(ApiException, requests.RequestException, Exception))
    def check_margin_requirement(self, legs: List[Dict]) -> float:
        charge_api = ChargeApi(self.api_client)
        instruments = []
        for leg in legs:
            instruments.append(upstox_client.Instrument(
                instrument_key=leg['key'],
                quantity=int(leg['qty']),
                transaction_type=leg['side'],
                product="D"
            ))
        margin_request = upstox_client.MarginRequest(instruments=instruments)
        response = charge_api.post_margin(margin_request)
        
        if response.status == 'success' and hasattr(response.data, 'required_margin'):
            margin = float(response.data.required_margin)
            logger.info(f"Margin requirement: ₹{margin:,.2f}")
            return margin
        
        logger.error(f"Margin check failed: {response}")
        return float('inf')

    @retry_with_backoff(max_retries=3)
    def get_funds(self) -> float:
        user_api = upstox_client.UserApi(self.api_client)
        response = user_api.get_user_fund_margin(api_version="2.0")
        if response.status != 'success' or not response.data:
            logger.error("Funds API returned no data")
            return 0.0
        data = response.data
        if hasattr(data, 'equity') and hasattr(data.equity, 'available_margin'):
            funds = float(data.equity.available_margin)
            logger.info(f"Available funds: ₹{funds:,.2f}")
            return funds
        return 0.0

    def place_order(self, instrument_key: str, qty: int, side: str, order_type: str = "LIMIT", price: float = 0.0) -> Optional[str]:
        if qty <= 0 or price < 0:
            logger.error(f"Invalid order parameters: qty={qty}, price={price}")
            return None
            
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                body = upstox_client.PlaceOrderV3Request(
                    quantity=int(qty),
                    product="D",
                    validity="DAY",
                    price=float(price),
                    tag="VG30",
                    instrument_token=instrument_key,
                    order_type=order_type,
                    transaction_type=side,
                    disclosed_quantity=0,
                    trigger_price=0.0,
                    is_amo=False,
                    slice=True
                )
                response = order_api.place_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'order_ids') and response.data.order_ids:
                    order_id = response.data.order_ids[0]
                    logger.info(f"ORDER PLACED: {side} {qty}x {instrument_key} @ {price} | ID={order_id}")
                    db_writer.log_order(order_id, instrument_key, side, qty, price, "PLACED")
                    return order_id
                else:
                    logger.warning(f"Order placement attempt {attempt+1} failed: {response}")
            except Exception as e:
                logger.error(f"Order placement error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error(f"Order placement failed after {ProductionConfig.MAX_API_RETRIES} attempts")
        db_writer.log_order("FAILED", instrument_key, side, qty, price, "FAILED", message="All retries exhausted")
        return None

    @retry_with_backoff(max_retries=5, base_delay=0.5)
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        with self.update_lock:
            if order_id in self.order_updates:
                update = self.order_updates[order_id]
                return {
                    'status': update.get('status', '').lower(),
                    'avg_price': float(update.get('average_price', 0)),
                    'filled_qty': int(update.get('filled_quantity', 0))
                }
        try:
            order_api = OrderApi(self.api_client)
            response = order_api.get_order_details(api_version="2.0", order_id=order_id)
            if response.status != 'success' or not response.data:
                return None
            order_data = response.data
            return {
                'status': order_data.status.lower() if hasattr(order_data, 'status') else 'unknown',
                'avg_price': float(order_data.average_price) if hasattr(order_data, 'average_price') and order_data.average_price else 0.0,
                'filled_qty': int(order_data.filled_quantity) if hasattr(order_data, 'filled_quantity') and order_data.filled_quantity else 0
            }
        except Exception as e:
            logger.error(f"Order status check failed: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_order(order_id=order_id)
                logger.info(f"ORDER CANCELLED: {order_id}")
                db_writer.log_order(order_id, "", "", 0, 0, "CANCELLED")
                return True
            except Exception as e:
                logger.error(f"Cancel order error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1:
                    time.sleep(0.5)
        return False

    def place_gtt_order(self, instrument_key: str, qty: int, side: str, stop_loss_price: float, target_price: float) -> Optional[str]:
        if qty <= 0 or stop_loss_price <= 0 or target_price <= 0:
            logger.error(f"Invalid GTT parameters")
            return None
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                sl_trigger = "BELOW" if side == "BUY" else "ABOVE"
                rules = [
                    upstox_client.GttRule(strategy="STOPLOSS", trigger_type=sl_trigger, trigger_price=float(stop_loss_price)),
                    upstox_client.GttRule(strategy="TARGET", trigger_type="IMMEDIATE", trigger_price=float(target_price))
                ]
                body = upstox_client.GttPlaceOrderRequest(
                    type="MULTIPLE",
                    quantity=int(qty),
                    product="D",
                    rules=rules,
                    instrument_token=instrument_key,
                    transaction_type=side
                )
                response = order_api.place_gtt_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'gtt_order_ids') and response.data.gtt_order_ids:
                    gtt_id = response.data.gtt_order_ids[0]
                    logger.info(f"GTT PLACED: {side} {qty}x {instrument_key} | SL={stop_loss_price} Target={target_price} | ID={gtt_id}")
                    return gtt_id
            except Exception as e:
                logger.error(f"GTT placement error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error("GTT placement failed after all retries")
        return None

    def get_gtt_order_details(self, gtt_id: str) -> Optional[str]:
        try:
            order_api = OrderApiV3(self.api_client)
            response = order_api.get_gtt_order_details(gtt_order_id=gtt_id)
            if response.status == 'success' and response.data:
                data = response.data[0] if isinstance(response.data, list) else response.data
                return data.status if hasattr(data, 'status') else None
            return None
        except Exception as e:
            logger.error(f"GTT check failed: {e}")
            return None

    def cancel_gtt_order(self, gtt_id: str) -> bool:
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_gtt_order(gtt_order_id=gtt_id)
                logger.info(f"GTT CANCELLED: {gtt_id}")
                return True
            except Exception as e:
                logger.error(f"GTT cancel error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1:
                    time.sleep(0.5)
        return False

    def get_brokerage_impact(self, legs: List[Dict]) -> float:
        try:
            charge_api = ChargeApi(self.api_client)
            total_brokerage = 0.0
            for leg in legs:
                response = charge_api.get_brokerage(
                    leg['key'],
                    leg['qty'],
                    'D',
                    leg['side'],
                    leg['ltp'],
                    "2.0"
                )
                if response.status == 'success' and response.data and hasattr(response.data, 'charges'):
                    total_brokerage += float(response.data.charges.total)
            logger.info(f"Estimated brokerage: ₹{total_brokerage:.2f}")
            return total_brokerage
        except Exception as e:
            logger.error(f"Brokerage calculation error: {e}")
            return 0.0

    def exit_all_positions(self, tag: Optional[str] = None) -> bool:
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApi(self.api_client)
                response = order_api.exit_positions()
                if response.status == 'success':
                    logger.critical("ATOMIC EXIT EXECUTED")
                    telegram.send("Server-side atomic exit completed", "CRITICAL")
                    return True
                else:
                    logger.warning(f"Atomic exit attempt {attempt+1} failed: {response}")
            except Exception as e:
                logger.error(f"Atomic exit error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error("Atomic exit failed after all retries")
        return False

    def verify_gtt(self, gtt_ids: List[str], max_retries: int = 3) -> bool:
        if not gtt_ids:
            return True
        
        for attempt in range(max_retries):
            all_active = True
            failed_ids = []
            
            for gtt_id in gtt_ids:
                status = self.get_gtt_order_details(gtt_id)
                
                if status == 'active':
                    continue
                elif status in ['pending', 'placed']:
                    all_active = False
                    failed_ids.append((gtt_id, status))
                else:
                    logger.error(f"GTT {gtt_id} in failed state: {status}")
                    telegram.send(f"GTT FAILED: {gtt_id} = {status}", "CRITICAL")
                    return False
            
            if all_active:
                logger.info(f"All {len(gtt_ids)} GTTs verified active")
                return True
            
            if attempt < max_retries - 1:
                logger.info(f"GTT verification attempt {attempt+1}/{max_retries}: {len(failed_ids)} still pending")
                time.sleep(2)
        
        logger.warning(f"GTT verification incomplete after {max_retries} attempts")
        telegram.send(f"GTT Status Uncertain: {len(failed_ids)} orders not confirmed active", "WARNING")
        return False

    def _execute_leg_atomic(self, leg):
        tolerance = 0.998 if leg['role'] == 'HEDGE' else (1.002 if leg['side'] == 'BUY' else 0.998)
        limit_price = round(leg['ltp'] * tolerance, 1)
        expected_price = leg['ltp']
        logger.info(f"PLACING {leg['side']} {leg['strike']} {leg['type']} @ {limit_price} (Role: {leg['role']})")
        order_id = self.place_order(leg['key'], leg['qty'], leg['side'], "LIMIT", limit_price)
        if not order_id:
            return None
        
        start = time.time()
        last_status = None
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 5
        
        while (time.time() - start) < ProductionConfig.ORDER_TIMEOUT:
            status = self.get_order_status(order_id)
            
            if not status:
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.critical(f"Order status API completely unresponsive for {order_id}")
                    self.cancel_order(order_id)
                    db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "API_FAILURE", 
                                       filled_qty=0, avg_price=0.0, message="Order status API unresponsive")
                    return None
                time.sleep(0.2)
                continue
            
            consecutive_failures = 0
            
            if status['status'] != last_status:
                logger.debug(f"Order {order_id}: {status['status']}")
                last_status = status['status']
            
            if status['status'] == 'complete':
                fill_threshold = ProductionConfig.HEDGE_FILL_TOLERANCE if leg['role'] == 'HEDGE' else ProductionConfig.PARTIAL_FILL_TOLERANCE
                if status['filled_qty'] < leg['qty'] * fill_threshold:
                    logger.critical(f"PARTIAL FILL: {status['filled_qty']}/{leg['qty']} for {leg['role']}")
                    self.cancel_order(order_id)
                    db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "PARTIAL_REJECTED", 
                                       filled_qty=status['filled_qty'], message=f"Below {fill_threshold*100:.0f}% threshold")
                    return None
                
                actual_price = status['avg_price']
                slippage = abs(actual_price - expected_price) / expected_price if expected_price > 0 else 0
                
                if leg['role'] == 'HEDGE':
                    if leg['side'] == 'BUY':
                        if actual_price > expected_price * 1.5:
                            logger.critical(f"HEDGE PRICE ANOMALY: {leg['key']} filled at ₹{actual_price} (expected ₹{expected_price})")
                            telegram.send(f"CRITICAL HEDGE FILL: {leg['strike']} {leg['type']} @ ₹{actual_price} (expected ₹{expected_price}). Slippage: {slippage*100:.1f}%", "CRITICAL")
                            
                            if actual_price > expected_price * 3.0:
                                logger.critical("Rejecting catastrophic hedge fill (>3x expected price)")
                                db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "REJECTED_PRICE_ANOMALY",
                                    filled_qty=status['filled_qty'], avg_price=actual_price,
                                    message=f"Hedge filled at {actual_price:.2f} vs expected {expected_price:.2f}")
                                return None
                
                if slippage > ProductionConfig.SLIPPAGE_TOLERANCE:
                    logger.warning(f"SLIPPAGE: {slippage*100:.2f}% on {leg['key']}")
                    circuit_breaker.record_slippage_event(slippage)
                
                leg['entry_price'] = actual_price
                leg['filled_qty'] = status['filled_qty']
                leg['slippage'] = slippage
                db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "FILLED", 
                                   filled_qty=status['filled_qty'], avg_price=actual_price)
                logger.info(f"FILLED: {leg['side']} {status['filled_qty']}x {leg['strike']} {leg['type']} @ {actual_price}")
                return leg
            
            elif status['status'] in ['rejected', 'cancelled']:
                logger.error(f"ORDER DEAD: {status['status']}")
                db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, status['status'].upper())
                return None
            
            time.sleep(0.2)
        
        logger.warning(f"TIMEOUT on {order_id}. Attempting cancel...")
        self.cancel_order(order_id)
        time.sleep(1)
        final_status = self.get_order_status(order_id)
        if final_status and final_status['status'] == 'complete':
            leg['entry_price'] = final_status['avg_price']
            leg['filled_qty'] = final_status['filled_qty']
            logger.info(f"Order filled during cancel: {final_status}")
            return leg
        
        db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "TIMEOUT")
        return None

    def _get_existing_positions(self) -> List[Dict]:
        try:
            portfolio_api = PortfolioApi(self.api_client)
            response = portfolio_api.get_positions(api_version="2.0")
            
            if response.status == 'success' and response.data:
                return [{'instrument_key': p.instrument_token, 'quantity': int(p.quantity)} for p in response.data]
            return []
        except Exception as e:
            logger.error(f"Failed to fetch existing positions: {e}")
            return []

    def _get_instrument_position_limit(self, instrument_key: str) -> int:
        return ProductionConfig.MAX_CONTRACTS_PER_INSTRUMENT

    def execute_strategy(self, legs: List[Dict]) -> List[Dict]:
        total_qty = sum(l['qty'] for l in legs)
        if total_qty > ProductionConfig.MAX_CONTRACTS_PER_INSTRUMENT:
            logger.critical(f"Position size {total_qty} exceeds limit {ProductionConfig.MAX_CONTRACTS_PER_INSTRUMENT}")
            telegram.send(f"Position size violation: {total_qty} contracts", "ERROR")
            return []
        
        existing_positions = self._get_existing_positions()
        
        for leg in legs:
            instrument_key = leg['key']
            proposed_qty = leg['qty']
            
            existing_qty = sum(abs(p['quantity']) for p in existing_positions if p['instrument_key'] == instrument_key)
            total_instrument_qty = existing_qty + proposed_qty
            instrument_limit = self._get_instrument_position_limit(instrument_key)
            
            if total_instrument_qty > instrument_limit:
                logger.critical(f"POSITION LIMIT BREACH: {instrument_key}\nExisting: {existing_qty}, Proposed: {proposed_qty}, Total: {total_instrument_qty}, Limit: {instrument_limit}")
                telegram.send(f"Position limit breach prevented:\n{instrument_key}\nWould exceed {instrument_limit} contract limit", "ERROR")
                return []
        
        if len(legs) >= 4:
            strikes = sorted([l['strike'] for l in legs])
            max_spread_width = max(strikes) - min(strikes)
            premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
            max_loss = (max_spread_width - premium) * legs[0]['qty']
            if max_loss > ProductionConfig.MAX_LOSS_PER_TRADE:
                logger.critical(f"Max loss ₹{max_loss:,.0f} exceeds limit ₹{ProductionConfig.MAX_LOSS_PER_TRADE:,.0f}")
                telegram.send(f"Max loss violation: ₹{max_loss:,.0f}", "ERROR")
                return []
        
        daily_stats = db_writer.get_daily_stats()
        if daily_stats:
            current_daily_pnl = daily_stats.get('total_pnl', 0)
            if not circuit_breaker.check_daily_loss_limit(current_daily_pnl):
                logger.critical(f"Daily loss limit breached: ₹{current_daily_pnl:,.0f}")
                telegram.send(f"Daily Loss Limit Breached\nCurrent P&L: ₹{current_daily_pnl:,.0f}\nLimit: {ProductionConfig.DAILY_LOSS_LIMIT*100:.0f}%\nTrade blocked", "CRITICAL")
                return []
        
        brokerage_cost = 0.0
        required_margin = self.check_margin_requirement(legs)
        available_funds = self.get_funds()
        usable_funds = available_funds * (1 - ProductionConfig.MARGIN_BUFFER)
        if required_margin > usable_funds:
            logger.critical(f"Margin ERROR: Need ₹{required_margin:,.2f}, Have ₹{usable_funds:,.2f} (with buffer)")
            telegram.send(f"Margin Shortfall: Need {required_margin/100000:.2f}L, Have {usable_funds/100000:.2f}L", "ERROR")
            return []
        projected_premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
        brokerage_cost = self.get_brokerage_impact(legs)
        if projected_premium > 0 and (projected_premium - brokerage_cost) < (projected_premium * 0.05):
            logger.critical(f"BROKERAGE TOO HIGH: Cost=₹{brokerage_cost:.2f}, Premium=₹{projected_premium:.2f}")
            telegram.send(f"Brokerage kills profit: ₹{brokerage_cost:.2f} on ₹{projected_premium:.2f} premium", "ERROR")
            return []
        
        hedges = [l for l in legs if l['role'] == 'HEDGE']
        cores = [l for l in legs if l['role'] == 'CORE']
        logger.info(f"Execution Plan: {len(hedges)} Hedges -> {len(cores)} Cores")
        
        hedge_results = []
        if hedges:
            logger.info(f"Executing {len(hedges)} Hedges in Parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(hedges), thread_name_prefix="Hedge-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in hedges}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result:
                        hedge_results.append(result)
                    else:
                        logger.critical("HEDGE EXECUTION FAILED - ABORTING STRATEGY")
                        if hedge_results:
                            logger.warning(f"Flattening {len(hedge_results)} filled hedges")
                            self._flatten_legs(hedge_results)
                        return []
            if len(hedge_results) != len(hedges):
                logger.critical(f"INCOMPLETE HEDGES: {len(hedge_results)}/{len(hedges)} - ABORTING")
                self._flatten_legs(hedge_results)
                return []
        
        logger.info(f"All {len(hedge_results)} Hedges Filled Successfully")
        
        core_results = []
        if cores:
            logger.info(f"Executing {len(cores)} Cores in Parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(cores), thread_name_prefix="Core-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in cores}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result:
                        core_results.append(result)
                    else:
                        logger.critical("CORE EXECUTION FAILED - FLATTENING ALL")
                        self._flatten_legs(hedge_results + core_results)
                        return []
            if len(core_results) != len(cores):
                logger.critical(f"INCOMPLETE CORES: {len(core_results)}/{len(cores)} - FLATTENING ALL")
                self._flatten_legs(hedge_results + core_results)
                return []
        
        executed = hedge_results + core_results
        structure = executed[0].get('structure', 'UNKNOWN') if executed else 'UNKNOWN'
        actual_premium = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'SELL')
        actual_debit   = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'BUY')
        net_premium = actual_premium - actual_debit
        db_writer.update_daily_stats(trades=1)
        logger.info(f"LIVE STRATEGY DEPLOYED: {structure} | Net Premium: ₹{net_premium:,.2f}")
        telegram.send(
            f"LIVE Position Opened\nStructure: {structure}\nLegs: {len(executed)}\nNet Premium: ₹{net_premium:,.2f}\nBrokerage: ₹{brokerage_cost:.2f}",
            "TRADE"
        )
        return executed

    def _flatten_legs(self, legs: List[Dict]):
        if not legs:
            return
        logger.critical(f"EMERGENCY FLATTEN: {len(legs)} legs")
        telegram.send(f"Emergency flattening {len(legs)} legs", "CRITICAL")
        for leg in legs:
            if leg.get('filled_qty', 0) <= 0:
                continue
            exit_side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
            success = False
            for attempt in range(2):
                try:
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "MARKET", 0.0)
                    if oid:
                        time.sleep(1)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"Market exit: {leg['key']}")
                            success = True
                            break
                except Exception as e:
                    logger.error(f"Market exit failed: {e}")
            if success:
                continue
            for attempt in range(3):
                try:
                    exit_price = leg.get('current_ltp', leg['entry_price'])
                    exit_price = exit_price * 1.10 if exit_side == 'BUY' else exit_price * 0.90
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "LIMIT", round(exit_price, 1))
                    if oid:
                        time.sleep(2)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"Limit exit: {leg['key']}")
                            success = True
                            break
                        elif status and status['status'] != 'complete':
                            self.cancel_order(oid)
                except Exception as e:
                    logger.error(f"Limit exit attempt {attempt+1} failed: {e}")
                time.sleep(1)
            if not success:
                msg = f"CRITICAL: FAILED TO CLOSE {leg['key']} - MANUAL INTERVENTION REQUIRED"
                logger.critical(msg)
                telegram.send(msg, "CRITICAL")
                db_writer.log_risk_event("FAILED_EXIT", "CRITICAL", f"Could not close {leg['key']}", "MANUAL_ACTION_REQUIRED")

# Risk Manager
class RiskManager:
    def __init__(self, api_client: upstox_client.ApiClient, legs: List[Dict], expiry_date: date, trade_id: str, gtt_ids: List[str] = None):
        self.api_client = api_client
        self.legs = legs
        self.expiry = expiry_date
        self.trade_id = trade_id
        self.gtt_ids = gtt_ids or []
        self.running = True
        self.last_price_update = time.time()
        
        self.correlation_manager = None
        self.orchestrator = None
        
        self.greeks_mgr = get_live_greeks_manager(api_client)
        self.greeks_mgr.update_subscriptions([l['key'] for l in legs])
        
        if self.greeks_mgr.subscribed_keys:
            logger.info("Waiting for initial Greek data...")
            time.sleep(3)
        
        initial_greeks = self.greeks_mgr.get_portfolio_greeks(legs, trade_id)
        if initial_greeks['legs_count'] > 0:
            logger.info(f"Initial Greeks | Delta:{initial_greeks['delta']:.1f} Theta:{initial_greeks['theta']/1000:.1f}k Vega:{initial_greeks['vega']:.3f} Theta/Vega:{initial_greeks['theta_vega_ratio']:.2f}")
        
        credit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'SELL')
        debit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'BUY')
        self.net_premium = credit - debit
        
        structure = legs[0].get('structure', 'UNKNOWN')
        qty = legs[0]['filled_qty'] if legs else 0
        
        if structure in ['IRON_FLY', 'IRON_CONDOR']:
            call_strikes = sorted([l['strike'] for l in legs if l['type'] == 'CE'])
            put_strikes = sorted([l['strike'] for l in legs if l['type'] == 'PE'])
            
            call_width = (call_strikes[-1] - call_strikes[0]) if len(call_strikes) >= 2 else 0
            put_width = (put_strikes[-1] - put_strikes[0]) if len(put_strikes) >= 2 else 0
            
            max_spread_width = max(call_width, put_width)
            total_width_value = max_spread_width * qty
            self.max_spread_loss = max(0, total_width_value - self.net_premium)
        else:
            self.max_spread_loss = self.net_premium * 2

        logger.info(f"Risk Manager Init: Trade={trade_id} | Premium=₹{self.net_premium:,.2f} | Max Risk=₹{self.max_spread_loss:,.2f} | GTTs={len(self.gtt_ids)}")
        self._dte_warning_logged = False

    def monitor(self):
        market_api = upstox_client.MarketQuoteV3Api(self.api_client)
        consecutive_errors = 0
        max_consecutive_errors = 10
        logger.info(f"Risk monitoring started for {self.trade_id}")
        
        while self.running:
            try:
                current_time = time.time()
                if int(current_time) % 5 == 0 and self.greeks_mgr.subscribed_keys:
                    warnings = self.greeks_mgr.check_risk_limits(self.legs, self.trade_id)
                    for warning in warnings:
                        if 'CRITICAL' in warning:
                            logger.critical(warning)
                            if 'Theta/Vega Ratio' in warning and 'CRITICAL' in warning:
                                logger.critical("Auto-flattening due to extreme Theta/Vega ratio")
                                self.flatten_all("THETA_VEGA_RATIO_CRITICAL")
                                return
                        else:
                            logger.warning(warning)
                    
                    if int(current_time) % 30 == 0:
                        port = self.greeks_mgr.get_portfolio_greeks(self.legs, self.trade_id)
                        logger.info(f"Live Portfolio Greeks | Delta:{port['delta']:.1f} | Theta:{port['theta']/1000:.1f}k | Vega:{port['vega']:.3f} | Gamma:{port['gamma']:.1f} | Theta/Vega:{port['theta_vega_ratio']:.2f} | Stale:{port['stale_count']}")
                
                days_to_expiry = (self.expiry - date.today()).days
                current_time_ist = datetime.now(ProductionConfig.IST).time()

                if days_to_expiry <= ProductionConfig.EXIT_DTE:
                    if current_time_ist >= ProductionConfig.SQUARE_OFF_TIME_IST:
                        logger.info(f"DTE EXIT TRIGGER: {days_to_expiry} DTE + Time {current_time_ist.strftime('%H:%M')}")
                        telegram.send(f"Auto-Exit Triggered\nDTE: {days_to_expiry}\nTime: {current_time_ist.strftime('%H:%M')}\nReason: {ProductionConfig.EXIT_DTE}-day exit rule at 2 PM", "WARNING")
                        self.flatten_all("DTE_EXIT_AT_2PM")
                        return
                    else:
                        if not self._dte_warning_logged:
                            logger.warning(f"DTE {days_to_expiry} - Will auto-exit at 2 PM today")
                            telegram.send(f"Exit Scheduled Today\nDTE: {days_to_expiry}\nCurrent Time: {current_time_ist.strftime('%H:%M')}\nExit Time: 14:00", "INFO")
                            self._dte_warning_logged = True

                keys = [l['key'] for l in self.legs]
                price_response = None
                for attempt in range(3):
                    try:
                        price_response = market_api.get_ltp(instrument_key=','.join(keys))
                        if price_response and price_response.status == 'success':
                            break
                    except Exception as e:
                        logger.warning(f"Price fetch attempt {attempt+1} failed: {e}")
                        time.sleep(0.5)
                
                if not price_response or price_response.status != 'success':
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.critical(f"Price feed failed {consecutive_errors} times - flattening for safety")
                        self.flatten_all("PRICE_FEED_FAILURE")
                        return
                    time.sleep(ProductionConfig.POLL_INTERVAL)
                    continue
                
                consecutive_errors = 0
                prices = price_response.data
                self.last_price_update = time.time()

                current_pnl = self._calculate_pnl(prices)
                
                if self.max_spread_loss > 0 and current_pnl < -(self.max_spread_loss * ProductionConfig.MAX_RISK_EXIT_PCT):
                    logger.critical(f"Max risk breached: P&L={current_pnl:.2f}, Limit={self.max_spread_loss * ProductionConfig.MAX_RISK_EXIT_PCT:.2f}")
                    self.flatten_all("STOP_LOSS_MAX_RISK")
                    return
                
                if self.max_spread_loss <= 0:
                    stop_threshold = -(self.net_premium * ProductionConfig.STOP_LOSS_PCT)
                    if self.net_premium > 0 and current_pnl < stop_threshold - ProductionConfig.STOP_LOSS_TOLERANCE:
                        logger.critical(f"FAILSAFE Stop loss: P&L={current_pnl:.2f}")
                        self.flatten_all("STOP_LOSS_PREMIUM_FAILSAFE")
                        return
                
                target_pnl = self.net_premium * ProductionConfig.TARGET_PROFIT_PCT
                if self.net_premium > 0 and (current_pnl >= target_pnl - ProductionConfig.PROFIT_TARGET_TOLERANCE):
                    logger.info(f"Target profit reached: P&L={current_pnl:.2f}, Target={target_pnl:.2f}")
                    self.flatten_all("TARGET_PROFIT")
                    return

                time.sleep(ProductionConfig.POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Risk monitor interrupted by user")
                self.running = False
                return
            except Exception as e:
                logger.error(f"Risk monitor error: {e}")
                traceback.print_exc()
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Too many errors in risk monitor - emergency exit")
                    self.flatten_all("MONITOR_ERROR")
                    return
                time.sleep(5)

    def _calculate_pnl(self, prices) -> float:
        pnl = 0.0
        missing_data_count = 0
        
        for leg in self.legs:
            current_price = 0.0
            
            if leg['key'] in prices and hasattr(prices[leg['key']], 'last_price'):
                current_price = prices[leg['key']].last_price
                leg['last_known_ltp'] = current_price
            
            elif 'last_known_ltp' in leg:
                current_price = leg['last_known_ltp']
                logger.warning(f"Using stale price for {leg['key']}: {current_price}")
            
            else:
                missing_data_count += 1
                logger.error(f"NO DATA for {leg['key']}")
                continue 

            if leg['side'] == 'SELL':
                leg_pnl = (leg['entry_price'] - current_price) * leg['filled_qty']
            else:
                leg_pnl = (current_price - leg['entry_price']) * leg['filled_qty']
            
            pnl += leg_pnl

        if missing_data_count > 0:
            logger.critical(f"Incomplete P&L calc due to missing data on {missing_data_count} legs")
            if missing_data_count > len(self.legs) / 2:
                self.flatten_all("DATA_FEED_LOSS")
        
        return pnl

    def flatten_all(self, reason="SIGNAL"):
        logger.critical(f"FLATTEN TRIGGERED: {reason}")
        telegram.send(f"Position Exit: {reason}", "CRITICAL")
        
        if self.correlation_manager:
            self.correlation_manager.remove_position(self.trade_id)
            logger.info(f"Removed {self.trade_id} from correlation tracker")
        
        if self.gtt_ids:
            logger.info(f"Cancelling {len(self.gtt_ids)} GTT orders...")
            executor = ExecutionEngine(self.api_client)
            
            for gtt_id in self.gtt_ids:
                cancelled = False
                for _ in range(3):
                    if executor.cancel_gtt_order(gtt_id):
                        cancelled = True
                        break
                    time.sleep(0.5)
                
                if not cancelled:
                    msg = f"FATAL: Could not cancel GTT {gtt_id}. Manual intervention required."
                    logger.critical(msg)
                    telegram.send(msg, "CRITICAL")
        
        executor = ExecutionEngine(self.api_client)
        atomic_success = False
        for attempt in range(2):
            logger.info(f"Atomic exit attempt {attempt+1}...")
            if executor.exit_all_positions(tag="VG30"):
                atomic_success = True
                logger.info("Atomic exit successful")
                break
            time.sleep(2)
            
        if not atomic_success:
            logger.critical("Atomic exit failed - falling back to leg-by-leg")
            telegram.send("Atomic exit failed - manual closure initiated", "CRITICAL")
            executor._flatten_legs(self.legs)
            
        final_pnl = self._get_final_pnl()
        db_writer.update_trade_exit(self.trade_id, reason, final_pnl)
        db_writer.log_risk_event("POSITION_EXIT", "INFO", reason, f"P&L: ₹{final_pnl:.2f}")
        circuit_breaker.record_trade_result(final_pnl)
        
        telegram.send(f"Position Closed\nReason: {reason}\nFinal P&L: ₹{final_pnl:,.2f}", "SUCCESS" if final_pnl > 0 else "WARNING")
        
        if self.orchestrator and self in self.orchestrator.active_risk_managers:
            self.orchestrator.active_risk_managers.remove(self)
            logger.info(f"Removed {self.trade_id} from active risk managers")
        
        self.running = False
        logger.info(f"Risk monitor shutdown complete for {self.trade_id}")

    def _get_final_pnl(self) -> float:
        try:
            portfolio_api = PortfolioApi(self.api_client)
            response = portfolio_api.get_positions(api_version="2.0")
            if response.status == 'success' and response.data:
                return sum(float(p.pnl) for p in response.data)
            return 0.0
        except Exception:
            return 0.0

# Startup Reconciliation
class StartupReconciliation:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client

    def reconcile(self) -> Optional[List[Dict]]:
        try:
            logger.info("Running startup reconciliation...")
            portfolio_api = PortfolioApi(self.api_client)
            pos_response = portfolio_api.get_positions(api_version="2.0")
            
            if pos_response.status != 'success' or not pos_response.data:
                logger.info("No open positions found")
                return None
                
            positions = pos_response.data
            
            options_api = OptionsApi(self.api_client)
            contract_resp = options_api.get_option_contracts(instrument_key=ProductionConfig.NIFTY_KEY)
            expiry_map = {}
            if contract_resp.status == 'success':
                for c in contract_resp.data:
                    if hasattr(c, 'instrument_key'):
                        expiry_map[c.instrument_key] = datetime.strptime(str(c.expiry).split('T')[0], "%Y-%m-%d").date()

            reconstructed_legs = []
            common_expiry = None
            
            for position in positions:
                qty = int(position.quantity) if hasattr(position, 'quantity') else 0
                if qty == 0: continue
                
                instrument_key = position.instrument_token
                expiry = expiry_map.get(instrument_key)
                if not expiry or expiry < date.today(): continue
                
                if common_expiry is None: common_expiry = expiry
                
                current_price = float(position.last_price)
                entry_price = float(position.average_price) if hasattr(position, 'average_price') else current_price
                
                role = 'HEDGE' if qty > 0 else 'CORE'
                
                leg = {
                    'key': instrument_key,
                    'strike': self._extract_strike_from_symbol(position.trading_symbol),
                    'type': self._extract_option_type(position.trading_symbol),
                    'side': 'SELL' if qty < 0 else 'BUY',
                    'qty': abs(qty),
                    'filled_qty': abs(qty),
                    'entry_price': entry_price,
                    'current_ltp': current_price,
                    'role': role,
                    'structure': 'RECONCILED',
                    'expiry': expiry
                }
                reconstructed_legs.append(leg)
                
            if reconstructed_legs:
                dte = (common_expiry - date.today()).days
                
                logger.info(f"Reconciled {len(reconstructed_legs)} legs. Expiry: {common_expiry} (DTE={dte})")
                
                if dte <= ProductionConfig.EXIT_DTE:
                    logger.critical(f"RECONCILED POSITIONS IN EXIT ZONE (DTE={dte})")
                    telegram.send(f"System Restart Alert\nReconciled: {len(reconstructed_legs)} positions\nExpiry: {common_expiry}\nDTE: {dte}\nIN EXIT ZONE - Will auto-exit at 2 PM", "CRITICAL")
                elif dte <= 2:
                    logger.warning(f"Reconciled positions in gamma week (DTE={dte})")
                    telegram.send(f"System Restart\nReconciled: {len(reconstructed_legs)} positions\nDTE: {dte} (Gamma week)\nMonitor closely", "WARNING")
                
                for leg in reconstructed_legs: 
                    leg['common_expiry'] = common_expiry
                    leg['dte_at_reconcile'] = dte
                return reconstructed_legs
                
            return None
            
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}")
            return None

    def _extract_strike_from_symbol(self, symbol: str) -> float:
        import re
        match = re.search(r'(\d{5})', symbol)
        return float(match.group(1)) if match else 0.0

    def _extract_option_type(self, symbol: str) -> str:
        return 'CE' if 'CE' in symbol.upper() else 'PE' if 'PE' in symbol.upper() else 'NA'

# Session Manager
class SessionManager:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.login_api = LoginApi(self.api_client)
        self.last_validation = 0
        self.validation_interval = 3600

    def validate_session(self, force: bool = False) -> bool:
        if not force and (time.time() - self.last_validation) < self.validation_interval:
            return True
        try:
            user_api = upstox_client.UserApi(self.api_client)
            response = user_api.get_profile(api_version="2.0")
            if response.status == 'success':
                self.last_validation = time.time()
                logger.debug("Session validated successfully")
                return True
            elif response.status == 'error':
                logger.warning("Session invalid - attempting refresh")
                return self._refresh_token()
            return False
        except Exception as e:
            logger.error(f"Session validation error: {e}")
            return self._refresh_token()

    @retry_with_backoff(max_retries=2)
    def _refresh_token(self) -> bool:
        if not ProductionConfig.UPSTOX_REFRESH_TOKEN:
            logger.critical("No refresh token configured - cannot refresh session")
            return False
        try:
            token_request = upstox_client.TokenRequest(
                client_id=ProductionConfig.UPSTOX_CLIENT_ID,
                client_secret=ProductionConfig.UPSTOX_CLIENT_SECRET,
                redirect_uri=ProductionConfig.UPSTOX_REDIRECT_URI,
                grant_type='refresh_token',
                code=ProductionConfig.UPSTOX_REFRESH_TOKEN
            )
            response = self.login_api.token(token_request)
            if response.status == 'success' and response.data and hasattr(response.data, 'access_token'):
                new_token = response.data.access_token
                self.api_client.configuration.access_token = new_token
                self.last_validation = time.time()
                logger.info("Token refreshed successfully")
                telegram.send("Access token refreshed", "SYSTEM")
                return True
            logger.critical(f"Token refresh failed: {response}")
            telegram.send("Token refresh failed - manual intervention required", "CRITICAL")
            return False
        except Exception as e:
            logger.critical(f"Token refresh exception: {e}")
            telegram.send(f"Token refresh error: {str(e)}", "CRITICAL")
            return False

    def check_market_status(self) -> bool:
        try:
            market_api = MarketHolidaysAndTimingsApi(self.api_client)
            status_response = market_api.get_market_status(exchange='NFO')
            if status_response.status == 'success' and status_response.data:
                market_status = status_response.data.status.upper() if hasattr(status_response.data, 'status') else 'UNKNOWN'
                if market_status == 'OPEN':
                    return True
                logger.info(f"Market status: {market_status}")
            today_str = date.today().strftime("%Y-%m-%d")
            holiday_response = market_api.get_holiday(today_str)
            if holiday_response.status == 'success' and holiday_response.data:
                holidays = holiday_response.data if isinstance(holiday_response.data, list) else [holiday_response.data]
                for holiday in holidays:
                    if hasattr(holiday, 'holiday_type') and holiday.holiday_type == 'TRADING_HOLIDAY':
                        logger.info(f"Market Closed: {getattr(holiday, 'description', 'Holiday')}")
                        return False
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 15:
                logger.warning("Could not determine market status - assuming open during market hours")
                return True
            return False
        except Exception as e:
            logger.error(f"Market status check failed: {e}")
            current_hour = datetime.now().hour
            return 9 <= current_hour <= 15

class HeartbeatMonitor:
    def __init__(self):
        self.last_heartbeat = time.time()
        self.is_alive = True
        self.lock = threading.Lock()

    def beat(self):
        with self.lock:
            self.last_heartbeat = time.time()
            self.is_alive = True

    def check(self) -> bool:
        with self.lock:
            elapsed = time.time() - self.last_heartbeat
            if elapsed > ProductionConfig.HEARTBEAT_INTERVAL * 3:
                logger.critical(f"System heartbeat lost for {elapsed:.0f}s")
                return False
            return True

    def stop(self):
        with self.lock:
            self.is_alive = False

heartbeat = HeartbeatMonitor()

# AI Analyst Module (PRESERVED - DO NOT MODIFY)
@dataclass
class MarketRegime:
    risk_score: int
    nifty_view: str
    strategy: str
    reasoning: str

class NewsScout:
    def __init__(self):
        self.rss_url = "https://news.google.com/rss/search?q=stock+market+india+business+sensex+nifty&hl=en-IN&gl=IN&ceid=IN:en"

    def get_headlines(self, limit=5):
        try:
            response = requests.get(self.rss_url, timeout=5)
            if response.status_code != 200: return []
            root = ET.fromstring(response.content)
            headlines = []
            for item in root.findall('./channel/item')[:limit]:
                title = item.find('title').text
                if "-" in title: title = title.rpartition('-')[0].strip()
                headlines.append(title)
            return headlines
        except Exception:
            return []

class EventRadar:
    def __init__(self):
        self.url = "https://economic-calendar.tradingview.com/events"
        self.headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://in.tradingview.com"}

    def get_todays_events(self):
        try:
            now_utc = datetime.now(pytz.utc)
            payload = {
                "from": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": (now_utc + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "countries": "IN,US",
                "minImportance": "1"
            }
            r = requests.get(self.url, params=payload, headers=self.headers, timeout=5)
            events = []
            for e in r.json().get('result', []):
                if e.get('importance', 0) > 0:
                    dt_utc = datetime.strptime(e['date'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                    dt_ist = dt_utc.astimezone(ProductionConfig.IST)
                    events.append(f"[{e['country']}] {e['title']} @ {dt_ist.strftime('%I:%M %p')} IST")
            return events[:7]
        except:
            return []

def get_market_metrics():
    tickers = {"^NSEI": "Nifty 50", "^INDIAVIX": "India VIX", "ES=F": "US Futures", "GC=F": "Gold"}
    try:
        data = yf.download(list(tickers.keys()), period="5d", progress=False)['Close']
        metrics = {}
        for symbol, name in tickers.items():
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if symbol in data.columns.levels[1]: s = data.xs(symbol, axis=1, level=1).dropna()
                    else: s = pd.Series()
                else: s = data[symbol].dropna()
                if s.empty and symbol in data: s = data[symbol].dropna()
                
                if len(s) >= 2:
                    metrics[name] = f"{((s.iloc[-1] - s.iloc[-2]) / s.iloc[-2]) * 100:+.2f}%"
                else: metrics[name] = "0.00%"
            except: metrics[name] = "Err"
        return metrics
    except: return {}

class IndiaAIAnalyst:
    def __init__(self, api_key):
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self.scout = NewsScout()
        self.radar = EventRadar()

    def get_morning_brief(self) -> MarketRegime:
        news = self.scout.get_headlines()
        events = self.radar.get_todays_events()
        data = get_market_metrics()

        system = "You are a Nifty 50 Risk Officer. Output JSON only."
        prompt = f"""
        Analyze Indian Market: {datetime.now(ProductionConfig.IST).strftime('%d %b %Y')}.
        DATA: {json.dumps(data)}
        EVENTS: {json.dumps(events)}
        NEWS: {json.dumps(news)}
        RISK RULES:
        - War/Terror/Sanctions + Gold UP = FEAR (Risk 8-10).
        - VIX > +5% = VOLATILITY (Risk 6-7).
        - Fed/RBI/CPI/Election = EVENT_RISK.
        OUTPUT JSON:
        {{
            "risk_score": (int 1-10),
            "nifty_view": "GAP_UP"|"GAP_DOWN"|"FLAT",
            "strategy": "IRON_CONDOR"|"CREDIT_SPREAD"|"CASH_ONLY",
            "reason": "Brief summary."
        }}
        """
        try:
            chat = self.client.chat.completions.create(
                messages=[{"role":"system","content":system}, {"role":"user","content":prompt}],
                model=self.model,
                response_format={"type": "json_object"}
            )
            res = json.loads(chat.choices[0].message.content)
            return MarketRegime(
                risk_score=res.get('risk_score', 5),
                nifty_view=res.get('nifty_view', 'FLAT'),
                strategy=res.get('strategy', 'IRON_CONDOR'),
                reasoning=res.get('reason', 'AI Analysis')
            )
        except Exception as e:
            return MarketRegime(5, "FLAT", "IRON_CONDOR", f"AI Error: {str(e)}")

# Trading Orchestrator
class TradingOrchestrator:
    def __init__(self):
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = ProductionConfig.UPSTOX_ACCESS_TOKEN
        self.api_client = upstox_client.ApiClient(self.configuration)
        
        self.greeks_manager = get_live_greeks_manager(self.api_client)
        
        self.regime_engine = RegimeEngineV33()
        self.strategy_factory = StrategyFactory(self.api_client)
        self.execution_engine = ExecutionEngine(self.api_client)
        self.session_manager = SessionManager(self.api_client)
        self.reconciliation = StartupReconciliation(self.api_client)
        self.last_analysis = None
        self.current_trade_id = None
        
        self.correlation_manager = PositionCorrelationManager()
        self.active_risk_managers: List[RiskManager] = []
        
        self.market_api = MarketHolidaysAndTimingsApi(self.api_client)
        self.cached_holiday_date = None
        self.is_holiday_today = False
        self.last_brief_date = None
        self.last_analysis_time = None
        atexit.register(self._cleanup_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _cleanup_handler(self):
        logger.info("Cleanup handler triggered")
        heartbeat.stop()
        if self.greeks_manager:
            self.greeks_manager.stop()
        # Note: No db_writer.shutdown() needed anymore (synchronous)

    def _signal_handler(self, signum, frame):
        logger.critical(f"Received signal {signum}")
        telegram.send(f"System shutdown signal received: {signum}", "CRITICAL")
        
        for rm in self.active_risk_managers[:]:
            if rm.running:
                logger.critical(f"Emergency position exit on shutdown for {rm.trade_id}")
                rm.flatten_all("SYSTEM_SHUTDOWN")
        
        self._cleanup_handler()
        sys.exit(0)

    def check_market_open_status(self, current_date):
        if self.cached_holiday_date == current_date:
            return not self.is_holiday_today

        try:
            logger.info(f"Checking Holiday API for {current_date}...")
            response = self.market_api.get_holiday(str(current_date))
            is_holiday = False
            if response.data:
                for h in response.data:
                    if h.holiday_type == "TRADING_HOLIDAY":
                        is_holiday = True
                        logger.info(f"Market Closed Today: {h.description}")
                        break
            
            self.cached_holiday_date = current_date
            self.is_holiday_today = is_holiday
            return not is_holiday
        except Exception as e:
            logger.error(f"Holiday API Failed: {e}")
            KNOWN_HOLIDAYS = ["2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-18"]
            if str(current_date) in KNOWN_HOLIDAYS: return False
            return current_date.weekday() < 5

    def run_morning_brief(self):
        today = datetime.now(ProductionConfig.IST).date()
        if self.last_brief_date == today: return

        try:
            saved = db_writer.get_state("daily_risk_regime")
            if saved and json.loads(saved).get("date") == str(today):
                logger.info("Morning Brief already in DB.")
                self.last_brief_date = today
                return
        except: pass

        logger.info("INITIATING MORNING BRIEF...")
        key = ProductionConfig.GROQ_API_KEY
        
        self.last_brief_date = today 
        
        if not key: return

        try:
            brief = IndiaAIAnalyst(key).get_morning_brief()
            emoji = "🟢" if brief.risk_score <= 4 else "🟡" if brief.risk_score <= 7 else "🔴"
            telegram.send(
                f"MORNING BRIEF\n{emoji} Risk: {brief.risk_score}/10\n"
                f"View: {brief.nifty_view}\nPlan: {brief.strategy}\n\n"
                f"_{brief.reasoning}_", "INFO"
            )
            db_writer.set_state("daily_risk_regime", json.dumps({
                "date": str(today), "risk_score": brief.risk_score, "reason": brief.reasoning
            }))
        except Exception as e:
            logger.error(f"Brief Failed: {e}")

    def run_analysis(self) -> Optional[Dict]:
        """Synchronous analysis - no multiprocessing"""
        logger.info("Starting market analysis...")
        try:
            config = {'access_token': ProductionConfig.UPSTOX_ACCESS_TOKEN}
            status, result = AnalyticsEngine().run(config)
            
            if status == 'success':
                weekly_mandate = self.regime_engine.generate_mandate(
                    self.regime_engine.calculate_scores(
                        result['vol_metrics'],
                        result['struct_metrics_weekly'],
                        result['edge_metrics'],
                        result['external_metrics'],
                        "WEEKLY",
                        result['time_metrics'].dte_weekly
                    ),
                    result['vol_metrics'],
                    result['struct_metrics_weekly'],
                    result['edge_metrics'],
                    result['external_metrics'],
                    result['time_metrics'],
                    "WEEKLY",
                    result['time_metrics'].weekly_exp,
                    result['time_metrics'].dte_weekly
                )
                monthly_mandate = self.regime_engine.generate_mandate(
                    self.regime_engine.calculate_scores(
                        result['vol_metrics'],
                        result['struct_metrics_monthly'],
                        result['edge_metrics'],
                        result['external_metrics'],
                        "MONTHLY",
                        result['time_metrics'].dte_monthly
                    ),
                    result['vol_metrics'],
                    result['struct_metrics_monthly'],
                    result['edge_metrics'],
                    result['external_metrics'],
                    result['time_metrics'],
                    "MONTHLY",
                    result['time_metrics'].monthly_exp,
                    result['time_metrics'].dte_monthly
                )
                
                next_weekly_mandate = None
                if (result['time_metrics'].dte_weekly <= ProductionConfig.EXIT_DTE and 
                    result['time_metrics'].dte_next_weekly > ProductionConfig.EXIT_DTE):
                    logger.info(f"Weekly DTE={result['time_metrics'].dte_weekly} - Exit mode, skipping next weekly analysis")
                    next_weekly_mandate = None
                elif result['time_metrics'].dte_weekly <= 1 and result['time_metrics'].dte_next_weekly > 1:
                    next_weekly_mandate = self.regime_engine.generate_mandate(
                        self.regime_engine.calculate_scores(
                            result['vol_metrics'],
                            result['struct_metrics_weekly'],
                            result['edge_metrics'],
                            result['external_metrics'],
                            "NEXT_WEEKLY",
                            result['time_metrics'].dte_next_weekly
                        ),
                        result['vol_metrics'],
                        result['struct_metrics_weekly'],
                        result['edge_metrics'],
                        result['external_metrics'],
                        result['time_metrics'],
                        "NEXT_WEEKLY",
                        result['time_metrics'].next_weekly_exp,
                        result['time_metrics'].dte_next_weekly
                    )
                
                self.last_analysis = {
                    'timestamp': datetime.now(),
                    'time_metrics': result['time_metrics'],
                    'vol_metrics': result['vol_metrics'],
                    'weekly_chain': result['weekly_chain'],
                    'monthly_chain': result['monthly_chain'],
                    'next_weekly_chain': result['next_weekly_chain'],
                    'lot_size': result['lot_size'],
                    'weekly_mandate': weekly_mandate,
                    'monthly_mandate': monthly_mandate,
                    'next_weekly_mandate': next_weekly_mandate
                }
                logger.info(
                    f"Analysis Complete\n"
                    f"Weekly: {weekly_mandate.regime_name} | Score: {weekly_mandate.score.composite:.2f} | {weekly_mandate.suggested_structure}\n"
                    f"Monthly: {monthly_mandate.regime_name} | Score: {monthly_mandate.score.composite:.2f} | {monthly_mandate.suggested_structure}"
                )
                return self.last_analysis
            else:
                logger.error(f"Analytics failed: {result}")
                telegram.send(f"Analysis failed: {result}", "ERROR")
                return None
                
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            traceback.print_exc()
            return None

    def execute_best_mandate(self, analysis: Dict) -> Optional[str]:
        weekly_mandate = analysis['weekly_mandate']
        monthly_mandate = analysis['monthly_mandate']
        
        mandate = weekly_mandate
        chain = analysis['weekly_chain']
        
        if weekly_mandate.dte <= ProductionConfig.EXIT_DTE:
            logger.info(f"Weekly DTE={weekly_mandate.dte} <= EXIT_DTE={ProductionConfig.EXIT_DTE} - Exit zone, no new trades")
        elif analysis.get('next_weekly_mandate'):
            next_mandate = analysis['next_weekly_mandate']
            if next_mandate.score.composite > weekly_mandate.score.composite:
                logger.info(f"Switching to NEXT WEEKLY: {next_mandate.score.composite:.2f} vs {weekly_mandate.score.composite:.2f}")
                mandate = next_mandate
                chain = analysis['next_weekly_chain']
            elif monthly_mandate.score.composite > weekly_mandate.score.composite:
                mandate = monthly_mandate
                chain = analysis['monthly_chain']
        elif monthly_mandate.score.composite > weekly_mandate.score.composite:
            mandate = monthly_mandate
            chain = analysis['monthly_chain']
            
        vol_metrics = analysis['vol_metrics']
        struct_metrics = analysis['struct_metrics_weekly'] if mandate == weekly_mandate else analysis['struct_metrics_monthly']
        
        logger.info(f"Selected mandate: {mandate.expiry_type} {mandate.regime_name} (Score: {mandate.score.composite:.2f})")
        
        if not mandate.is_trade_allowed:
            logger.info(f"Trade not allowed: {mandate.veto_reasons}")
            telegram.send(f"Trade Blocked: {', '.join(mandate.veto_reasons)}", "WARNING")
            return None
            
        if mandate.max_lots == 0:
            logger.info("Mandate is CASH - no trade executed")
            return None
            
        if circuit_breaker.is_active():
            logger.warning("Circuit breaker active - trade blocked")
            telegram.send("Trade blocked by circuit breaker", "WARNING")
            return None
            
        if not circuit_breaker.check_daily_trade_limit():
            logger.warning("Daily trade limit reached - trade blocked")
            telegram.send("Daily trade limit reached", "WARNING")
            return None
        
        legs, calculated_max_risk = self.strategy_factory.generate(
            mandate, chain, analysis['lot_size'], vol_metrics, vol_metrics.spot, struct_metrics
        )
        
        if not legs:
            logger.error("Failed to generate valid strategy legs")
            telegram.send("Strategy generation failed", "ERROR")
            return None
            
        estimated_greeks = self._estimate_position_greeks(legs)
        estimated_greeks['margin'] = self.execution_engine.check_margin_requirement(legs)
        estimated_greeks['max_risk'] = calculated_max_risk
        
        is_allowed, messages = self.correlation_manager.check_correlation(
            legs, 
            mandate.expiry_date,
            mandate.strategy_type,
            estimated_greeks,
            vol_metrics.spot
        )
        
        if not is_allowed:
            logger.warning("CORRELATION BLOCK")
            for msg in messages:
                logger.warning(msg)
            
            telegram.send(f"Trade Blocked - Correlation Rules\n" + "\n".join(messages), "WARNING")
            return None
        
        for msg in messages:
            if "⚠️" in msg:
                logger.info(msg)
        
        logger.info(f"Generated {len(legs)} legs. Exact Max Risk: ₹{calculated_max_risk:,.2f}")
        
        filled_legs = self.execution_engine.execute_strategy(legs)
        if not filled_legs:
            logger.error("Strategy execution failed")
            telegram.send("Execution failed - no position opened", "ERROR")
            return None
            
        trade_id = f"VG33_LIVE_{int(datetime.now().timestamp())}"
        self.current_trade_id = trade_id
        
        entry_premium = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'SELL')
        entry_debit   = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'BUY')
        net_premium = entry_premium - entry_debit
        
        db_writer.save_trade(trade_id, mandate.strategy_type, mandate.expiry_date, filled_legs, net_premium, calculated_max_risk)
        
        actual_greeks = self.greeks_manager.get_portfolio_greeks(filled_legs, trade_id)
        actual_greeks['margin'] = estimated_greeks['margin']
        actual_greeks['max_risk'] = calculated_max_risk
        
        self.correlation_manager.register_position(
            filled_legs,
            trade_id,
            mandate.expiry_date,
            mandate.strategy_type,
            actual_greeks
        )
        
        portfolio = self.correlation_manager.get_portfolio_summary()
        logger.info(f"Portfolio: {portfolio['position_count']} positions | Delta:{portfolio['total_delta']:.1f} | Theta:{portfolio['total_theta']:.0f} | Vega:{portfolio['total_vega']:.0f}")
        
        gtt_ids = []
        short_legs = [l for l in filled_legs if l['side'] == 'SELL']
        if short_legs:
            logger.info(f"Setting up GTT orders for {len(short_legs)} short legs...")
            
            for leg in short_legs:
                sl_price = leg['entry_price'] * 2.0
                target_price = leg['entry_price'] * 0.30
                
                gtt_id = self.execution_engine.place_gtt_order(
                    leg['key'], leg['filled_qty'], 'BUY', 
                    round(sl_price, 1), round(target_price, 1)
                )
                
                if gtt_id:
                    gtt_ids.append(gtt_id)
                else:
                    logger.critical(f"GTT placement failed for {leg['key']}")
            
            if len(gtt_ids) != len(short_legs):
                logger.critical(f"PARTIAL GTT FAILURE: {len(gtt_ids)}/{len(short_legs)} GTTs placed")
                telegram.send(f"CRITICAL: Only {len(gtt_ids)}/{len(short_legs)} GTTs placed. FLATTENING POSITION FOR SAFETY", "CRITICAL")
                
                for gtt_id in gtt_ids:
                    self.execution_engine.cancel_gtt_order(gtt_id)
                
                self.execution_engine._flatten_legs(filled_legs)
                self.correlation_manager.remove_position(trade_id)
                
                db_writer.log_risk_event(
                    "GTT_PLACEMENT_FAILURE",
                    "CRITICAL",
                    f"Partial GTT failure: {len(gtt_ids)}/{len(short_legs)}",
                    "Position flattened for safety"
                )
                
                return None
        
        risk_manager = RiskManager(
            self.api_client,
            filled_legs,
            mandate.expiry_date,
            trade_id,
            gtt_ids
        )

        risk_manager.correlation_manager = self.correlation_manager
        risk_manager.orchestrator = self
        self.active_risk_managers.append(risk_manager)

        risk_thread = threading.Thread(
            target=risk_manager.monitor,
            daemon=True,
            name=f"Risk-{trade_id}"
        )
        risk_thread.start()
        
        logger.info(f"Trade {trade_id} opened successfully")
        return trade_id

    def _estimate_position_greeks(self, legs: List[Dict]) -> Dict:
        estimated = {
            'delta': 0.0,
            'theta': 0.0,
            'gamma': 0.0,
            'vega': 0.0
        }
        
        for leg in legs:
            mult = -1 if leg['side'] == 'SELL' else 1
            estimated['delta'] += leg.get('delta', 0) * leg['qty'] * mult
            estimated['theta'] += -50 * leg['qty'] * mult
            estimated['vega'] += -100 * leg['qty'] * mult
        
        return estimated
    
    def get_portfolio_greeks(self) -> Dict:
        portfolio = {
            'delta': 0.0,
            'theta': 0.0,
            'gamma': 0.0,
            'vega': 0.0,
            'position_count': 0
        }
        
        for rm in self.active_risk_managers:
            if rm.running:
                greeks = self.greeks_manager.get_portfolio_greeks(rm.legs, rm.trade_id)
                portfolio['delta'] += greeks['delta']
                portfolio['theta'] += greeks['theta']
                portfolio['gamma'] += greeks['gamma']
                portfolio['vega'] += greeks['vega']
                portfolio['position_count'] += 1
        
        return portfolio

    def run_auto_mode(self):
        logger.info("VOLGUARD AWS SCHEDULER STARTED")
        telegram.send("Bot active on AWS.", "SYSTEM")
        
        self.greeks_manager.start()
        logger.info("Live Greeks WebSocket started")
        telegram.send("Live Greeks monitoring active", "SYSTEM")
        
        atexit.register(self.greeks_manager.stop)
        
        if not self.session_manager.validate_session(force=True):
            logger.error("Initial Session Check Failed")

        TIME_BRIEF = dtime(8, 45)
        TIME_OPEN  = dtime(9, 15)
        TIME_CLOSE = dtime(15, 30)

        while True:
            try:
                now_ist = datetime.now(ProductionConfig.IST)
                curr_time = now_ist.time()
                today_date = now_ist.date()
                heartbeat.beat()

                if today_date.weekday() >= 5:
                    if now_ist.minute == 0 and now_ist.second == 0: logger.info("Weekend.")
                    time.sleep(60)
                    continue

                if not self.check_market_open_status(today_date):
                    if now_ist.minute == 0 and now_ist.second == 0: logger.info("Holiday.")
                    time.sleep(60)
                    continue

                if curr_time >= TIME_BRIEF and self.last_brief_date != today_date:
                    self.run_morning_brief()

                if TIME_OPEN <= curr_time <= TIME_CLOSE:
                    
                    current_30_block = (now_ist.hour, now_ist.minute // 30)
                    if now_ist.minute % 30 == 0 and self.last_analysis_time != current_30_block:
                        logger.info(f"Scheduled Analysis: {curr_time.strftime('%H:%M')}")
                        analysis = self.run_analysis()
                        if analysis: self.execute_best_mandate(analysis)
                        self.last_analysis_time = current_30_block

                    if now_ist.minute % 30 == 0 and now_ist.second == 0:
                         if not self.session_manager.validate_session():
                             telegram.send("Session Expired.", "WARNING")
                             if not self.session_manager.validate_session(force=True): break

                    if now_ist.minute % 5 == 0 and now_ist.second == 0:
                         self.reconciliation.reconcile()

                    time.sleep(1)
                else:
                    time.sleep(10)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Loop Error: {e}")
                time.sleep(60)

# FastAPI Application
app = FastAPI(title="VolGuard 3.3 API", version="3.3.0")

# Global orchestrator instance
orchestrator: Optional[TradingOrchestrator] = None
trading_thread: Optional[threading.Thread] = None
trading_running = False

class TradeRequest(BaseModel):
    force: bool = False

class AnalysisResponse(BaseModel):
    status: str
    weekly_regime: Optional[str] = None
    monthly_regime: Optional[str] = None
    weekly_score: Optional[float] = None
    monthly_score: Optional[float] = None
    veto_reasons: List[str] = []
    timestamp: Optional[str] = None

@app.on_event("startup")
async def startup_event():
    """Initialize configuration on startup"""
    try:
        ProductionConfig.validate()
        logger.info("FastAPI startup: Configuration validated")
    except Exception as e:
        logger.critical(f"Configuration error: {e}")
        raise

@app.get("/")
async def root():
    return {
        "service": "VolGuard 3.3",
        "status": "active",
        "trading_active": trading_running,
        "timestamp": datetime.now(ProductionConfig.IST).isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for AWS load balancers"""
    is_healthy = heartbeat.check() if hasattr(heartbeat, 'last_heartbeat') else True
    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "timestamp": datetime.now(ProductionConfig.IST).isoformat(),
        "circuit_breaker": circuit_breaker.is_active(),
        "trading_running": trading_running
    }

@app.post("/trading/start")
async def start_trading(background_tasks: BackgroundTasks):
    """Start the trading loop in background"""
    global orchestrator, trading_thread, trading_running
    
    if trading_running:
        raise HTTPException(status_code=400, detail="Trading already running")
    
    try:
        orchestrator = TradingOrchestrator()
        trading_running = True
        trading_thread = threading.Thread(target=orchestrator.run_auto_mode, daemon=True)
        trading_thread.start()
        
        telegram.send("Trading started via API", "SYSTEM")
        return {"status": "started", "timestamp": datetime.now(ProductionConfig.IST).isoformat()}
    except Exception as e:
        trading_running = False
        logger.error(f"Failed to start trading: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trading/stop")
async def stop_trading():
    """Stop trading loop gracefully"""
    global trading_running, orchestrator
    
    if not trading_running:
        raise HTTPException(status_code=400, detail="Trading not running")
    
    try:
        trading_running = False
        if orchestrator:
            # Signal orchestrator to stop via cleanup
            orchestrator._cleanup_handler()
        
        telegram.send("Trading stopped via API", "SYSTEM")
        return {"status": "stopped", "timestamp": datetime.now(ProductionConfig.IST).isoformat()}
    except Exception as e:
        logger.error(f"Error stopping trading: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analysis/run", response_model=AnalysisResponse)
async def run_analysis_endpoint():
    """Run market analysis on demand"""
    global orchestrator
    
    try:
        if not orchestrator:
            # Create temporary orchestrator just for analysis
            temp_orch = TradingOrchestrator()
            analysis = temp_orch.run_analysis()
        else:
            analysis = orchestrator.run_analysis()
        
        if analysis:
            return AnalysisResponse(
                status="success",
                weekly_regime=analysis['weekly_mandate'].regime_name,
                monthly_regime=analysis['monthly_mandate'].regime_name,
                weekly_score=analysis['weekly_mandate'].score.composite,
                monthly_score=analysis['monthly_mandate'].score.composite,
                veto_reasons=analysis['weekly_mandate'].veto_reasons + analysis['monthly_mandate'].veto_reasons,
                timestamp=analysis['timestamp'].isoformat()
            )
        else:
            return AnalysisResponse(status="failed", veto_reasons=["Analysis returned no data"])
            
    except Exception as e:
        logger.error(f"Analysis endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/morning-brief")
async def trigger_morning_brief():
    """Trigger morning brief manually"""
    global orchestrator
    
    try:
        if orchestrator:
            orchestrator.run_morning_brief()
        else:
            temp_orch = TradingOrchestrator()
            temp_orch.run_morning_brief()
        
        return {"status": "brief_triggered", "timestamp": datetime.now(ProductionConfig.IST).isoformat()}
    except Exception as e:
        logger.error(f"Morning brief error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/portfolio/status")
async def portfolio_status():
    """Get current portfolio status"""
    global orchestrator
    
    if not orchestrator:
        return {"status": "no_orchestrator", "positions": []}
    
    try:
        portfolio = orchestrator.correlation_manager.get_portfolio_summary()
        greeks = orchestrator.get_portfolio_greeks()
        
        return {
            "status": "active",
            "positions": portfolio,
            "greeks": greeks,
            "active_risk_managers": len(orchestrator.active_risk_managers),
            "circuit_breaker_active": circuit_breaker.is_active(),
            "daily_stats": db_writer.get_daily_stats()
        }
    except Exception as e:
        logger.error(f"Portfolio status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/positions/exit-all")
async def exit_all_positions():
    """Emergency exit all positions"""
    global orchestrator
    
    if not orchestrator:
        raise HTTPException(status_code=400, detail="No orchestrator running")
    
    try:
        for rm in orchestrator.active_risk_managers[:]:
            if rm.running:
                rm.flatten_all("API_EMERGENCY_EXIT")
        
        return {"status": "exit_triggered", "timestamp": datetime.now(ProductionConfig.IST).isoformat()}
    except Exception as e:
        logger.error(f"Exit all error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/market/status")
async def market_status():
    """Check if market is open"""
    global orchestrator
    
    try:
        if not orchestrator:
            temp_orch = TradingOrchestrator()
            is_open = temp_orch.check_market_open_status(date.today())
        else:
            is_open = orchestrator.check_market_open_status(date.today())
        
        return {
            "market_open": is_open,
            "date": str(date.today()),
            "time": datetime.now(ProductionConfig.IST).strftime("%H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Market status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Main entry point for direct execution (backward compatibility)
def main():
    import argparse
    parser = argparse.ArgumentParser(description="VOLGUARD 3.3 - FastAPI Edition")
    parser.add_argument('--mode', choices=['api', 'standalone'], default='api', help='Run mode')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='API host')
    parser.add_argument('--port', type=int, default=8000, help='API port')
    parser.add_argument('--export-journal', type=str, help='Export trade journal to directory')
    args = parser.parse_args()

    print("=" * 80)
    print("VOLGUARD 3.3 - FASTAPI EDITION")
    print("Refactored: No Prometheus, No Multiprocessing, Synchronous DB")
    print("Preserved: AI Morning Brief, Event Radar, Market Intelligence")
    print("=" * 80)

    if args.export_journal:
        os.makedirs(args.export_journal, exist_ok=True)
        if db_writer.export_trade_journal(args.export_journal):
            print(f"Trade journal exported to {args.export_journal}")
        else:
            print("Export failed")
        return

    try:
        ProductionConfig.validate()
        logger.info("Configuration validated")
    except Exception as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)

    db_writer.set_state("system_version", "3.3-FASTAPI")
    db_writer.set_state("startup_time", datetime.now().isoformat())
    logger.info("Database initialized (Synchronous Mode)")

    telegram.send(
        f"System Startup\n"
        f"Version: 3.3 FastAPI (Refactored)\n"
        f"Mode: {args.mode.upper()}\n"
        f"Environment: {ProductionConfig.ENVIRONMENT}",
        "SUCCESS"
    )

    if args.mode == 'standalone':
        # Run traditional standalone mode
        orchestrator = TradingOrchestrator()
        try:
            orchestrator.run_auto_mode()
        except Exception as e:
            logger.critical(f"Unhandled exception: {e}")
            traceback.print_exc()
            telegram.send(f"System crashed: {str(e)}", "CRITICAL")
            sys.exit(1)
        finally:
            logger.info("System shutdown sequence initiated")
            heartbeat.stop()
            telegram.send("System shutdown complete", "SYSTEM")
            logger.info("Goodbye.")
    else:
        # Run FastAPI server
        logger.info(f"Starting FastAPI server on {args.host}:{args.port}")
        uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
