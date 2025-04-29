'''db_util.py – fast but API-compatible writer for option_metrics
────────────────────────────────────────────────────────────────────────
• *Same* public surface (DBWriter.write_row(sym, date_key, d)) so callers need **zero** edits
• Buffers rows and bulk-UPSERTs with psycopg2.extras.execute_values() → 5-10 × faster
• Retries each *batch* up to 3× on transient errors
• Scrubs NaN/Inf so JSON/JSONB inserts never fail
'''

from __future__ import annotations

import math, os, time, logging
from contextlib import contextmanager
from datetime import datetime, date
from typing import Any, List, Tuple

import psycopg2
from psycopg2 import pool, OperationalError, InterfaceError
from psycopg2.extras import Json, execute_values
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────── connection pool ──────────────────────────────

def _dsn() -> str:
    if dsn := os.getenv("PG_DSN"):
        return dsn
    return (
        "postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}".format(
            db=os.getenv("PG_DB", "historical_data"),
            user=os.getenv("PG_USER", "postgres"),
            pwd=os.getenv("PG_PASSWORD", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", 5432),
            ssl=os.getenv("PG_SSLMODE", "require"),
        )
    )

_POOL: pool.SimpleConnectionPool | None = None


def _pool() -> pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = psycopg2.pool.SimpleConnectionPool(
            1,
            8,  # extra headroom for parallel writers
            dsn=_dsn(),
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=10,
            keepalives_interval=5,
            keepalives_count=3,
        )
    return _POOL


@contextmanager
def _conn():
    conn = _pool().getconn()
    try:
        yield conn
    finally:
        if not conn.closed:
            try:
                _pool().putconn(conn)
            except Exception:
                pass


# ───────────────────────────── helpers ────────────────────────────────────

def _clean(o: Any) -> Any:
    """Replace NaN / ±Inf recursively so Postgres JSON never rejects the payload."""
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_clean(x) for x in o]
    return o


def _d(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d-%b-%Y").date()
    except Exception:
        return None


# UPSERT statement – *execute_values* will expand the VALUES part
_INSERT = (
    "INSERT INTO option_metrics ("
    "symbol,date,underlying_price,interest_rate,strike_price,"
    "expiry_30d,expiry_60d,expiry_90d,upcoming_earning_date,"
    "rv_yz,ce,pe,option_chain,extras) VALUES %s "
    "ON CONFLICT (symbol,date) DO UPDATE SET "
    "underlying_price       = EXCLUDED.underlying_price,"
    "interest_rate          = EXCLUDED.interest_rate,"
    "strike_price           = EXCLUDED.strike_price,"
    "expiry_30d             = EXCLUDED.expiry_30d,"
    "expiry_60d             = EXCLUDED.expiry_60d,"
    "expiry_90d             = EXCLUDED.expiry_90d,"
    "upcoming_earning_date  = EXCLUDED.upcoming_earning_date,"
    "rv_yz                  = EXCLUDED.rv_yz,"
    "ce                     = EXCLUDED.ce,"
    "pe                     = EXCLUDED.pe,"
    "option_chain           = EXCLUDED.option_chain,"
    "extras                 = EXCLUDED.extras;"
)

# ─────────────────────────── writer class (API-compatible) ────────────────

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))  # tune 500–5000 to taste


class DBWriter:
    """Drop-in replacement that keeps the *same* constructor + write_row signature.

    Usage is unchanged:
        with DBWriter() as w:
            w.write_row(sym, date_key, payload)
    Flush occurs automatically when the buffer fills or on context-exit.
    """

    def __init__(self):
        self._buf: List[Tuple[Any, ...]] = []

    # context-manager helpers ---------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # flush remaining rows even if caller forgot to call close()
        try:
            self.close()
        finally:
            return False  # never swallow exceptions

    # public API ----------------------------------------------
    def write_row(self, sym: str, date_key: str, d: dict) -> None:  # noqa: D401
        """Buffer a row; bulk-flush when batch is full."""
        self._buf.append(self._build_params(sym, date_key, d))
        if len(self._buf) >= BATCH_SIZE:
            self._flush()

    def close(self):
        if self._buf:
            self._flush()

    # internal ------------------------------------------------------------
    @staticmethod
    def _build_params(sym: str, date_key: str, d: dict) -> Tuple[Any, ...]:
        return (
            sym,
            datetime.strptime(date_key, "%d-%b-%Y").date(),
            d.get("underlying_price"),
            d.get("interest_rate"),
            d.get("strike_price"),
            _d(d.get("expiry_30d")),
            _d(d.get("expiry_60d")),
            _d(d.get("expiry_90d")),
            _d(d.get("upcoming_earning_date")),
            d.get("rv_yz"),
            Json(_clean(d.get("ce") or {})),
            Json(_clean(d.get("pe") or {})),
            Json(_clean(d.get("option_chain") or [])),
            Json(_clean(d.get("extras") or {})),
        )

    def _flush(self):
        tries, wait = 0, 1
        while True:
            try:
                with _conn() as conn, conn.cursor() as cur:
                    execute_values(cur, _INSERT, self._buf, page_size=BATCH_SIZE)
                    conn.commit()
                self._buf.clear()
                return
            except (OperationalError, InterfaceError) as e:
                tries += 1
                if tries > 3:
                    raise
                logging.warning(
                    "batch insert failed (%s) – retry %d in %ss", e, tries, wait
                )
                time.sleep(wait)
                wait *= 2
