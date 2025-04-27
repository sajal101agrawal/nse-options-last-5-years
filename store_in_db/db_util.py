# db_util.py – resilient bulk writer for option_metrics      © Sajal Tech 2025
# ═════════════════════════════════════════════════════════════════════════════
"""
• Reads connection details from either **PG_DSN** or individual vars
  (PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SSLMODE).
• Replaces every NaN / ±Inf in JSON payloads with **null** so PostgreSQL’s
  json/jsonb types never reject a batch.
• Retries writes with exponential back-off.
"""

from __future__ import annotations

import json, logging, math, os, time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, List, Tuple

import psycopg2
from psycopg2 import pool, OperationalError, InterfaceError
from psycopg2.extras import Json, execute_batch
from dotenv import load_dotenv

load_dotenv()

# ────────────────────────────────────────────────────────────────────── DSN / pool
def _dsn() -> str:
    if full := os.getenv("PG_DSN"):
        return full                                                 # highest-level
    return (
        "postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"

    ).format(
        db=os.getenv("PG_DB", "historical_data"),
        user=os.getenv("PG_USER", "postgres"),
        pwd=os.getenv("PG_PASSWORD", ""),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", 5432),
        ssl=os.getenv("PG_SSLMODE", "require"),
    )


_POOL: pool.SimpleConnectionPool | None = None


def _pool() -> pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=2,
            dsn=_dsn(),
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
    return _POOL


@contextmanager
def _get_conn():
    conn = _pool().getconn()
    try:
        yield conn
    except (OperationalError, InterfaceError):
        conn.close()
        conn = psycopg2.connect(_dsn())
        yield conn
    finally:
        try:
            _pool().putconn(conn)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────── sanitiser
def _clean(obj: Any) -> Any:
    """Recursively replace NaN / ±Inf with None to produce JSON-safe payloads."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(x) for x in obj]
    return obj


# ─────────────────────────────────────────────────────────────────────── writer
class DBWriter:
    FLUSH_EVERY = 2_000
    RETRY_DELAY = 5        # s

    __slots__ = ("_buffer", "_total")

    def __init__(self):
        self._buffer: List[Tuple[Any, ...]] = []
        self._total: int = 0

    # — context —──────────────────────────────────────────────────────────────
    def __enter__(self):
        self._buffer = getattr(self, "_buffer", [])
        self._total = getattr(self, "_total", 0)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # — helpers —──────────────────────────────────────────────────────────────
    @staticmethod
    def _safe_date(s: str | None):
        try:
            return datetime.strptime(s, "%d-%b-%Y").date() if s else None
        except Exception:
            return None

    # — public API —───────────────────────────────────────────────────────────
    def write_row(self, symbol: str, date_key: str, data: dict) -> None:
        row = (
            symbol,
            datetime.strptime(date_key, "%d-%b-%Y").date(),
            data.get("underlying_price"),
            data.get("interest_rate"),
            data.get("strike_price"),
            self._safe_date(data.get("expiry_30d")),
            self._safe_date(data.get("expiry_60d")),
            self._safe_date(data.get("expiry_90d")),
            self._safe_date(data.get("upcoming_earning_date")),
            data.get("rv_yz"),
            Json(_clean(data.get("ce")           or {})),
            Json(_clean(data.get("pe")           or {})),
            Json(_clean(data.get("option_chain") or [])),
            Json(_clean(data.get("extras")       or {})),
        )

        self._buffer.append(row)
        if len(self._buffer) >= self.FLUSH_EVERY:
            self.flush()

    # — flush —───────────────────────────────────────────────────────────────
    def flush(self):
        if not self._buffer:
            return

        sql = """
        INSERT INTO option_metrics (
            symbol,date,underlying_price,interest_rate,strike_price,
            expiry_30d,expiry_60d,expiry_90d,upcoming_earning_date,
            rv_yz,ce,pe,option_chain,extras
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol,date) DO UPDATE SET
            underlying_price      = EXCLUDED.underlying_price,
            interest_rate         = EXCLUDED.interest_rate,
            strike_price          = EXCLUDED.strike_price,
            expiry_30d            = EXCLUDED.expiry_30d,
            expiry_60d            = EXCLUDED.expiry_60d,
            expiry_90d            = EXCLUDED.expiry_90d,
            upcoming_earning_date = EXCLUDED.upcoming_earning_date,
            rv_yz                 = EXCLUDED.rv_yz,
            ce                    = EXCLUDED.ce,
            pe                    = EXCLUDED.pe,
            option_chain          = EXCLUDED.option_chain,
            extras                = EXCLUDED.extras;
        """

        tries = 0
        while True:
            try:
                with _get_conn() as conn, conn.cursor() as cur:
                    execute_batch(cur, sql, self._buffer, page_size=1400)
                    conn.commit()
                break
            except OperationalError as err:
                tries += 1
                wait = self.RETRY_DELAY * 2 ** (tries - 1)
                logging.warning("batch-insert failed (%s) – retry in %ss", err, wait)
                time.sleep(wait)

        self._total += len(self._buffer)
        self._buffer.clear()

    # — close —───────────────────────────────────────────────────────────────
    def close(self):
        self.flush()
        logging.info("rows written by writer: %s", self._total)
