# db_utill.py  – utilities for writing option_metrics          ☰ Sajal Tech ©2025
# ───────────────────────────────────────────────────────────────────────────────
import os, time, logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Tuple, List

import psycopg2
from psycopg2 import pool, OperationalError, InterfaceError
from psycopg2.extras import Json, execute_batch
from dotenv import load_dotenv

load_dotenv()

_PG_DSN = (
    "postgres://{user}:{pwd}@{host}:{port}/{db}"
).format(
    user=os.getenv("PG_USER", "postgres"),
    pwd=os.getenv("PG_PASSWORD", ""),
    host=os.getenv("PG_HOST", "localhost"),
    port=os.getenv("PG_PORT", 5432),
    db=os.getenv("PG_DB", "historical-data"),
)

_POOL = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=4,
    dsn=_PG_DSN,
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
)

@contextmanager
def _get_conn():
    conn = _POOL.getconn()
    try:
        yield conn
    except (OperationalError, InterfaceError) as e:
        logging.warning("DB connection dropped, recycling: %s", e)
        conn.close()
        conn = _POOL._connect()
    finally:
        _POOL.putconn(conn)

# ───────────────────────────────────────────────────────────────────────────────
class DBWriter:
    """
    Buffers complete rows and flushes them in bulk.  A row is *never* half-filled:
    every column defined in the table is supplied on first insert.
    """
    FLUSH_EVERY = 2_000          # rows per batch
    RETRY_DELAY = 5              # s (exponential back-off base)

    __slots__ = ("_buffer", "_total")

    def __init__(self):
        self._buffer: List[Tuple[Any, ...]] = []
        self._total  = 0

    # ── public API ────────────────────────────────────────────────────────────
    def write_row(self, symbol: str, date_key: str, data: dict) -> None:
        """
        Queue a *complete* option-metrics record for (symbol, date).
        `data` is the payload produced by process_and_store.py.
        """
        date_obj = datetime.strptime(date_key, "%d-%b-%Y").date()

        # build row tuple in the *exact* column order used in INSERT below
        row = (
            symbol,
            date_obj,
            data.get("underlying_price"),
            data.get("interest_rate"),
            data.get("strike_price"),
            _safe_date(data.get("expiry_30d")),
            _safe_date(data.get("expiry_60d")),
            _safe_date(data.get("expiry_90d")),
            _safe_date(data.get("upcoming_earning_date")),  # may be None
            data.get("rv_yz"),
            Json(data.get("ce")            or {}),
            Json(data.get("pe")            or {}),
            Json(data.get("option_chain")  or []),
            Json(data.get("extras")        or {}),
        )

        self._buffer.append(row)
        if len(self._buffer) >= self.FLUSH_EVERY:
            self.flush()

    def flush(self) -> None:
        """Write any buffered rows to PostgreSQL."""
        if not self._buffer:
            return

        sql = """
            INSERT INTO option_metrics (
                symbol, date, underlying_price, interest_rate,
                strike_price, expiry_30d, expiry_60d, expiry_90d,
                upcoming_earning_date, rv_yz, ce, pe, option_chain, extras
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (symbol, date) DO UPDATE SET
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

        tries, done = 0, False
        while not done:
            try:
                with _get_conn() as conn:
                    with conn.cursor() as cur:
                        execute_batch(cur, sql, self._buffer, page_size=1000)
                    conn.commit()
                done = True
            except OperationalError as err:
                tries += 1
                wait = self.RETRY_DELAY * 2 ** (tries - 1)
                logging.error("DB batch write failed (%s) – retry in %ss", err, wait)
                time.sleep(wait)

        self._total += len(self._buffer)
        self._buffer.clear()

    def close(self) -> None:
        self.flush()
        logging.info("Total rows written: %s", self._total)

# ───────────────────────────────────────────────────────────────────────────────
def _safe_date(s: str):
    """Return date or None for bad / missing strings."""
    try:
        return datetime.strptime(s, "%d-%b-%Y").date() if s else None
    except Exception:
        return None
