#!/usr/bin/env python3
# store_s.py – robust symbol‑wise ETL into PostgreSQL           © Sajal Tech 2025
# ═══════════════════════════════════════════════════════════════════════════════
"""
    $ python store_s.py
"""
from __future__ import annotations

import json, logging, os, sys, time
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from pandas.errors import ParserError
from db_util import DBWriter

sys.path.append(str(Path(__file__).resolve().parents[1]))      # project root

from process_data import (                               # noqa: E402
    black_scholes_greeks,
    implied_volatility_bisection,
    compute_yz_rolling_vol,
    get_rate_with_fallback,
    pick_strike_nearest_underlying,      # ← already patched to pick monthly expiries
    get_time_to_expiry_in_years,
)

# ───────────────────────── global constants ────────────────────────────────
WINDOW, MAX_LOOKBACK = 30, 90
MAX_WORKERS = min(8, os.cpu_count() or 4)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# ensure project‑root imports work
sys.path.append(str(Path(__file__).resolve().parents[1]))

# ───────────────────────── helper utilities ────────────────────────────────
def _safe_T(date_key: str, expiry: str | None) -> float:
    return get_time_to_expiry_in_years(date_key, expiry) if expiry else 0.0


def _load_bhavcopy(fp: Path) -> pd.DataFrame | None:
    """Try reading with 0/2 skipped rows to handle header variants."""
    for skip in (0, 2):
        try:
            df = pd.read_csv(fp, skiprows=skip)
            if "SYMBOL" in df.columns:
                return df
        except (ParserError, OSError):
            pass
    logging.warning("skip corrupt bhavcopy: %s", fp.name)
    return None


def _iv_safe(*args, **kwargs) -> float:
    """Implied vol helper that never raises & always returns pct."""
    try:
        iv = implied_volatility_bisection(*args, **kwargs)
        return iv * 100 if np.isfinite(iv) and iv > 0 else 0.0
    except Exception:
        return 0.0


# ═════════════════════════ main worker ═════════════════════════════════════
def _process_symbol(
    sym: str,
    bhav_files: list[Path],
    spot_price_map: Dict[str, Dict[str, float]],
    DAILY_RATE: Dict[datetime.date, float],
    earnings_map: Dict[str, List[datetime]],
) -> None:
    logging.info("┌─ processing %-10s", sym)
    time_map: Dict[str, dict] = {}
    daily_ohlc: List[dict] = []

    for fp in bhav_files:
        date_obj = datetime.strptime(fp.name[2:-8], "%d%b%Y")
        date_key = date_obj.strftime("%d-%b-%Y")

        if len(time_map) % 100 == 0 and len(time_map) > 0:
            logging.info("... %s rows accumulated for %s", len(time_map), sym)

        df = _load_bhavcopy(fp)
        if df is None:
            continue
        df_sym = df[df["SYMBOL"] == sym]
        if df_sym.empty:
            continue

        spot = spot_price_map.get(sym, {}).get(date_key)
        if spot is None or not np.isfinite(spot):
            continue

        rate = get_rate_with_fallback(date_obj.date(), DAILY_RATE)
        r_dec = (rate or 0) / 100.0

        # ───────── FUTURES (for OHLC + expiries) ─────────
        fut = (
            df_sym[df_sym["INSTRUMENT"].isin(["FUTSTK", "FUTIDX"])]
            .assign(EXPIRY_DT=lambda d: pd.to_datetime(
                d["EXPIRY_DT"], format="%d-%b-%Y", errors="coerce"))
            .sort_values("EXPIRY_DT")
        )
        if not fut.empty:
            fr = fut.iloc[0]
            daily_ohlc.append(
                dict(Date=date_obj,
                     Open=float(fr["OPEN"]), High=float(fr["HIGH"]),
                     Low=float(fr["LOW"]),  Close=float(fr["CLOSE"]))
            )

        rec = time_map.setdefault(
            date_key,
            dict(
                underlying_price=spot, interest_rate=rate,
                upcoming_earning_date=None,
                expiry_30d=None, expiry_60d=None, expiry_90d=None,
                rv_yz=None, strike_price=None,
                ce={}, pe={}, option_chain=[],
            ),
        )

        fut_exp = sorted(fut["EXPIRY_DT"].dropna().unique())
        if len(fut_exp) > 0:
            rec["expiry_30d"] = fut_exp[0].strftime("%d-%b-%Y")
        if len(fut_exp) > 1:
            rec["expiry_60d"] = fut_exp[1].strftime("%d-%b-%Y")
        if len(fut_exp) > 2:
            rec["expiry_90d"] = fut_exp[2].strftime("%d-%b-%Y")

        # ───────── OPTIONS (chain + IV/Greeks) ─────────
        opts = df_sym[df_sym["INSTRUMENT"].isin(["OPTSTK", "OPTIDX"])].copy()
        if opts.empty:
            continue
        opts["EXPIRY_DT"] = pd.to_datetime(
            opts["EXPIRY_DT"], format="%d-%b-%Y", errors="coerce"
        )

        # full chain snapshot – useful later
        for _, row in opts.iterrows():
            expiry = pd.to_datetime(row["EXPIRY_DT"]).strftime("%d-%b-%Y")
            T_row = _safe_T(date_key, expiry)
            iv = _iv_safe(
                market_price=float(row["SETTLE_PR"]), S=float(spot),
                K=float(row["STRIKE_PR"]), T=T_row, r=r_dec,
                is_call=(row["OPTION_TYP"] == "CE"),
            )
            delta = 0.0
            if T_row > 0 and iv > 0:
                try:
                    delta = black_scholes_greeks(
                        float(spot), float(row["STRIKE_PR"]),
                        T_row, r_dec, iv/100,
                        is_call=(row["OPTION_TYP"] == "CE"),
                    )["delta"]
                except Exception:
                    pass

            rec["option_chain"].append(
                dict(expiry=expiry, strike=float(row["STRIKE_PR"]),
                     type=row["OPTION_TYP"], settle=float(row["SETTLE_PR"]),
                     open=float(row["OPEN"]),   high=float(row["HIGH"]),
                     low=float(row["LOW"]),     close=float(row["CLOSE"]),
                     volume=int(row["CONTRACTS"]), iv=iv, delta=delta)
            )

        picked = pick_strike_nearest_underlying(spot, opts)
        if not picked:
            continue
        ce30, ce60, ce90, pe30, pe60, pe90, strike = picked
        if any(x is None for x in (ce30, ce60, ce90, pe30, pe60, pe90)):
            continue  # incomplete chain

        rec["strike_price"] = strike

        T30 = _safe_T(date_key, rec["expiry_30d"])
        T60 = _safe_T(date_key, rec["expiry_60d"])
        T90 = _safe_T(date_key, rec["expiry_90d"])

        def _iv(row, T, call): return _iv_safe(
            market_price=float(row["SETTLE_PR"]), S=float(spot),
            K=float(strike), T=T, r=r_dec, is_call=call
        )

        iv_ce30 = _iv(ce30, T30, True)
        iv_pe30 = _iv(pe30, T30, False)

        rec["ce"] = dict(
            iv_30=iv_ce30, iv_60=_iv(ce60, T60, True), iv_90=_iv(ce90, T90, True),
            volume=int(opts[opts["OPTION_TYP"] == "CE"]["CONTRACTS"].sum()),
            last_price_30d=float(ce30["SETTLE_PR"]),
            close=float(ce30["CLOSE"]), open=float(ce30["OPEN"]),
            high=float(ce30["HIGH"]),   low=float(ce30["LOW"]),
            ivp=None, ivr=None,
            greeks=black_scholes_greeks(
                float(spot), float(strike), T30, r_dec,
                iv_ce30/100 if iv_ce30 else 0.0, True,
            ) if T30 and iv_ce30 else {},
        )

        rec["pe"] = dict(
            iv_30=iv_pe30, iv_60=_iv(pe60, T60, False), iv_90=_iv(pe90, T90, False),
            volume=int(opts[opts["OPTION_TYP"] == "PE"]["CONTRACTS"].sum()),
            last_price_30d=float(pe30["SETTLE_PR"]),
            close=float(pe30["CLOSE"]), open=float(pe30["OPEN"]),
            high=float(pe30["HIGH"]),   low=float(pe30["LOW"]),
            ivp=None, ivr=None,
            greeks=black_scholes_greeks(
                float(spot), float(strike), T30, r_dec,
                iv_pe30/100 if iv_pe30 else 0.0, False,
            ) if T30 and iv_pe30 else {},
        )

    # ───────── realized volatility (YZ) ─────────
    if daily_ohlc:
        yz = compute_yz_rolling_vol(
            pd.DataFrame(daily_ohlc),
            window=WINDOW, max_lookback=MAX_LOOKBACK, trading_periods=252,
        )
        for idx, rv in yz.items():
            d_str = idx.strftime("%d-%b-%Y")
            if d_str in time_map and np.isfinite(rv):
                time_map[d_str]["rv_yz"] = rv

    # ───────── upcoming earnings ─────────
    if future := earnings_map.get(sym):
        fut_sorted = sorted(future)
        for d_str, rec in time_map.items():
            now = datetime.strptime(d_str, "%d-%b-%Y")
            nxt = next((dt for dt in fut_sorted if dt > now), None)
            if nxt:
                rec["upcoming_earning_date"] = nxt.strftime("%d-%b-%Y")

    # ───────── IV percentile / rank ─────────
    def _iv_stats(rows: List[tuple[datetime, float]]):
        if len(rows) < WINDOW:
            return {}
        df = pd.DataFrame(rows, columns=["Date", "iv"]).dropna().sort_values("Date")
        ivp, ivr = [], []
        for i, cur in enumerate(df["iv"]):
            sub = df["iv"].iloc[max(0, i - WINDOW + 1): i + 1]
            pct = (sub <= cur).sum() / len(sub)
            rng = sub.max() - sub.min()
            rank = (cur - sub.min()) / rng if rng else None
            ivp.append(pct * 100)
            ivr.append(rank * 100 if rank is not None else None)
        df["ivp"], df["ivr"] = ivp, ivr
        return df.set_index("Date")[["ivp", "ivr"]].to_dict("index")

    ce_stat = _iv_stats([
        (datetime.strptime(k, "%d-%b-%Y"), v["ce"]["iv_30"])
        for k, v in time_map.items() if v.get("ce", {}).get("iv_30")
    ])
    pe_stat = _iv_stats([
        (datetime.strptime(k, "%d-%b-%Y"), v["pe"]["iv_30"])
        for k, v in time_map.items() if v.get("pe", {}).get("iv_30")
    ])

    for k, rec in time_map.items():
        dt = datetime.strptime(k, "%d-%b-%Y")
        if dt in ce_stat:
            rec["ce"]["ivp"], rec["ce"]["ivr"] = ce_stat[dt]["ivp"], ce_stat[dt]["ivr"]
        if dt in pe_stat:
            rec["pe"]["ivp"], rec["pe"]["ivr"] = pe_stat[dt]["ivp"], pe_stat[dt]["ivr"]

    # ───────── write to DB ─────────
    if time_map:
        logging.info("--> building %s rows for %s, writing to DB …", len(time_map), sym)
        with DBWriter() as w:
            for d_str, payload in time_map.items():
                w.write_row(sym, d_str, payload)
        logging.info("└─ %s rows stored for %s", len(time_map), sym)
    else:
        logging.info("└─ no data rows for %s", sym)


# ═════════════════════════ orchestrator ════════════════════════════════════
def main() -> None:
    root = Path(__file__).resolve().parent
    bhav_dir      = root.parent / "bhavcopy"       / "extracted"
    interest_csv  = root.parent / "interest_rates" / "ADJUSTED_MIFOR.csv"
    yahoo_dir     = root.parent / "yahoo_finance"
    earnings_json = root.parent / "earning_dates"  / "earning_dates.json"
    scripts_json  = root.parent / "nse_fno_scripts.json"

    # ───────── symbol list ─────────
    scripts = json.loads(scripts_json.read_text())
    SYMBOLS = [x["symbol"] for x in scripts["index_futures"]] + \
              [x["symbol"] for x in scripts["individual_securities"]]
    logging.info("total symbols: %s", len(SYMBOLS))

    # ───────── interest‑rate map ─────────
    ir_df = pd.read_csv(interest_csv, skiprows=2)
    ir_df["Date_parsed"] = pd.to_datetime(ir_df["Date"], format="%d %b %Y", errors="coerce")
    DAILY_RATE = {
        dt.date(): float(row["FBIL ADJUSTED MIFOR(%)"])
        for dt, row in ir_df.set_index("Date_parsed").iterrows()
        if pd.notna(dt)
    }

    # ───────── earnings map ─────────
    earnings_map: Dict[str, List[datetime]] = {}
    if earnings_json.exists():
        for item in json.loads(earnings_json.read_text()):
            if (
                item.get("event_type") == "stock_results"
                and (sym := item.get("trading_symbol"))
                and (ds := item.get("date"))
            ):
                try:
                    earnings_map.setdefault(sym, []).append(datetime.strptime(ds, "%Y-%m-%d"))
                except ValueError:
                    pass

    # ───────── spot‑price map ─────────
    spot_price_map: Dict[str, Dict[str, float]] = {}
    for sym in SYMBOLS:
        fp = yahoo_dir / f"{sym}_processed.json"
        if not fp.exists():
            continue
        try:
            ts = json.loads(fp.read_text())["historical"]["scripts"][sym]["timestamps"]
            spot_price_map[sym] = {
                d: v["underlying_price"] for d, v in ts.items() if "underlying_price" in v
            }
        except Exception:
            continue

    # ───────── bhavcopy list ─────────
    bhav_files = sorted(bhav_dir.glob("*.csv"))
    logging.info("bhav‑copies to scan: %s", len(bhav_files))

    # ───────── threaded execution ─────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {
            pool.submit(_process_symbol, s, bhav_files, spot_price_map,
                        DAILY_RATE, earnings_map): s
            for s in SYMBOLS
        }
        for f in as_completed(futs):
            sym = futs[f]
            try:
                f.result()
            except Exception as e:
                logging.exception("worker for %s failed: %s", sym, e)

    # ───────── graceful pool shutdown ─────────
    from db_util import _POOL      # type: ignore
    if _POOL:
        _POOL.closeall()
        logging.info("connection pool closed")


# ═════════════════════════ entry‑point ═════════════════════════════════════
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("manual abort – shutting down")
